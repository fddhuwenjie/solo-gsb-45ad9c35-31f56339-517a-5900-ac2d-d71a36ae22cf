"""账户间信用划转：把来源账户已形成的信用划转到目标账户抵扣欠费。

口径（完全基于 app/ledger.py 落盘的已记凭证、app/payments.py 落盘的收款
核销、app/payouts.py 落盘的退付与本模块落盘的划转主行；凭证金额一律取
入账时冻结的计费栏，不重新核算）：
1. 调用方提交**全平台唯一划转单号** transfer_no、**来源账户** from_account_id、
   **目标账户** to_account_id 与**正金额** amount_yuan；
2. 两账户须**不同**，且**都已有台账**（至少一张已入账凭证）：来源或目标
   账户无台账、两户相同都**整笔拒绝、不留划转流水**；
3. 划转金额不得超过来源账户**当前信用** credit_yuan（与 app/payouts 退付
   共用同一头寸口径：应退 − 已退付 − 已划出 + 已划入后 FIFO 抵减剩余）：
   超过 → 422 transfer_exceeds_credit，整笔拒绝、不写划转主行；
4. 划转成功后：
   - **来源信用扣减**划转额；划转只动信用池，不改变来源任何凭证的尚欠
     与已收款（落盘时刻已核验「划转额 ≤ 当时信用」，扣后池仍为非负）；
   - **目标按入账顺序**（entry_no 升序）用转入额抵扣尚未付清的正额凭证
     （full/supplement），可只部分付清一张；抵扣后剩余的转入额形成目标
     **信用**（目标既有信用不受影响，叠加在剩余转入额之上）；
   - **转入额不算现金实收**：不写逐凭证现金核销，目标 total_received_yuan
     不变；已记凭证、旧收款分配、退付与各首次回执都**不改写**；
5. 台账、收款、退付查询及后续收款和退付都按**划转后的两户头寸**处理：
   划出额与退付一样在信用池重算时最先扣除，划入额在扣除后加回再 FIFO
   抵减（见 app/payments.account_position）；
6. 返回划转金额、划转时间、目标户逐凭证抵扣明细与**双方划转后最新尚欠 /
   信用**（及两侧累计划转额与笔数），并提供按账户查询划转记录的 API；
7. **幂等**：同一 transfer_no + 同来源 + 同目标 + 同金额重试，原样返回
   **首次回执**（result=deduped，逐凭证抵扣、时间与双方头寸快照均不漂移，
   即使后来两户头寸又被其他业务改变）；同一单号**换账户或换金额** →
   **409 冲突**（transfer_no_conflict），不写入；
8. 双方信用核对、目标逐凭证抵扣与划转落盘与原入账、收款、退付**共用同一
   把 SQLite 写事务锁**（RLock + BEGIN IMMEDIATE）：transfer_no 主键 +
   (划出账户, from_seq) / (划入账户, to_seq) 唯一键在数据库层兜底，并发
   划转与退付不得重复支用信用（临界区串行重算头寸），并发同单号只有一笔
   落盘；信用不足或账户不合要求一律回滚、不留流水；划转记录持久化在
   SQLite 中，进程重启后仍可查询、重试仍返回首次回执。

HTTP 错误码：
- transfer_same_account：来源与目标账户相同（422）；
- transfer_account_no_ledger：来源或目标账户尚无已入账凭证（422，错误体
  附 account_id / side）；
- transfer_amount_not_positive：划转金额非正（422，正常由请求体校验拦截）；
- transfer_exceeds_credit：划转额超过来源当前信用，整笔拒绝、不留流水
  （422，错误体附 from_credit_yuan / from_outstanding_yuan）；
- transfer_no_conflict：划转单号已使用但金额或账户不一致（409，错误体
  附已登记的 from_account_id / to_account_id / amount_yuan）。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.payments import _EPS, _round6, account_position
from app.storage import SessionStore, get_store


class TransferError(Exception):
    """划转操作失败：code 决定 HTTP 错误体 error 字段，status_code 决定状态码。"""

    def __init__(
        self,
        code: str,
        reason: str,
        status_code: int = 422,
        extra: Optional[dict] = None,
    ):
        super().__init__(reason)
        self.code = code
        self.status_code = status_code
        # 附加到错误响应体顶层的结构化字段（如冲突单号的账户/金额、来源当前信用）
        self.extra = extra or {}


def _utc_now_iso() -> str:
    # 与 sessions/ledger/payments/payouts._utc_now_iso 同口径：UTC、秒精度、带 Z。
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _application_response(entry: dict, amount: float, application_no: int) -> dict:
    """目标户一笔转入额对一张未付清正额凭证的抵扣（允许部分付清）。"""
    return {
        "application_no": application_no,
        "entry_no": entry["entry_no"],
        "session_id": entry["session_id"],
        "voucher_no": entry["voucher_no"],
        "applied_yuan": _round6(amount),
        "outstanding_before_yuan": entry["outstanding_yuan"],
        "outstanding_after_yuan": _round6(entry["outstanding_yuan"] - amount),
    }


def submit_transfer(
    from_account_id: str,
    to_account_id: str,
    transfer_no: str,
    amount_yuan: float,
    store: Optional[SessionStore] = None,
) -> dict:
    """把一笔信用从来源账户划转到目标账户：核对双方头寸后原子落盘。

    超过来源当前信用 / 账户无台账 / 两户相同均整笔拒绝（不留流水）；
    同单号同双方同金额重试返回首次回执；换账户或换金额 → 409。
    """
    store = store or get_store()
    amount_yuan = _round6(amount_yuan)
    # 请求模型已要求 gt=0；这里防住亚微级金额（如 1e-12 被 6 位舍入为 0）：
    # 归一后非正视为非法，不允许「0 元划转」生成空记录。
    if amount_yuan <= 0.0:
        raise TransferError(
            "transfer_amount_not_positive",
            "划转金额必须为正且至少达到 0.000001 元",
            422,
            extra={"transfer_no": transfer_no, "amount_yuan": amount_yuan},
        )

    with store.write_transaction() as conn:
        # 1) 划转单号全平台唯一：先查重，区分「原样重试」与「单号冲突」。
        existing = store.get_transfer(conn, transfer_no)
        if existing is not None:
            same_from = existing["from_account_id"] == from_account_id
            same_to = existing["to_account_id"] == to_account_id
            same_amount = abs(float(existing["amount_yuan"]) - amount_yuan) < _EPS
            if same_from and same_to and same_amount:
                # 原样重试：冻结回执原样取回，仅 result 标记 deduped
                # （即使后来两户头寸已被划转/退付/入账改变，回执也不漂移）。
                receipt = json.loads(existing["receipt_json"])
                receipt["result"] = "deduped"
                return receipt
            raise TransferError(
                "transfer_no_conflict",
                f"划转单号 {transfer_no} 已登记为账户 "
                f"{existing['from_account_id']} → {existing['to_account_id']}、"
                f"金额 {float(existing['amount_yuan'])} 元；同一单号不能换账户"
                "或换金额使用，本次划转整笔拒绝、不留流水",
                409,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": existing["from_account_id"],
                    "to_account_id": existing["to_account_id"],
                    "amount_yuan": float(existing["amount_yuan"]),
                },
            )

        # 2) 来源与目标必须是两个不同的账户。
        if from_account_id == to_account_id:
            raise TransferError(
                "transfer_same_account",
                f"划转来源账户与目标账户不能相同（均为 {from_account_id}）；"
                "信用划转只能在两个不同账户之间办理，本次整笔拒绝、不留流水",
                422,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                },
            )

        # 3) 重算双方当前头寸（信用池已含已落盘退付与历史划转）。
        src = account_position(conn, store, from_account_id)
        dst = account_position(conn, store, to_account_id)
        if src["totals"]["entry_count"] == 0:
            raise TransferError(
                "transfer_account_no_ledger",
                f"来源账户 {from_account_id} 尚无已入账凭证、没有可划转的信用；"
                "请先经凭证入账接口为来源账户建立台账并形成信用后再提交划转",
                422,
                extra={"transfer_no": transfer_no, "account_id": from_account_id, "side": "from"},
            )
        if dst["totals"]["entry_count"] == 0:
            raise TransferError(
                "transfer_account_no_ledger",
                f"目标账户 {to_account_id} 尚无已入账凭证，转入额无处抵扣；"
                "请先经凭证入账接口为目标账户建立台账后再提交划转",
                422,
                extra={"transfer_no": transfer_no, "account_id": to_account_id, "side": "to"},
            )

        src_outstanding = src["totals"]["outstanding_yuan"]
        src_credit = src["totals"]["credit_yuan"]
        dst_outstanding = dst["totals"]["outstanding_yuan"]
        dst_credit = dst["totals"]["credit_yuan"]

        # 4) 划转额不得超过来源当前信用（无信用时任何正金额都在此拒绝）。
        #    金额与头寸均经 _round6 归一，亚微级浮点残差先归零再比较，
        #    业务金额（6 位小数）边界精确：等于信用可划，大于信用整笔拒绝。
        if amount_yuan > src_credit:
            raise TransferError(
                "transfer_exceeds_credit",
                f"划转 {amount_yuan} 元超过来源账户 {from_account_id} 当前信用 "
                f"{src_credit} 元（来源当前尚欠 {src_outstanding} 元）；"
                "超过当前信用的划转整笔拒绝，不登记划转单、双方均不留流水",
                422,
                extra={
                    "transfer_no": transfer_no,
                    "amount_yuan": amount_yuan,
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                    "from_credit_yuan": src_credit,
                    "from_outstanding_yuan": src_outstanding,
                },
            )

        # 5) 目标按原入账顺序（entry_no 升序）用转入额抵扣尚未付清的正额
        #    凭证。直接在内存头寸上推进；转入不是现金，只推进
        #    refund_applied / outstanding（与应退池抵减同口径），不动 paid。
        remaining = amount_yuan
        applications: List[dict] = []
        application_no = 0
        applied_total = 0.0
        for entry in dst["entries"]:
            if remaining <= _EPS:
                break
            due = entry["outstanding_yuan"]
            if due <= _EPS:
                continue
            application_no += 1
            applied = _round6(min(due, remaining))
            applications.append(_application_response(entry, applied, application_no))
            entry["outstanding_yuan"] = _round6(due - applied)
            entry["refund_applied_yuan"] = _round6(
                entry["refund_applied_yuan"] + applied
            )
            remaining = _round6(remaining - applied)
            applied_total = _round6(applied_total + applied)

        # 6) 组装双方划转后头寸快照。
        #    来源：只扣信用、不动尚欠；目标：转入抵掉 applied_total 的尚欠，
        #    未用掉的转入额叠加在既有信用之上。
        new_src_credit = _round6(src_credit - amount_yuan)
        if new_src_credit < _EPS:
            new_src_credit = 0.0
        new_dst_outstanding = _round6(
            sum(e["outstanding_yuan"] for e in dst["entries"])
        )
        if new_dst_outstanding < _EPS:
            new_dst_outstanding = 0.0
        new_dst_credit = _round6(dst_credit + amount_yuan - applied_total)
        if new_dst_credit < _EPS:
            new_dst_credit = 0.0

        transferred_at = _utc_now_iso()
        global_seq = store.count_all_transfers(conn) + 1
        from_seq = store.count_transfers_as_from(conn, from_account_id) + 1
        to_seq = store.count_transfers_as_to(conn, to_account_id) + 1
        new_total_out = _round6(
            src["totals"]["total_transferred_out_yuan"] + amount_yuan
        )
        new_total_in = _round6(
            dst["totals"]["total_transferred_in_yuan"] + amount_yuan
        )
        receipt = {
            "transfer_no": transfer_no,
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "result": "transferred",
            "amount_yuan": amount_yuan,
            "transferred_at": transferred_at,
            "applications": applications,
            "from_outstanding_yuan": src_outstanding,
            "from_credit_yuan": new_src_credit,
            "from_transfer_count": from_seq,
            "total_transferred_out_yuan": new_total_out,
            "to_outstanding_yuan": new_dst_outstanding,
            "to_credit_yuan": new_dst_credit,
            "to_transfer_count": to_seq,
            "total_transferred_in_yuan": new_total_in,
        }

        # 7) 划转主行（含冻结回执 JSON）落盘；transfer_no 主键与两侧
        #    (账户, 序号) 唯一键兜底，并发同单号只有一笔成功、序号不重号。
        store.insert_transfer(
            conn,
            transfer_id=str(uuid.uuid4()),
            global_seq=global_seq,
            transfer_no=transfer_no,
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            from_seq=from_seq,
            to_seq=to_seq,
            amount_yuan=amount_yuan,
            receipt_json=json.dumps(receipt, ensure_ascii=False),
            transferred_at=transferred_at,
        )
        return receipt


def list_transfers(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按账户视角划转先后查询与该账户相关的全部划转记录（划出与划入合并）。

    记录按全平台划转落盘先后（global_seq）排列：每笔另附该账户视角的
    role（from/to）、序位 seq（该账户相关划转中的第几笔，从 1 起连续）
    与对方账户 other_account_id；每条记录本身为首次成功时冻结的回执
    （含目标户逐凭证抵扣、两侧侧序号 from/to_transfer_count 与双方头寸
    快照）。账户级尚欠/信用为按当前全部凭证、核销、退付与划转重算的
    最新值。从未入账也无划转的账户返回全 0/空。
    """
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        rows = store.list_transfer_rows_for_account(account_id)

    transfers: List[dict] = []
    for idx, row in enumerate(rows, start=1):
        receipt = json.loads(row["receipt_json"])
        # 正常列表查询：划转记录保持 transferred（重试时才在提交接口标 deduped）。
        receipt["result"] = "transferred"
        receipt["role"] = row["role"]
        # seq：该账户视角（划出与划入合并、按全局落盘先后）的划转序位。
        receipt["seq"] = idx
        receipt["other_account_id"] = row["other_account_id"]
        transfers.append(receipt)

    return {
        "account_id": account_id,
        "transfer_count": len(transfers),
        "total_transferred_out_yuan": totals["total_transferred_out_yuan"],
        "total_transferred_in_yuan": totals["total_transferred_in_yuan"],
        "outstanding_yuan": totals["outstanding_yuan"],
        "credit_yuan": totals["credit_yuan"],
        "transfers": transfers,
    }
