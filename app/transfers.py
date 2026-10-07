"""账户间信用划转：把一户已形成的信用划转给另一户抵扣欠费。

口径（完全基于 app/ledger.py 落盘的已记凭证、app/payments.py 落盘的收款
核销、app/payouts.py 落盘的退付与本模块落盘的划转主行；凭证金额一律取
入账时冻结的计费栏，不重新核算）：
1. 调用方提交全平台唯一划转单号 transfer_no、来源账户、目标账户与**正金额**；
2. **账户合要求**：来源与目标必须不同（transfer_same_account），且两户都
   已有台账（各至少一条已入账凭证；来源空 → transfer_from_empty_account、
   目标空 → transfer_to_empty_account），不合要求一律 422 且双方都不留流水；
3. **金额不能超过来源当前信用** credit_yuan（应退池扣除已付退付与已划出、
   加入已转入后的剩余；重算口径见 app/payments.account_position）：
   超过来源信用 → 422 transfer_insufficient_credit（错误体附来源当前
   信用/尚欠与划转金额），整笔拒绝、不写划转主行；金额必须为正；
4. **来源信用扣减**：划出额与退付同口径，在头寸重算时从应退池扣除
   （只动信用、不改变任何凭证的尚欠与已收款；出账时刻已核验
   「划出 ≤ 当时信用」，池子不会为负）；
5. **目标按入账顺序抵扣**：转入额**不算现金实收**（不产生逐凭证现金核销、
   不计入目标累计实收 total_received_yuan），而是加入目标应退池，按原入账
   顺序（entry_no 升序）抵减尚未付清的正额凭证（full / supplement）的
   未核销部分，逐张 applied = min(剩余转入额, 该凭证当前尚欠)，**可只部分
   抵扣一张**；抵扣完尚未用完的转入额形成目标**信用** credit_yuan；
6. **不改写历史**：已记凭证、旧收款的逐凭证分配、退付与各首次回执都不
   改写；划转落盘后，台账 / 收款 / 退付查询及后续收款和退付都按划转后的
   两户头寸处理——后续新收款按划转后尚欠 FIFO 分配，后续退付只能退
   划转后剩余的信用；
7. 返回划转金额、划转时间、目标本次逐凭证抵扣 applications[] 与双方划转
   后最新头寸（from_position / to_position 各含 outstanding_yuan /
   credit_yuan / total_received_yuan / total_paid_out_yuan），并提供按
   账户查询划转记录的 API；
8. **幂等**：同一 transfer_no + 同来源 + 同目标 + 同金额重试，原样返回
   **首次回执**（result=deduped，划转时间、目标逐凭证抵扣与双方头寸快照
   均为首次落盘值，即使后来两户头寸又被后续业务改变）；同一单号换了来源
   / 目标账户或换了金额 → **409 冲突**（transfer_no_conflict，错误体附
   已登记的 from_account_id / to_account_id / amount_yuan），不写入；
9. 两户信用核对与划转落盘与原入账、收款、退付**共用同一把 SQLite 写事务
   锁**（RLock + BEGIN IMMEDIATE）：transfer_no 主键 + 全平台/双方账户
   序号唯一键在数据库层兜底，并发划转与退付经同一临界区串行核对头寸，
   **不会重复支用同一笔信用**；核对失败一律回滚、不留划转记录；划转记录
   持久化在 SQLite 中，进程重启后仍可按账户查询。

HTTP 错误码：
- transfer_same_account：来源与目标账户相同（422）；
- transfer_from_empty_account / transfer_to_empty_account：来源 / 目标
  账户尚无已入账凭证（422）；
- transfer_amount_not_positive：划转金额非正（422，正常由请求体校验拦截）；
- transfer_insufficient_credit：划转金额超过来源当前信用，整笔拒绝、
  不留流水（422，错误体附 from_credit_yuan / from_outstanding_yuan）；
- transfer_no_conflict：划转单号已使用但来源 / 目标账户或金额不一致
  （409，错误体附已登记的 from_account_id / to_account_id / amount_yuan）。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.payments import _EPS, _round6, account_position
from app.storage import SessionStore, get_store


class TransferError(Exception):
    """信用划转操作失败：code 决定 HTTP 错误体 error 字段，status_code 决定状态码。"""

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
        # 附加到错误响应体顶层的结构化字段（如冲突单号的双方账户/金额、来源信用）
        self.extra = extra or {}


def _utc_now_iso() -> str:
    # 与 sessions/ledger/payments/payouts._utc_now_iso 同口径：UTC、秒精度、带 Z。
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _position_snapshot(totals: dict) -> dict:
    """从头寸重算的 totals 中取回执用的四要素（尚欠/信用/累计实收/累计退付）。"""
    return {
        "outstanding_yuan": _round6(totals["outstanding_yuan"]),
        "credit_yuan": _round6(totals["credit_yuan"]),
        "total_received_yuan": _round6(totals["total_received_yuan"]),
        "total_paid_out_yuan": _round6(totals["total_paid_out_yuan"]),
    }


def _transfer_application(entry: dict, amount: float, application_no: int) -> dict:
    """目标账户本次转入对一张正额凭证的抵扣（允许部分抵扣），均为首次落盘值。

    转入额不是现金：只按应退池口径抵减尚欠，不产生逐凭证现金核销，
    目标累计实收 total_received_yuan 不增加。
    """
    return {
        "application_no": application_no,
        "entry_no": entry["entry_no"],
        "session_id": entry["session_id"],
        "voucher_no": entry["voucher_no"],
        "applied_yuan": _round6(amount),
        "outstanding_before_yuan": _round6(entry["outstanding_yuan"]),
        "outstanding_after_yuan": _round6(entry["outstanding_yuan"] - amount),
    }


def submit_transfer(
    transfer_no: str,
    from_account_id: str,
    to_account_id: str,
    amount_yuan: float,
    store: Optional[SessionStore] = None,
) -> dict:
    """把来源账户的一笔信用划转给目标账户：双方头寸核对后原子落盘。

    双方账户须不同且都已有台账；金额超过来源当前信用整笔拒绝（不留流水）；
    同单号同双方同金额重试返回首次回执；同单号换账户或换金额 → 409。
    """
    store = store or get_store()
    amount_yuan = _round6(amount_yuan)
    # 请求模型已要求 gt=0；这里防住亚微级金额（如 1e-12 被 6 位舍入为 0）：
    # 归一后非正视为非法，不允许「0 元划转」生成空抵扣。
    if amount_yuan <= 0.0:
        raise TransferError(
            "transfer_amount_not_positive",
            "划转金额必须为正且至少达到 0.000001 元",
            422,
            extra={"transfer_no": transfer_no, "amount_yuan": amount_yuan},
        )
    if from_account_id == to_account_id:
        raise TransferError(
            "transfer_same_account",
            f"来源账户与目标账户不能相同（均为 {from_account_id}）：信用划转"
            "只能在两个不同账户之间办理；本次整笔拒绝，双方均不留流水",
            422,
            extra={
                "transfer_no": transfer_no,
                "from_account_id": from_account_id,
                "to_account_id": to_account_id,
            },
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
                # （即使后来两户头寸又被后续凭证/收款/退付改变，回执也不漂移）。
                receipt = json.loads(existing["receipt_json"])
                receipt["result"] = "deduped"
                return receipt
            raise TransferError(
                "transfer_no_conflict",
                f"划转单号 {transfer_no} 已登记为账户 {existing['from_account_id']}"
                f" → {existing['to_account_id']}、金额 "
                f"{float(existing['amount_yuan'])} 元；同一单号不能换账户或换"
                "金额使用，本次划转整笔拒绝、双方均不留流水",
                409,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": existing["from_account_id"],
                    "to_account_id": existing["to_account_id"],
                    "amount_yuan": float(existing["amount_yuan"]),
                },
            )

        # 2) 两户都须已有台账（各至少一条已入账凭证）。
        from_position = account_position(conn, store, from_account_id)
        to_position = account_position(conn, store, to_account_id)
        if from_position["totals"]["entry_count"] == 0:
            raise TransferError(
                "transfer_from_empty_account",
                f"来源账户 {from_account_id} 尚无已入账凭证、没有可划转的信用；"
                "请先经凭证入账接口建立来源台账后再提交划转",
                422,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                },
            )
        if to_position["totals"]["entry_count"] == 0:
            raise TransferError(
                "transfer_to_empty_account",
                f"目标账户 {to_account_id} 尚无已入账凭证、没有可抵扣的欠费台账；"
                "请先经凭证入账接口建立目标台账后再提交划转",
                422,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                },
            )

        from_totals = from_position["totals"]
        from_credit = from_totals["credit_yuan"]

        # 3) 金额不能超过来源当前信用：超过整笔拒绝（无信用时任何正金额都拒绝）。
        #    金额与头寸均经 _round6 归一，亚微级浮点残差先归零再比较，
        #    业务金额（6 位小数）边界精确：等于信用可划，大于信用整笔拒绝。
        if amount_yuan > from_credit:
            raise TransferError(
                "transfer_insufficient_credit",
                f"划转 {amount_yuan} 元超过来源账户 {from_account_id} 当前信用 "
                f"{from_credit} 元（来源当前尚欠 "
                f"{from_totals['outstanding_yuan']} 元）；超过来源当前信用的划转"
                "整笔拒绝，不登记划转单、双方均不留流水",
                422,
                extra={
                    "transfer_no": transfer_no,
                    "from_account_id": from_account_id,
                    "to_account_id": to_account_id,
                    "amount_yuan": amount_yuan,
                    "from_credit_yuan": from_credit,
                    "from_outstanding_yuan": from_totals["outstanding_yuan"],
                },
            )

        # 4) 目标按原入账顺序（entry_no 升序）用转入额抵扣尚未付清的正额
        #    凭证（与应退池 FIFO 抵减同口径：现金已核销部分受保护，只抵
        #    未核销尚欠），逐张预演本次抵扣；剩余形成目标信用。
        applications: List[dict] = []
        remaining = amount_yuan
        application_no = 0
        for entry in to_position["entries"]:
            if remaining <= _EPS:
                break
            due = entry["outstanding_yuan"]
            if due <= _EPS:
                continue
            applied = _round6(min(due, remaining))
            application_no += 1
            applications.append(_transfer_application(entry, applied, application_no))
            remaining = _round6(remaining - applied)
        # 转入额抵扣后剩余 = 目标新增信用；目标划转后尚欠/信用以落盘后
        # account_position 重算为准（下面的回执快照）。

        # 5) 组装首次回执并落盘。序号先按当前计数分配；随后先插入主行，再在
        #    同一连接内重算双方划转后头寸（本连接可见刚插入的行），用最终
        #    头寸覆盖回执 JSON——全部在同一写事务内提交，外部只能读到最终版本。
        transferred_at = _utc_now_iso()
        global_seq = store.count_transfers_in_txn(conn) + 1
        # 划出 / 转入序号在两户各自方向上独立连续，须分别按账户计数。
        from_out_count, _ = store.count_transfers_for_account(conn, from_account_id)
        _, to_in_count = store.count_transfers_for_account(conn, to_account_id)
        from_transfer_seq = from_out_count + 1
        to_transfer_seq = to_in_count + 1

        store.insert_transfer(
            conn,
            transfer_id=str(uuid.uuid4()),
            transfer_no=transfer_no,
            global_seq=global_seq,
            from_account_id=from_account_id,
            to_account_id=to_account_id,
            from_transfer_seq=from_transfer_seq,
            to_transfer_seq=to_transfer_seq,
            amount_yuan=amount_yuan,
            # 占位回执，下面立即用带最终头寸的版本覆盖（同一事务，外部不可见中间态）。
            receipt_json=json.dumps({"transfer_no": transfer_no}, ensure_ascii=False),
            transferred_at=transferred_at,
        )

        # 6) 落盘后重算双方头寸，回填进冻结回执（与各查询 account_position 同口径）。
        final_from = account_position(conn, store, from_account_id)["totals"]
        final_to = account_position(conn, store, to_account_id)["totals"]
        receipt = {
            "transfer_no": transfer_no,
            "result": "transferred",
            "from_account_id": from_account_id,
            "to_account_id": to_account_id,
            "amount_yuan": amount_yuan,
            "transferred_at": transferred_at,
            "applications": applications,
            "global_seq": global_seq,
            "from_transfer_seq": from_transfer_seq,
            "to_transfer_seq": to_transfer_seq,
            "from_position": _position_snapshot(final_from),
            "to_position": _position_snapshot(final_to),
            "transfer_count": global_seq,
        }
        conn.execute(
            "UPDATE account_credit_transfers SET receipt_json = ? WHERE transfer_no = ?",
            (json.dumps(receipt, ensure_ascii=False), transfer_no),
        )
        return receipt


def list_transfers(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按账户查询其作为划出方或转入方参与的全部划转记录与当前最新头寸。

    记录按全平台划转次序（global_seq 升序）返回；每条均为首次成功时冻结的
    回执（列表中 result 恒为 transferred），另附 direction（out=本账户划出、
    in=转入本账户）与本账户在该方向上的账户内序号 account_transfer_seq，
    以及对方账户 counterpart_account_id。账户级头寸为当前重算最新值。
    """
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        rows = store.list_transfer_rows_for_account(account_id)

    transfers: List[dict] = []
    for row in rows:
        receipt = json.loads(row["receipt_json"])
        # 正常列表查询：划转记录保持 transferred（重试时才在提交接口标 deduped）。
        receipt["result"] = "transferred"
        is_from = row["from_account_id"] == account_id
        receipt["direction"] = "out" if is_from else "in"
        receipt["counterpart_account_id"] = (
            row["to_account_id"] if is_from else row["from_account_id"]
        )
        receipt["account_transfer_seq"] = (
            int(row["from_transfer_seq"]) if is_from else int(row["to_transfer_seq"])
        )
        transfers.append(receipt)

    out_count = sum(1 for t in transfers if t["direction"] == "out")
    in_count = len(transfers) - out_count
    return {
        "account_id": account_id,
        "transfer_count": len(transfers),
        "out_count": out_count,
        "in_count": in_count,
        "total_transferred_out_yuan": totals["total_transferred_out_yuan"],
        "total_transferred_in_yuan": totals["total_transferred_in_yuan"],
        "outstanding_yuan": totals["outstanding_yuan"],
        "credit_yuan": totals["credit_yuan"],
        "transfers": transfers,
    }
