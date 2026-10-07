"""账户信用退付：把应退抵减未付凭证后剩余的账户信用按单实际退付。

口径（完全基于 app/ledger.py 落盘的已记凭证、app/payments.py 落盘的收款
核销与本模块落盘的退付主行；凭证金额一律取入账时冻结的计费栏，不重新核算）：
1. 调用方按账户提交全平台唯一退付单号 payout_no 与**正金额**；
2. 可退付上限为**当前信用** credit_yuan：账户内全部已记应退
   （billing=refund）先扣除全部已落盘退付，再按原入账顺序抵减尚未付清的
   正额凭证后剩余的净额（重算口径见 app/payments.account_position）；
3. 退付金额**超过当前信用整笔拒绝**：返回 422、不写退付主行（不留记录）；
   金额必须为正；
4. 退付成功后，台账查询与收款查询的当前信用同步扣除已付退款；退付本身
   不改变任何凭证的尚欠与已收款（退付时刻已核验「退付 ≤ 当时信用」，
   应退池扣除退付后仍能全额覆盖既有的逐凭证抵减）；
5. **后续入账的应补/应退仍按原规则**改变最新尚欠与信用：应退池扣除已退付
   后重新 FIFO 抵减未付清正额；已记凭证、收款分配与各首次回执保持原样；
6. 返回本次退付金额、退付时间与退付后的头寸（累计退付、最新尚欠/信用），
   并提供按退付次序查询退付记录的 API；
7. **幂等**：同一 payout_no + 同账户 + 同金额重试，原样返回**首次回执**
   （result=deduped，金额、退付时间与当时头寸快照均不漂移，即使后来又
   入账新的应补/应退凭证）；同一单号换金额或跨账户使用 → **409 冲突**
   （payout_no_conflict），不写入；
8. 信用核对与退付登记与原入账、收款**共用同一把 SQLite 写事务锁**
   （RLock + BEGIN IMMEDIATE）：payout_no 主键 + (账户, 退付序号) 唯一键
   在数据库层兜底，并发提交同一单号只有一笔落盘、并发不同单号也不会
   超额退付，进程重启后退付记录仍在、不会重复支付；超额/冲突一律回滚，
   不留部分数据。

HTTP 错误码：
- payout_empty_account：账户尚无已入账凭证，没有可退付的信用（422）；
- payout_amount_not_positive：退付金额非正（422，正常由请求体校验拦截）；
- payout_exceeds_credit：退付超过当前信用，整笔拒绝、不留记录
  （422，错误体附 credit_yuan / outstanding_yuan）；
- payout_no_conflict：退付单号已使用但金额或账户不一致（409，错误体
  附已登记的 account_id / amount_yuan）。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.payments import _EPS, _round6, account_position
from app.storage import SessionStore, get_store


class PayoutError(Exception):
    """退付操作失败：code 决定 HTTP 错误体 error 字段，status_code 决定状态码。"""

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
        # 附加到错误响应体顶层的结构化字段（如冲突单号的账户/金额、当前信用）
        self.extra = extra or {}


def _utc_now_iso() -> str:
    # 与 sessions/ledger/payments._utc_now_iso 同口径：UTC、秒精度、带 Z。
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def submit_payout(
    account_id: str,
    payout_no: str,
    amount_yuan: float,
    store: Optional[SessionStore] = None,
) -> dict:
    """按账户退付一笔信用：核对当前信用后原子落盘，返回冻结回执。

    超过当前信用整笔拒绝（不留记录）；同单号同账户同金额重试返回首次回执；
    同单号换金额或跨账户 → 409。
    """
    store = store or get_store()
    amount_yuan = _round6(amount_yuan)
    # 请求模型已要求 gt=0；这里防住亚微级金额（如 1e-12 被 6 位舍入为 0）：
    # 归一后非正视为非法，不允许「0 元退付」生成空记录。
    if amount_yuan <= 0.0:
        raise PayoutError(
            "payout_amount_not_positive",
            "退付金额必须为正且至少达到 0.000001 元",
            422,
            extra={"payout_no": payout_no, "amount_yuan": amount_yuan},
        )

    with store.write_transaction() as conn:
        # 1) 退付单号全平台唯一：先查重，区分「原样重试」与「单号冲突」。
        existing = store.get_payout(conn, payout_no)
        if existing is not None:
            same_account = existing["account_id"] == account_id
            same_amount = abs(float(existing["amount_yuan"]) - amount_yuan) < _EPS
            if same_account and same_amount:
                # 原样重试：冻结回执原样取回，仅 result 标记 deduped
                # （即使后来信用已被后续补退改变，回执也不漂移）。
                receipt = json.loads(existing["receipt_json"])
                receipt["result"] = "deduped"
                return receipt
            raise PayoutError(
                "payout_no_conflict",
                f"退付单号 {payout_no} 已登记到账户 {existing['account_id']}、"
                f"金额 {float(existing['amount_yuan'])} 元；同一单号不能换金额"
                "或跨账户使用，本次退付整笔拒绝、不留记录",
                409,
                extra={
                    "payout_no": payout_no,
                    "account_id": existing["account_id"],
                    "amount_yuan": float(existing["amount_yuan"]),
                },
            )

        # 2) 重算当前头寸（应退池已扣除全部已落盘退付后的逐凭证尚欠与信用）。
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        if totals["entry_count"] == 0:
            raise PayoutError(
                "payout_empty_account",
                f"账户 {account_id} 尚无已入账凭证，没有可退付的信用；"
                "请先经凭证入账接口建立台账并形成信用后再提交退付",
                422,
                extra={"payout_no": payout_no},
            )

        outstanding = totals["outstanding_yuan"]
        credit = totals["credit_yuan"]

        # 3) 超过当前信用整笔拒绝（无信用时任何正金额都在此拒绝）。
        #    金额与头寸均经 _round6 归一，亚微级浮点残差先归零再比较，
        #    业务金额（6 位小数）边界精确：等于信用可退，大于信用整笔拒绝。
        if amount_yuan > credit:
            raise PayoutError(
                "payout_exceeds_credit",
                f"退付 {amount_yuan} 元超过账户 {account_id} 当前信用 "
                f"{credit} 元（当前尚欠 {outstanding} 元）；超过当前信用的退付"
                "整笔拒绝，不登记退付单、不留记录",
                422,
                extra={
                    "payout_no": payout_no,
                    "amount_yuan": amount_yuan,
                    "credit_yuan": credit,
                    "outstanding_yuan": outstanding,
                },
            )

        # 4) 组装首次回执（落盘冻结）：退付金额、退付时间与退付后的头寸快照。
        #    退付只消耗信用，不改变任何凭证的尚欠与已收款，故尚欠快照即当前值。
        paid_at = _utc_now_iso()
        new_total_paid_out = _round6(totals["total_paid_out_yuan"] + amount_yuan)
        new_credit = _round6(credit - amount_yuan)
        if new_credit < _EPS:
            new_credit = 0.0
        payout_seq = store.count_payouts(conn, account_id) + 1
        receipt = {
            "account_id": account_id,
            "payout_no": payout_no,
            "result": "paid",
            "amount_yuan": amount_yuan,
            "paid_at": paid_at,
            "total_paid_out_yuan": new_total_paid_out,
            "outstanding_yuan": outstanding,
            "credit_yuan": new_credit,
            "payout_count": payout_seq,
        }

        # 5) 退付主行（含冻结回执 JSON）落盘；payout_no 主键兜底，
        #    并发同单号只有一笔成功。
        store.insert_payout(
            conn,
            payout_id=str(uuid.uuid4()),
            payout_no=payout_no,
            account_id=account_id,
            payout_seq=payout_seq,
            amount_yuan=amount_yuan,
            receipt_json=json.dumps(receipt, ensure_ascii=False),
            paid_at=paid_at,
        )
        return receipt


def list_payouts(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按退付次序查询账户全部退付记录（均为落盘时冻结的首次回执）。"""
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        rows = store.list_payout_rows(account_id)

    payouts: List[dict] = []
    total_paid_out_check = 0.0
    for row in rows:
        receipt = json.loads(row["receipt_json"])
        # 正常列表查询：退付记录保持 paid（重试时才在提交接口标 deduped）。
        receipt["result"] = "paid"
        payouts.append(receipt)
        total_paid_out_check = _round6(
            total_paid_out_check + float(row["amount_yuan"])
        )

    return {
        "account_id": account_id,
        "payout_count": len(payouts),
        "total_paid_out_yuan": _round6(total_paid_out_check),
        "outstanding_yuan": totals["outstanding_yuan"],
        "credit_yuan": totals["credit_yuan"],
        "payouts": payouts,
    }
