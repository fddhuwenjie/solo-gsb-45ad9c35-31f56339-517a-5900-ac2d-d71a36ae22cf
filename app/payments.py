"""账户收款登记：把收款单按账户核销到已记应收，并展示最新尚欠 / 信用。

口径（完全基于 app/ledger.py 落盘的已记凭证与本模块落盘的收款核销，
凭证金额一律取入账时冻结的计费栏，不重新核算）：
1. 调用方按账户提交全平台唯一收款单号 payment_no 与**正金额**；
2. 收款核销前，先把账户内**已记应退凭证**（billing=refund）按原入账顺序
   用于抵减**尚未付清的正额凭证**（full / supplement）；抵减后剩余的应退
   净额形成账户**信用**（credit_yuan）；随后收款按**原入账顺序**
   （entry_no 升序）分配给尚未付清的正额凭证，允许只部分付清一张；
3. 收款金额**超过当前应付（outstanding_yuan）整笔拒绝**：返回 422、
   不写收款主行、不写任何逐凭证核销（不留流水）；金额必须为正；
4. 返回本次**逐凭证分配**（只含实际分到钱的正额凭证）、账户**累计实收**
   与本次后的**最新尚欠或信用**（两者不同时为正），并提供按收款次序
   查询收款记录的 API；
5. **幂等**：同一 payment_no + 同账户 + 同金额重试，原样返回**首次回执**
   （result=deduped，逐凭证分配、累计实收与当时尚欠/信用快照均为首次
   落盘值，不随后来新入账的应补/应退凭证漂移）；同一单号换了金额、或
   拿到别的账户使用 → **409 冲突**（payment_no_conflict），不写入；
6. **后续补退不改写旧收款分配**：逐凭证核销一旦落盘即冻结；应退抵减与
   尚欠/信用在每次查询/提交时由「当前全部已记凭证 + 已落盘核销」重算，
   因此后来入账的应补/应退会改变**最新尚欠/信用**（允许显示信用），
   但历史收款回执与其核销内容永不改变；
7. 收款核对、逐凭证分配与落盘与原凭证入账**共用同一把 SQLite 写事务锁**
   （RLock + BEGIN IMMEDIATE）：payment_no 主键 + (账户, 序号) 唯一键在
   数据库层兜底，并发提交同一单号只有一笔落盘，进程重启后核销仍在、
   不会重复核销；超额/冲突一律回滚，不留部分数据。

当前尚欠 / 信用重算（对账户全部已记明细）：
- 先把账户内**全部已记应退**（无论其入账在正额凭证之前还是之后）汇成一个
  应退池，并依次**扣除全部已落盘退付与全部已落盘信用划出额、加入全部已落盘
  信用转入额**（退付与划出只消耗信用、不产生逐凭证核销；转入额不是现金实收，
  只加入池子参与抵减，不产生任何逐凭证现金核销），再按原入账顺序
  （entry_no 升序）逐张处理正额凭证：
  每张正额凭证上，已落盘且**未被冲正**的收款核销（有效现金）先受保护，
  池子只抵减「应收 − 有效已收款」的未核销部分
  （applied = min(剩余池, 应收 − 有效已收款)），抵减后其余额即该凭证最新尚欠；
- 遍历结束后池中剩余即账户信用 credit_yuan；
  全部正额凭证尚欠之和即 outstanding_yuan（与信用不同时为正）。
这套「当前重算」只读取冻结的现金核销、已落盘退付与已落盘信用划转，从不改写
它们：后来补退只会增大池子、改变最新尚欠/信用（现金已付清的凭证直接被池子
越过），旧收款分配与旧退付/划转回执原样保留。退付与划出在池中最先扣除：
每笔退付/划转出账时刻都已核验「金额 ≤ 当时信用」，故池不会为负，且退付/
划转本身不改变任何凭证的尚欠；信用转入只加入池子（转入额不算现金实收，
不产生逐凭证现金核销，账户累计实收 total_received_yuan 不含转入额），
之后入账的应补/应退仍按同一口径与已扣退付/已划转出的池子重新抵减。
**整笔冲正**（见 app/reversals.py）把误记收款的核销排除出有效实收：
原收款主行、逐凭证分配与首次收款回执全部保留，只是重算时该笔核销不再
计入——逐凭证已收、账户累计实收与当前尚欠/信用随之按「有效收款 + 已记
应退 + 已付退付」重算；已发生的后续收款分配与退付保持原样，不回填历史
分配，后续新收款按重算后尚欠分配。

HTTP 错误码：
- payment_empty_account：账户尚无已入账凭证，没有可登记的应收（422）；
- payment_amount_not_positive：收款金额非正（422，正常由请求体校验拦截）；
- payment_exceeds_outstanding：收款超过当前应付，整笔拒绝、不留流水
  （422，错误体附 outstanding_yuan / credit_yuan）；
- payment_no_conflict：收款单号已使用但金额或账户不一致（409，错误体
  附已登记的 account_id / amount_yuan）。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.ledger import _entry_response
from app.storage import SessionStore, get_store

# 金额比较与舍入容差：费用为 6 位小数额，浮点残差在 1e-9 内视为 0 / 相等。
_EPS = 1e-9


class PaymentError(Exception):
    """收款操作失败：code 决定 HTTP 错误体 error 字段，status_code 决定状态码。"""

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
        # 附加到错误响应体顶层的结构化字段（如冲突单号的账户/金额、当前尚欠）
        self.extra = extra or {}


def _utc_now_iso() -> str:
    # 与 sessions/ledger._utc_now_iso 同口径：UTC、秒精度、带 Z。
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _round6(x: float) -> float:
    return round(x + 0.0, 6) + 0.0


def account_position(conn, store: SessionStore, account_id: str,
                     exclude_payment_nos: Optional[frozenset] = None) -> dict:
    """在给定连接上重算账户当前头寸：逐凭证尚欠、账户尚欠与信用、累计实收。

    应退抵减与凭证入账先后无关：账户内**全部已记应退**（billing=refund）汇成
    一个应退池，**先扣除全部已落盘退付**（退付只消耗信用，不产生逐凭证核销），
    再按原入账顺序（entry_no 升序）逐张抵减正额凭证
    （full/supplement）**尚未被有效收款核销的部分**：
      每张正额凭证上 applied = min(剩余应退池, 应收 − 有效收款核销)，
    抵减后剩余为该凭证最新尚欠；遍历结束后池中剩余即账户信用。
    已落盘且**未被冲正**的收款核销（现金）优先于应退池受保护：后来补退最多
    抵掉「应收 − 已收款」，旧收款分配永不被改写；**已整笔冲正的收款其核销
    不再计入有效实收**（原收款与核销行保留，见 app/reversals.py）；
    no_adjustment 为 0 元明细，不参与。
    exclude_payment_nos 用于冲正落盘前预演「冲正后」口径（把待冲正单号
    一并按无效排除）。
    返回 entries（与 ledger 明细同序，附 receivable/refund_applied/paid/
    outstanding）与账户级 totals（含 total_received_yuan 累计有效实收、
    total_paid_out_yuan 累计退付与 total_transferred_out_yuan /
    total_transferred_in_yuan 累计信用划出/转入，均不含逐凭证现金核销）。
    """
    rows = store.list_ledger_entry_rows_in_txn(conn, account_id)
    # 应退池按账户整体汇总（与入账先后无关），先扣除已落盘退付与划出额、
    # 加入转入额，再按 FIFO 抵减正额凭证。退付/划出落盘时都已核验
    # 「金额 ≤ 当时信用」，池不会为负；浮点尘埃兜底归零。
    total_paid_out = _round6(store.sum_payouts(conn, account_id))
    total_transferred_out, total_transferred_in = store.sum_transfer_flows(
        conn, account_id
    )
    total_transferred_out = _round6(total_transferred_out)
    total_transferred_in = _round6(total_transferred_in)
    refund_pool = _round6(
        sum(
            abs(float(r["signed_amount_yuan"]))
            for r in rows
            if float(r["signed_amount_yuan"]) < -_EPS
        )
        - total_paid_out
        - total_transferred_out
        + total_transferred_in
    )
    if refund_pool < _EPS:
        refund_pool = 0.0
    total_received = 0.0
    entries: List[dict] = []

    for row in rows:
        signed = float(row["signed_amount_yuan"])
        entry = _entry_response(row)

        if signed > _EPS:
            # 正额凭证：已落盘且未冲正的收款核销先受保护，应退池只抵减尚未核销的部分。
            cash = _round6(
                store.sum_cash_by_voucher(
                    conn, account_id, row["session_id"], int(row["voucher_no"]),
                    exclude_payment_nos,
                )
            )
            if cash > signed + _EPS:  # 不变量兜底：现金不可能超过应收
                cash = signed
            applied = min(refund_pool, _round6(signed - cash))
            refund_pool = _round6(refund_pool - applied)
            outstanding = _round6(signed - cash - applied)
            total_received = _round6(total_received + cash)
            entry.update(
                receivable_yuan=_round6(signed),
                refund_applied_yuan=_round6(applied),
                paid_yuan=cash,
                outstanding_yuan=outstanding,
            )
        else:
            # 应退（signed<0）：金额已计入应退池，本行不产生应收/现金；
            # 零调整（=0）：0 元明细，不参与抵减、分配与尚欠。
            entry.update(
                receivable_yuan=0.0,
                refund_applied_yuan=0.0,
                paid_yuan=0.0,
                outstanding_yuan=0.0,
            )

        entries.append(entry)

    total_outstanding = _round6(sum(e["outstanding_yuan"] for e in entries))
    credit = _round6(refund_pool)
    # 浮点尘埃：尚欠与信用不同时为正，极小残差归零
    if total_outstanding < _EPS:
        total_outstanding = 0.0
    if credit < _EPS:
        credit = 0.0

    totals = {
        "outstanding_yuan": total_outstanding,
        "credit_yuan": credit,
        "total_received_yuan": total_received,
        "total_paid_out_yuan": total_paid_out,
        "total_transferred_out_yuan": total_transferred_out,
        "total_transferred_in_yuan": total_transferred_in,
        "entry_count": len(entries),
    }
    return {"account_id": account_id, "entries": entries, "totals": totals}


def _allocation_response(entry: dict, amount: float, allocation_no: int) -> dict:
    """一条逐凭证收款分配（只对实际分到钱的正额凭证明细生成）。"""
    return {
        "allocation_no": allocation_no,
        "entry_no": entry["entry_no"],
        "session_id": entry["session_id"],
        "voucher_no": entry["voucher_no"],
        "allocated_yuan": _round6(amount),
        "outstanding_before_yuan": entry["outstanding_yuan"],
        "outstanding_after_yuan": _round6(entry["outstanding_yuan"] - amount),
    }


def submit_payment(
    account_id: str,
    payment_no: str,
    amount_yuan: float,
    store: Optional[SessionStore] = None,
) -> dict:
    """按账户登记一笔收款：应退抵减 → 信用 → FIFO 逐凭证核销，原子落盘。

    超过当前应付整笔拒绝（不留流水）；同单号同账户同金额重试返回首收回执；
    同单号换金额或跨账户 → 409。
    """
    store = store or get_store()
    amount_yuan = _round6(amount_yuan)
    # 请求模型已要求 gt=0；这里防住亚微级金额（如 1e-12 被 6 位舍入为 0）：
    # 归一后非正视为非法，不允许「0 元收款」生成空分配。
    if amount_yuan <= 0.0:
        raise PaymentError(
            "payment_amount_not_positive",
            "收款金额必须为正且至少达到 0.000001 元",
            422,
            extra={"payment_no": payment_no, "amount_yuan": amount_yuan},
        )

    with store.write_transaction() as conn:
        # 1) 收款单号全平台唯一：先查重，区分「原样重试」与「单号冲突」。
        existing = store.get_payment(conn, payment_no)
        if existing is not None:
            same_account = existing["account_id"] == account_id
            same_amount = abs(float(existing["amount_yuan"]) - amount_yuan) < _EPS
            if same_account and same_amount:
                # 原样重试：冻结回执原样取回，仅 result 标记 deduped
                # （即使后来又入账了新的应补/应退凭证，回执也不漂移）。
                receipt = json.loads(existing["receipt_json"])
                receipt["result"] = "deduped"
                return receipt
            raise PaymentError(
                "payment_no_conflict",
                f"收款单号 {payment_no} 已登记到账户 {existing['account_id']}、"
                f"金额 {float(existing['amount_yuan'])} 元；同一单号不能换金额"
                "或跨账户使用，本次收款整笔拒绝、不留流水",
                409,
                extra={
                    "payment_no": payment_no,
                    "account_id": existing["account_id"],
                    "amount_yuan": float(existing["amount_yuan"]),
                },
            )

        # 2) 重算当前头寸（应退抵减后的逐凭证尚欠、信用、累计实收）。
        position = account_position(conn, store, account_id)
        entries = position["entries"]
        totals = position["totals"]
        if totals["entry_count"] == 0:
            raise PaymentError(
                "payment_empty_account",
                f"账户 {account_id} 尚无已入账凭证，没有可登记的应收；"
                "请先经凭证入账接口建立台账后再提交收款",
                422,
                extra={"payment_no": payment_no},
            )

        outstanding = totals["outstanding_yuan"]
        credit = totals["credit_yuan"]

        # 3) 超过当前应付整笔拒绝（无应收 / 只有信用时任何正金额都在此拒绝）。
        #    金额与头寸均经 _round6 归一，亚微级浮点残差先归零再比较，
        #    业务金额（6 位小数）边界精确：等于应付可收，大于应付整笔拒绝。
        if amount_yuan > outstanding:
            raise PaymentError(
                "payment_exceeds_outstanding",
                f"收款 {amount_yuan} 元超过账户 {account_id} 当前应付 "
                f"{outstanding} 元（账户信用 {credit} 元）；超过当前应付的收款"
                "整笔拒绝，不登记收款单、不留逐凭证分配流水",
                422,
                extra={
                    "payment_no": payment_no,
                    "amount_yuan": amount_yuan,
                    "outstanding_yuan": outstanding,
                    "credit_yuan": credit,
                },
            )

        # 4) 按原入账顺序（entry_no 升序）把收款分给尚未付清的正额凭证。
        received_at = _utc_now_iso()
        remaining = amount_yuan
        allocations: List[dict] = []
        allocation_no = 0
        # 直接在内存头寸上推进；落盘的核销行以此为准。
        for entry in entries:
            if remaining <= _EPS:
                break
            due = entry["outstanding_yuan"]
            if due <= _EPS:
                continue
            allocation_no += 1
            pay = min(due, remaining)
            pay = _round6(pay)
            allocations.append(_allocation_response(entry, pay, allocation_no))
            entry["outstanding_yuan"] = _round6(due - pay)
            entry["paid_yuan"] = _round6(entry["paid_yuan"] + pay)
            remaining = _round6(remaining - pay)

        # 5) 组装首次回执（落盘冻结）。累计实收与最新尚欠/信用为本次后的快照。
        new_total_received = _round6(totals["total_received_yuan"] + amount_yuan)
        new_outstanding = _round6(sum(e["outstanding_yuan"] for e in entries))
        receipt = {
            "account_id": account_id,
            "payment_no": payment_no,
            "result": "received",
            "amount_yuan": amount_yuan,
            "received_at": received_at,
            "allocations": allocations,
            "total_received_yuan": new_total_received,
            "outstanding_yuan": new_outstanding,
            "credit_yuan": credit,
            "payment_count": store.count_payments(conn, account_id) + 1,
        }

        # 6) 收款主行（含冻结回执 JSON）与逐凭证核销同一事务落盘；
        #    payment_no 主键兜底，并发同单号只有一笔成功。
        payment_seq = store.count_payments(conn, account_id) + 1
        store.insert_payment(
            conn,
            payment_id=str(uuid.uuid4()),
            payment_no=payment_no,
            account_id=account_id,
            payment_seq=payment_seq,
            amount_yuan=amount_yuan,
            receipt_json=json.dumps(receipt, ensure_ascii=False),
            received_at=received_at,
        )
        for alloc in allocations:
            store.insert_payment_allocation(
                conn,
                payment_no=payment_no,
                account_id=account_id,
                allocation_no=alloc["allocation_no"],
                session_id=alloc["session_id"],
                voucher_no=alloc["voucher_no"],
                entry_no=alloc["entry_no"],
                allocated_yuan=alloc["allocated_yuan"],
                received_at=received_at,
            )
        return receipt


def list_payments(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按收款次序查询账户全部收款记录（均为落盘时冻结的首收回执）。

    每条记录附冲正状态（reversed / reversed_at）：被整笔冲正的收款其记录、
    逐凭证分配与首收回执保持原样，只是不再计入账户级累计有效实收
    （total_received_yuan 只汇总未冲正收款）。
    """
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        rows = store.list_payment_rows(account_id)
        reversal_rows = store.list_reversal_rows(account_id)

    reversed_at_by_no = {r["payment_no"]: r["reversed_at"] for r in reversal_rows}
    payments: List[dict] = []
    total_received_valid = 0.0
    for row in rows:
        receipt = json.loads(row["receipt_json"])
        # 正常列表查询：首收记录保持 received（重试时才在提交接口标 deduped）。
        receipt["result"] = "received"
        reversed_at = reversed_at_by_no.get(row["payment_no"])
        receipt["reversed"] = reversed_at is not None
        receipt["reversed_at"] = reversed_at
        payments.append(receipt)
        if reversed_at is None:  # 只把有效（未冲正）收款计入累计实收
            total_received_valid = _round6(
                total_received_valid + float(row["amount_yuan"])
            )

    return {
        "account_id": account_id,
        "payment_count": len(payments),
        "total_received_yuan": _round6(total_received_valid),
        "outstanding_yuan": totals["outstanding_yuan"],
        "credit_yuan": totals["credit_yuan"],
        "payments": payments,
    }


def ledger_detail_with_position(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """在原入账明细查询上补充逐凭证与账户级最新尚欠/信用、累计实收。

    明细的金额/计费栏/余额快照仍为入账冻结值（后续补退不改写）；
    receivable_yuan / refund_applied_yuan / paid_yuan / outstanding_yuan
    为由当前全部凭证与已落盘核销重算的最新值。
    """
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)

    # 原 GET ledger 响应字段（明细取冻结值）+ 最新头寸字段。
    entries = position["entries"]
    balance = entries[-1]["balance_after_yuan"] if entries else 0.0
    return {
        "account_id": account_id,
        "entry_count": len(entries),
        "balance_yuan": balance,
        "outstanding_yuan": position["totals"]["outstanding_yuan"],
        "credit_yuan": position["totals"]["credit_yuan"],
        "total_received_yuan": position["totals"]["total_received_yuan"],
        "entries": entries,
    }
