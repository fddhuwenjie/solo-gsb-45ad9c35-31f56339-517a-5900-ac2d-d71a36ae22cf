"""误记收款的整笔冲正：按账户与原收款单号把一笔已登记收款整笔冲正。

口径（完全基于 app/payments.py 落盘的收款主行与逐凭证核销、app/ledger.py
落盘的已记凭证与 app/payouts.py 落盘的退付；凭证金额一律取入账时冻结的
计费栏，不重新核算）：
1. 调用方按账户提交**原收款单号** payment_no：冲正只接受原收款所属账户办理；
2. **明确拒绝**（均不留下冲正记录）：原收款单不存在 → 404 payment_not_found；
   单号登记在别的账户 → 409 reversal_account_mismatch（错误体附原收款账户）；
3. **整笔冲正**：冲正金额即原收款金额。原收款主行、逐凭证分配与首次收款
   回执全部保留（收款查询中该笔标记 reversed 并附冲正时间），只是其核销
   **不再计入有效实收**；
4. **冲正后重算**：台账明细的逐凭证已收、账户累计实收，以及台账 / 收款 /
   退付查询的当前尚欠与信用，都按「有效收款（未冲正）+ 已记应退 + 已付
   退付」重算（见 app/payments.account_position）；已发生的后续收款分配
   与退付保持原样，**不回填历史分配**；后续新收款按重算后尚欠 FIFO 分配；
   原收款单号重试收款仍返回首次收款回执（result=deduped，不因冲正改变）；
5. **幂等**：同一账户对同一单号重复冲正 → result=deduped，原样返回**首次
   冲正时冻结的回执**（冲正金额、冲正时间与冲正后头寸快照都不漂移，即使
   后来又入账新凭证、登记新收款或退付）；
6. 冲正核对、头寸重算与落盘和原入账、收款、退付**共用同一把 SQLite 写事务
   锁**（RLock + BEGIN IMMEDIATE）：payment_no 主键在数据库层兜底，并发
   冲正同一笔收款只有一笔生效，其余取回首次冲正回执；核对失败一律回滚、
   不留冲正记录；冲正记录持久化在 SQLite 中，进程重启后仍可查询。

HTTP 错误码：
- payment_not_found：原收款单不存在（404，错误体附 payment_no）；
- reversal_account_mismatch：原收款登记在别的账户（409，错误体附原收款
  account_id 与 payment_no）；
- invalid_input：收款单号为空 / 超长等请求体非法（422，由请求模型拦截）。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from app.payments import _round6, account_position
from app.storage import SessionStore, get_store


class ReversalError(Exception):
    """冲正操作失败：code 决定 HTTP 错误体 error 字段，status_code 决定状态码。"""

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
        # 附加到错误响应体顶层的结构化字段（如原收款单号、原收款所属账户）
        self.extra = extra or {}


def _utc_now_iso() -> str:
    # 与 sessions/ledger/payments._utc_now_iso 同口径：UTC、秒精度、带 Z。
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def submit_reversal(
    account_id: str,
    payment_no: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按账户把一笔已登记收款整笔冲正：核对原收款后原子落盘，返回冻结回执。

    原单不存在 → 404；账户不符 → 409；同账户同单号重复冲正返回首次冲正回执。
    """
    store = store or get_store()

    with store.write_transaction() as conn:
        # 1) 原收款必须存在且属于本账户：不存在 / 账户不符都明确拒绝、不留记录。
        payment = store.get_payment(conn, payment_no)
        if payment is None:
            raise ReversalError(
                "payment_not_found",
                f"收款单号 {payment_no} 没有已登记的收款，无法冲正；"
                "请核对原收款单号（收款单号全平台唯一，冲正须按原收款所属账户办理）",
                404,
                extra={"payment_no": payment_no},
            )
        if payment["account_id"] != account_id:
            raise ReversalError(
                "reversal_account_mismatch",
                f"收款单号 {payment_no} 的收款登记在账户 {payment['account_id']}，"
                f"不属于本账户 {account_id}；冲正只接受原收款所属账户办理，"
                "本次拒绝、不留下冲正记录",
                409,
                extra={
                    "payment_no": payment_no,
                    "account_id": payment["account_id"],
                },
            )

        # 2) 该笔收款已冲正过：重复冲正原样返回首次冲正冻结回执（不漂移）。
        existing = store.get_reversal(conn, payment_no)
        if existing is not None:
            receipt = json.loads(existing["receipt_json"])
            receipt["result"] = "deduped"
            return receipt

        # 3) 冲正后头寸预演：把本笔收款的核销排除出有效实收后重算
        #    （后续收款分配与已付退付保持原样，不回填历史分配）。
        amount_yuan = _round6(float(payment["amount_yuan"]))
        position = account_position(
            conn, store, account_id, exclude_payment_nos=frozenset({payment_no})
        )
        totals = position["totals"]

        # 4) 组装首次冲正回执（落盘冻结）：冲正金额、冲正时间与冲正后头寸快照。
        reversed_at = _utc_now_iso()
        reversal_seq = store.count_reversals(conn, account_id) + 1
        receipt = {
            "account_id": account_id,
            "payment_no": payment_no,
            "result": "reversed",
            "amount_yuan": amount_yuan,
            "reversed_at": reversed_at,
            "total_received_yuan": totals["total_received_yuan"],
            "outstanding_yuan": totals["outstanding_yuan"],
            "credit_yuan": totals["credit_yuan"],
            "reversal_count": reversal_seq,
        }

        # 5) 冲正行（含冻结回执 JSON）落盘；payment_no 主键兜底，
        #    并发冲正同一笔收款只有一笔成功。
        store.insert_reversal(
            conn,
            reversal_id=str(uuid.uuid4()),
            payment_no=payment_no,
            account_id=account_id,
            reversal_seq=reversal_seq,
            amount_yuan=amount_yuan,
            receipt_json=json.dumps(receipt, ensure_ascii=False),
            reversed_at=reversed_at,
        )
        return receipt


def list_reversals(
    account_id: str,
    store: Optional[SessionStore] = None,
) -> dict:
    """按冲正次序查询账户全部冲正记录（均为落盘时冻结的首次冲正回执）。"""
    store = store or get_store()
    with store._connect() as conn:
        position = account_position(conn, store, account_id)
        totals = position["totals"]
        rows = store.list_reversal_rows(account_id)

    reversals: List[dict] = []
    total_reversed = 0.0
    for row in rows:
        receipt = json.loads(row["receipt_json"])
        # 正常列表查询：冲正记录保持 reversed（重试时才在提交接口标 deduped）。
        receipt["result"] = "reversed"
        reversals.append(receipt)
        total_reversed = _round6(total_reversed + float(row["amount_yuan"]))

    return {
        "account_id": account_id,
        "reversal_count": len(reversals),
        "total_reversed_yuan": _round6(total_reversed),
        "outstanding_yuan": totals["outstanding_yuan"],
        "credit_yuan": totals["credit_yuan"],
        "reversals": reversals,
    }
