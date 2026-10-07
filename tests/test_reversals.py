"""误记收款整笔冲正行为测试（按账户 + 原收款单号办理）。

覆盖：
- 基本冲正：原收款、逐凭证分配与首次收款回执保留，冲正后该笔不再计入
  有效实收；台账明细逐凭证已收、账户累计实收与台账/收款/退付查询的
  当前尚欠、信用按「有效收款 + 已记应退 + 已付退付」重算；
- 冲正回执：冲正金额、冲正时间与冲正后头寸；重复冲正返回首次冻结回执
  （result=deduped，即使后来入账新凭证、登记新收款或退付也不漂移）；
- 明确拒绝：原单不存在 404 payment_not_found、账户不符 409
  reversal_account_mismatch（附原收款账户），均不留冲正记录、头寸不变；
- 已发生的后续收款分配与退付保持原样，不回填历史分配；后续新收款按
  重算后尚欠 FIFO 分配；原收款单号重试收款仍返回首次收款回执；
- 信用与退付联动：冲正后应退池重新抵减、信用重算，已付退付不变；
- 收款查询展示冲正状态（reversed / reversed_at）；
- 冲正记录按冲正次序查询、内容为首次冻结值、账户级头寸为最新值；
- 并发：同一笔收款多线程冲正只有一笔生效（其余 deduped）；
  冲正与新收款并发串行一致；
- 按同一路径重建存储模拟进程重启：冲正记录、冲正状态与重算口径仍在，
  可继续冲正（序号接续）；
- 旧库（无冲正表）打开自动补建；既有收款/退付/入账接口行为不变。
"""

import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.reversals import submit_reversal
from app.storage import configure_store, get_store

client = TestClient(app)


DISPATCH = dict(
    prices=[0.0, 1.0, 1.0, 0.0],
    base_load_kw=[0.2, 0.2, 0.2, 0.2],
    pv_kw=[0.0, 0.0, 0.0, 0.0],
    capacity_kwh=0.4,
    max_charge_kw=1.0,
    max_discharge_kw=1.0,
    charge_efficiency=0.9,
    discharge_efficiency=0.9,
    initial_soc_kwh=0.0,
    min_terminal_soc_kwh=0.0,
    degradation_cost_per_kwh=0.2,
)

RECORDS = [
    dict(
        base_load_kw=0.2, pv_kw=0.0,
        charge_ac_energy_kwh=round(0.2 / 0.9, 6), discharge_ac_energy_kwh=0.0,
        grid_purchase_energy_kwh=round(0.1 + 0.2 / 0.9, 6),
        soc_start_kwh=0.0, soc_end_kwh=0.2,
    ),
    dict(
        base_load_kw=0.2, pv_kw=0.0,
        charge_ac_energy_kwh=0.0, discharge_ac_energy_kwh=0.09,
        grid_purchase_energy_kwh=0.01,
        soc_start_kwh=0.2, soc_end_kwh=0.1,
    ),
    dict(
        base_load_kw=0.2, pv_kw=0.0,
        charge_ac_energy_kwh=0.0, discharge_ac_energy_kwh=0.09,
        grid_purchase_energy_kwh=0.01,
        soc_start_kwh=0.1, soc_end_kwh=0.0,
    ),
    dict(
        base_load_kw=0.2, pv_kw=0.0,
        charge_ac_energy_kwh=0.0, discharge_ac_energy_kwh=0.0,
        grid_purchase_energy_kwh=0.1,
        soc_start_kwh=0.0, soc_end_kwh=0.0,
    ),
]

TOL = 0.01


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    configure_store(str(tmp_path / "reversals.db"))
    yield
    configure_store(None)


def _plan():
    return client.post("/dispatch", json=DISPATCH).json()


def _create_complete():
    sid = client.post(
        "/dispatch/sessions",
        json={"dispatch": DISPATCH, "plan_snapshot": _plan(), "audit_tolerance_kwh": TOL},
    ).json()["session_id"]
    assert client.post(
        f"/dispatch/sessions/{sid}/batches",
        json={"start_index": 0, "records": RECORDS[:2]},
    ).status_code == 200
    assert client.post(
        f"/dispatch/sessions/{sid}/batches",
        json={"start_index": 2, "records": RECORDS[2:]},
    ).status_code == 200
    return sid


def _rev(sid, expected, changes):
    return client.post(
        f"/dispatch/sessions/{sid}/revisions",
        json={"expected_revision": expected, "changes": changes},
    )


def _issue(sid, revision):
    return client.post(
        f"/dispatch/sessions/{sid}/vouchers",
        json={"expected_revision": revision},
    )


def _ch(index, record):
    return {"index": index, "record": record}


def _post_vouchers(account, items):
    return client.post(
        f"/accounts/{account}/ledger",
        json={"items": [{"session_id": s, "voucher_no": n} for s, n in items]},
    )


def _pay(account, payment_no, amount):
    return client.post(
        f"/accounts/{account}/payments",
        json={"payment_no": payment_no, "amount_yuan": amount},
    )


def _payout(account, payout_no, amount):
    return client.post(
        f"/accounts/{account}/payouts",
        json={"payout_no": payout_no, "amount_yuan": amount},
    )


def _reverse(account, payment_no):
    return client.post(
        f"/accounts/{account}/payment-reversals",
        json={"payment_no": payment_no},
    )


def _ledger(account):
    return client.get(f"/accounts/{account}/ledger").json()


def _payments(account):
    return client.get(f"/accounts/{account}/payments").json()


def _payouts(account):
    return client.get(f"/accounts/{account}/payouts").json()


def _reversals(account):
    return client.get(f"/accounts/{account}/payment-reversals").json()


def _make_voucher_chain(sid):
    """凭证 1=full 0.10、2=supplement 0.01、3=refund 0.02（跳过 rev2 对 rev3 出单）。"""
    _issue(sid, 0)
    up1 = dict(RECORDS[1]); up1.update(base_load_kw=0.22, grid_purchase_energy_kwh=0.02)
    _rev(sid, 0, [_ch(1, up1)])
    v2 = _issue(sid, 1).json()
    assert v2["billing"]["type"] == "supplement"
    up2 = dict(RECORDS[1]); up2.update(base_load_kw=0.24, grid_purchase_energy_kwh=0.03)
    _rev(sid, 1, [_ch(1, up2)])
    down = dict(RECORDS[1]); down.update(base_load_kw=0.18, grid_purchase_energy_kwh=0.0)
    _rev(sid, 2, [_ch(1, down)])
    v3 = _issue(sid, 3).json()
    assert v3["billing"]["type"] == "refund"
    assert v3["billing"]["amount_yuan"] == pytest.approx(0.02, abs=1e-9)


def _raw_entry(account, entry_no, sid, vno, signed, billing):
    """直接落一条台账明细（绕过费用非负的业务约束，精确控制头寸口径）。"""
    store = get_store()
    with store.write_transaction() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sessions(session_id, dispatch_json,"
            " plan_snapshot_json, audit_tolerance_kwh, num_periods, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (sid, "{}", "{}", TOL, 4, "t"),
        )
        conn.execute(
            "INSERT OR IGNORE INTO session_settlement_vouchers(voucher_id, session_id,"
            " voucher_no, revision_no, current_revision_at_issue, ledger_json,"
            " settlement_json, total_cost_yuan, billing_type, amount_yuan,"
            " relative_to_revision, relative_to_voucher_no, issued_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), sid, vno, 0, 0, "[]", "{}",
             abs(signed), billing, abs(signed), None, None, "t"),
        )
        conn.execute(
            "INSERT INTO account_ledger_entries(entry_id, account_id, entry_no,"
            " session_id, voucher_no, voucher_id, revision_no, billing_type,"
            " voucher_amount_yuan, relative_to_revision, relative_to_voucher_no,"
            " signed_amount_yuan, balance_after_yuan, total_cost_yuan, posted_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), account, entry_no, sid, vno, str(uuid.uuid4()), 0,
             billing, abs(signed), None, None, signed, signed, abs(signed), "t"),
        )


def _paid_off_account():
    """full 0.10 + supplement 0.01 + refund 0.02 入账并收两笔（0.05、0.04）全部付清。"""
    sid = _create_complete()
    _make_voucher_chain(sid)
    _post_vouchers("fam", [(sid, 1), (sid, 2), (sid, 3)])
    assert _pay("fam", "P1", 0.05).status_code == 200
    assert _pay("fam", "P2", 0.04).status_code == 200
    return sid


# ---------------------------------------------------------------------------
# 基本冲正：保留原收款与首收回执，有效实收与各处头寸重算
# ---------------------------------------------------------------------------


def test_reversal_basic_recomputes_positions_and_keeps_original():
    """full 0.10（应退已抵 0.02）+ supplement 0.01；P1 0.05、P2 0.04 付清。
    冲正 P1 后：P1 的 0.05 不再计入有效实收，entry1 已收只剩 P2 的 0.03，
    尚欠回到 0.05；原收款记录、逐凭证分配与首收回执原样保留。"""
    _paid_off_account()
    before = _payments("fam")
    assert before["total_received_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert before["outstanding_yuan"] == 0.0

    r = _reverse("fam", "P1")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "reversed"
    assert body["account_id"] == "fam"
    assert body["payment_no"] == "P1"
    assert body["amount_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["reversed_at"].endswith("Z")
    # 冲正后头寸：有效实收只剩 P2 的 0.04，entry1 尚欠回到 0.05，无信用
    assert body["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert body["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["credit_yuan"] == 0.0
    assert body["reversal_count"] == 1

    # 台账明细：entry1 已收只剩 P2 的 0.03，应退仍抵 0.02，尚欠 0.05
    led = _ledger("fam")
    assert led["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert led["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert led["credit_yuan"] == 0.0
    e1, e2, _ = led["entries"]
    assert e1["paid_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert e1["refund_applied_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert e1["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert e2["paid_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert e2["outstanding_yuan"] == 0.0

    # 收款查询：P1 展示冲正状态，P2 未冲正；账户级累计实收只算有效收款
    lst = _payments("fam")
    assert lst["payment_count"] == 2  # 原收款记录保留
    assert lst["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert lst["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    p1, p2 = lst["payments"]
    assert p1["payment_no"] == "P1"
    assert p1["reversed"] is True
    assert p1["reversed_at"] == body["reversed_at"]
    assert p2["payment_no"] == "P2"
    assert p2["reversed"] is False
    assert p2["reversed_at"] is None
    # P1 首收回执冻结：逐凭证分配与当时快照不因冲正改变
    assert p1["result"] == "received"
    assert p1["amount_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert p1["total_received_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert p1["outstanding_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert len(p1["allocations"]) == 1
    assert p1["allocations"][0]["entry_no"] == 1
    assert p1["allocations"][0]["allocated_yuan"] == pytest.approx(0.05, abs=1e-9)
    # P2 的后续收款分配保持原样，不回填历史分配
    assert [(a["entry_no"], a["allocated_yuan"]) for a in p2["allocations"]] == [
        (1, pytest.approx(0.03, abs=1e-9)),
        (2, pytest.approx(0.01, abs=1e-9)),
    ]

    # 退付查询的当前头寸同步重算
    po = _payouts("fam")
    assert po["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert po["credit_yuan"] == 0.0


def test_reversal_retry_returns_first_frozen_receipt():
    """重复冲正返回首次冲正回执；即使后来入账新凭证、登记新收款也不漂移。"""
    sid = _paid_off_account()
    first = _reverse("fam", "P1")
    assert first.status_code == 200
    first_body = first.json()

    # 冲正后又有新业务：新收款付清、再入账新应补凭证
    assert _pay("fam", "P3", 0.05).status_code == 200
    up = dict(RECORDS[1]); up.update(base_load_kw=0.30, grid_purchase_energy_kwh=0.06)
    up2 = dict(RECORDS[2]); up2.update(base_load_kw=0.30, grid_purchase_energy_kwh=0.06)
    _rev(sid, 3, [_ch(1, up), _ch(2, up2)])
    _issue(sid, 4)  # supplement 0.11
    _post_vouchers("fam", [(sid, 4)])

    retry = _reverse("fam", "P1")
    assert retry.status_code == 200
    rb = retry.json()
    assert rb["result"] == "deduped"
    for key in ("account_id", "payment_no", "amount_yuan", "reversed_at",
                "total_received_yuan", "outstanding_yuan", "credit_yuan",
                "reversal_count"):
        assert rb[key] == first_body[key]
    # 不新增冲正记录
    assert _reversals("fam")["reversal_count"] == 1


# ---------------------------------------------------------------------------
# 明确拒绝：原单不存在 / 账户不符，均不留冲正记录
# ---------------------------------------------------------------------------


def test_reversal_payment_not_found():
    _paid_off_account()
    r = _reverse("fam", "NO-SUCH-PAYMENT")
    assert r.status_code == 404
    body = r.json()
    assert body["error"] == "payment_not_found"
    assert body["payment_no"] == "NO-SUCH-PAYMENT"
    # 失败不留冲正记录，头寸不变
    assert _reversals("fam")["reversal_count"] == 0
    assert _reversals("fam")["reversals"] == []
    assert _ledger("fam")["outstanding_yuan"] == 0.0
    assert _payments("fam")["total_received_yuan"] == pytest.approx(0.09, abs=1e-9)


def test_reversal_account_mismatch():
    s1 = _create_complete(); _issue(s1, 0)
    s2 = _create_complete(); _issue(s2, 0)
    _post_vouchers("left", [(s1, 1)])
    _post_vouchers("right", [(s2, 1)])
    assert _pay("left", "SHARED-PAY", 0.05).status_code == 200

    r = _reverse("right", "SHARED-PAY")
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "reversal_account_mismatch"
    assert body["payment_no"] == "SHARED-PAY"
    assert body["account_id"] == "left"
    # 两边都不留冲正记录，左账户头寸不变
    assert _reversals("left")["reversal_count"] == 0
    assert _reversals("right")["reversal_count"] == 0
    assert _payments("left")["total_received_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _payments("left")["payments"][0]["reversed"] is False


def test_reversal_request_validation():
    for bad in ("", "x" * 129):
        r = _reverse("fam", bad)
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_input"


# ---------------------------------------------------------------------------
# 原单号重试收款仍返回原收款回执；后续新收款按重算后尚欠分配
# ---------------------------------------------------------------------------


def test_original_payment_no_retry_still_returns_original_receipt():
    _paid_off_account()
    first_pay = _pay("fam", "P1", 0.05)  # 已落盘；再取首收回执用于比对
    # 重新拿首次冻结回执（上面是 deduped 语义外的又一次提交，内容相同）
    assert first_pay.status_code == 200
    _reverse("fam", "P1")

    # 原单号 + 同账户 + 同金额重试收款：仍返回首次收款回执（不因冲正改变）
    retry = _pay("fam", "P1", 0.05)
    assert retry.status_code == 200
    rb = retry.json()
    assert rb["result"] == "deduped"
    assert rb["total_received_yuan"] == pytest.approx(0.05, abs=1e-9)  # 首收时快照
    assert rb["outstanding_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert len(rb["allocations"]) == 1
    assert rb["allocations"][0]["allocated_yuan"] == pytest.approx(0.05, abs=1e-9)
    # 不新增收款记录
    assert _payments("fam")["payment_count"] == 2

    # 原单号换金额仍是 409 冲突（冲正不释放单号）
    conflict = _pay("fam", "P1", 0.06)
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "payment_no_conflict"


def test_new_payment_after_reversal_allocates_by_recomputed_outstanding():
    """冲正 P1 后尚欠回到 0.05：新收款 P3 按重算后尚欠 FIFO 补到 entry1；
    已发生的 P2 分配保持原样不回填；超过重算后尚欠仍整笔拒绝。"""
    _paid_off_account()
    _reverse("fam", "P1")

    # 超过重算后尚欠（0.05）整笔拒绝，不留流水
    over = _pay("fam", "P3", 0.06)
    assert over.status_code == 422
    assert over.json()["error"] == "payment_exceeds_outstanding"
    assert over.json()["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _payments("fam")["payment_count"] == 2

    # 新收款 0.05 恰好补平：全部 FIFO 分给 entry1（P1 留下的缺口）
    ok = _pay("fam", "P3", 0.05)
    assert ok.status_code == 200, ok.text
    b = ok.json()
    assert b["total_received_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert b["outstanding_yuan"] == 0.0
    assert [(a["entry_no"], a["allocated_yuan"]) for a in b["allocations"]] == [
        (1, pytest.approx(0.05, abs=1e-9))
    ]
    assert b["allocations"][0]["outstanding_before_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert b["allocations"][0]["outstanding_after_yuan"] == 0.0

    led = _ledger("fam")
    assert led["outstanding_yuan"] == 0.0
    assert led["total_received_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert led["entries"][0]["paid_yuan"] == pytest.approx(0.08, abs=1e-9)

    # P2 的冻结分配仍是 0.03/0.01，不回填 P1 的缺口
    p2 = [p for p in _payments("fam")["payments"] if p["payment_no"] == "P2"][0]
    assert [(a["entry_no"], a["allocated_yuan"]) for a in p2["allocations"]] == [
        (1, pytest.approx(0.03, abs=1e-9)),
        (2, pytest.approx(0.01, abs=1e-9)),
    ]


# ---------------------------------------------------------------------------
# 信用与退付联动：冲正后应退池重抵、信用重算，已付退付不变
# ---------------------------------------------------------------------------


def test_reversal_with_credit_and_payout_recomputes():
    """full 0.10 被 P1 付清后入账应退 0.05 并退付 0.02（信用 0.03）。
    冲正 P1：应退池（0.05 − 已退付 0.02 = 0.03）重抵 entry1 → 尚欠 0.07、
    信用 0；已付退付与退付回执保持原样。"""
    _raw_entry("fam", 1, "rs1", 1, 0.10, "full")
    assert _pay("fam", "P1", 0.10).status_code == 200
    _raw_entry("fam", 2, "rs2", 1, -0.05, "refund")
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    po = _payout("fam", "PO1", 0.02)
    assert po.status_code == 200
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)

    r = _reverse("fam", "P1")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total_received_yuan"] == 0.0
    assert body["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert body["credit_yuan"] == 0.0

    # 台账 / 收款 / 退付查询的当前头寸一致重算
    for view in (_ledger("fam"), _payments("fam"), _payouts("fam")):
        assert view["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
        assert view["credit_yuan"] == 0.0
    e1 = _ledger("fam")["entries"][0]
    assert e1["paid_yuan"] == 0.0
    assert e1["refund_applied_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert e1["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)

    # 已付退付不变：累计退付仍 0.02，退付回执冻结
    pol = _payouts("fam")
    assert pol["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert pol["payouts"][0]["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert pol["payouts"][0]["credit_yuan"] == pytest.approx(0.03, abs=1e-9)  # 当时快照

    # 冲正后新收款按重算后尚欠分配
    ok = _pay("fam", "P2", 0.07)
    assert ok.status_code == 200
    assert ok.json()["outstanding_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 冲正记录查询
# ---------------------------------------------------------------------------


def test_reversal_list_order_and_frozen_content():
    sid = _create_complete()
    _make_voucher_chain(sid)
    _post_vouchers("fam", [(sid, 1), (sid, 2), (sid, 3)])
    _pay("fam", "P1", 0.05)
    _pay("fam", "P2", 0.04)
    _reverse("fam", "P1")
    _pay("fam", "P3", 0.05)  # 补平后再冲正 P2
    _reverse("fam", "P2")

    lst = _reversals("fam")
    assert lst["account_id"] == "fam"
    assert lst["reversal_count"] == 2
    assert lst["total_reversed_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert [r["payment_no"] for r in lst["reversals"]] == ["P1", "P2"]
    assert all(r["result"] == "reversed" for r in lst["reversals"])
    assert [r["reversal_count"] for r in lst["reversals"]] == [1, 2]
    # 每条快照为当时值：P1 冲正后尚欠 0.05、有效实收 0.04；
    # P2 冲正后有效现金只剩 P3 的 0.05（entry1），尚欠 0.03 + 0.01 = 0.04
    r1, r2 = lst["reversals"]
    assert r1["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert r1["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert r2["outstanding_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert r2["total_received_yuan"] == pytest.approx(0.05, abs=1e-9)
    # 账户级头寸为最新值
    assert lst["outstanding_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert lst["credit_yuan"] == 0.0

    # 空账户查询
    empty = _reversals("never-used")
    assert empty == {
        "account_id": "never-used",
        "reversal_count": 0,
        "total_reversed_yuan": 0.0,
        "outstanding_yuan": 0.0,
        "credit_yuan": 0.0,
        "reversals": [],
    }


# ---------------------------------------------------------------------------
# 并发：同一笔收款冲正只有一笔生效；冲正与新收款并发串行一致
# ---------------------------------------------------------------------------


def test_concurrent_reversal_only_one_effective():
    _paid_off_account()
    outcomes = []

    def worker():
        try:
            res = submit_reversal("fam", "P1")
            outcomes.append(res["result"])
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    reversed_once = [o for o in outcomes if o == "reversed"]
    deduped = [o for o in outcomes if o == "deduped"]
    assert len(reversed_once) == 1
    assert len(deduped) == 11
    assert set(outcomes) <= {"reversed", "deduped"}
    lst = _reversals("fam")
    assert lst["reversal_count"] == 1
    assert lst["total_reversed_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("fam")["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)


def test_concurrent_reversal_and_new_payment_serialize():
    """冲正 P1 与新收款 P3 并发：共用同一写事务锁串行，两种次序的最终
    头寸一致（尚欠 0.01、有效实收 0.08），不会重复核销也不留半吊子状态。"""
    sid = _create_complete()
    _issue(sid, 0)  # full 0.10
    _post_vouchers("fam", [(sid, 1)])
    assert _pay("fam", "P1", 0.06).status_code == 200  # 尚欠 0.04

    results = []

    def rev_worker():
        try:
            results.append(("rev", submit_reversal("fam", "P1")["result"]))
        except Exception as exc:  # noqa: BLE001
            results.append(("rev", getattr(exc, "code", "error")))

    def pay_worker():
        try:
            from app.payments import submit_payment
            results.append(("pay", submit_payment("fam", "P3", 0.04)["result"]))
        except Exception as exc:  # noqa: BLE001
            results.append(("pay", getattr(exc, "code", "error")))

    threads = [threading.Thread(target=rev_worker), threading.Thread(target=pay_worker)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(results) == [("pay", "received"), ("rev", "reversed")]
    led = _ledger("fam")
    # 两种串行次序殊途同归：P1 的 0.06 被冲正，P3 的 0.04 有效
    assert led["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert led["outstanding_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert _reversals("fam")["reversal_count"] == 1
    assert _payments("fam")["payment_count"] == 2


# ---------------------------------------------------------------------------
# 重启持久化、旧库补建、既有接口不变
# ---------------------------------------------------------------------------


def test_reversal_persist_across_restart(tmp_path):
    db_path = str(tmp_path / "persist-rev.db")
    configure_store(db_path)
    _paid_off_account()
    first = _reverse("fam", "P1").json()

    configure_store(db_path)  # 模拟进程重启
    # 冲正状态与重算口径仍在
    lst = _payments("fam")
    assert lst["payments"][0]["reversed"] is True
    assert lst["payments"][0]["reversed_at"] == first["reversed_at"]
    assert lst["total_received_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert lst["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    revs = _reversals("fam")
    assert revs["reversal_count"] == 1
    assert revs["reversals"][0]["reversed_at"] == first["reversed_at"]

    # 重启后重复冲正仍返回首次回执
    retry = _reverse("fam", "P1")
    assert retry.status_code == 200
    assert retry.json()["result"] == "deduped"
    assert retry.json()["reversed_at"] == first["reversed_at"]
    assert _reversals("fam")["reversal_count"] == 1

    # 重启后可继续冲正其他收款（序号接续），新收款按重算后尚欠分配
    nxt = _reverse("fam", "P2")
    assert nxt.status_code == 200
    assert nxt.json()["reversal_count"] == 2
    assert nxt.json()["outstanding_yuan"] == pytest.approx(0.09, abs=1e-9)
    ok = _pay("fam", "P3", 0.09)
    assert ok.status_code == 200
    assert ok.json()["outstanding_yuan"] == 0.0


def test_legacy_db_reversal_table_auto_created(tmp_path):
    """旧库（只有 sessions / session_records）打开即补建冲正表：可查可冲正。"""
    import json
    import sqlite3

    db_path = str(tmp_path / "legacy-rev.db")
    plan = client.post("/dispatch", json=DISPATCH).json()
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY, dispatch_json TEXT NOT NULL,
            plan_snapshot_json TEXT NOT NULL, audit_tolerance_kwh REAL NOT NULL,
            num_periods INTEGER NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE session_records (
            session_id TEXT NOT NULL, idx INTEGER NOT NULL, record_json TEXT NOT NULL,
            stored_at TEXT NOT NULL, PRIMARY KEY (session_id, idx));
        """
    )
    sid = "legacy-rev-session"
    conn.execute(
        "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)",
        (sid, json.dumps(DISPATCH), json.dumps(plan), TOL, 4, "2026-01-01T00:00:00Z"),
    )
    for idx, rec in enumerate(RECORDS):
        conn.execute(
            "INSERT INTO session_records VALUES (?, ?, ?, ?)",
            (sid, idx, json.dumps(rec), "2026-01-01T00:00:01Z"),
        )
    conn.commit()
    conn.close()

    configure_store(db_path)
    # 冲正表自动补建：空账户冲正查询为 0
    assert _reversals("new-acct")["reversal_count"] == 0
    # 端到端：出单 → 入账 → 收款 → 冲正
    assert _issue(sid, 0).status_code == 200
    assert _post_vouchers("new-acct", [(sid, 1)]).status_code == 200
    assert _pay("new-acct", "LEGACY-PAY", 0.10).status_code == 200
    rev = _reverse("new-acct", "LEGACY-PAY")
    assert rev.status_code == 200, rev.text
    assert rev.json()["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _payments("new-acct")["payments"][0]["reversed"] is True
    # 存储层表结构就位
    store = get_store()
    with store._connect() as c:
        names = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "account_payment_reversals" in names


def test_existing_payment_payout_flows_unchanged_after_reversal():
    """冲正不破坏既有口径：收款幂等/冲突、退付、入账行为不变（端到端抽验）。"""
    sid = _create_complete()
    _make_voucher_chain(sid)
    _post_vouchers("fam", [(sid, 1), (sid, 2), (sid, 3)])
    _pay("fam", "P1", 0.05)
    _pay("fam", "P2", 0.04)
    _reverse("fam", "P1")

    # 收款幂等与冲突判定照常（P2 未冲正：重试 deduped、换金额 409）
    assert _pay("fam", "P2", 0.04).json()["result"] == "deduped"
    assert _pay("fam", "P2", 0.01).status_code == 409
    # 重复入账仍 409
    assert _post_vouchers("fam", [(sid, 1)]).status_code == 409
    # 无信用账户退付仍整笔拒绝
    no_credit = _payout("fam", "PO-X", 0.01)
    assert no_credit.status_code == 422
    assert no_credit.json()["error"] == "payout_exceeds_credit"
    # 凭证列表仍可查
    vouchers = client.get(f"/dispatch/sessions/{sid}/vouchers")
    assert vouchers.status_code == 200
    assert [v["voucher_no"] for v in vouchers.json()["vouchers"]] == [1, 2, 3]
