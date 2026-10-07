"""账户信用退付行为测试（应退抵减未付凭证后剩余的信用按单实际退付）。

覆盖：
- 退付只动信用：退付后台账与收款查询的当前信用同步扣除已付退款，
  逐凭证尚欠/已收款与旧收款回执不变；
- 只能支付当前信用：超过信用整笔拒绝（422 payout_exceeds_credit，
  附当前信用/尚欠）且不留任何记录；等于信用的边界退付成功；
- 无台账账户（payout_empty_account）、非正金额（invalid_input /
  payout_amount_not_positive）、单号为空或超长拦截；
- 同一单号 + 同账户 + 同金额重试返回首次冻结回执（result=deduped），
  即使后来入账新的应补/应退凭证，回执（金额、时间、当时头寸）不漂移；
- 同一单号换金额、跨账户使用 → 409 payout_no_conflict，附已登记账户/金额；
- 后续入账的应补/应退仍按原规则改变最新尚欠与信用：应退池先扣已退付
  再 FIFO 抵减（退付过的信用不会回补成新抵减），后续应退重新形成信用
  可继续退付；
- 退付记录按退付次序（payout_seq 升序）、内容为首付冻结值、账户级头寸
  为最新值；
- 并发：同一单号多线程提交只有一笔落盘（其余 deduped），不同单号并发
  不会超额退付、累计退付恰为各笔之和；
- 按同一路径重建存储模拟进程重启：退付记录、幂等重试与信用扣除仍正确；
- 旧库（无退付表）打开自动补建；既有入账/收款/出单等接口行为不变。
"""

import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.payouts import submit_payout
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
    configure_store(str(tmp_path / "payouts.db"))
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


def _ledger(account):
    return client.get(f"/accounts/{account}/ledger").json()


def _payments(account):
    return client.get(f"/accounts/{account}/payments").json()


def _payouts(account):
    return client.get(f"/accounts/{account}/payouts").json()


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


def _credit_account(account="fam", credit=0.05):
    """造一个「full 0.10 已付清 + 应退 0.05 全部形成信用」的账户。"""
    tag = uuid.uuid4().hex[:8]  # 台账按 (会话, 凭证)、收款按单号全平台唯一，需互不冲突
    _raw_entry(account, 1, f"ps1-{tag}", 1, 0.10, "full")
    assert _pay(account, f"PAY-FULL-{tag}", 0.10).status_code == 200
    _raw_entry(account, 2, f"ps2-{tag}", 1, -credit, "refund")
    led = _ledger(account)
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(credit, abs=1e-9)
    return led


# ---------------------------------------------------------------------------
# 基本退付：只动信用、台账与收款查询同步扣除、等于信用边界
# ---------------------------------------------------------------------------


def test_payout_deducts_credit_in_ledger_and_payment_queries():
    _credit_account("fam", 0.05)

    r = _payout("fam", "PO-1", 0.02)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "paid"
    assert body["account_id"] == "fam"
    assert body["payout_no"] == "PO-1"
    assert body["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert body["paid_at"].endswith("Z")
    assert body["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert body["outstanding_yuan"] == 0.0
    assert body["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert body["payout_count"] == 1

    # 台账查询与收款查询的当前信用同步扣除已付退款
    led = _ledger("fam")
    assert led["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert led["outstanding_yuan"] == 0.0
    pays = _payments("fam")
    assert pays["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert pays["outstanding_yuan"] == 0.0

    # 退付只动信用：逐凭证已收款与旧收款回执不变
    assert led["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert led["entries"][0]["paid_yuan"] == pytest.approx(0.10, abs=1e-9)
    old = pays["payments"][0]
    assert old["payment_no"].startswith("PAY-FULL-")
    assert old["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)


def test_payout_equal_to_credit_boundary_and_second_payout():
    _credit_account("fam", 0.05)

    # 恰好等于当前信用：边界允许，退付后信用归零
    r = _payout("fam", "PO-ALL", 0.05)
    assert r.status_code == 200, r.text
    assert r.json()["credit_yuan"] == 0.0
    assert _ledger("fam")["credit_yuan"] == 0.0

    # 信用已退完：任何正金额都整笔拒绝
    over = _payout("fam", "PO-MORE", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "payout_exceeds_credit"
    assert _payouts("fam")["payout_count"] == 1


def test_payout_exceeds_credit_rejected_without_any_trace():
    _credit_account("fam", 0.05)

    r = _payout("fam", "TOO-BIG", 0.06)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "payout_exceeds_credit"
    assert body["payout_no"] == "TOO-BIG"
    assert body["amount_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert body["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["outstanding_yuan"] == 0.0

    # 整笔拒绝不留记录：退付列表为空、信用不变
    assert _payouts("fam")["payout_count"] == 0
    assert _payouts("fam")["total_paid_out_yuan"] == 0.0
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)


def test_payout_does_not_change_outstanding_of_unpaid_vouchers():
    """full 0.10 未付 + 应退 0.15：抵减后尚欠 0、信用 0.05；退付 0.05 后
    应退池只剩 0.10 恰好仍全额抵减 full，尚欠仍为 0（退付不动尚欠）。"""
    _raw_entry("fam", 1, "pu1", 1, 0.10, "full")
    _raw_entry("fam", 2, "pu2", 1, -0.15, "refund")
    led = _ledger("fam")
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert led["entries"][0]["refund_applied_yuan"] == pytest.approx(0.10, abs=1e-9)

    r = _payout("fam", "PO-1", 0.05)
    assert r.status_code == 200, r.text
    assert r.json()["outstanding_yuan"] == 0.0
    assert r.json()["credit_yuan"] == 0.0

    led = _ledger("fam")
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == 0.0
    assert led["entries"][0]["refund_applied_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert led["entries"][0]["outstanding_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 后续补退仍按原规则改变最新头寸（应退池先扣已退付再抵减）
# ---------------------------------------------------------------------------


def test_later_supplement_after_payout_becomes_outstanding():
    """退付 0.05 耗尽信用后入账应补 0.03：应退池 0.15−0.05=0.10 恰好仍抵掉
    full 0.10，应补无人抵 → 最新尚欠 0.03、信用 0（退付过的信用不回补）。"""
    _raw_entry("fam", 1, "pu1", 1, 0.10, "full")
    _raw_entry("fam", 2, "pu2", 1, -0.15, "refund")
    assert _payout("fam", "PO-1", 0.05).status_code == 200

    _raw_entry("fam", 3, "pu1", 2, 0.03, "supplement")
    led = _ledger("fam")
    assert led["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert led["credit_yuan"] == 0.0
    assert led["entries"][2]["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert led["entries"][2]["refund_applied_yuan"] == 0.0
    # 收款查询口径一致
    assert _payments("fam")["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)

    # 旧退付回执冻结：当时信用 0.05 → 退后 0
    old = _payouts("fam")["payouts"][0]
    assert old["credit_yuan"] == 0.0
    assert old["outstanding_yuan"] == 0.0


def test_later_refund_rebuilds_credit_and_allows_new_payout():
    _credit_account("fam", 0.05)
    assert _payout("fam", "PO-1", 0.05).status_code == 200
    assert _ledger("fam")["credit_yuan"] == 0.0

    # 后续入账新的应退 0.04：重新形成信用 0.04，可继续退付（序号接续）
    _raw_entry("fam", 3, "ps3", 1, -0.04, "refund")
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.04, abs=1e-9)

    r = _payout("fam", "PO-2", 0.04)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["payout_count"] == 2
    assert body["total_paid_out_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert body["credit_yuan"] == 0.0
    assert _payouts("fam")["payout_count"] == 2


def test_later_vouchers_do_not_rewrite_old_payout_receipt():
    _credit_account("fam", 0.05)
    first = _payout("fam", "PO-OLD", 0.02).json()
    assert first["credit_yuan"] == pytest.approx(0.03, abs=1e-9)

    # 之后入账应补 0.03 吃掉剩余信用
    _raw_entry("fam", 3, "ps1", 2, 0.03, "supplement")
    led = _ledger("fam")
    assert led["credit_yuan"] == 0.0
    assert led["outstanding_yuan"] == pytest.approx(0.0, abs=1e-9)

    # 旧退付记录冻结不变；账户级头寸为最新值
    lst = _payouts("fam")
    old = lst["payouts"][0]
    for key in ("amount_yuan", "paid_at", "total_paid_out_yuan",
                "outstanding_yuan", "credit_yuan", "payout_count"):
        assert old[key] == first[key]
    assert lst["credit_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 幂等重试与单号冲突
# ---------------------------------------------------------------------------


def test_same_payout_no_retry_returns_first_frozen_receipt():
    _credit_account("fam", 0.05)
    first = _payout("fam", "DUP", 0.02)
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["result"] == "paid"

    # 之后又入账新凭证改变头寸（应补 0.03 吃掉剩余信用），重试仍返回首次回执
    _raw_entry("fam", 3, "ps1", 2, 0.03, "supplement")
    assert _ledger("fam")["credit_yuan"] == 0.0

    retry = _payout("fam", "DUP", 0.02)
    assert retry.status_code == 200
    rb = retry.json()
    assert rb["result"] == "deduped"
    for key in ("account_id", "payout_no", "amount_yuan", "paid_at",
                "total_paid_out_yuan", "outstanding_yuan", "credit_yuan",
                "payout_count"):
        assert rb[key] == first_body[key]
    # 不新增退付记录、不重复支付
    assert _payouts("fam")["payout_count"] == 1
    assert _payouts("fam")["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)


def test_same_no_different_amount_conflicts():
    _credit_account("fam", 0.05)
    assert _payout("fam", "XNO", 0.02).status_code == 200

    r = _payout("fam", "XNO", 0.03)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "payout_no_conflict"
    assert body["payout_no"] == "XNO"
    assert body["account_id"] == "fam"
    assert body["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 冲突不写入：仍只有一笔
    assert _payouts("fam")["payout_count"] == 1
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)


def test_same_no_cross_account_conflicts():
    _credit_account("left", 0.05)
    _credit_account("right", 0.05)
    assert _payout("left", "SHARED", 0.02).status_code == 200

    r = _payout("right", "SHARED", 0.02)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "payout_no_conflict"
    assert body["account_id"] == "left"
    assert body["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 右账户没有这笔退付、信用未被动
    assert _payouts("right")["payout_count"] == 0
    assert _ledger("right")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)


# ---------------------------------------------------------------------------
# 非法输入
# ---------------------------------------------------------------------------


def test_non_positive_amount_rejected_by_validation():
    for bad in (0.0, -1.0):
        r = client.post(
            "/accounts/fam/payouts", json={"payout_no": "Z", "amount_yuan": bad}
        )
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_input"


def test_empty_or_long_payout_no_rejected():
    r = client.post("/accounts/fam/payouts", json={"payout_no": "", "amount_yuan": 1.0})
    assert r.status_code == 422
    r = client.post(
        "/accounts/fam/payouts", json={"payout_no": "x" * 129, "amount_yuan": 1.0}
    )
    assert r.status_code == 422


def test_sub_micro_amount_rounded_to_zero_rejected_without_trace():
    """亚微级金额（1e-12，6 位舍入后为 0）不得生成 0 元退付：整笔拒绝不留记录。"""
    _credit_account("micro", 0.05)
    r = _payout("micro", "TINY", 1e-12)
    assert r.status_code == 422
    assert r.json()["error"] == "payout_amount_not_positive"
    assert _payouts("micro")["payout_count"] == 0

    # 6 位小数边界：0.02 + 0.03 恰好退完 0.05（舍入不产生尾差）
    assert _payout("micro", "T1", 0.02).status_code == 200
    assert _payout("micro", "T2", 0.03).status_code == 200
    assert _ledger("micro")["credit_yuan"] == 0.0


def test_payout_to_account_without_ledger_rejected():
    r = _payout("no-ledger", "N1", 1.0)
    assert r.status_code == 422
    assert r.json()["error"] == "payout_empty_account"
    assert _payouts("no-ledger")["payout_count"] == 0


def test_account_with_entries_but_no_credit_rejected():
    """有台账但只有尚欠、没有信用：任何正金额退付都按超额整笔拒绝。"""
    _raw_entry("fam", 1, "nc1", 1, 0.10, "full")
    r = _payout("fam", "N2", 0.01)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "payout_exceeds_credit"
    assert body["credit_yuan"] == 0.0
    assert body["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _payouts("fam")["payout_count"] == 0


# ---------------------------------------------------------------------------
# 退付记录查询
# ---------------------------------------------------------------------------


def test_payout_list_order_and_frozen_content_vs_latest_totals():
    _credit_account("fam", 0.05)
    _payout("fam", "PO-1", 0.02)
    _raw_entry("fam", 3, "ps3", 1, -0.04, "refund")  # 信用回到 0.07
    _payout("fam", "PO-2", 0.03)

    lst = _payouts("fam")
    assert lst["account_id"] == "fam"
    assert lst["payout_count"] == 2
    assert [p["payout_no"] for p in lst["payouts"]] == ["PO-1", "PO-2"]
    assert all(p["result"] == "paid" for p in lst["payouts"])
    assert lst["payouts"][0]["payout_count"] == 1
    assert lst["payouts"][1]["payout_count"] == 2
    # 每条的快照字段为当时值（累计退付递增、当时信用）
    assert lst["payouts"][0]["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert lst["payouts"][0]["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert lst["payouts"][1]["total_paid_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert lst["payouts"][1]["credit_yuan"] == pytest.approx(0.04, abs=1e-9)
    # 列表账户级字段为当前最新值
    assert lst["total_paid_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert lst["credit_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert lst["outstanding_yuan"] == 0.0

    # 空账户查询
    empty = _payouts("never-used")
    assert empty == {
        "account_id": "never-used",
        "payout_count": 0,
        "total_paid_out_yuan": 0.0,
        "outstanding_yuan": 0.0,
        "credit_yuan": 0.0,
        "payouts": [],
    }


# ---------------------------------------------------------------------------
# 并发：同单号只落一笔；不同单号不会超额退付
# ---------------------------------------------------------------------------


def test_concurrent_same_payout_no_only_one_lands():
    _credit_account("race", 0.05)
    outcomes = []

    def worker():
        try:
            res = submit_payout("race", "RACE-NO", 0.05)
            outcomes.append(res["result"])
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    paid = [o for o in outcomes if o == "paid"]
    deduped = [o for o in outcomes if o == "deduped"]
    assert len(paid) == 1
    assert len(deduped) == 11
    assert set(outcomes) <= {"paid", "deduped"}
    lst = _payouts("race")
    assert lst["payout_count"] == 1
    assert lst["total_paid_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("race")["credit_yuan"] == 0.0


def test_concurrent_distinct_payouts_never_overpay_credit():
    """多笔不同单号退付并发打到同一账户：信用 0.10、每笔 0.02，
    恰好 5 笔成功、其余超额拒绝，累计退付恰为 0.10，序号连续。"""
    _credit_account("fam", 0.10)
    outcomes = []

    def worker(idx):
        try:
            submit_payout("fam", f"C{idx}", 0.02)
            outcomes.append("paid")
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("paid") == 5
    assert outcomes.count("payout_exceeds_credit") == 3
    lst = _payouts("fam")
    assert lst["payout_count"] == 5
    assert lst["total_paid_out_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _ledger("fam")["credit_yuan"] == 0.0
    # 退付序号 1..5 连续
    assert sorted(p["payout_count"] for p in lst["payouts"]) == list(range(1, 6))


# ---------------------------------------------------------------------------
# 重启持久化、旧库补建、既有接口不变
# ---------------------------------------------------------------------------


def test_payouts_persist_across_restart_and_retry_idempotent(tmp_path):
    db_path = str(tmp_path / "persist-payout.db")
    configure_store(db_path)
    _credit_account("fam", 0.05)
    _payout("fam", "PO-1", 0.02)

    configure_store(db_path)  # 模拟进程重启
    led = _ledger("fam")
    assert led["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    lst = _payouts("fam")
    assert lst["payout_count"] == 1
    assert lst["payouts"][0]["payout_no"] == "PO-1"
    assert lst["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)

    # 重启后同单号重试仍 deduped 且回执冻结
    retry = _payout("fam", "PO-1", 0.02)
    assert retry.status_code == 200
    assert retry.json()["result"] == "deduped"
    assert retry.json()["total_paid_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert _payouts("fam")["payout_count"] == 1

    # 重启后可继续退付，序号接续
    cont = _payout("fam", "PO-2", 0.03)
    assert cont.status_code == 200
    assert cont.json()["payout_count"] == 2
    assert cont.json()["credit_yuan"] == 0.0

    # 重启后超额仍拒绝
    over = _payout("fam", "PO-3", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "payout_exceeds_credit"


def test_legacy_db_payout_table_auto_created(tmp_path):
    """旧库（只有 sessions / session_records）打开即补建退付表：可查可退付。"""
    import json
    import sqlite3

    db_path = str(tmp_path / "legacy-payout.db")
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
    sid = "legacy-payout-session"
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
    # 退付表自动补建：空账户退付查询为 0
    assert _payouts("new-acct")["payout_count"] == 0
    # 端到端：出单 → 入账 → 收款付清；再补一条应退明细形成信用（模拟后续
    # 更正出单入账，voucher_no 取 99 避开已入账的真实凭证 1）
    assert _issue(sid, 0).status_code == 200
    assert _post_vouchers("new-acct", [(sid, 1)]).status_code == 200
    assert _pay("new-acct", "PAY-1", 0.10).status_code == 200
    store = get_store()
    with store.write_transaction() as conn:
        conn.execute(
            "INSERT INTO account_ledger_entries(entry_id, account_id, entry_no,"
            " session_id, voucher_no, voucher_id, revision_no, billing_type,"
            " voucher_amount_yuan, relative_to_revision, relative_to_voucher_no,"
            " signed_amount_yuan, balance_after_yuan, total_cost_yuan, posted_at)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), "new-acct", 2, sid, 99, str(uuid.uuid4()), 0,
             "refund", 0.04, None, None, -0.04, 0.06, 0.0, "t"),
        )
    assert _ledger("new-acct")["credit_yuan"] == pytest.approx(0.04, abs=1e-9)
    po = _payout("new-acct", "LEGACY-PO", 0.04)
    assert po.status_code == 200, po.text
    assert po.json()["credit_yuan"] == 0.0
    # 存储层表结构就位
    with store._connect() as c:
        names = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "account_payouts" in names


def test_end_to_end_credit_payout_via_voucher_flow():
    """全 API 链路：满期会话出单入账 → 收款付清 → 更正出应退凭证入账形成
    信用 → 退付 → 台账/收款/退付查询口径一致。"""
    sid = _create_complete()
    assert _issue(sid, 0).status_code == 200  # full 0.10
    assert _post_vouchers("e2e", [(sid, 1)]).status_code == 200
    assert _pay("e2e", "PAY-1", 0.10).status_code == 200

    # 更正 t1：负荷 0.18 → 购电 0（总费用 0.10 → 0.09），出应退 0.01 并入账
    down = dict(RECORDS[1]); down.update(base_load_kw=0.18, grid_purchase_energy_kwh=0.0)
    assert _rev(sid, 0, [_ch(1, down)]).status_code == 200
    v2 = _issue(sid, 1).json()
    assert v2["billing"]["type"] == "refund"
    assert v2["billing"]["amount_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert _post_vouchers("e2e", [(sid, 2)]).status_code == 200
    assert _ledger("e2e")["credit_yuan"] == pytest.approx(0.01, abs=1e-9)

    # 退付全部信用
    r = _payout("e2e", "PO-E2E", 0.01)
    assert r.status_code == 200, r.text
    assert r.json()["credit_yuan"] == 0.0
    assert _ledger("e2e")["credit_yuan"] == 0.0
    assert _payments("e2e")["credit_yuan"] == 0.0
    assert _payouts("e2e")["payouts"][0]["result"] == "paid"


def test_existing_payment_and_ledger_flows_unchanged_by_payouts():
    """退付不影响既有收款核销口径：信用账户的收款仍只按尚欠核销，
    收款回执与逐凭证分配不受退付影响。"""
    # full 0.10 + 应退 0.15：尚欠 0、信用 0.05；退付 0.05 后尚欠仍 0
    _raw_entry("fam", 1, "x1", 1, 0.10, "full")
    _raw_entry("fam", 2, "x2", 1, -0.15, "refund")
    assert _payout("fam", "PO-1", 0.05).status_code == 200
    # 只有信用（已退完）没有尚欠：收款仍整笔拒绝
    r = _pay("fam", "PAY-X", 0.01)
    assert r.status_code == 422
    assert r.json()["error"] == "payment_exceeds_outstanding"

    # 新入账应补 0.04 → 尚欠 0.04（应退池已被退付耗尽，无人抵减）
    _raw_entry("fam", 3, "x1", 2, 0.04, "supplement")
    led = _ledger("fam")
    assert led["outstanding_yuan"] == pytest.approx(0.04, abs=1e-9)
    # 收款照常 FIFO 核销到 entry3
    pay = _pay("fam", "PAY-Y", 0.04)
    assert pay.status_code == 200, pay.text
    alloc = pay.json()["allocations"]
    assert [(a["entry_no"], a["allocated_yuan"]) for a in alloc] == [
        (3, pytest.approx(0.04, abs=1e-9))
    ]
    assert _ledger("fam")["outstanding_yuan"] == 0.0
    # 台账明细冻结字段仍为入账口径
    e = led["entries"][0]
    assert e["billing"]["type"] == "full"
    assert e["amount_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert e["balance_after_yuan"] == pytest.approx(0.10, abs=1e-9)
