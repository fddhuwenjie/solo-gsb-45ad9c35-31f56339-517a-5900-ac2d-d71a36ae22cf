"""账户间信用划转行为测试（来源信用扣减，目标按入账顺序抵欠费、剩余形成信用）。

覆盖：
- 成功划转：来源信用扣减（不动来源逐凭证尚欠/已收款），目标按入账顺序
  FIFO 抵扣未付清正额凭证（可部分付清），剩余转入额叠加形成目标信用；
- 转入额不算现金实收：目标 paid_yuan / total_received_yuan 不变，
  只增加 refund_applied_yuan；台账、收款、退付查询按划转后头寸展示；
- 边界与拒绝：划转额等于来源信用允许、超过 → 422 transfer_exceeds_credit
  （附来源当前信用/尚欠）且双方均不留流水；两户相同 → transfer_same_account；
  来源或目标无台账 → transfer_account_no_ledger（附 side）；非正 / 亚微级
  金额、单号或账户为空超长拦截；
- 幂等：同单号同双方同金额重试返回首次冻结回执（result=deduped，即使
  后来两户头寸又变化也不漂移）；同单号换账户或换金额 → 409 transfer_no_conflict；
- 划转记录按账户视角划转先后（全局落盘先后，划出/划入合并）查询，附
  role / seq / other_account_id；冻结回执另保留两侧独立侧序号，
  内容为首次冻结值、账户级头寸为最新值；
- 后续收款按划转后尚欠 FIFO（转入抵掉的部分不再收现金、超额仍拒绝），
  剩余转入形成的信用可退付，后续应补/应退仍按统一信用池口径重算，
  被冲正收款排除后转入池会重新抵扣；已记凭证、旧收款分配、退付与各首次
  回执不改写；
- 并发：同单号多线程恰好一笔 transferred 其余 deduped；不同单号划转与
  退付并发不会重复支用同一笔信用，累计划出 + 退付恰为信用之和；
- 按同一路径重建存储模拟进程重启：划转记录、幂等重试与两侧序号仍正确；
- 旧库（无划转表）打开自动补建；既有入账/收款/退付等接口行为不变。
"""

import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.payouts import submit_payout
from app.transfers import submit_transfer
from app.storage import configure_store, get_store

client = TestClient(app)


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    configure_store(str(tmp_path / "transfers.db"))
    yield
    configure_store(None)


def _raw_entry(account, entry_no, sid, vno, signed, billing):
    """直接落一条台账明细（绕过费用非负的业务约束，精确控制头寸口径）。"""
    store = get_store()
    with store.write_transaction() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO sessions(session_id, dispatch_json,"
            " plan_snapshot_json, audit_tolerance_kwh, num_periods, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (sid, "{}", "{}", 0.01, 4, "t"),
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


def _pay(account, payment_no, amount):
    return client.post(
        f"/accounts/{account}/payments",
        json={"payment_no": payment_no, "amount_yuan": amount},
    )


def _credit_account(account="fam", credit=0.05, paid_voucher=0.10):
    """造一个「full paid_voucher 已付清 + 应退 credit 全部形成信用」的账户。"""
    tag = uuid.uuid4().hex[:8]
    _raw_entry(account, 1, f"ps1-{tag}", 1, paid_voucher, "full")
    assert _pay(account, f"PAY-FULL-{tag}", paid_voucher).status_code == 200
    _raw_entry(account, 2, f"ps2-{tag}", 1, -credit, "refund")
    led = client.get(f"/accounts/{account}/ledger").json()
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(credit, abs=1e-9)
    return led


def _debtor_account(account, *amounts):
    """造一个只有若干未付清正额凭证（按入账顺序）的账户，返回其尚欠。"""
    tag = uuid.uuid4().hex[:8]
    for i, amount in enumerate(amounts, start=1):
        _raw_entry(account, i, f"db-{tag}-{i}", 1, amount, "full")
    led = client.get(f"/accounts/{account}/ledger").json()
    assert led["outstanding_yuan"] == pytest.approx(sum(amounts), abs=1e-9)
    assert led["credit_yuan"] == 0.0
    return led


def _transfer(fr, to, transfer_no, amount):
    return client.post(
        "/account-transfers",
        json={
            "transfer_no": transfer_no,
            "from_account_id": fr,
            "to_account_id": to,
            "amount_yuan": amount,
        },
    )


def _transfers(account):
    return client.get(f"/accounts/{account}/transfers").json()


def _ledger(account):
    return client.get(f"/accounts/{account}/ledger").json()


def _payments(account):
    return client.get(f"/accounts/{account}/payments").json()


def _payouts(account):
    return client.get(f"/accounts/{account}/payouts").json()


# ---------------------------------------------------------------------------
# 基本划转：来源扣信用、目标 FIFO 抵扣、剩余形成信用、转入不算现金
# ---------------------------------------------------------------------------


def test_transfer_basic_source_debit_target_application():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)

    r = _transfer("src", "dst", "TR-1", 0.03)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "transferred"
    assert body["transfer_no"] == "TR-1"
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["amount_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert body["transferred_at"].endswith("Z")
    # 来源：只扣信用、不动尚欠
    assert body["from_outstanding_yuan"] == 0.0
    assert body["from_credit_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert body["from_transfer_count"] == 1
    assert body["total_transferred_out_yuan"] == pytest.approx(0.03, abs=1e-9)
    # 目标：转入抵掉 0.03 尚欠、无剩余
    assert body["to_outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert body["to_credit_yuan"] == 0.0
    assert body["to_transfer_count"] == 1
    assert body["total_transferred_in_yuan"] == pytest.approx(0.03, abs=1e-9)
    # 逐凭证抵扣明细
    assert len(body["applications"]) == 1
    app = body["applications"][0]
    assert app["application_no"] == 1
    assert app["entry_no"] == 1
    assert app["applied_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert app["outstanding_before_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert app["outstanding_after_yuan"] == pytest.approx(0.07, abs=1e-9)

    # 台账 / 收款 / 退付查询均按划转后头寸
    src_led = _ledger("src")
    assert src_led["credit_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert src_led["outstanding_yuan"] == 0.0
    dst_led = _ledger("dst")
    assert dst_led["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert dst_led["credit_yuan"] == 0.0
    e = dst_led["entries"][0]
    # 转入不是现金：paid 仍为 0，应退/转入抵减额增加
    assert e["paid_yuan"] == 0.0
    assert e["refund_applied_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert e["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert dst_led["total_received_yuan"] == 0.0
    assert _payments("dst")["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert _payouts("dst")["outstanding_yuan"] == pytest.approx(0.07, abs=1e-9)


def test_transfer_fifo_across_vouchers_and_leftover_becomes_credit():
    _credit_account("src", 0.10)
    _debtor_account("dst", 0.02, 0.03)

    r = _transfer("src", "dst", "TR-FIFO", 0.06)
    assert r.status_code == 200, r.text
    body = r.json()
    assert [(a["entry_no"], a["applied_yuan"], a["outstanding_after_yuan"])
            for a in body["applications"]] == [
        (1, pytest.approx(0.02, abs=1e-9), 0.0),
        (2, pytest.approx(0.03, abs=1e-9), 0.0),
    ]
    assert body["to_outstanding_yuan"] == 0.0
    # 剩余 0.01 转入额形成目标信用
    assert body["to_credit_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert body["from_credit_yuan"] == pytest.approx(0.04, abs=1e-9)

    dst_led = _ledger("dst")
    assert dst_led["credit_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert dst_led["outstanding_yuan"] == 0.0


def test_transfer_into_account_with_only_credit_all_becomes_credit():
    _credit_account("src", 0.04)
    _credit_account("dst", 0.02)  # 目标没有尚欠、已有 0.02 信用（含 0.10 现金）

    r = _transfer("src", "dst", "TR-C", 0.04)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applications"] == []
    assert body["to_outstanding_yuan"] == 0.0
    # 剩余转入额叠加在目标既有信用之上
    assert body["to_credit_yuan"] == pytest.approx(0.06, abs=1e-9)
    # 转入不算现金：累计实收仍是原有 0.10
    assert _ledger("dst")["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _payments("dst")["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _ledger("dst")["credit_yuan"] == pytest.approx(0.06, abs=1e-9)


def test_transfer_equal_to_source_credit_boundary():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)

    r = _transfer("src", "dst", "TR-ALL", 0.05)
    assert r.status_code == 200, r.text
    assert r.json()["from_credit_yuan"] == 0.0
    assert _ledger("src")["credit_yuan"] == 0.0
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)

    # 来源信用已划完：再划任何正金额整笔拒绝
    over = _transfer("src", "dst", "TR-MORE", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "transfer_exceeds_credit"


def test_source_with_outstanding_and_credit_can_still_transfer():
    """full 0.20 未付 + 应退 0.15：尚欠 0.05、信用 0；无信用不能划。
    full 0.10 未付 + 应退 0.15：尚欠 0、信用 0.05；划 0.05 后尚欠仍 0。"""
    tag = uuid.uuid4().hex[:8]
    _raw_entry("poor", 1, f"p1-{tag}", 1, 0.20, "full")
    _raw_entry("poor", 2, f"p2-{tag}", 1, -0.15, "refund")
    led = _ledger("poor")
    assert led["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert led["credit_yuan"] == 0.0
    _debtor_account("dst", 0.10)
    r = _transfer("poor", "dst", "TR-X", 0.01)
    assert r.status_code == 422
    assert r.json()["error"] == "transfer_exceeds_credit"
    assert r.json()["from_credit_yuan"] == 0.0
    assert r.json()["from_outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _transfers("poor")["transfer_count"] == 0
    assert _transfers("dst")["transfer_count"] == 0

    tag2 = uuid.uuid4().hex[:8]
    _raw_entry("rich", 1, f"r1-{tag2}", 1, 0.10, "full")
    _raw_entry("rich", 2, f"r2-{tag2}", 1, -0.15, "refund")
    assert _ledger("rich")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    ok = _transfer("rich", "dst", "TR-Y", 0.05)
    assert ok.status_code == 200, ok.text
    assert ok.json()["from_outstanding_yuan"] == 0.0  # 划出不动来源尚欠
    assert ok.json()["from_credit_yuan"] == 0.0
    # 来源应退池扣除划出后仍全额覆盖既有抵减
    assert _ledger("rich")["entries"][0]["refund_applied_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _ledger("rich")["outstanding_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 整笔拒绝：超额、同户、无台账、非法金额/单号/账户，双方均不留流水
# ---------------------------------------------------------------------------


def test_transfer_exceeds_credit_rejected_without_any_trace():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)

    r = _transfer("src", "dst", "TOO-BIG", 0.06)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "transfer_exceeds_credit"
    assert body["transfer_no"] == "TOO-BIG"
    assert body["amount_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["from_credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["from_outstanding_yuan"] == 0.0

    # 双方均不留流水、头寸不变
    assert _transfers("src")["transfer_count"] == 0
    assert _transfers("dst")["transfer_count"] == 0
    assert _transfers("src")["total_transferred_out_yuan"] == 0.0
    assert _transfers("dst")["total_transferred_in_yuan"] == 0.0
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)


def test_transfer_same_account_rejected():
    _credit_account("fam", 0.05)
    r = _transfer("fam", "fam", "SELF", 0.01)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "transfer_same_account"
    assert body["from_account_id"] == "fam"
    assert body["to_account_id"] == "fam"
    assert _transfers("fam")["transfer_count"] == 0
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)


def test_transfer_requires_ledger_on_both_accounts():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)

    # 来源无台账
    r = _transfer("ghost-src", "dst", "T1", 0.01)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "transfer_account_no_ledger"
    assert body["account_id"] == "ghost-src"
    assert body["side"] == "from"
    assert _transfers("dst")["transfer_count"] == 0

    # 目标无台账
    r = _transfer("src", "ghost-dst", "T2", 0.01)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "transfer_account_no_ledger"
    assert body["account_id"] == "ghost-dst"
    assert body["side"] == "to"
    assert _transfers("src")["transfer_count"] == 0
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)

    # 双方都无台账（且不同）：先报来源侧
    r = _transfer("nobody-a", "nobody-b", "T3", 0.01)
    assert r.status_code == 422
    assert r.json()["side"] == "from"


def test_non_positive_amount_rejected_by_validation():
    for bad in (0.0, -1.0):
        r = client.post(
            "/account-transfers",
            json={"transfer_no": "Z", "from_account_id": "a",
                  "to_account_id": "b", "amount_yuan": bad},
        )
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_input"


def test_empty_or_long_no_and_account_ids_rejected():
    def post(no, fr, to):
        return client.post(
            "/account-transfers",
            json={"transfer_no": no, "from_account_id": fr,
                  "to_account_id": to, "amount_yuan": 1.0},
        )

    assert post("", "a", "b").status_code == 422
    assert post("x" * 129, "a", "b").status_code == 422
    assert post("Z", "", "b").status_code == 422
    assert post("Z", "a", "").status_code == 422
    assert post("Z", "x" * 129, "b").status_code == 422
    assert post("Z", "a", "x" * 129).status_code == 422


def test_sub_micro_amount_rounded_to_zero_rejected_without_trace():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)
    r = _transfer("src", "dst", "TINY", 1e-12)
    assert r.status_code == 422
    assert r.json()["error"] == "transfer_amount_not_positive"
    assert _transfers("src")["transfer_count"] == 0
    assert _transfers("dst")["transfer_count"] == 0

    # 6 位小数边界：0.02 + 0.03 恰好划完 0.05（舍入不产生尾差）
    assert _transfer("src", "dst", "T1", 0.02).status_code == 200
    assert _transfer("src", "dst", "T2", 0.03).status_code == 200
    assert _ledger("src")["credit_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 幂等重试与单号冲突
# ---------------------------------------------------------------------------


def test_same_transfer_no_retry_returns_first_frozen_receipt():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)
    first = _transfer("src", "dst", "DUP", 0.02)
    assert first.status_code == 200
    first_body = first.json()
    assert first_body["result"] == "transferred"

    # 之后两户头寸都变化：src 再入账应退 0.03（信用回到 0.06），
    # dst 再入账新正额 0.02（尚欠 0.10）——重试仍逐字返回首次回执。
    _raw_entry("src", 3, f"late-{uuid.uuid4().hex[:8]}", 1, -0.03, "refund")
    _raw_entry("dst", 2, f"late2-{uuid.uuid4().hex[:8]}", 1, 0.02, "supplement")

    retry = _transfer("src", "dst", "DUP", 0.02)
    assert retry.status_code == 200
    rb = retry.json()
    assert rb["result"] == "deduped"
    for key in ("transfer_no", "from_account_id", "to_account_id", "amount_yuan",
                "transferred_at", "applications",
                "from_outstanding_yuan", "from_credit_yuan", "from_transfer_count",
                "total_transferred_out_yuan",
                "to_outstanding_yuan", "to_credit_yuan", "to_transfer_count",
                "total_transferred_in_yuan"):
        assert rb[key] == first_body[key]
    # 不新增流水、不重复支用
    assert _transfers("src")["transfer_count"] == 1
    assert _transfers("dst")["transfer_count"] == 1
    assert _transfers("src")["total_transferred_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 重试不改最新头寸：src 当前信用 0.06、dst 当前尚欠 0.10
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)


def test_same_no_different_amount_conflicts():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)
    assert _transfer("src", "dst", "XNO", 0.02).status_code == 200

    r = _transfer("src", "dst", "XNO", 0.03)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "transfer_no_conflict"
    assert body["transfer_no"] == "XNO"
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert _transfers("src")["transfer_count"] == 1
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)


def test_same_no_different_parties_conflict():
    _credit_account("a", 0.05)
    _credit_account("b", 0.05)
    _debtor_account("c", 0.10)
    _debtor_account("d", 0.10)
    assert _transfer("a", "c", "SHARED", 0.02).status_code == 200

    # 换来源账户
    r = _transfer("b", "c", "SHARED", 0.02)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "transfer_no_conflict"
    assert body["from_account_id"] == "a"
    assert body["to_account_id"] == "c"
    assert _transfers("b")["transfer_count"] == 0

    # 换目标账户
    r = _transfer("a", "d", "SHARED", 0.02)
    assert r.status_code == 409
    assert body["from_account_id"] == "a"
    assert _transfers("d")["transfer_count"] == 0
    # 未卷入账户头寸不变
    assert _ledger("b")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("d")["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)


# ---------------------------------------------------------------------------
# 划转记录查询：两侧序号独立、role/other_account_id、冻结 vs 最新
# ---------------------------------------------------------------------------


def test_transfer_list_both_sides_seq_and_frozen_snapshots():
    _credit_account("src", 0.10)
    _credit_account("mid", 0.03)
    _debtor_account("dst", 0.05)

    assert _transfer("src", "dst", "T-1", 0.05).status_code == 200   # dst 尚欠 0
    assert _transfer("src", "dst", "T-2", 0.02).status_code == 200   # dst 信用 0.02
    assert _transfer("mid", "src", "T-3", 0.03).status_code == 200   # src 划入成信用

    # 来源侧列表：按全平台落盘先后——T-1/T-2 划出、T-3 划入（seq 为该账户视角序位）
    src_lst = _transfers("src")
    assert src_lst["account_id"] == "src"
    assert src_lst["transfer_count"] == 3
    assert src_lst["total_transferred_out_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert src_lst["total_transferred_in_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert [(t["transfer_no"], t["role"], t["seq"], t["other_account_id"])
            for t in src_lst["transfers"]] == [
        ("T-1", "from", 1, "dst"),
        ("T-2", "from", 2, "dst"),
        ("T-3", "to", 3, "mid"),
    ]
    assert all(t["result"] == "transferred" for t in src_lst["transfers"])
    # src 当前头寸：信用 0.10 − 0.07 + 0.03（无尚欠）= 0.06
    assert src_lst["credit_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert src_lst["outstanding_yuan"] == 0.0
    # 冻结快照：T-1 划出后信用 0.05；T-2 划出后信用 0.03（不随后续漂移）
    t1, t2 = src_lst["transfers"][0], src_lst["transfers"][1]
    assert t1["from_credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert t1["from_transfer_count"] == 1
    assert t2["from_credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert t2["from_transfer_count"] == 2
    t3 = src_lst["transfers"][2]
    assert t3["role"] == "to"
    assert t3["to_credit_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert t3["to_transfer_count"] == 1

    # 目标侧列表：两笔划入，侧序号独立从 1 起
    dst_lst = _transfers("dst")
    assert dst_lst["transfer_count"] == 2
    assert dst_lst["total_transferred_in_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert [(t["transfer_no"], t["role"], t["seq"], t["other_account_id"])
            for t in dst_lst["transfers"]] == [
        ("T-1", "to", 1, "src"),
        ("T-2", "to", 2, "src"),
    ]
    assert dst_lst["credit_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert dst_lst["transfers"][0]["applications"][0]["applied_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert dst_lst["transfers"][1]["applications"] == []

    # 空账户查询
    assert _transfers("never-used") == {
        "account_id": "never-used",
        "transfer_count": 0,
        "total_transferred_out_yuan": 0.0,
        "total_transferred_in_yuan": 0.0,
        "outstanding_yuan": 0.0,
        "credit_yuan": 0.0,
        "transfers": [],
    }


# ---------------------------------------------------------------------------
# 划转后的后续收款 / 退付 / 冲正 / 补退都按新头寸处理
# ---------------------------------------------------------------------------


def test_later_payments_follow_transferred_position():
    _credit_account("src", 0.10)
    _debtor_account("dst", 0.10)
    assert _transfer("src", "dst", "TR-1", 0.04).status_code == 200

    # 转入已抵 0.04：收款只收剩余 0.06
    pay = _pay("dst", "PAY-1", 0.06)
    assert pay.status_code == 200, pay.text
    alloc = pay.json()["allocations"]
    assert [
        (
            a["entry_no"],
            a["allocated_yuan"],
            a["outstanding_before_yuan"],
            a["outstanding_after_yuan"],
        )
        for a in alloc
    ] == [
        (1, pytest.approx(0.06, abs=1e-9),
         pytest.approx(0.06, abs=1e-9), 0.0),
    ]
    assert pay.json()["total_received_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert _ledger("dst")["outstanding_yuan"] == 0.0
    # 已无尚欠：再多收 0.01 整笔拒绝（信用来自转入、不是现金应收）
    over = _pay("dst", "PAY-2", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "payment_exceeds_outstanding"


def test_leftover_transferred_credit_is_payable_by_payout():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.02)  # 转入 0.05：抵 0.02 后剩 0.03 形成信用
    assert _transfer("src", "dst", "TR-1", 0.05).status_code == 200
    assert _ledger("dst")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)

    # 剩余转入形成的信用可实际退付
    po = client.post(
        "/accounts/dst/payouts",
        json={"payout_no": "PO-1", "amount_yuan": 0.03},
    )
    assert po.status_code == 200, po.text
    assert po.json()["credit_yuan"] == 0.0
    assert _ledger("dst")["credit_yuan"] == 0.0
    # 退完后再退任何正金额拒绝
    assert client.post(
        "/accounts/dst/payouts", json={"payout_no": "PO-2", "amount_yuan": 0.01}
    ).status_code == 422
    # 已抵扣的 0.02 不回补为信用
    assert _ledger("dst")["outstanding_yuan"] == 0.0


def test_source_credit_after_transfer_limits_later_payout():
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)
    assert _transfer("src", "dst", "TR-1", 0.02).status_code == 200

    po = client.post(
        "/accounts/src/payouts",
        json={"payout_no": "PO-1", "amount_yuan": 0.03},
    )
    assert po.status_code == 200, po.text
    assert po.json()["credit_yuan"] == 0.0
    over = client.post(
        "/accounts/src/payouts",
        json={"payout_no": "PO-2", "amount_yuan": 0.01},
    )
    assert over.status_code == 422
    assert over.json()["error"] == "payout_exceeds_credit"


def test_later_supplement_after_transfer_becomes_outstanding():
    """转入 0.10 恰好抵完 full 0.10（无剩余），之后应补 0.03 无人抵 → 尚欠。"""
    _credit_account("src", 0.10)
    _debtor_account("dst", 0.10)
    assert _transfer("src", "dst", "TR-1", 0.10).status_code == 200
    assert _ledger("dst")["credit_yuan"] == 0.0

    _raw_entry("dst", 2, f"sup-{uuid.uuid4().hex[:8]}", 1, 0.03, "supplement")
    led = _ledger("dst")
    assert led["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert led["credit_yuan"] == 0.0
    # 旧划转回执冻结：to_outstanding_yuan 仍为首次划转当时的 0
    old = _transfers("dst")["transfers"][0]
    assert old["to_outstanding_yuan"] == 0.0
    assert old["to_credit_yuan"] == 0.0


def test_later_refund_rebuilds_credit_alongside_transferred_credit():
    _credit_account("src", 0.02)
    _debtor_account("dst", 0.05)
    # 转入 0.02 抵掉部分尚欠
    assert _transfer("src", "dst", "TR-1", 0.02).status_code == 200
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)
    # dst 之后入账应退 0.05：池子（含转入）重新 FIFO 抵减 → 尚欠 0、信用 0.02
    _raw_entry("dst", 2, f"rf-{uuid.uuid4().hex[:8]}", 1, -0.05, "refund")
    led = _ledger("dst")
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(0.02, abs=1e-9)


def test_payment_reversal_reopens_outstanding_covered_by_transfer():
    """目标 full 0.10：先收现金 0.04，再转入 0.10（抵剩余 0.06，剩 0.04 信用）；
    冲正该收款后现金清零，转入池重新全额抵扣 → 尚欠仍为 0、信用回落为 0。"""
    tag = uuid.uuid4().hex[:8]
    _raw_entry("dst", 1, f"rv-{tag}", 1, 0.10, "full")
    assert _pay("dst", f"PAY-RV-{tag}", 0.04).status_code == 200
    _credit_account("src", 0.10)
    assert _transfer("src", "dst", "TR-RV", 0.10).status_code == 200
    led = _ledger("dst")
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(0.04, abs=1e-9)

    rev = client.post(
        "/accounts/dst/payment-reversals",
        json={"payment_no": f"PAY-RV-{tag}"},
    )
    assert rev.status_code == 200, rev.text
    rb = rev.json()
    # 转入池随冲正重算重新抵扣：现金清零后转入 0.10 全额覆盖，信用回落
    assert rb["outstanding_yuan"] == 0.0
    assert rb["credit_yuan"] == 0.0
    assert rb["total_received_yuan"] == 0.0
    assert _ledger("dst")["outstanding_yuan"] == 0.0
    assert _ledger("dst")["credit_yuan"] == 0.0
    # 划转记录与首次回执保持原样
    tr = _transfers("dst")["transfers"][0]
    assert tr["to_credit_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert tr["applications"][0]["applied_yuan"] == pytest.approx(0.06, abs=1e-9)


def test_transfer_does_not_rewrite_existing_receipts():
    """划转前后：已记凭证、旧收款分配、退付与首次回执均不改写。"""
    _credit_account("src", 0.05)
    tag = uuid.uuid4().hex[:8]
    _raw_entry("dst", 1, f"keep-{tag}", 1, 0.10, "full")
    pay = _pay("dst", f"PAY-KEEP-{tag}", 0.03)
    assert pay.status_code == 200
    pay_snapshot = pay.json()

    assert _transfer("src", "dst", "TR-K", 0.04).status_code == 200

    # 旧收款回执逐字不变（分配 0.03 到 entry1，before 0.10 / after 0.07）
    old = _payments("dst")["payments"][0]
    for key in ("payment_no", "amount_yuan", "received_at", "total_received_yuan",
                "outstanding_yuan", "credit_yuan", "payment_count"):
        assert old[key] == pay_snapshot[key]
    assert old["allocations"] == pay_snapshot["allocations"]
    # 该凭证现金已收仍为 0.03（转入抵减单列在 refund_applied）
    e = _ledger("dst")["entries"][0]
    assert e["paid_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert e["refund_applied_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert e["outstanding_yuan"] == pytest.approx(0.03, abs=1e-9)


# ---------------------------------------------------------------------------
# 多户串联：A → B（部分抵欠费、部分成信用）→ C
# ---------------------------------------------------------------------------


def test_three_account_chain_transfer_then_transfer():
    _credit_account("a", 0.10)
    _debtor_account("b", 0.06)
    _debtor_account("c", 0.10)

    r1 = _transfer("a", "b", "CHAIN-1", 0.10)
    assert r1.status_code == 200, r1.text
    # B：转入抵掉 0.06 尚欠，剩 0.04 形成信用
    assert r1.json()["to_outstanding_yuan"] == 0.0
    assert r1.json()["to_credit_yuan"] == pytest.approx(0.04, abs=1e-9)

    # B 用划入形成的信用继续划给 C
    r2 = _transfer("b", "c", "CHAIN-2", 0.04)
    assert r2.status_code == 200, r2.text
    assert r2.json()["from_credit_yuan"] == 0.0
    assert r2.json()["to_outstanding_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert r2.json()["to_credit_yuan"] == 0.0

    # B 的列表按全平台落盘先后：先划入 CHAIN-1（seq 1）、再划出 CHAIN-2（seq 2）；
    # 两侧侧序号（from/to_transfer_count）各自独立
    b_lst = _transfers("b")
    assert [(t["transfer_no"], t["role"], t["seq"]) for t in b_lst["transfers"]] == [
        ("CHAIN-1", "to", 1),
        ("CHAIN-2", "from", 2),
    ]
    assert b_lst["transfers"][0]["to_transfer_count"] == 1
    assert b_lst["transfers"][1]["from_transfer_count"] == 1
    assert b_lst["total_transferred_in_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert b_lst["total_transferred_out_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert b_lst["credit_yuan"] == 0.0
    # C 只看到一笔划入
    assert _transfers("c")["transfer_count"] == 1
    assert _ledger("c")["outstanding_yuan"] == pytest.approx(0.06, abs=1e-9)


# ---------------------------------------------------------------------------
# 并发：同单号只落一笔；不同单号划转 / 退付不重复支用信用
# ---------------------------------------------------------------------------


def test_concurrent_same_transfer_no_only_one_lands():
    _credit_account("race", 0.05)
    _debtor_account("sink", 0.10)
    outcomes = []

    def worker():
        try:
            res = submit_transfer("race", "sink", "RACE-NO", 0.05)
            outcomes.append(res["result"])
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("transferred") == 1
    assert outcomes.count("deduped") == 11
    assert set(outcomes) <= {"transferred", "deduped"}
    assert _transfers("race")["transfer_count"] == 1
    assert _transfers("race")["total_transferred_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _transfers("sink")["transfer_count"] == 1
    assert _ledger("sink")["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("race")["credit_yuan"] == 0.0


def test_concurrent_distinct_transfers_never_overspend_credit():
    """多笔不同单号划转并发划出：信用 0.10、每笔 0.03，恰好 3 笔成功、
    其余超额拒绝，累计划出恰为 0.09，划出序号 1..3 连续。"""
    _credit_account("fam", 0.10)
    _debtor_account("sink", 1.0)
    outcomes = []

    def worker(idx):
        try:
            submit_transfer("fam", "sink", f"C{idx}", 0.03)
            outcomes.append("transferred")
        except Exception as exc:  # noqa: BLE001
            outcomes.append(getattr(exc, "code", "error"))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert outcomes.count("transferred") == 3
    assert outcomes.count("transfer_exceeds_credit") == 5
    lst = _transfers("fam")
    assert lst["transfer_count"] == 3
    assert lst["total_transferred_out_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert _ledger("fam")["credit_yuan"] == pytest.approx(0.01, abs=1e-9)
    # 账户视角划转序位 1..3 连续（与划出侧序号一致，本账户只划出过）
    assert sorted(t["seq"] for t in lst["transfers"]) == [1, 2, 3]
    # 冻结回执中的划出侧序号 1..3 连续
    assert sorted(t["from_transfer_count"] for t in lst["transfers"]) == [1, 2, 3]
    # 目标侧序号同样连续，转入累计 0.09
    sink = _transfers("sink")
    assert sink["total_transferred_in_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert sorted(t["seq"] for t in sink["transfers"]) == [1, 2, 3]
    assert sorted(t["to_transfer_count"] for t in sink["transfers"]) == [1, 2, 3]


def test_concurrent_transfer_and_payout_never_double_spend_credit():
    """划转与退付并发支用同一账户的同一笔信用：信用 0.10、每笔 0.02，
    恰好 5 笔成功（划出 + 退付合计），累计支用恰为 0.10。"""
    _credit_account("fam", 0.10)
    _debtor_account("sink", 1.0)
    outcomes = []

    def transfer_worker(idx):
        try:
            submit_transfer("fam", "sink", f"T{idx}", 0.02)
            outcomes.append(("transfer", "ok"))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("transfer", getattr(exc, "code", "error")))

    def payout_worker(idx):
        try:
            submit_payout("fam", f"P{idx}", 0.02)
            outcomes.append(("payout", "ok"))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("payout", getattr(exc, "code", "error")))

    threads = []
    for i in range(4):
        threads.append(threading.Thread(target=transfer_worker, args=(i,)))
        threads.append(threading.Thread(target=payout_worker, args=(i,)))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [o for o in outcomes if o[1] == "ok"]
    rejected = [o for o in outcomes if o[1] != "ok"]
    assert len(ok) == 5
    assert {code for _, code in rejected} <= {"transfer_exceeds_credit", "payout_exceeds_credit"}
    assert len(rejected) == 3
    spent = _round(
        _transfers("fam")["total_transferred_out_yuan"]
        + _payouts("fam")["total_paid_out_yuan"]
    )
    assert spent == pytest.approx(0.10, abs=1e-9)
    assert _ledger("fam")["credit_yuan"] == 0.0


def _round(x):
    return round(x + 0.0, 6) + 0.0


# ---------------------------------------------------------------------------
# 重启持久化与旧库补建
# ---------------------------------------------------------------------------


def test_transfers_persist_across_restart_and_retry_idempotent(tmp_path):
    db_path = str(tmp_path / "persist-transfer.db")
    configure_store(db_path)
    _credit_account("src", 0.05)
    _debtor_account("dst", 0.10)
    assert _transfer("src", "dst", "TR-1", 0.02).status_code == 200

    configure_store(db_path)  # 模拟进程重启
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.08, abs=1e-9)
    src_lst = _transfers("src")
    dst_lst = _transfers("dst")
    assert src_lst["transfer_count"] == 1
    assert dst_lst["transfer_count"] == 1
    assert src_lst["transfers"][0]["transfer_no"] == "TR-1"
    assert dst_lst["transfers"][0]["role"] == "to"

    # 重启后同单号重试仍 deduped 且回执冻结
    retry = _transfer("src", "dst", "TR-1", 0.02)
    assert retry.status_code == 200
    assert retry.json()["result"] == "deduped"
    assert retry.json()["from_credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert _transfers("src")["transfer_count"] == 1

    # 重启后可继续划转，两侧序号接续
    cont = _transfer("src", "dst", "TR-2", 0.03)
    assert cont.status_code == 200, cont.text
    assert cont.json()["from_transfer_count"] == 2
    assert cont.json()["to_transfer_count"] == 2
    assert cont.json()["from_credit_yuan"] == 0.0

    # 重启后超额仍拒绝
    over = _transfer("src", "dst", "TR-3", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "transfer_exceeds_credit"


def test_legacy_db_transfer_table_auto_created(tmp_path):
    """旧库（只有 sessions / session_records）打开即补建划转表：可查可划转。"""
    import sqlite3

    db_path = str(tmp_path / "legacy-transfer.db")
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
    conn.commit()
    conn.close()

    configure_store(db_path)
    assert _transfers("new-acct")["transfer_count"] == 0
    # 直接造头寸（旧库补建后各台账表也由 IF NOT EXISTS 就位）
    _credit_account("new-src", 0.04)
    _debtor_account("new-dst", 0.10)
    r = _transfer("new-src", "new-dst", "LEGACY-TR", 0.04)
    assert r.status_code == 200, r.text
    assert r.json()["from_credit_yuan"] == 0.0
    assert r.json()["to_outstanding_yuan"] == pytest.approx(0.06, abs=1e-9)

    store = get_store()
    with store._connect() as c:
        names = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "account_transfers" in names


def test_existing_flows_unchanged_when_no_transfers():
    """无划转时账户头寸口径与此前完全一致：信用、收款、退付照常。"""
    _credit_account("fam", 0.05)
    led = _ledger("fam")
    assert led["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    po = client.post(
        "/accounts/fam/payouts", json={"payout_no": "PO-1", "amount_yuan": 0.05}
    )
    assert po.status_code == 200
    assert po.json()["credit_yuan"] == 0.0
    assert _payouts("fam")["total_paid_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _transfers("fam") == {
        "account_id": "fam",
        "transfer_count": 0,
        "total_transferred_out_yuan": 0.0,
        "total_transferred_in_yuan": 0.0,
        "outstanding_yuan": 0.0,
        "credit_yuan": 0.0,
        "transfers": [],
    }
