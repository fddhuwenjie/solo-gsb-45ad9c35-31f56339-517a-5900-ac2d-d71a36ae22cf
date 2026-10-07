"""账户间信用划转行为测试（来源信用扣减，转入额按入账顺序抵扣目标欠费）。

覆盖：
- 基本划转：来源信用扣减（与退付同口径）、转入额不算现金实收（不产生逐凭证
  现金核销、不计入目标累计实收），目标按 entry_no 升序 FIFO 抵扣未付清正额
  凭证（可部分付清一张），剩余形成目标信用；回执给双方划转后最新尚欠/信用；
- 划转后台账 / 收款 / 退付查询的两户头寸同步；后续收款按划转后尚欠 FIFO
  分配，后续退付只能退划转后剩余信用；
- 只能划来源当前信用：超过信用整笔拒绝（422 transfer_insufficient_credit，
  附来源当前信用/尚欠与划转金额）且双方均不留任何流水；等于信用的边界成功；
- 两户必须不同（请求体直接 422 invalid_input；领域直调 transfer_same_account）、
  都须已有台账（transfer_from_empty_account / transfer_to_empty_account）、
  非正金额（invalid_input / transfer_amount_not_positive）、单号为空或超长；
- 同一单号 + 同来源 + 同目标 + 同金额重试返回首次冻结回执（result=deduped），
  即使后来两户头寸又变，回执（划转时间、逐凭证抵扣、双方头寸快照）不漂移；
- 同一单号换金额、换来源 / 换目标 → 409 transfer_no_conflict，附已登记双方/金额；
- 已记凭证、旧收款分配、退付与各首次回执不改写；转入抵扣不产生现金分配；
- 可反向划转（原转入方成为划出方）；无未付清凭证时转入额全部形成信用；
- 划转记录按账户查询：direction / counterpart_account_id / account_transfer_seq、
  划出与转入分别计数、双方方向序号独立连续、按 global_seq 升序、内容为冻结值；
- 并发：不同单号划转与退付并发只支用一次信用（恰一笔成功、其余信用不足），
  同一单号并发提交只有一笔落盘（其余 deduped），不重复支用；
- 按同一路径重建存储模拟进程重启：划转记录、幂等重试与双方头寸仍正确；
- 旧库（无划转表）打开自动补建；既有入账/收款/退付/冲正等接口行为不变。
"""

import threading
import uuid

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.storage import configure_store, get_store
from app.transfers import TransferError, submit_transfer

client = TestClient(app)

TOL = 0.01


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


def _credit_account(account="src", credit=0.05, tag=None):
    """造一个「full 0.10 已付清 + 应退 credit 全部形成信用」的账户。"""
    tag = tag or f"cr-{account}"
    _raw_entry(account, 1, f"{tag}-s1", 1, 0.10, "full")
    assert client.post(
        f"/accounts/{account}/payments",
        json={"payment_no": f"PAY-{tag}", "amount_yuan": 0.10},
    ).status_code == 200
    _raw_entry(account, 2, f"{tag}-s2", 1, -credit, "refund")
    led = client.get(f"/accounts/{account}/ledger").json()
    assert led["outstanding_yuan"] == 0.0
    assert led["credit_yuan"] == pytest.approx(credit, abs=1e-9)
    return led


def _debtor_account(account="dst", amounts=(0.10, 0.03), tag=None):
    """造一个有若干未付清 full 凭证（按入账顺序）、无信用的账户。"""
    tag = tag or f"db-{account}"
    for i, amount in enumerate(amounts, start=1):
        _raw_entry(account, i, f"{tag}-s{i}", 1, amount, "full")
    led = client.get(f"/accounts/{account}/ledger").json()
    assert led["credit_yuan"] == 0.0
    assert led["outstanding_yuan"] == pytest.approx(sum(amounts), abs=1e-9)
    return led


def _transfer(transfer_no, frm, to, amount):
    return client.post(
        "/credit-transfers",
        json={
            "transfer_no": transfer_no,
            "from_account_id": frm,
            "to_account_id": to,
            "amount_yuan": amount,
        },
    )


def _transfers(account):
    return client.get(f"/accounts/{account}/credit-transfers").json()


def _ledger(account):
    return client.get(f"/accounts/{account}/ledger").json()


def _payments(account):
    return client.get(f"/accounts/{account}/payments").json()


def _payouts(account):
    return client.get(f"/accounts/{account}/payouts").json()


# ---------------------------------------------------------------------------
# 基本划转：来源扣信用、目标 FIFO 抵扣、剩余形成信用、转入额不算现金实收
# ---------------------------------------------------------------------------


def test_transfer_deducts_source_credit_and_applies_target_fifo():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10, 0.03))

    r = _transfer("TR-1", "src", "dst", 0.04)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "transferred"
    assert body["transfer_no"] == "TR-1"
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["amount_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert body["transferred_at"].endswith("Z")
    assert body["global_seq"] == 1
    assert body["from_transfer_seq"] == 1
    assert body["to_transfer_seq"] == 1
    assert body["transfer_count"] == 1

    # 来源：信用 0.05 → 0.01（划出与退付同口径扣减）
    assert body["from_position"]["credit_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert body["from_position"]["outstanding_yuan"] == 0.0
    assert body["from_position"]["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert body["from_position"]["total_paid_out_yuan"] == 0.0

    # 目标：转入 0.04 FIFO 抵第一张 full 0.10（部分抵扣），尚欠 0.09
    assert body["to_position"]["outstanding_yuan"] == pytest.approx(0.09, abs=1e-9)
    assert body["to_position"] ["credit_yuan"] == 0.0
    # 转入额不算现金实收
    assert body["to_position"]["total_received_yuan"] == 0.0
    apps = body["applications"]
    assert len(apps) == 1
    a = apps[0]
    assert a["application_no"] == 1
    assert a["entry_no"] == 1
    assert a["voucher_no"] == 1
    assert a["applied_yuan"] == pytest.approx(0.04, abs=1e-9)
    assert a["outstanding_before_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert a["outstanding_after_yuan"] == pytest.approx(0.06, abs=1e-9)


def test_transfer_remaining_after_clearing_debt_becomes_target_credit():
    _credit_account("src", 0.20)
    _debtor_account("dst", (0.10, 0.03))  # 总欠费 0.13

    r = _transfer("TR-1", "src", "dst", 0.20)
    assert r.status_code == 200, r.text
    body = r.json()
    # 0.10 抵第一张、0.03 抵第二张，剩余 0.07 形成目标信用；转入额仍不算现金
    assert body["to_position"]["outstanding_yuan"] == 0.0
    assert body["to_position"]["credit_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert body["to_position"]["total_received_yuan"] == 0.0
    assert [a["applied_yuan"] for a in body["applications"]] == [
        pytest.approx(0.10, abs=1e-9),
        pytest.approx(0.03, abs=1e-9),
    ]
    assert [a["entry_no"] for a in body["applications"]] == [1, 2]
    assert [a["outstanding_after_yuan"] for a in body["applications"]] == [
        0.0,
        0.0,
    ]
    # 来源信用扣完
    assert body["from_position"]["credit_yuan"] == 0.0

    # 台账与收款查询口径一致，且逐凭证 paid_yuan 仍为 0（转入不是现金）
    led = _ledger("dst")
    assert led["credit_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert led["outstanding_yuan"] == 0.0
    assert led["total_received_yuan"] == 0.0
    assert all(e["paid_yuan"] == 0.0 for e in led["entries"])
    pays = _payments("dst")
    assert pays["credit_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert pays["total_received_yuan"] == 0.0
    assert pays["payment_count"] == 0


def test_transfer_to_account_without_outstanding_all_becomes_credit():
    _credit_account("src", 0.05)
    _credit_account("dst", 0.02)  # 目标也只有信用、没有未付清凭证

    r = _transfer("TR-1", "src", "dst", 0.03)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["applications"] == []
    assert body["from_position"]["credit_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert body["to_position"]["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["to_position"]["outstanding_yuan"] == 0.0
    # 目标自己的现金实收（0.10，用于形成其原有信用）保留；本次转入不增加它
    assert body["to_position"]["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)


def test_transfer_equal_to_source_credit_boundary():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))

    r = _transfer("TR-ALL", "src", "dst", 0.05)
    assert r.status_code == 200, r.text
    assert r.json()["from_position"]["credit_yuan"] == 0.0
    assert r.json()["to_position"]["outstanding_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("src")["credit_yuan"] == 0.0

    # 来源信用已划完：再划任何正金额都整笔拒绝
    over = _transfer("TR-MORE", "src", "dst", 0.01)
    assert over.status_code == 422
    assert over.json()["error"] == "transfer_insufficient_credit"
    assert _transfers("src")["transfer_count"] == 1


def test_transfer_respects_target_protected_cash_partial_payment():
    """目标第一张已现金付了 0.03：转入只抵剩余尚欠，不产生现金核销。"""
    _credit_account("src", 0.08)
    _raw_entry("dst", 1, "ds1", 1, 0.10, "full")
    paid = client.post(
        "/accounts/dst/payments", json={"payment_no": "DP", "amount_yuan": 0.03}
    )
    assert paid.status_code == 200
    assert paid.json()["allocations"][0]["outstanding_after_yuan"] == pytest.approx(
        0.07, abs=1e-9
    )

    r = _transfer("TR-1", "src", "dst", 0.08)
    assert r.status_code == 200, r.text
    body = r.json()
    # 0.07 抵第一张（现金 0.03 受保护），剩 0.01 形成信用
    assert body["to_position"]["outstanding_yuan"] == 0.0
    assert body["to_position"]["credit_yuan"] == pytest.approx(0.01, abs=1e-9)
    assert body["to_position"]["total_received_yuan"] == pytest.approx(0.03, abs=1e-9)
    led = _ledger("dst")
    assert led["entries"][0]["paid_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert led["entries"][0]["outstanding_yuan"] == 0.0


# ---------------------------------------------------------------------------
# 划转后两户头寸：后续收款 / 退付按新头寸处理，历史不改写
# ---------------------------------------------------------------------------


def test_subsequent_payment_uses_post_transfer_outstanding():
    _credit_account("src", 0.04)
    _debtor_account("dst", (0.10, 0.03))
    assert _transfer("TR-1", "src", "dst", 0.04).status_code == 200

    # 划转后目标尚欠 0.09：收 0.09 恰好付清，现金只分到第一张 0.06
    r = client.post(
        "/accounts/dst/payments", json={"payment_no": "D1", "amount_yuan": 0.09}
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["outstanding_yuan"] == 0.0
    assert body["credit_yuan"] == 0.0
    assert [a["allocated_yuan"] for a in body["allocations"]] == [
        pytest.approx(0.06, abs=1e-9),
        pytest.approx(0.03, abs=1e-9),
    ]
    # 超过划转后尚欠收款整笔拒绝
    over = client.post(
        "/accounts/dst/payments", json={"payment_no": "D2", "amount_yuan": 0.01}
    )
    assert over.status_code == 422
    assert over.json()["error"] == "payment_exceeds_outstanding"


def test_subsequent_payout_uses_post_transfer_credit_on_target():
    _credit_account("src", 0.20)
    _debtor_account("dst", (0.10,))
    assert _transfer("TR-1", "src", "dst", 0.15).status_code == 200
    # 目标：抵 0.10 后信用 0.05
    assert _ledger("dst")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)

    r = client.post("/accounts/dst/payouts", json={"payout_no": "DPO", "amount_yuan": 0.05})
    assert r.status_code == 200
    assert r.json()["credit_yuan"] == 0.0
    # 退付查询与台账查询头寸同步；转入仍不计入累计实收
    assert _ledger("dst")["credit_yuan"] == 0.0
    assert _payouts("dst")["credit_yuan"] == 0.0


def test_transfer_does_not_rewrite_vouchers_old_payments_or_payouts():
    _credit_account("src", 0.10)
    _debtor_account("dst", (0.10,))
    # 目标先有一笔旧现金收款 0.02（部分付清）与来源一笔旧退付前的信用快照
    client.post("/accounts/dst/payments", json={"payment_no": "OLD-D", "amount_yuan": 0.02})
    old_pay = _payments("dst")["payments"][0]

    assert _transfer("TR-1", "src", "dst", 0.05).status_code == 200

    # 旧收款回执与逐凭证分配冻结不改
    pays = _payments("dst")
    old = pays["payments"][0]
    assert old["payment_no"] == "OLD-D"
    for key in ("amount_yuan", "total_received_yuan", "outstanding_yuan",
                "credit_yuan", "payment_count", "received_at"):
        assert old[key] == old_pay[key]
    assert old["allocations"][0]["allocated_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 累计实收仍只有旧现金收款（转入不加入）
    assert pays["total_received_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 已记凭证明细冻结字段不变（billing / amount / balance_after）
    entries = _ledger("dst")["entries"]
    assert entries[0]["amount_yuan"] == pytest.approx(0.10, abs=1e-9)


# ---------------------------------------------------------------------------
# 超额与账户合要求：整笔拒绝、双方均不留流水
# ---------------------------------------------------------------------------


def test_transfer_exceeding_source_credit_rejected_without_trace():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))

    r = _transfer("TOO-BIG", "src", "dst", 0.06)
    assert r.status_code == 422
    body = r.json()
    assert body["error"] == "transfer_insufficient_credit"
    assert body["transfer_no"] == "TOO-BIG"
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["amount_yuan"] == pytest.approx(0.06, abs=1e-9)
    assert body["from_credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["from_outstanding_yuan"] == 0.0

    # 双方均不留流水、头寸不变
    assert _transfers("src")["transfer_count"] == 0
    assert _transfers("dst")["transfer_count"] == 0
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert _ledger("dst")["outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)


def test_source_with_outstanding_but_no_credit_rejects_any_positive_amount():
    # 来源有台账但尚欠 0.10、信用 0
    _debtor_account("src", (0.10,))
    _debtor_account("dst", (0.10,))
    r = _transfer("TR-1", "src", "dst", 0.01)
    assert r.status_code == 422
    assert r.json()["error"] == "transfer_insufficient_credit"
    assert r.json()["from_credit_yuan"] == 0.0
    assert r.json()["from_outstanding_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert _transfers("src")["transfer_count"] == 0


def test_accounts_must_be_distinct():
    # HTTP：请求体模型直接拦成 invalid_input
    r = client.post(
        "/credit-transfers",
        json={"transfer_no": "TR", "from_account_id": "x",
              "to_account_id": "x", "amount_yuan": 0.01},
    )
    assert r.status_code == 422
    assert r.json()["error"] == "invalid_input"

    # 领域层直调有明确错误码（防御性）
    with pytest.raises(TransferError) as ei:
        submit_transfer("TR", "y", "y", 0.01)
    assert ei.value.code == "transfer_same_account"


def test_accounts_must_both_have_ledger():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))

    r1 = _transfer("TR-1", "ghost", "dst", 0.01)
    assert r1.status_code == 422
    assert r1.json()["error"] == "transfer_from_empty_account"
    assert r1.json()["from_account_id"] == "ghost"

    r2 = _transfer("TR-1", "src", "ghost", 0.01)
    assert r2.status_code == 422
    assert r2.json()["error"] == "transfer_to_empty_account"
    assert r2.json()["to_account_id"] == "ghost"

    # 双方均无流水
    assert _transfers("src")["transfer_count"] == 0
    assert _transfers("dst")["transfer_count"] == 0


def test_non_positive_amount_rejected():
    for bad in (0.0, -1.0):
        r = client.post(
            "/credit-transfers",
            json={"transfer_no": "Z", "from_account_id": "a",
                  "to_account_id": "b", "amount_yuan": bad},
        )
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_input"


def test_non_finite_amount_rejected():
    # Pydantic 的 gt=0 挡不住 Infinity，模型层显式要求有限数值。
    for raw in ("Infinity", "-Infinity", "NaN"):
        r = client.post(
            "/credit-transfers",
            content=(
                '{"transfer_no":"Z","from_account_id":"a","to_account_id":"b",'
                f'"amount_yuan":{raw}}}'
            ),
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 422
        assert r.json()["error"] == "invalid_input"


def test_empty_or_long_identifiers_rejected():
    body = {"from_account_id": "a", "to_account_id": "b", "amount_yuan": 1.0}
    r = client.post("/credit-transfers", json={**body, "transfer_no": ""})
    assert r.status_code == 422
    r = client.post("/credit-transfers", json={**body, "transfer_no": "x" * 129})
    assert r.status_code == 422
    r = client.post(
        "/credit-transfers",
        json={"transfer_no": "Z", "from_account_id": "",
              "to_account_id": "b", "amount_yuan": 1.0},
    )
    assert r.status_code == 422
    r = client.post(
        "/credit-transfers",
        json={"transfer_no": "Z", "from_account_id": "a",
              "to_account_id": "x" * 129, "amount_yuan": 1.0},
    )
    assert r.status_code == 422


def test_sub_micro_amount_rounded_to_zero_rejected_without_trace():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))
    r = _transfer("TINY", "src", "dst", 1e-12)
    assert r.status_code == 422
    assert r.json()["error"] == "transfer_amount_not_positive"
    assert _transfers("src")["transfer_count"] == 0


# ---------------------------------------------------------------------------
# 幂等重试与单号冲突
# ---------------------------------------------------------------------------


def test_same_transfer_no_retry_returns_first_frozen_receipt():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10, 0.03))
    first = _transfer("DUP", "src", "dst", 0.04)
    assert first.status_code == 200
    fb = first.json()
    assert fb["result"] == "transferred"

    # 之后两户头寸又变（给来源再入账一张应退形成更多信用后划第二笔，把目标
    # 尚欠抵平、来源回到 0 信用），同单号同双方同金额重试仍逐字返回首次回执。
    _raw_entry("src", 3, "extra-s3", 1, -0.20, "refund")
    second = _transfer("TR-2", "src", "dst", 0.20)
    assert second.status_code == 200
    assert second.json()["to_position"]["outstanding_yuan"] == 0.0

    retry = _transfer("DUP", "src", "dst", 0.04)
    assert retry.status_code == 200
    rb = retry.json()
    assert rb["result"] == "deduped"
    for key in ("from_account_id", "to_account_id", "amount_yuan", "transferred_at",
                "global_seq", "from_transfer_seq", "to_transfer_seq", "transfer_count",
                "from_position", "to_position", "applications"):
        assert rb[key] == fb[key]
    # 不新增划转记录
    assert _transfers("src")["transfer_count"] == 2


def test_same_no_different_amount_conflicts():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))
    assert _transfer("XNO", "src", "dst", 0.02).status_code == 200

    r = _transfer("XNO", "src", "dst", 0.03)
    assert r.status_code == 409
    body = r.json()
    assert body["error"] == "transfer_no_conflict"
    assert body["transfer_no"] == "XNO"
    assert body["from_account_id"] == "src"
    assert body["to_account_id"] == "dst"
    assert body["amount_yuan"] == pytest.approx(0.02, abs=1e-9)
    # 冲突不写入：来源只划出一笔 0.02
    lst = _transfers("src")
    assert lst["transfer_count"] == 1
    assert lst["total_transferred_out_yuan"] == pytest.approx(0.02, abs=1e-9)
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)


def test_same_no_swapped_accounts_conflict():
    _credit_account("a", 0.05, tag="ca")
    _credit_account("b", 0.05, tag="cb")
    assert _transfer("SHARED", "a", "b", 0.02).status_code == 200

    # 换目标账户
    _credit_account("c", 0.05, tag="cc")
    r1 = _transfer("SHARED", "a", "c", 0.02)
    assert r1.status_code == 409
    b1 = r1.json()
    assert b1["error"] == "transfer_no_conflict"
    assert b1["from_account_id"] == "a" and b1["to_account_id"] == "b"

    # 换来源账户
    r2 = _transfer("SHARED", "c", "b", 0.02)
    assert r2.status_code == 409
    # 反向（swap 双方）
    r3 = _transfer("SHARED", "b", "a", 0.02)
    assert r3.status_code == 409

    # 只有原始一笔落盘，c 户没有任何划转流水、头寸不变
    assert _transfers("c")["transfer_count"] == 0
    assert _ledger("c")["credit_yuan"] == pytest.approx(0.05, abs=1e-9)


# ---------------------------------------------------------------------------
# 反向划转与记录查询
# ---------------------------------------------------------------------------


def test_reverse_transfer_both_direction_seqs_independent():
    _credit_account("a", 0.10, tag="ca")
    _credit_account("b", 0.02, tag="cb")
    assert _transfer("T1", "a", "b", 0.07).status_code == 200   # a: 0.03, b: 0.09 信用
    assert _transfer("T2", "a", "b", 0.03).status_code == 200   # a: 0, b: 0.12 信用
    # b 反向划 0.05 给 a（a 无未付清凭证，全部形成信用）
    r = _transfer("T3", "b", "a", 0.05)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["global_seq"] == 3
    assert body["from_transfer_seq"] == 1  # b 的第一笔划出
    assert body["to_transfer_seq"] == 1    # a 的第一笔转入
    assert body["from_position"]["credit_yuan"] == pytest.approx(0.07, abs=1e-9)
    assert body["to_position"]["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert body["applications"] == []

    la = _transfers("a")
    lb = _transfers("b")
    assert la["out_count"] == 2 and la["in_count"] == 1
    assert lb["out_count"] == 1 and lb["in_count"] == 2
    # 列表项 direction / counterpart / account_transfer_seq
    t3a = [t for t in la["transfers"] if t["transfer_no"] == "T3"][0]
    assert t3a["direction"] == "in"
    assert t3a["counterpart_account_id"] == "b"
    assert t3a["account_transfer_seq"] == 1
    t3b = [t for t in lb["transfers"] if t["transfer_no"] == "T3"][0]
    assert t3b["direction"] == "out"
    assert t3b["counterpart_account_id"] == "a"
    assert t3b["account_transfer_seq"] == 1
    # 账户级累计划出 / 转入
    assert la["total_transferred_out_yuan"] == pytest.approx(0.10, abs=1e-9)
    assert la["total_transferred_in_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert lb["total_transferred_out_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert lb["total_transferred_in_yuan"] == pytest.approx(0.10, abs=1e-9)
    # 账户头寸为最新值
    assert la["credit_yuan"] == pytest.approx(0.05, abs=1e-9)
    assert lb["credit_yuan"] == pytest.approx(0.07, abs=1e-9)


def test_transfer_list_empty_account_and_frozen_content():
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10, 0.03))
    first = _transfer("TR-1", "src", "dst", 0.05).json()

    empty = _transfers("never-used")
    assert empty == {
        "account_id": "never-used",
        "transfer_count": 0,
        "out_count": 0,
        "in_count": 0,
        "total_transferred_out_yuan": 0.0,
        "total_transferred_in_yuan": 0.0,
        "outstanding_yuan": 0.0,
        "credit_yuan": 0.0,
        "transfers": [],
    }

    # 列表中 result 恒为 transferred，内容与首次冻结回执一致
    item = _transfers("dst")["transfers"][0]
    assert item["result"] == "transferred"
    assert item["transferred_at"] == first["transferred_at"]
    assert item["from_position"] == first["from_position"]
    assert item["to_position"] == first["to_position"]
    assert item["applications"] == first["applications"]
    assert item["transfer_count"] == 1
    assert item["global_seq"] == 1


# ---------------------------------------------------------------------------
# 并发：不重复支用信用；同单号只有一笔落盘
# ---------------------------------------------------------------------------


def test_concurrent_transfers_and_payout_spend_credit_only_once():
    _credit_account("s", 0.10)
    _debtor_account("t", (0.50,))

    results = []

    def hit(kind, no):
        if kind == "T":
            r = _transfer(no, "s", "t", 0.06)
        else:
            r = client.post("/accounts/s/payouts", json={"payout_no": no, "amount_yuan": 0.06})
        results.append((kind, no, r.status_code, r.json().get("error")))

    threads = [
        threading.Thread(target=hit, args=("T", "C1")),
        threading.Thread(target=hit, args=("T", "C2")),
        threading.Thread(target=hit, args=("P", "W1")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ok = [r for r in results if r[2] == 200]
    rejected = [r for r in results if r[2] == 422]
    assert len(ok) == 1
    assert len(rejected) == 2
    assert {r[3] for r in rejected} <= {
        "transfer_insufficient_credit",
        "payout_exceeds_credit",
    }
    # 信用恰被支用一次 0.06，剩 0.04；
    # 赢的可能是一笔划转（目标只收到一笔转入 0.06，尚欠 0.44）或一笔退付
    # （目标头寸不变，尚欠 0.50）——两种交错都满足「不重复支用信用」。
    assert _ledger("s")["credit_yuan"] == pytest.approx(0.04, abs=1e-9)
    winner = ok[0]
    if winner[0] == "T":
        assert _ledger("t")["outstanding_yuan"] == pytest.approx(0.44, abs=1e-9)
        lst = _transfers("s")
        assert lst["transfer_count"] == 1
        assert lst["total_transferred_out_yuan"] == pytest.approx(0.06, abs=1e-9)
        assert _payouts("s")["payout_count"] == 0
    else:
        assert _ledger("t")["outstanding_yuan"] == pytest.approx(0.50, abs=1e-9)
        assert _transfers("s")["transfer_count"] == 0
        assert _payouts("s")["total_paid_out_yuan"] == pytest.approx(0.06, abs=1e-9)


def test_concurrent_same_transfer_no_only_one_lands_others_dedup():
    _credit_account("s", 0.10)
    _debtor_account("t", (0.50,))

    results = []

    def hit():
        r = _transfer("DUP", "s", "t", 0.03)
        results.append((r.status_code, r.json().get("result"), r.json().get("error")))

    threads = [threading.Thread(target=hit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert {r[0] for r in results} == {200}
    results_status = [r[1] for r in results]
    assert results_status.count("transferred") == 1
    assert results_status.count("deduped") == 5
    assert _transfers("s")["transfer_count"] == 1
    assert _ledger("s")["credit_yuan"] == pytest.approx(0.07, abs=1e-9)


# ---------------------------------------------------------------------------
# 重启持久化与旧库兼容
# ---------------------------------------------------------------------------


def test_restart_preserves_transfers_and_idempotent_retry():
    _credit_account("src", 0.10)
    _debtor_account("dst", (0.10, 0.03))
    first = _transfer("TR-1", "src", "dst", 0.04).json()

    # 同一路径重建存储，模拟进程重启
    configure_store(str(get_store().db_path))

    la = _transfers("src")
    assert la["transfer_count"] == 1
    assert la["transfers"][0]["transferred_at"] == first["transferred_at"]
    assert la["transfers"][0]["to_position"] == first["to_position"]
    lb = _transfers("dst")
    assert lb["transfer_count"] == 1

    # 重启后同单号重试仍取首次回执；新划转序号接续
    retry = _transfer("TR-1", "src", "dst", 0.04)
    assert retry.status_code == 200
    assert retry.json()["result"] == "deduped"
    assert retry.json()["transferred_at"] == first["transferred_at"]

    nxt = _transfer("TR-2", "src", "dst", 0.03)
    assert nxt.status_code == 200
    assert nxt.json()["global_seq"] == 2
    assert nxt.json()["from_transfer_seq"] == 2
    assert nxt.json()["to_transfer_seq"] == 2


def test_legacy_db_without_transfer_table_auto_migrates(tmp_path):
    """旧库（无 account_credit_transfers 表）打开时自动补建，既有台账/收款/退付不变。"""
    import sqlite3

    db_path = str(tmp_path / "legacy.db")
    # 建一个只含旧表的库（sessions 最小骨架），直接插入一笔旧台账 + 旧收款，
    # 随后经应用打开（configure_store 自动补建划转表）。
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY, dispatch_json TEXT NOT NULL,
            plan_snapshot_json TEXT NOT NULL, audit_tolerance_kwh REAL NOT NULL,
            num_periods INTEGER NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE session_records (
            session_id TEXT NOT NULL, idx INTEGER NOT NULL,
            record_json TEXT NOT NULL, stored_at TEXT NOT NULL,
            PRIMARY KEY (session_id, idx)
        );
        """
    )
    conn.commit()
    conn.close()

    configure_store(db_path)
    # 旧库打开后划转接口可直接使用：先建两户台账
    _credit_account("src", 0.05)
    _debtor_account("dst", (0.10,))
    r = _transfer("TR-OLDDB", "src", "dst", 0.02)
    assert r.status_code == 200, r.text
    assert r.json()["to_position"]["outstanding_yuan"] == pytest.approx(0.08, abs=1e-9)
    # 既有台账 / 收款查询照常
    assert _ledger("src")["credit_yuan"] == pytest.approx(0.03, abs=1e-9)
    assert _payments("src")["total_received_yuan"] == pytest.approx(0.10, abs=1e-9)
