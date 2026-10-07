"""执行会话持久化：SQLite（进程重启后会话与已确认记录仍在）。

设计要点：
- 单文件 SQLite，默认 /data/sessions.db（容器内挂命名卷），可用环境变量
  DISPATCH_SESSION_DB 覆盖；无外部依赖，随 Compose 一起起；
- 所有写事务走 process 内 RLock + BEGIN IMMEDIATE：同一进程的并发上报在
  临界区内串行判定，只有一份能写入；SQLite 文件锁同时防多进程抢写；
- 记录按 (session_id, idx) 主键落库，天然防重复下标；整批 INSERT 在同一
  事务内，任一条失败即回滚，满足「整批不入库」；
- session_batches 按 (session_id, start_index) 主键记录每个已确认批次的
  边界（start_index + batch_size）与首次成功时的完整回执（receipt_json），
  与逐段记录在同一写事务内落库：重试判定以批次边界为准，只有起点、长度、
  内容三者都与某已确认整批一致才算原样重试（deduped），且回执保持首次
  成功时的值不随后续批次漂移；
- session_revisions 按 (session_id, revision_no) 主键保存计量表更正：
  每次提交存原始变更（下标 + 完整替换记录）与叠加后的整段修订台账，
  修订号与会话进度无关、从 1 起单调递增；最初上报的 session_records 与
  session_batches 永不被更正改写；
- session_settlement_vouchers 按 (session_id, voucher_no) 记录出单次序，
  (session_id, revision_no) 主键保证同一修订号只出一张凭证（重试幂等）：
  每张凭证冻结出单时该修订的整段台账（ledger_json）与完整结算结果
  （settlement_json）、首张/应补/应退计费信息与出单时间；凭证一经落盘
  即不可变，后续更正只新增修订行，从不改写已有凭证；
- account_ledger_entries 为家庭应收台账：每张已入账凭证一行，按账户维度
  以 entry_no 记录入账次序；(session_id, voucher_no) 唯一键保证同一张
  凭证全平台只能入账一次（跨账户 / 并发 / 重试都不可能重复计费），
  (account_id, entry_no) 唯一键保证账户内入账次序连续不重号。
  入账金额取凭证冻结的计费栏（full=+全额、supplement=+应补、
  refund=−应退、no_adjustment=0），账户余额允许为负；
- account_payments 为账户收款登记：payment_no 为调用方提交的全平台唯一
  收款单号（主键），整行冻结首次成功时的完整回执 JSON（含逐凭证分配、
  累计实收与当时尚欠/信用快照）；同一单号 + 同账户 + 同金额重试原样取
  回首收回执，单号换金额或跨账户复用在数据库层按主键冲突外显式判 409；
  (account_id, payment_seq) 唯一键保证账户内收款次序连续。
  account_payment_allocations 记录每笔收款按原入账顺序对正额凭证的逐笔
  核销（含部分付清），同一写事务内与收款主行一起落盘，使「当前尚欠」
  可由已入账凭证与已落盘核销重算；超额收款整笔拒绝时两表都不写。
- account_payouts 为账户信用退付：payout_no 为调用方提交的全平台唯一
  退付单号（主键），整行冻结首次成功时的完整回执 JSON（退付金额、
  退付时间与当时头寸快照）；同一单号 + 同账户 + 同金额重试原样取回
  首次回执，单号换金额或跨账户复用判 409；(account_id, payout_seq)
  唯一键保证账户内退付次序连续。已落盘退付在头寸重算时从应退池中
  扣除（只动信用、不动逐凭证核销），信用不足时整笔拒绝、本表不写。
- account_payment_reversals 为误记收款的整笔冲正：payment_no 为被冲正的
  原收款单号（主键，一笔收款至多冲正一次），整行冻结首次冲正时的完整
  回执 JSON（冲正金额、冲正时间与冲正后头寸快照）；同一账户对同一单号
  重复冲正原样取回首次冲正回执；(account_id, reversal_seq) 唯一键保证
  账户内冲正次序连续。被冲正收款的逐凭证核销在头寸重算时不再计入有效
  实收（原收款主行与核销行本身保留，永不改写）；冲正核对失败（原单不
  存在、账户不符）一律回滚，本表不写。
- account_credit_transfers 为账户间已形成信用的划转：transfer_no 为调用方
  提交的全平台唯一划转单号（主键），整行冻结首次成功时的完整回执 JSON
  （双方账户、划转金额、目标按入账顺序的逐凭证抵扣、划转后双方头寸快照
  与时间）；同一单号 + 同来源 + 同目标 + 同金额重试原样取回首次回执，
  单号换账户或换金额判 409；global_seq 为全平台划转次序，
  (from_account_id, from_transfer_seq) / (to_account_id, to_transfer_seq)
  两个唯一键分别保证划出 / 转入两方账户内划转次序连续、不重号。转入额在
  头寸重算时加入应退池（不产生现金核销、不计入累计实收），划出额从池中
  扣除（与退付同口径）；信用不足、账户不合要求时整笔拒绝、本表不写。
- 旧库只有 sessions / session_records 两张表时，session_batches、
  session_revisions、session_settlement_vouchers、account_ledger_entries、
  account_payments、account_payment_allocations、account_payouts、
  account_payment_reversals 与 account_credit_transfers 由
  CREATE TABLE IF NOT EXISTS 自动补齐
  （空表）：查询与从 next_index 续报不受影响；已确认区间内没有批次边界
  可核对，不按前缀猜测去重。
"""

from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator, List, Optional

# 单条记录为半小时实测，计划最长 200 段，记录列只存 JSON，无需额外规模设计。
# session_batches 为批次边界与首收回执表；session_revisions 为计量表更正表
# （存每次提交的原始变更与叠加后的整段修订台账）；session_settlement_vouchers
# 为逐版结算凭证表（冻结该版台账、完整结算结果与首单/补退计费），均为后加的
# 新表，旧库打开时由 IF NOT EXISTS 自动补建。account_payouts 为信用退付表、
# account_payment_reversals 为收款冲正表、account_credit_transfers 为账户间
# 信用划转表，同样由 IF NOT EXISTS 自动补建。
_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    dispatch_json TEXT NOT NULL,
    plan_snapshot_json TEXT NOT NULL,
    audit_tolerance_kwh REAL NOT NULL,
    num_periods INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS session_records (
    session_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    record_json TEXT NOT NULL,
    stored_at TEXT NOT NULL,
    PRIMARY KEY (session_id, idx),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
CREATE TABLE IF NOT EXISTS session_batches (
    session_id TEXT NOT NULL,
    start_index INTEGER NOT NULL,
    batch_size INTEGER NOT NULL,
    receipt_json TEXT NOT NULL,
    stored_at TEXT NOT NULL,
    PRIMARY KEY (session_id, start_index),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
CREATE TABLE IF NOT EXISTS session_revisions (
    session_id TEXT NOT NULL,
    revision_no INTEGER NOT NULL,
    changes_json TEXT NOT NULL,
    ledger_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (session_id, revision_no),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
CREATE TABLE IF NOT EXISTS session_settlement_vouchers (
    voucher_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    voucher_no INTEGER NOT NULL,
    revision_no INTEGER NOT NULL,
    current_revision_at_issue INTEGER NOT NULL,
    ledger_json TEXT NOT NULL,
    settlement_json TEXT NOT NULL,
    total_cost_yuan REAL NOT NULL,
    billing_type TEXT NOT NULL,
    amount_yuan REAL NOT NULL,
    relative_to_revision INTEGER,
    relative_to_voucher_no INTEGER,
    issued_at TEXT NOT NULL,
    PRIMARY KEY (session_id, revision_no),
    UNIQUE (session_id, voucher_no),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
CREATE INDEX IF NOT EXISTS idx_vouchers_order
    ON session_settlement_vouchers(session_id, voucher_no);
CREATE TABLE IF NOT EXISTS account_ledger_entries (
    entry_id TEXT NOT NULL,
    account_id TEXT NOT NULL,
    entry_no INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    voucher_no INTEGER NOT NULL,
    voucher_id TEXT NOT NULL,
    revision_no INTEGER NOT NULL,
    billing_type TEXT NOT NULL,
    voucher_amount_yuan REAL NOT NULL,
    relative_to_revision INTEGER,
    relative_to_voucher_no INTEGER,
    signed_amount_yuan REAL NOT NULL,
    balance_after_yuan REAL NOT NULL,
    total_cost_yuan REAL NOT NULL,
    posted_at TEXT NOT NULL,
    PRIMARY KEY (session_id, voucher_no),
    UNIQUE (account_id, entry_no),
    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
);
CREATE INDEX IF NOT EXISTS idx_ledger_account_order
    ON account_ledger_entries(account_id, entry_no);
CREATE TABLE IF NOT EXISTS account_payments (
    payment_id TEXT NOT NULL,
    payment_no TEXT NOT NULL,
    account_id TEXT NOT NULL,
    payment_seq INTEGER NOT NULL,
    amount_yuan REAL NOT NULL,
    receipt_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (payment_no),
    UNIQUE (account_id, payment_seq)
);
CREATE INDEX IF NOT EXISTS idx_payments_account_order
    ON account_payments(account_id, payment_seq);
CREATE TABLE IF NOT EXISTS account_payment_allocations (
    payment_no TEXT NOT NULL,
    account_id TEXT NOT NULL,
    allocation_no INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    voucher_no INTEGER NOT NULL,
    entry_no INTEGER NOT NULL,
    allocated_yuan REAL NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (payment_no, allocation_no),
    UNIQUE (payment_no, session_id, voucher_no),
    FOREIGN KEY (payment_no) REFERENCES account_payments(payment_no)
);
CREATE INDEX IF NOT EXISTS idx_allocations_voucher
    ON account_payment_allocations(account_id, session_id, voucher_no);
CREATE TABLE IF NOT EXISTS account_payouts (
    payout_id TEXT NOT NULL,
    payout_no TEXT NOT NULL,
    account_id TEXT NOT NULL,
    payout_seq INTEGER NOT NULL,
    amount_yuan REAL NOT NULL,
    receipt_json TEXT NOT NULL,
    paid_at TEXT NOT NULL,
    PRIMARY KEY (payout_no),
    UNIQUE (account_id, payout_seq)
);
CREATE INDEX IF NOT EXISTS idx_payouts_account_order
    ON account_payouts(account_id, payout_seq);
CREATE TABLE IF NOT EXISTS account_payment_reversals (
    reversal_id TEXT NOT NULL,
    payment_no TEXT NOT NULL,
    account_id TEXT NOT NULL,
    reversal_seq INTEGER NOT NULL,
    amount_yuan REAL NOT NULL,
    receipt_json TEXT NOT NULL,
    reversed_at TEXT NOT NULL,
    PRIMARY KEY (payment_no),
    UNIQUE (account_id, reversal_seq)
);
CREATE INDEX IF NOT EXISTS idx_reversals_account_order
    ON account_payment_reversals(account_id, reversal_seq);
CREATE TABLE IF NOT EXISTS account_credit_transfers (
    transfer_id TEXT NOT NULL,
    transfer_no TEXT NOT NULL,
    global_seq INTEGER NOT NULL,
    from_account_id TEXT NOT NULL,
    to_account_id TEXT NOT NULL,
    from_transfer_seq INTEGER NOT NULL,
    to_transfer_seq INTEGER NOT NULL,
    amount_yuan REAL NOT NULL,
    receipt_json TEXT NOT NULL,
    transferred_at TEXT NOT NULL,
    PRIMARY KEY (transfer_no),
    UNIQUE (global_seq),
    UNIQUE (from_account_id, from_transfer_seq),
    UNIQUE (to_account_id, to_transfer_seq)
);
CREATE INDEX IF NOT EXISTS idx_transfers_from_order
    ON account_credit_transfers(from_account_id, from_transfer_seq);
CREATE INDEX IF NOT EXISTS idx_transfers_to_order
    ON account_credit_transfers(to_account_id, to_transfer_seq);
CREATE INDEX IF NOT EXISTS idx_transfers_global_order
    ON account_credit_transfers(global_seq);
"""

# 台账表后加列时的轻量迁移：旧库已建表但缺列，按列名幂等 ALTER 补齐。
# （CREATE TABLE IF NOT EXISTS 不会改已存在的表结构。）
_LEDGER_COLUMN_MIGRATIONS = (
    ("relative_to_revision", "INTEGER"),
    ("relative_to_voucher_no", "INTEGER"),
)


def default_db_path() -> str:
    """数据库路径：环境变量优先；容器内优先可写的 /data，否则落到工作目录 data/。"""
    env = os.environ.get("DISPATCH_SESSION_DB")
    if env:
        return env
    if os.path.isdir("/data") and os.access("/data", os.W_OK):
        return "/data/sessions.db"
    return os.path.join(os.getcwd(), "data", "sessions.db")


class SessionStore:
    """SQLite 会话存储。方法均为短事务，调用方见 app/sessions.py。"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or default_db_path()
        parent = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(parent, exist_ok=True)
        self._lock = threading.RLock()
        with self._connect() as conn:
            # WAL：读不阻塞写、写不阻塞读；同一时刻仍只有一个写事务。
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(_SCHEMA)
            self._migrate_ledger_columns(conn)

    @staticmethod
    def _migrate_ledger_columns(conn: sqlite3.Connection) -> None:
        """台账表后加列：对已存在的旧表幂等 ALTER 补列（新表已含这些列）。"""
        existing = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(account_ledger_entries)").fetchall()
        }
        if not existing:  # 表不存在的极端情况不应发生（schema 已建），直接跳过
            return
        for column, ddl in _LEDGER_COLUMN_MIGRATIONS:
            if column not in existing:
                conn.execute(
                    f"ALTER TABLE account_ledger_entries ADD COLUMN {column} {ddl}"
                )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def write_transaction(self) -> Iterator[sqlite3.Connection]:
        """加进程锁后开启 IMMEDIATE 事务，异常自动回滚。

        会话追加的「查重 — 边界核对 — 审计 — 入库」整段都在此临界区内完成，
        保证并发上报只有一份生效。
        """
        conn = self._connect()
        try:
            with self._lock:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def insert_session(
        self,
        session_id: str,
        dispatch_json: str,
        plan_snapshot_json: str,
        audit_tolerance_kwh: float,
        num_periods: int,
        created_at: str,
    ) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT INTO sessions"
                "(session_id, dispatch_json, plan_snapshot_json, audit_tolerance_kwh, "
                "num_periods, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    session_id,
                    dispatch_json,
                    plan_snapshot_json,
                    audit_tolerance_kwh,
                    num_periods,
                    created_at,
                ),
            )
            conn.commit()

    def get_session_row(self, session_id: str) -> Optional[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchone()

    def list_session_rows(self) -> List[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT s.*, "
                    " (SELECT COUNT(*) FROM session_records r WHERE r.session_id = s.session_id)"
                    " AS confirmed_periods, "
                    " (SELECT MAX(stored_at) FROM session_records r WHERE r.session_id = s.session_id)"
                    " AS updated_at, "
                    " (SELECT record_json FROM session_records r WHERE r.session_id = s.session_id"
                    "  ORDER BY r.idx DESC LIMIT 1) AS last_record_json"
                    " FROM sessions s ORDER BY s.created_at, s.session_id"
                ).fetchall()
            )

    def count_records(self, conn: sqlite3.Connection, session_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM session_records WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row["c"])

    def count_records_readonly(self, session_id: str) -> int:
        """短连接只读计数（写事务之外使用）。"""
        with self._lock, self._connect() as conn:
            return self.count_records(conn, session_id)

    def get_records_from(
        self, conn: sqlite3.Connection, session_id: str, start_index: int
    ) -> List[sqlite3.Row]:
        return list(
            conn.execute(
                "SELECT idx, record_json FROM session_records "
                "WHERE session_id = ? AND idx >= ? ORDER BY idx",
                (session_id, start_index),
            ).fetchall()
        )

    def get_all_records(self, session_id: str) -> List[sqlite3.Row]:
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT idx, record_json, stored_at FROM session_records "
                    "WHERE session_id = ? ORDER BY idx",
                    (session_id,),
                ).fetchall()
            )

    def insert_record(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        idx: int,
        record_json: str,
        stored_at: str,
    ) -> None:
        conn.execute(
            "INSERT INTO session_records(session_id, idx, record_json, stored_at)"
            " VALUES (?, ?, ?, ?)",
            (session_id, idx, record_json, stored_at),
        )

    def get_batch(
        self, conn: sqlite3.Connection, session_id: str, start_index: int
    ) -> Optional[sqlite3.Row]:
        """取 start_index 处已确认批次的边界与首收回执；无边界记录返回 None。"""
        return conn.execute(
            "SELECT start_index, batch_size, receipt_json, stored_at FROM session_batches "
            "WHERE session_id = ? AND start_index = ?",
            (session_id, start_index),
        ).fetchone()

    def insert_batch(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        start_index: int,
        batch_size: int,
        receipt_json: str,
        stored_at: str,
    ) -> None:
        """与逐段记录同一事务写入批次边界与首次成功回执（重试时原样取回）。"""
        conn.execute(
            "INSERT INTO session_batches"
            "(session_id, start_index, batch_size, receipt_json, stored_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (session_id, start_index, batch_size, receipt_json, stored_at),
        )

    def get_latest_revision(
        self, conn: sqlite3.Connection, session_id: str
    ) -> Optional[sqlite3.Row]:
        """取当前最新修订行（修订号最大的一条）；从未更正返回 None（视为修订号 0）。"""
        return conn.execute(
            "SELECT revision_no, changes_json, ledger_json, created_at"
            " FROM session_revisions WHERE session_id = ?"
            " ORDER BY revision_no DESC LIMIT 1",
            (session_id,),
        ).fetchone()

    def list_revision_rows(self, session_id: str) -> List[sqlite3.Row]:
        """按修订号升序列出会话的全部更正行（首条即修订号 1）。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT revision_no, changes_json, ledger_json, created_at"
                    " FROM session_revisions WHERE session_id = ?"
                    " ORDER BY revision_no",
                    (session_id,),
                ).fetchall()
            )

    def get_current_revision_no(self, session_id: str) -> int:
        """当前修订号：无更正为 0，否则为最大修订号。"""
        with self._lock, self._connect() as conn:
            row = self.get_latest_revision(conn, session_id)
            return int(row["revision_no"]) if row is not None else 0

    def insert_revision(
        self,
        conn: sqlite3.Connection,
        session_id: str,
        revision_no: int,
        changes_json: str,
        ledger_json: str,
        created_at: str,
    ) -> None:
        """与「核对修订号、整段重核」同一事务原子写入新修订号、变更与整段台账。"""
        conn.execute(
            "INSERT INTO session_revisions"
            "(session_id, revision_no, changes_json, ledger_json, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (session_id, revision_no, changes_json, ledger_json, created_at),
        )

    def get_all_records_in_txn(
        self, conn: sqlite3.Connection, session_id: str
    ) -> List[sqlite3.Row]:
        """写事务内取全部最初上报记录（按下标升序）；修订号 0 的台账即它。"""
        return list(
            conn.execute(
                "SELECT idx, record_json, stored_at FROM session_records "
                "WHERE session_id = ? ORDER BY idx",
                (session_id,),
            ).fetchall()
        )

    def get_revision_row(
        self, conn: sqlite3.Connection, session_id: str, revision_no: int
    ) -> Optional[sqlite3.Row]:
        """写事务内取指定修订号的更正行；不存在（含修订号 0）返回 None。"""
        return conn.execute(
            "SELECT revision_no, changes_json, ledger_json, created_at"
            " FROM session_revisions WHERE session_id = ? AND revision_no = ?",
            (session_id, revision_no),
        ).fetchone()

    def count_vouchers(self, conn: sqlite3.Connection, session_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM session_settlement_vouchers WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        return int(row["c"])

    def get_voucher(
        self, conn: sqlite3.Connection, session_id: str, revision_no: int
    ) -> Optional[sqlite3.Row]:
        """按修订号取已出凭证；该修订未出单返回 None（重试幂等判定以此为准）。"""
        return conn.execute(
            "SELECT * FROM session_settlement_vouchers"
            " WHERE session_id = ? AND revision_no = ?",
            (session_id, revision_no),
        ).fetchone()

    def get_latest_voucher(
        self, conn: sqlite3.Connection, session_id: str
    ) -> Optional[sqlite3.Row]:
        """按出单次序取最后一张凭证（voucher_no 最大）；从未出单返回 None。"""
        return conn.execute(
            "SELECT * FROM session_settlement_vouchers"
            " WHERE session_id = ? ORDER BY voucher_no DESC LIMIT 1",
            (session_id,),
        ).fetchone()

    def list_voucher_rows(self, session_id: str) -> List[sqlite3.Row]:
        """按出单次序（voucher_no 升序）列出会话的全部凭证。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM session_settlement_vouchers"
                    " WHERE session_id = ? ORDER BY voucher_no",
                    (session_id,),
                ).fetchall()
            )

    def insert_voucher(
        self,
        conn: sqlite3.Connection,
        voucher_id: str,
        session_id: str,
        voucher_no: int,
        revision_no: int,
        current_revision_at_issue: int,
        ledger_json: str,
        settlement_json: str,
        total_cost_yuan: float,
        billing_type: str,
        amount_yuan: float,
        relative_to_revision: Optional[int],
        relative_to_voucher_no: Optional[int],
        issued_at: str,
    ) -> None:
        """与「核对修订号、取该版台账、核算」同一事务原子落盘凭证。

        (session_id, revision_no) 主键使同修订并发出单只有一份成功；
        (session_id, voucher_no) 唯一键使出单次序号不重复。
        """
        conn.execute(
            "INSERT INTO session_settlement_vouchers"
            "(voucher_id, session_id, voucher_no, revision_no, current_revision_at_issue,"
            " ledger_json, settlement_json, total_cost_yuan, billing_type, amount_yuan,"
            " relative_to_revision, relative_to_voucher_no, issued_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                voucher_id,
                session_id,
                voucher_no,
                revision_no,
                current_revision_at_issue,
                ledger_json,
                settlement_json,
                total_cost_yuan,
                billing_type,
                amount_yuan,
                relative_to_revision,
                relative_to_voucher_no,
                issued_at,
            ),
        )


    # ------------------------------------------------------------------
    # 家庭应收台账
    # ------------------------------------------------------------------

    def count_ledger_entries(self, conn: sqlite3.Connection, account_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM account_ledger_entries WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return int(row["c"])

    def get_ledger_entry(
        self, conn: sqlite3.Connection, session_id: str, voucher_no: int
    ) -> Optional[sqlite3.Row]:
        """按凭证（会话 + 出单序号）取已入账行；未入账返回 None（防重复计费）。"""
        return conn.execute(
            "SELECT * FROM account_ledger_entries"
            " WHERE session_id = ? AND voucher_no = ?",
            (session_id, voucher_no),
        ).fetchone()

    def get_ledger_entry_for_account(
        self,
        conn: sqlite3.Connection,
        account_id: str,
        session_id: str,
        voucher_no: int,
    ) -> Optional[sqlite3.Row]:
        """同 get_ledger_entry，但限定账户（用于区分跨账户与重复）。"""
        return conn.execute(
            "SELECT * FROM account_ledger_entries"
            " WHERE account_id = ? AND session_id = ? AND voucher_no = ?",
            (account_id, session_id, voucher_no),
        ).fetchone()

    def list_ledger_entry_rows(self, account_id: str) -> List[sqlite3.Row]:
        """按入账次序（entry_no 升序）列出账户全部已记明细。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM account_ledger_entries"
                    " WHERE account_id = ? ORDER BY entry_no",
                    (account_id,),
                ).fetchall()
            )

    def get_voucher_by_no(
        self, conn: sqlite3.Connection, session_id: str, voucher_no: int
    ) -> Optional[sqlite3.Row]:
        """写事务内按出单序号取会话凭证（含冻结的计费栏）；未出单返回 None。"""
        return conn.execute(
            "SELECT * FROM session_settlement_vouchers"
            " WHERE session_id = ? AND voucher_no = ?",
            (session_id, voucher_no),
        ).fetchone()

    def insert_ledger_entry(
        self,
        conn: sqlite3.Connection,
        entry_id: str,
        account_id: str,
        entry_no: int,
        session_id: str,
        voucher_no: int,
        voucher_id: str,
        revision_no: int,
        billing_type: str,
        voucher_amount_yuan: float,
        relative_to_revision: Optional[int],
        relative_to_voucher_no: Optional[int],
        signed_amount_yuan: float,
        balance_after_yuan: float,
        total_cost_yuan: float,
        posted_at: str,
    ) -> None:
        """与「整批核对凭证与已记明细」同一事务原子落盘。

        (session_id, voucher_no) 主键使同一张凭证全平台只能入账一次：
        并发两批同时命中时数据库直接拒绝其中一个 INSERT；
        (account_id, entry_no) 唯一键保证账户内入账次序不重号。
        """
        conn.execute(
            "INSERT INTO account_ledger_entries"
            "(entry_id, account_id, entry_no, session_id, voucher_no, voucher_id,"
            " revision_no, billing_type, voucher_amount_yuan, relative_to_revision,"
            " relative_to_voucher_no, signed_amount_yuan,"
            " balance_after_yuan, total_cost_yuan, posted_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                entry_id,
                account_id,
                entry_no,
                session_id,
                voucher_no,
                voucher_id,
                revision_no,
                billing_type,
                voucher_amount_yuan,
                relative_to_revision,
                relative_to_voucher_no,
                signed_amount_yuan,
                balance_after_yuan,
                total_cost_yuan,
                posted_at,
            ),
        )

    def list_ledger_entry_rows_in_txn(
        self, conn: sqlite3.Connection, account_id: str
    ) -> List[sqlite3.Row]:
        """写事务内按入账次序（entry_no 升序）列出账户全部已记明细。"""
        return list(
            conn.execute(
                "SELECT * FROM account_ledger_entries"
                " WHERE account_id = ? ORDER BY entry_no",
                (account_id,),
            ).fetchall()
        )

    # ------------------------------------------------------------------
    # 账户收款（收款单幂等 + 逐凭证核销）
    # ------------------------------------------------------------------

    def get_payment(self, conn: sqlite3.Connection, payment_no: str) -> Optional[sqlite3.Row]:
        """写事务内按全平台唯一收款单号取收款行；从未提交过返回 None。"""
        return conn.execute(
            "SELECT * FROM account_payments WHERE payment_no = ?",
            (payment_no,),
        ).fetchone()

    def count_payments(self, conn: sqlite3.Connection, account_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM account_payments WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return int(row["c"])

    def insert_payment(
        self,
        conn: sqlite3.Connection,
        payment_id: str,
        payment_no: str,
        account_id: str,
        payment_seq: int,
        amount_yuan: float,
        receipt_json: str,
        received_at: str,
    ) -> None:
        """与「尚欠核对 → 逐凭证分配」同一事务原子落盘收款主行（含冻结回执）。

        payment_no 主键在数据库层兜底：并发同单号只有一笔成功落盘。
        """
        conn.execute(
            "INSERT INTO account_payments"
            "(payment_id, payment_no, account_id, payment_seq, amount_yuan,"
            " receipt_json, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                payment_id,
                payment_no,
                account_id,
                payment_seq,
                amount_yuan,
                receipt_json,
                received_at,
            ),
        )

    def insert_payment_allocation(
        self,
        conn: sqlite3.Connection,
        payment_no: str,
        account_id: str,
        allocation_no: int,
        session_id: str,
        voucher_no: int,
        entry_no: int,
        allocated_yuan: float,
        received_at: str,
    ) -> None:
        """与收款主行同一事务落盘一条逐凭证核销（可部分付清）。"""
        conn.execute(
            "INSERT INTO account_payment_allocations"
            "(payment_no, account_id, allocation_no, session_id, voucher_no,"
            " entry_no, allocated_yuan, received_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                payment_no,
                account_id,
                allocation_no,
                session_id,
                voucher_no,
                entry_no,
                allocated_yuan,
                received_at,
            ),
        )

    def list_payment_rows(self, account_id: str) -> List[sqlite3.Row]:
        """按收款次序（payment_seq 升序）列出账户全部已落盘收款行。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM account_payments"
                    " WHERE account_id = ? ORDER BY payment_seq",
                    (account_id,),
                ).fetchall()
            )

    def sum_cash_by_voucher(
        self,
        conn: sqlite3.Connection,
        account_id: str,
        session_id: str,
        voucher_no: int,
        exclude_payment_nos: Optional[frozenset] = None,
    ) -> float:
        """写事务内汇总某张已入账凭证名下全部**有效**收款核销额（用于重算尚欠）。

        已被整笔冲正的收款（account_payment_reversals 中有记录）其核销不再
        计入有效实收；exclude_payment_nos 用于冲正落盘前预演「冲正后」口径
        （把待冲正单号一并按无效排除），原收款与核销行本身永不改写。
        """
        sql = (
            "SELECT COALESCE(SUM(a.allocated_yuan), 0.0) AS s"
            " FROM account_payment_allocations a"
            " WHERE a.account_id = ? AND a.session_id = ? AND a.voucher_no = ?"
            " AND NOT EXISTS (SELECT 1 FROM account_payment_reversals r"
            "                WHERE r.payment_no = a.payment_no)"
        )
        params: list = [account_id, session_id, voucher_no]
        if exclude_payment_nos:
            placeholders = ", ".join("?" for _ in exclude_payment_nos)
            sql += f" AND a.payment_no NOT IN ({placeholders})"
            params.extend(sorted(exclude_payment_nos))
        row = conn.execute(sql, params).fetchone()
        return float(row["s"])

    # ------------------------------------------------------------------
    # 账户信用退付（退付单幂等，已付退付从头寸重算的应退池中扣除）
    # ------------------------------------------------------------------

    def get_payout(self, conn: sqlite3.Connection, payout_no: str) -> Optional[sqlite3.Row]:
        """写事务内按全平台唯一退付单号取退付行；从未提交过返回 None。"""
        return conn.execute(
            "SELECT * FROM account_payouts WHERE payout_no = ?",
            (payout_no,),
        ).fetchone()

    def count_payouts(self, conn: sqlite3.Connection, account_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM account_payouts WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return int(row["c"])

    def sum_payouts(self, conn: sqlite3.Connection, account_id: str) -> float:
        """写事务内汇总账户全部已落盘退付金额（头寸重算时从应退池扣除）。"""
        row = conn.execute(
            "SELECT COALESCE(SUM(amount_yuan), 0.0) AS s FROM account_payouts"
            " WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return float(row["s"])

    def insert_payout(
        self,
        conn: sqlite3.Connection,
        payout_id: str,
        payout_no: str,
        account_id: str,
        payout_seq: int,
        amount_yuan: float,
        receipt_json: str,
        paid_at: str,
    ) -> None:
        """与「信用核对」同一事务原子落盘退付主行（含冻结回执）。

        payout_no 主键在数据库层兜底：并发同单号只有一笔成功落盘。
        """
        conn.execute(
            "INSERT INTO account_payouts"
            "(payout_id, payout_no, account_id, payout_seq, amount_yuan,"
            " receipt_json, paid_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                payout_id,
                payout_no,
                account_id,
                payout_seq,
                amount_yuan,
                receipt_json,
                paid_at,
            ),
        )

    def list_payout_rows(self, account_id: str) -> List[sqlite3.Row]:
        """按退付次序（payout_seq 升序）列出账户全部已落盘退付行。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM account_payouts"
                    " WHERE account_id = ? ORDER BY payout_seq",
                    (account_id,),
                ).fetchall()
            )

    # ------------------------------------------------------------------
    # 收款冲正（整笔冲正幂等，被冲正收款的核销不再计入有效实收）
    # ------------------------------------------------------------------

    def get_reversal(self, conn: sqlite3.Connection, payment_no: str) -> Optional[sqlite3.Row]:
        """写事务内按原收款单号取冲正行；该笔收款未冲正返回 None。"""
        return conn.execute(
            "SELECT * FROM account_payment_reversals WHERE payment_no = ?",
            (payment_no,),
        ).fetchone()

    def count_reversals(self, conn: sqlite3.Connection, account_id: str) -> int:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM account_payment_reversals WHERE account_id = ?",
            (account_id,),
        ).fetchone()
        return int(row["c"])

    def insert_reversal(
        self,
        conn: sqlite3.Connection,
        reversal_id: str,
        payment_no: str,
        account_id: str,
        reversal_seq: int,
        amount_yuan: float,
        receipt_json: str,
        reversed_at: str,
    ) -> None:
        """与「原收款核对 → 冲正后头寸重算」同一事务原子落盘冲正行（含冻结回执）。

        payment_no 主键在数据库层兜底：同一笔收款并发冲正只有一笔成功落盘。
        """
        conn.execute(
            "INSERT INTO account_payment_reversals"
            "(reversal_id, payment_no, account_id, reversal_seq, amount_yuan,"
            " receipt_json, reversed_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                reversal_id,
                payment_no,
                account_id,
                reversal_seq,
                amount_yuan,
                receipt_json,
                reversed_at,
            ),
        )

    def list_reversal_rows(self, account_id: str) -> List[sqlite3.Row]:
        """按冲正次序（reversal_seq 升序）列出账户全部已落盘冲正行。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM account_payment_reversals"
                    " WHERE account_id = ? ORDER BY reversal_seq",
                    (account_id,),
                ).fetchall()
            )

    # ------------------------------------------------------------------
    # 账户间信用划转（划转单幂等；划出扣减应退池、转入加入应退池）
    # ------------------------------------------------------------------

    def get_transfer(self, conn: sqlite3.Connection, transfer_no: str) -> Optional[sqlite3.Row]:
        """写事务内按全平台唯一划转单号取划转行；从未提交过返回 None。"""
        return conn.execute(
            "SELECT * FROM account_credit_transfers WHERE transfer_no = ?",
            (transfer_no,),
        ).fetchone()

    def count_transfers(self) -> int:
        """全平台已落盘划转笔数（global_seq 从 1 起）。"""
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM account_credit_transfers"
            ).fetchone()
            return int(row["c"])

    def count_transfers_in_txn(self, conn: sqlite3.Connection) -> int:
        """写事务内取全平台已落盘划转笔数（用于分配下一个 global_seq）。"""
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM account_credit_transfers"
        ).fetchone()
        return int(row["c"])

    def count_transfers_for_account(
        self, conn: sqlite3.Connection, account_id: str
    ) -> tuple:
        """写事务内取账户参与的划转笔数（划出与转入分别计数后的较大序号基数）。

        from_transfer_seq / to_transfer_seq 各自从 1 起连续：本笔在一方的
        序号 = 该方在对应方向的已落盘笔数 + 1。

        两个子查询的位置参数若都用 ? 会按 SQL 文本顺序跨子查询绑定，
        故用命名参数 :acct 明确各取同一账户值。
        """
        row = conn.execute(
            "SELECT (SELECT COUNT(*) FROM account_credit_transfers"
            "         WHERE from_account_id = :acct) AS c_from,"
            " (SELECT COUNT(*) FROM account_credit_transfers"
            "         WHERE to_account_id = :acct) AS c_to",
            {"acct": account_id},
        ).fetchone()
        return int(row["c_from"]), int(row["c_to"])

    def sum_transfer_flows(
        self, conn: sqlite3.Connection, account_id: str
    ) -> tuple:
        """写事务内汇总账户已落盘的划出额与转入额（头寸重算时进出应退池）。

        两个子查询的位置参数若都用 ? 会按 SQL 文本顺序跨子查询绑定，
        故用命名参数 :acct 明确各取同一账户值。
        """
        row = conn.execute(
            "SELECT"
            " COALESCE((SELECT SUM(amount_yuan) FROM account_credit_transfers"
            "           WHERE from_account_id = :acct), 0.0) AS sent,"
            " COALESCE((SELECT SUM(amount_yuan) FROM account_credit_transfers"
            "           WHERE to_account_id = :acct), 0.0) AS received",
            {"acct": account_id},
        ).fetchone()
        return float(row["sent"]), float(row["received"])

    def insert_transfer(
        self,
        conn: sqlite3.Connection,
        transfer_id: str,
        transfer_no: str,
        global_seq: int,
        from_account_id: str,
        to_account_id: str,
        from_transfer_seq: int,
        to_transfer_seq: int,
        amount_yuan: float,
        receipt_json: str,
        transferred_at: str,
    ) -> None:
        """与「双方头寸核对 → 信用检查 → 目标 FIFO 抵扣」同一事务原子落盘。

        transfer_no 主键在数据库层兜底：并发同一单号只有一笔成功落盘；
        global_seq 唯一键保证全平台划转次序不重号，(from_account_id,
        from_transfer_seq) / (to_account_id, to_transfer_seq) 两个唯一键
        保证两户各自的划出 / 转入次序连续。
        """
        conn.execute(
            "INSERT INTO account_credit_transfers"
            "(transfer_id, transfer_no, global_seq, from_account_id, to_account_id,"
            " from_transfer_seq, to_transfer_seq, amount_yuan, receipt_json,"
            " transferred_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                transfer_id,
                transfer_no,
                global_seq,
                from_account_id,
                to_account_id,
                from_transfer_seq,
                to_transfer_seq,
                amount_yuan,
                receipt_json,
                transferred_at,
            ),
        )

    def list_transfer_rows_for_account(self, account_id: str) -> List[sqlite3.Row]:
        """按全平台划转次序（global_seq 升序）列出账户作为划出方或转入方
        参与的全部划转行（GET /accounts/{id}/credit-transfers 用）。"""
        with self._lock, self._connect() as conn:
            return list(
                conn.execute(
                    "SELECT * FROM account_credit_transfers"
                    " WHERE from_account_id = ? OR to_account_id = ?"
                    " ORDER BY global_seq",
                    (account_id, account_id),
                ).fetchall()
            )


_store: Optional[SessionStore] = None
_store_lock = threading.Lock()


def configure_store(db_path: Optional[str]) -> Optional[SessionStore]:
    """替换全局存储（测试用同一路径重建即可模拟进程重启）；

    传 None 表示重置为「未初始化」状态，下一次 get_store() 再按默认路径懒加载，
    避免测试拆卸时意外创建工作目录下的 data/sessions.db。
    """
    global _store
    with _store_lock:
        _store = SessionStore(db_path) if db_path is not None else None
    return _store


def get_store() -> SessionStore:
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = SessionStore()
    return _store
