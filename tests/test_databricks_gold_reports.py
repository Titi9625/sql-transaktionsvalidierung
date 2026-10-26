"""Portable SQL/package checks. SQLite cents are a logic adapter, not Spark/Delta.

Uses actual validation and Gold SQL constants. Does not execute cloud capture,
Spark nested schemas, saved-result views or the atomic Delta append.
"""
import ast
from collections import Counter
import copy
from decimal import Decimal, ROUND_HALF_UP
import os
from pathlib import Path
import sqlite3
import sys
import unittest

ROOT = Path(os.environ.get("PORTFOLIO_TEST_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(ROOT / "tests"))
from test_databricks_validation_rules import adapt, database, fixture, notebook_queries, payment, booking

NOTEBOOK = Path(os.environ.get("DATABRICKS_GOLD_NOTEBOOK_UNDER_TEST",
                              ROOT / "databricks/notebooks/04_report_gold.py"))


def load_source():
    tree = ast.parse(NOTEBOOK.read_text(encoding="utf-8"))
    sql = {}
    namespace = {"Counter": Counter, "Decimal": Decimal, "ROUND_HALF_UP": ROUND_HALF_UP}
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"summary_sql", "daily_sql"}:
                    sql[target.id] = node.value.value
        if isinstance(node, ast.FunctionDef) and node.name == "check_report_package":
            exec(compile(ast.Module(body=[node], type_ignores=[]), str(NOTEBOOK), "exec"), namespace)
    return sql, namespace["check_report_package"]


def adapt_gold(sql):
    for original, replacement in {
        "workspace.reconciliation_silver.stg_payment_transactions": "prepared_payments",
        "workspace.reconciliation_silver.stg_bank_transactions": "prepared_bank",
        "workspace.reconciliation_silver.invalid_transactions": "findings",
        "workspace.reconciliation_silver.valid_transactions": "valid",
        "workspace.reconciliation_bronze.ingestion_batches": "registry",
    }.items():
        sql = sql.replace(original, replacement)
    return sql.replace("CAST(payment_date AS STRING)", "CAST(payment_date AS TEXT)")


class GoldReports(unittest.TestCase):
    def build(self, payments, banks):
        conn = database(payments, banks)
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        conn.create_function("CONCAT", -1, lambda *items: None if None in items else "".join(map(str, items)))
        conn.execute("ALTER TABLE prepared_payments ADD COLUMN source_system TEXT DEFAULT 'stripe_sample_csv'")
        conn.execute("CREATE TABLE registry (batch_id TEXT, status TEXT)")
        conn.executemany("INSERT INTO registry VALUES (?, 'ready')",
                         [(batch,) for batch in sorted({r[0] for r in payments + banks})])
        for key, view in (("findings_sql", "findings"), ("valid_sql", "valid")):
            conn.execute(f"CREATE VIEW {view} AS " + adapt(notebook_queries()[key]))
        sql, checker = load_source()
        conn.execute("CREATE VIEW summary AS " + adapt_gold(sql["summary_sql"]))
        conn.execute("CREATE VIEW daily AS " + adapt_gold(sql["daily_sql"]))
        return conn, checker

    def packet(self, conn, batch="fixture"):
        summary = dict(conn.execute("SELECT * FROM summary WHERE batch_id=?", (batch,)).fetchone())
        if summary["validation_pass_rate_pct"] is not None:
            summary["validation_pass_rate_pct"] = Decimal(str(summary["validation_pass_rate_pct"]))
        return {"batch_id": batch, "summary": summary,
                "valid_payments": [dict(r) for r in conn.execute("SELECT * FROM valid WHERE batch_id=?", (batch,))],
                "findings": [dict(r) for r in conn.execute("SELECT * FROM findings WHERE batch_id=?", (batch,))],
                "daily_reports": [dict(r) | {"validation_pass_rate_pct": Decimal(str(r["validation_pass_rate_pct"]))}
                                  for r in conn.execute("SELECT * FROM daily WHERE batch_id=?", (batch,))]}

    def test_initial_distinguishes_rows_ids_and_findings(self):
        conn, check = self.build(*fixture(ROOT / "testdaten/batch_scenarios/01_initial"))
        packet = self.packet(conn)
        check(packet, "01_initial")
        summary = packet["summary"]
        self.assertEqual((summary["total_payment_records"], summary["distinct_payment_count"],
                          summary["valid_payment_count"], summary["invalid_payment_transactions"],
                          summary["total_validation_errors"]), (5, 4, 1, 3, 4))
        daily = packet["daily_reports"][0]
        self.assertEqual(daily["duplicate_payment_id_count"], 1)
        self.assertEqual(daily["valid_net_amount_eur"], 9800)  # Adapter units are cents.

    def test_updated_snapshot(self):
        conn, check = self.build(*fixture(ROOT / "testdaten/batch_scenarios/02_updated"))
        packet = self.packet(conn)
        check(packet, "02_updated")
        self.assertEqual(packet["summary"]["validation_pass_rate_pct"], Decimal("50.00"))
        self.assertEqual(packet["daily_reports"][0]["valid_net_amount_eur"], 14700)

    def test_original_fixture_daily_totals_reconcile(self):
        conn, check = self.build(*fixture(ROOT / "testdaten", "sample_stripe_transactions_large.csv",
                                          "sample_bank_transactions_large.csv"))
        packet = self.packet(conn)
        check(packet)
        summary = packet["summary"]
        self.assertEqual((summary["total_payment_records"], summary["total_bank_records"],
                          summary["valid_payment_count"], summary["invalid_payment_transactions"],
                          summary["total_validation_errors"]), (50, 40, 20, 25, 46))
        self.assertEqual(summary["validation_pass_rate_pct"], Decimal("44.44"))

    def test_bank_only_findings_are_separate_from_payment_daily_errors(self):
        p, b = fixture(ROOT / "testdaten/batch_scenarios/01_initial")
        b += [booking("fixture", 4, "txn_999"), booking("fixture", 5, None)]
        conn, check = self.build(p, b)
        packet = self.packet(conn)
        check(packet)
        self.assertEqual(packet["summary"]["total_validation_errors"], 6)
        self.assertEqual(packet["summary"]["bank_error_rows"], 2)
        self.assertEqual(packet["daily_reports"][0]["payment_error_row_count"], 4)

    def test_batches_do_not_mix_in_summary_or_reporting_key(self):
        p1, b1 = fixture(ROOT / "testdaten/batch_scenarios/01_initial")
        p2, b2 = fixture(ROOT / "testdaten/batch_scenarios/02_updated")
        change = lambda rows, batch: [(batch, *r[1:]) for r in rows]
        conn, check = self.build(change(p1, "first") + change(p2, "second"),
                                 change(b1, "first") + change(b2, "second"))
        first, second = self.packet(conn, "first"), self.packet(conn, "second")
        check(first, "01_initial")
        check(second, "02_updated")
        self.assertNotEqual(first["daily_reports"][0]["reporting_key"],
                            second["daily_reports"][0]["reporting_key"])

    def test_unknown_status_is_visible_as_unclassified(self):
        conn, check = self.build([payment("a", 1, "txn_1", status="cancelled")],
                                 [booking("a", 1, "txn_1")])
        packet = self.packet(conn, "a")
        check(packet)
        self.assertEqual(packet["summary"]["unclassified_payment_count"], 1)
        self.assertEqual(packet["summary"]["validation_pass_rate_pct"], Decimal("0"))

    def test_duplicate_bank_match_does_not_double_count_daily_payment_or_net(self):
        conn, check = self.build([payment("a", 1, "txn_1")],
                                 [booking("a", 1, "txn_1"), booking("a", 2, "txn_1")])
        packet = self.packet(conn, "a")
        check(packet)
        self.assertEqual(packet["summary"]["valid_records"], 2)
        self.assertEqual(packet["summary"]["valid_payment_count"], 1)
        self.assertEqual(packet["daily_reports"][0]["valid_net_amount_eur"], 9800)

    def test_rounding_uses_half_up_like_spark_round(self):
        p = [payment("a", i, f"txn_{i}", status="available" if i == 1 else "pending")
             for i in range(1, 33)]
        conn, check = self.build(p, [booking("a", 1, "txn_1")])
        packet = self.packet(conn, "a")
        check(packet)
        self.assertEqual(packet["summary"]["validation_pass_rate_pct"], Decimal("3.13"))

    def test_package_checker_rejects_inconsistent_results(self):
        conn, check = self.build(*fixture(ROOT / "testdaten/batch_scenarios/01_initial"))
        original = self.packet(conn)
        for case in ("count", "rate", "batch", "overlap", "daily", "daily_net", "daily_rate", "unclassified"):
            with self.subTest(case=case):
                packet = copy.deepcopy(original)
                if case == "count":
                    packet["summary"]["valid_records"] = 2
                elif case == "rate":
                    packet["summary"]["validation_pass_rate_pct"] = Decimal("20")
                elif case == "batch":
                    packet["findings"][0]["batch_id"] = "wrong"
                elif case == "overlap":
                    packet["findings"][0]["transaction_id"] = "txn_101"
                elif case == "daily":
                    packet["daily_reports"][0]["payment_error_row_count"] = 99
                elif case == "daily_net":
                    packet["daily_reports"][0]["valid_net_amount_eur"] = 19600
                elif case == "daily_rate":
                    packet["daily_reports"][0]["validation_pass_rate_pct"] = Decimal("20")
                else:
                    packet["summary"]["unclassified_payment_count"] = 99
                with self.assertRaises(ValueError):
                    check(packet)

    def test_oversized_package_is_rejected(self):
        conn, check = self.build(*fixture(ROOT / "testdaten/batch_scenarios/01_initial"))
        packet = self.packet(conn)
        packet["findings"] = [packet["findings"][0]] * 10001
        with self.assertRaisesRegex(ValueError, "10,000-row"):
            check(packet)

    def test_bank_only_batch_has_no_misleading_zero_division_rate(self):
        conn, check = self.build([], [booking("a", 1, "txn_1")])
        packet = self.packet(conn, "a")
        check(packet)
        self.assertIsNone(packet["summary"]["validation_pass_rate_pct"])
        self.assertEqual(packet["daily_reports"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
