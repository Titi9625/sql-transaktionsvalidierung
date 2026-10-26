"""Portable checks of actual notebook SQL, with SQLite as a logic adapter.

Prepared monetary fields use INTEGER cents in this adapter, so EUR 0.01 is 1.
Only qualified table names and that threshold are adapted. These checks do not
run Spark, validate Spark DECIMAL typing, or prove Databricks cloud execution.
The PostgreSQL reference SQL is compared separately within each batch.
"""
import ast
from collections import Counter
import csv
from decimal import Decimal
import os
from pathlib import Path
import re
import sqlite3
import unittest

ROOT = Path(os.environ.get("PORTFOLIO_TEST_ROOT", Path(__file__).resolve().parents[1]))
NOTEBOOK = Path(os.environ.get(
    "DATABRICKS_VALIDATION_NOTEBOOK_UNDER_TEST",
    ROOT / "databricks/notebooks/03_validate_silver.py",
))


def notebook_queries():
    result = {}
    for node in ast.parse(NOTEBOOK.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in {"findings_sql", "valid_sql"}:
                    result[target.id] = node.value.value
    if set(result) != {"findings_sql", "valid_sql"}:
        raise ValueError("Actual notebook SQL definitions were not found.")
    return result


def adapt(query):
    return (query.replace("workspace.reconciliation_silver.stg_payment_transactions", "prepared_payments")
            .replace("workspace.reconciliation_silver.stg_bank_transactions", "prepared_bank")
            .replace("workspace.reconciliation_silver.invalid_transactions", "findings")
            .replace("CAST(0.01 AS DECIMAL(18,2))", "1"))


def payment(batch, position, tid, gross=10000, net=9800, currency="EUR",
            status="available", kind="charge", fee=200):
    return (batch, position, tid, "2026-10-01", currency, gross, fee, net, status, kind)


def booking(batch, position, tid, amount=9800):
    return (batch, position, f"bank_{position}", "2026-10-01", "source reference",
            tid, amount, "EUR")


def database(payments, banks):
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE prepared_payments (
            batch_id TEXT, source_row_number INTEGER, transaction_id TEXT,
            transaction_date TEXT, currency TEXT, amount_gross INTEGER,
            fee_amount INTEGER, amount_net INTEGER, status TEXT, transaction_type TEXT
        );
        CREATE TABLE prepared_bank (
            batch_id TEXT, source_row_number INTEGER, bank_transaction_id TEXT,
            booking_date TEXT, reference_text TEXT, matched_transaction_id TEXT,
            bank_amount INTEGER, currency TEXT
        );
    """)
    conn.executemany("INSERT INTO prepared_payments VALUES (?,?,?,?,?,?,?,?,?,?)", payments)
    conn.executemany("INSERT INTO prepared_bank VALUES (?,?,?,?,?,?,?,?)", banks)
    return conn


def result_sets(conn):
    findings = Counter(conn.execute(
        "SELECT transaction_id, record_source, error_code, error_field FROM findings"
    ).fetchall())
    valid = Counter(conn.execute(
        "SELECT transaction_id, transaction_date, amount_gross, fee_amount, "
        "amount_net, bank_amount, currency, status, validation_status FROM valid"
    ).fetchall())
    return findings, valid


def fixture(folder, payment_name="payments.csv", bank_name="bank.csv"):
    with (folder / payment_name).open(newline="", encoding="utf-8-sig") as stream:
        payments = []
        for pos, row in enumerate(csv.DictReader(stream), 1):
            integer = lambda key: int(row[key]) if row[key].strip() else None
            record = list(payment("fixture", pos, row["provider_transaction_id"],
                                  integer("amount"), integer("net"), row["currency"].upper(),
                                  row["status"], row["type"], integer("fee")))
            record[3] = row["created"]
            payments.append(tuple(record))
    with (folder / bank_name).open(newline="", encoding="utf-8-sig") as stream:
        banks = []
        for pos, row in enumerate(csv.DictReader(stream), 1):
            match = re.search(r"(txn_[0-9]+|pp_[0-9]+)", row["reference_text"])
            amount = int(Decimal(row["amount"]) * 100) if row["amount"].strip() else None
            banks.append(("fixture", pos, row["bank_transaction_id"], row["booking_date"],
                          row["reference_text"], match[0] if match else None,
                          amount, row["currency"].upper()))
    return payments, banks


class DatabricksValidationRules(unittest.TestCase):
    def execute_rules(self, payments, banks):
        conn = database(payments, banks)
        self.addCleanup(conn.close)
        queries = notebook_queries()
        conn.execute("CREATE VIEW findings AS " + adapt(queries["findings_sql"]))
        conn.execute("CREATE VIEW valid AS " + adapt(queries["valid_sql"]))
        return conn

    def assert_reference_parity(self, payments, banks):
        # The reference intentionally selects one batch at a time.
        for batch in sorted({r[0] for r in payments + banks}):
            p = [r for r in payments if r[0] == batch]
            b = [r for r in banks if r[0] == batch]
            actual = self.execute_rules(p, b)
            reference = database(p, b)
            self.addCleanup(reference.close)
            directory = ROOT / "dbt_validation_pipeline/models/validation"
            replacements = {
                "dbt_stg_payment_transactions": "prepared_payments",
                "dbt_stg_bank_transactions": "prepared_bank",
                "dbt_invalid_transactions": "findings",
            }
            for model, view in (("dbt_invalid_transactions.sql", "findings"),
                                ("dbt_valid_transactions.sql", "valid")):
                sql = (directory / model).read_text(encoding="utf-8")
                sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}",
                             lambda match: replacements[match[1]], sql)
                sql = sql.replace("0.01", "1")
                reference.execute(f"CREATE VIEW {view} AS " + sql)
            self.assertEqual(result_sets(actual), result_sets(reference), batch)

    def test_initial_exact_findings_and_valid_payment(self):
        p, b = fixture(ROOT / "testdaten/batch_scenarios/01_initial")
        conn = self.execute_rules(p, b)
        self.assertEqual(Counter(conn.execute("SELECT error_code FROM findings").fetchall()),
                         Counter({("duplicate_transaction_id",): 2,
                                  ("pending_transaction",): 1, ("amount_mismatch",): 1}))
        self.assertEqual(conn.execute("SELECT transaction_id FROM valid").fetchall(), [("txn_101",)])
        self.assertEqual(conn.execute(
            "SELECT source_row_number FROM findings WHERE error_code='duplicate_transaction_id' "
            "ORDER BY source_row_number").fetchall(), [(3,), (4,)])
        self.assert_reference_parity(p, b)

    def test_updated_snapshot_matches_reference(self):
        p, b = fixture(ROOT / "testdaten/batch_scenarios/02_updated")
        conn = self.execute_rules(p, b)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0], 3)
        self.assertEqual(conn.execute("SELECT transaction_id FROM valid ORDER BY transaction_id").fetchall(),
                         [("txn_101",), ("txn_102",)])
        self.assert_reference_parity(p, b)

    def test_original_thesis_fixture_matches_reference(self):
        p, b = fixture(ROOT / "testdaten", "sample_stripe_transactions_large.csv",
                       "sample_bank_transactions_large.csv")
        conn = self.execute_rules(p, b)
        self.assertEqual((len(p), len(b)), (50, 40))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0], 46)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM valid").fetchone()[0], 20)
        self.assertEqual(conn.execute(
            "SELECT COUNT(DISTINCT transaction_id) FROM findings WHERE record_source='payment_provider'"
        ).fetchone()[0], 25)
        self.assert_reference_parity(p, b)

    def test_all_nine_rules_match_reference(self):
        p = [payment("a", 1, "dup"), payment("a", 2, "dup"),
             payment("a", 3, "missing", gross=None),
             payment("a", 4, "currency", currency="USD"),
             payment("a", 5, "pending", status="pending"),
             payment("a", 6, "refund", kind="refund"),
             payment("a", 7, "mismatch"), payment("a", 8, "no_bank")]
        b = [booking("a", 1, "dup"), booking("a", 2, "missing"),
             booking("a", 3, "currency"), booking("a", 4, "mismatch", 9798),
             booking("a", 5, "orphan"), booking("a", 6, None)]
        conn = self.execute_rules(p, b)
        codes = Counter(dict(conn.execute("SELECT error_code, COUNT(*) FROM findings GROUP BY error_code")))
        self.assertEqual(codes, Counter({
            "duplicate_transaction_id": 2, "missing_amount": 1, "invalid_currency": 1,
            "pending_transaction": 1, "refund_transaction": 1, "amount_mismatch": 1,
            "missing_bank_match": 1, "unmatched_bank_transaction": 1,
            "bank_reference_id_not_extractable": 1,
        }))
        self.assert_reference_parity(p, b)

    def test_batch_isolation_for_duplicates_joins_and_error_exclusion(self):
        p = [payment("a", 1, "same"), payment("b", 1, "same"),
             payment("c", 1, "other")]
        b = [booking("a", 1, "same"), booking("b", 1, "same", 9700),
             booking("d", 1, "other")]
        conn = self.execute_rules(p, b)
        self.assertEqual(conn.execute("SELECT batch_id, transaction_id FROM valid").fetchall(), [("a", "same")])
        self.assertEqual(set(conn.execute("SELECT batch_id, error_code FROM findings")),
                         {("b", "amount_mismatch"), ("c", "missing_bank_match"),
                          ("d", "unmatched_bank_transaction")})
        self.assert_reference_parity(p, b)

    def test_one_cent_boundary_including_negative_amounts(self):
        p = [payment("a", 1, "one"), payment("a", 2, "two"),
             payment("a", 3, "negative", gross=-10000, net=-9800)]
        b = [booking("a", 1, "one", 9801), booking("a", 2, "two", 9802),
             booking("a", 3, "negative", -9801)]
        conn = self.execute_rules(p, b)
        self.assertEqual(set(conn.execute("SELECT transaction_id FROM valid")), {("one",), ("negative",)})
        self.assertEqual(conn.execute("SELECT transaction_id, error_code FROM findings").fetchall(),
                         [("two", "amount_mismatch")])
        self.assert_reference_parity(p, b)

    def test_missing_zero_and_multiple_findings_are_distinct(self):
        p = [payment("a", 1, "zero", gross=0, net=0, fee=None),
             payment("a", 2, "multi", gross=None, currency="USD"),
             payment("a", 3, "missing_net", net=None)]
        b = [booking("a", 1, "zero", 0), booking("a", 2, "multi"),
             booking("a", 3, "missing_net")]
        conn = self.execute_rules(p, b)
        self.assertEqual(conn.execute("SELECT transaction_id FROM valid").fetchall(), [("zero",)])
        self.assertEqual(Counter(conn.execute("SELECT transaction_id, error_code FROM findings")),
                         Counter({("multi", "missing_amount"): 1, ("multi", "invalid_currency"): 1,
                                  ("missing_net", "missing_amount"): 1}))
        self.assert_reference_parity(p, b)

    def test_duplicate_bank_combinations_preserve_source_lineage(self):
        p = [payment("a", 1, "duplicate_bank")]
        b = [booking("a", 1, "duplicate_bank", 9700),
             booking("a", 2, "duplicate_bank", 9600)]
        conn = self.execute_rules(p, b)
        self.assertEqual(conn.execute(
            "SELECT related_bank_row_number FROM findings ORDER BY related_bank_row_number"
        ).fetchall(), [(1,), (2,)])
        self.assert_reference_parity(p, b)

    def test_existing_null_currency_and_unknown_status_limits_are_preserved(self):
        # No new rules are silently invented when porting the thesis logic.
        p = [payment("a", 1, "null_currency", currency=None),
             payment("a", 2, "unknown_status", status="cancelled")]
        b = [booking("a", 1, "null_currency"), booking("a", 2, "unknown_status")]
        conn = self.execute_rules(p, b)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM findings").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM valid").fetchone()[0], 0)
        self.assert_reference_parity(p, b)


if __name__ == "__main__":
    unittest.main(verbosity=2)
