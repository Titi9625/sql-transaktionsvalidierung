"""Check recovery-test routing and fault injection without Spark or Delta."""
import ast
import os
from pathlib import Path
import re
import unittest

ROOT = Path(os.environ.get("PORTFOLIO_TEST_ROOT", Path(__file__).resolve().parents[1]))
SOURCE = Path(os.environ.get("DATABRICKS_RECOVERY_CHECK_UNDER_TEST",
                             ROOT / "databricks/checks/check_recovery_cell.py"))


def routing_classes():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    classes = [node for node in tree.body if isinstance(node, ast.ClassDef)]
    namespace = {"re": re}
    exec(compile(ast.Module(body=classes, type_ignores=[]), str(SOURCE), "exec"), namespace)
    return namespace["RecoverySparkProxy"], namespace["SimulatedBankWriteFailure"]


class RecordingSpark:
    def __init__(self):
        self.calls = []

    def table(self, name):
        self.calls.append(("table", name))
        return name

    def createDataFrame(self, *args, **kwargs):
        self.calls.append(("frame", args, kwargs))
        return "frame"

    def sql(self, query, *args, **kwargs):
        self.calls.append(("sql", query, args, kwargs))
        return "result"


class RecoveryRouting(unittest.TestCase):
    def setUp(self):
        self.Proxy, self.Fault = routing_classes()
        self.real = RecordingSpark()
        self.mapping = {f"workspace.reconciliation_bronze.{name}":
                        f"workspace.reconciliation_checks.{name}_0123456789ab"
                        for name in ("ingestion_batches", "raw_payment_history", "raw_bank_history")}
        self.proxy = self.Proxy(self.real, self.mapping)

    def test_table_reads_go_only_to_checks(self):
        for original, private in self.mapping.items():
            self.assertEqual(self.proxy.table(original), private)
        count = len(self.real.calls)
        with self.assertRaises(ValueError):
            self.proxy.table("workspace.reconciliation_gold.validation_run_history")
        self.assertEqual(len(self.real.calls), count)

    def test_unsafe_or_mismatched_targets_are_rejected(self):
        for case in ("main", "missing", "different_suffix", "wrong_stem"):
            with self.subTest(case=case):
                mapping = dict(self.mapping)
                original = "workspace.reconciliation_bronze.raw_bank_history"
                if case == "main":
                    mapping[original] = original
                elif case == "missing":
                    mapping.pop(original)
                elif case == "different_suffix":
                    mapping[original] = mapping[original][:-12] + "bbbbbbbbbbbb"
                else:
                    mapping[original] = "workspace.reconciliation_checks.ingestion_batches_0123456789ab"
                with self.assertRaises(ValueError):
                    self.Proxy(self.real, mapping)
        self.assertEqual(self.real.calls, [])

    def test_metadata_update_and_payment_merge_are_redirected(self):
        registry = "workspace.reconciliation_bronze.ingestion_batches"
        query = f"UPDATE {registry} SET status='failed' WHERE batch_id=:batch"
        self.proxy.sql(query, args={"batch": "example"})
        self.assertIn(self.mapping[registry], self.real.calls[-1][1])
        self.assertEqual(self.real.calls[-1][3], {"args": {"batch": "example"}})
        pay = "workspace.reconciliation_bronze.raw_payment_history"
        self.proxy.sql(f"MERGE INTO {pay} AS target USING _bronze_source_rows AS incoming ON 1=0 WHEN NOT MATCHED THEN INSERT *")
        self.assertIn(self.mapping[pay], self.real.calls[-1][1])
        self.assertNotIn("workspace.reconciliation_bronze", self.real.calls[-1][1])

    def test_one_bank_merge_is_blocked_then_retry_reaches_only_checks(self):
        bank = "workspace.reconciliation_bronze.raw_bank_history"
        query = f"MERGE INTO {bank} AS target USING _bronze_source_rows AS incoming ON 1=0 WHEN NOT MATCHED THEN INSERT *"
        with self.assertRaises(self.Fault):
            self.proxy.sql(query)
        self.assertEqual(self.real.calls, [])
        self.assertEqual(self.proxy.sql(query), "result")
        self.assertIn(self.mapping[bank], self.real.calls[-1][1])
        self.assertEqual(self.proxy.blocked_bank_writes, 1)

    def test_unexpected_operations_and_unmapped_references_never_execute(self):
        registry = "workspace.reconciliation_bronze.ingestion_batches"
        for query in ("DROP TABLE workspace.reconciliation_bronze.raw_payment_history",
                      "UPDATE workspace.other_schema.other_table SET value=1",
                      f"UPDATE {registry} SET status=(SELECT status FROM workspace.reconciliation_bronze.unknown_table)"):
            with self.subTest(query=query), self.assertRaises(ValueError):
                self.proxy.sql(query)
        self.assertEqual(self.real.calls, [])

    def test_dataframe_creation_is_delegated_without_rewriting_values(self):
        rows = [{"batch_id": "x", "source": "original value"}]
        self.assertEqual(self.proxy.createDataFrame(rows, schema="test schema"), "frame")
        self.assertEqual(self.real.calls, [("frame", (rows,), {"schema": "test schema"})])


if __name__ == "__main__":
    unittest.main(verbosity=2)
