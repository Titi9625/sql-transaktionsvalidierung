r"""Integration tests: use only the local portfolio_validation database.

Run from the repository root:
    .\.venv\Scripts\python.exe -m unittest discover -s tests -v

Each test leaves one deliberate failed entry in public.import_runs.
The source CSV files are not changed.
"""

import sys
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

import psycopg2
from psycopg2 import sql
from dotenv import dotenv_values


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python_import"))

import import_payment_csv_large as payment
import import_bank_csv_large as bank


class ImportRollbackTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        settings = dotenv_values(ROOT / ".env")
        if settings.get("DB_NAME") != "portfolio_validation":
            raise RuntimeError("These tests require DB_NAME=portfolio_validation.")

        cls.connection = psycopg2.connect(
            host=settings["DB_HOST"],
            port=settings["DB_PORT"],
            dbname=settings["DB_NAME"],
            user=settings["DB_USER"],
            password=settings["DB_PASSWORD"],
        )
        cls.addClassCleanup(cls.connection.close)
        cls.connection.autocommit = True

    def table_contents(self, table):
        with self.connection.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT * FROM public.{}").format(sql.Identifier(table))
            )
            return Counter(cursor.fetchall())

    def check_rollback(self, importer, table, converter_name, fail_on_call):
        before = self.table_contents(table)
        self.assertGreater(sum(before.values()), 1, "Import the sample CSV first.")

        with self.connection.cursor() as cursor:
            cursor.execute("SELECT COALESCE(MAX(run_id), 0) FROM public.import_runs")
            previous_run_id = cursor.fetchone()[0]

        original_converter = getattr(importer, converter_name)
        calls = 0

        def controlled_failure(value):
            nonlocal calls
            calls += 1
            if calls == fail_on_call:
                raise ValueError("Deliberate rollback integration test")
            return original_converter(value)

        # Fail while preparing the second row, after the first INSERT.
        # The patch is automatically removed when this block exits.
        with patch.object(importer, converter_name, side_effect=controlled_failure):
            with self.assertRaisesRegex(ValueError, "Deliberate rollback"):
                importer.main()

        self.assertEqual(self.table_contents(table), before,
                         "Rollback must restore every original row and value.")

        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT status, rows_imported, error_message,
                       finished_at IS NOT NULL
                FROM public.import_runs
                WHERE run_id > %s AND target_table = %s
                ORDER BY run_id
                """,
                (previous_run_id, table),
            )
            self.assertEqual(cursor.fetchall(), [("failed", 0, "ValueError", True)])

    def test_payment_failure_restores_data_and_logs_failure(self):
        self.check_rollback(payment, "raw_payment_transactions", "to_int", 4)

    def test_bank_failure_restores_data_and_logs_failure(self):
        self.check_rollback(bank, "raw_bank_transactions", "to_decimal", 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
