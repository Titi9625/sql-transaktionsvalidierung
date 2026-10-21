"""Portable parameter/selection checks; no Databricks Jobs or Delta execution.

Exercise the actual notebook selection helpers against an SQLite registry.
Cloud task-value resolution, permissions and serverless execution need a real job.
"""
import ast
import os
from pathlib import Path
import sqlite3
from types import SimpleNamespace
import unittest

ROOT = Path(os.environ.get('PORTFOLIO_TEST_ROOT', Path(__file__).resolve().parents[1]))
NOTEBOOKS = Path(os.environ.get('DATABRICKS_JOB_NOTEBOOK_DIR', ROOT / 'databricks/notebooks'))
DOWNSTREAM = ('02_prepare_silver.py', '03_validate_silver.py', '04_report_gold.py')
INITIAL, UPDATED = 'a' * 64, 'b' * 64


def helpers(filename):
    tree = ast.parse((NOTEBOOKS / filename).read_text(encoding='utf-8'))
    functions = [n for n in tree.body if isinstance(n, ast.FunctionDef)
                 and n.name in {'select_ready_delivery', 'parse_input_folder', 'parse_expected_scenario'}]
    namespace = {}
    exec(compile(ast.Module(body=functions, type_ignores=[]), filename, 'exec'), namespace)
    return namespace


class RegistrySpark:
    """Execute selection SQL on a small real registry; refuse all writes."""
    def __init__(self, rows):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.execute('CREATE TABLE ingestion_batches ('
            'batch_id TEXT, payment_row_count INTEGER, bank_row_count INTEGER, status TEXT)')
        self.conn.executemany('INSERT INTO ingestion_batches VALUES (?, 5, ?, ?)', rows)
        self.calls = []

    def sql(self, query, args=None):
        self.calls.append((query, args))
        if not query.lstrip().startswith('SELECT'):
            raise AssertionError('No stage writes are allowed in a parameter check')
        query = query.replace('workspace.reconciliation_bronze.ingestion_batches', 'ingestion_batches')
        result = self.conn.execute(query, args or {}).fetchall()
        return SimpleNamespace(collect=lambda: [dict(row) for row in result])


class JobParameters(unittest.TestCase):
    def registry(self, rows):
        spark = RegistrySpark(rows)
        self.addCleanup(spark.conn.close)
        return spark

    def test_input_folder_accepts_existing_volume_delivery_paths(self):
        parse = helpers('01_ingest_bronze.py')['parse_input_folder']
        base = '/Volumes/workspace/reconciliation_bronze/incoming_files/'
        for scenario in ('01_initial', '02_updated', '03_broken_format', 'new_delivery'):
            self.assertEqual(parse(' ' + base + scenario + '/ '), base + scenario)

    def test_input_folder_rejects_wrong_volume_and_traversal(self):
        parse = helpers('01_ingest_bronze.py')['parse_input_folder']
        base = '/Volumes/workspace/reconciliation_bronze/incoming_files'
        for path in ('', 'C:\\Downloads', '/Volumes/other/a', base, base + '/../other',
                     base + '/01/../02', base + '/./01', base + '//01',
                     base + '/01\\other', base + '/01\x00bad'):
            with self.subTest(path=path), self.assertRaises(ValueError):
                parse(path)

    def test_all_stages_select_requested_older_delivery_not_another_ready_one(self):
        for notebook in DOWNSTREAM:
            with self.subTest(notebook=notebook):
                spark = self.registry([(UPDATED, 4, 'ready'), (INITIAL, 3, 'ready')])
                selected = helpers(notebook)['select_ready_delivery'](spark, INITIAL)
                self.assertEqual(selected['batch_id'], INITIAL)
                self.assertEqual(selected['bank_row_count'], 3)
                self.assertEqual(spark.calls[0][1], {'batch': INITIAL})

    def test_blank_invalid_and_unresolved_batch_ids_fail_before_database_access(self):
        for notebook in DOWNSTREAM:
            select = helpers(notebook)['select_ready_delivery']
            for label in ('', ' ', 'a' * 63, 'A' * 64, 'z' * 64,
                          '{{tasks.ingest.values.batch_id}}', "' OR 1=1 --"):
                with self.subTest(notebook=notebook, label=label), self.assertRaises(ValueError):
                    select(None, label)

    def test_missing_failed_and_loading_delivery_cannot_fall_back_to_ready(self):
        for notebook in DOWNSTREAM:
            for status in ('missing', 'registered', 'loading', 'failed'):
                with self.subTest(notebook=notebook, status=status):
                    rows = [(UPDATED, 4, 'ready')]
                    if status != 'missing':
                        rows.append((INITIAL, 3, status))
                    spark = self.registry(rows)
                    with self.assertRaisesRegex(ValueError, 'exactly one ready'):
                        helpers(notebook)['select_ready_delivery'](spark, INITIAL)

    def test_duplicate_ready_registry_rows_are_rejected(self):
        for notebook in DOWNSTREAM:
            with self.subTest(notebook=notebook):
                spark = self.registry([(INITIAL, 3, 'ready'), (INITIAL, 3, 'ready')])
                with self.assertRaisesRegex(ValueError, 'exactly one ready'):
                    helpers(notebook)['select_ready_delivery'](spark, INITIAL)

    def test_scenario_none_skips_only_fixed_example_answers(self):
        for notebook in DOWNSTREAM[1:]:
            parse = helpers(notebook)['parse_expected_scenario']
            self.assertIsNone(parse('none'))
            self.assertEqual(parse('01_initial'), '01_initial')
            self.assertEqual(parse('02_updated'), '02_updated')
            for value in ('', 'None', '03_broken_format', 'unknown'):
                with self.subTest(notebook=notebook, value=value), self.assertRaises(ValueError):
                    parse(value)

    def test_notebook_entry_rejects_missing_selected_id_before_stage_writes(self):
        for notebook in DOWNSTREAM:
            source = (NOTEBOOKS / notebook).read_text(encoding='utf-8')
            configuration = source.split('# END JOB PARAMETERS', 1)[0]
            widgets = SimpleNamespace(text=lambda *args: None,
                get=lambda key: '01_initial' if key == 'expected_scenario' else '')
            spark = self.registry([(UPDATED, 4, 'ready')])
            with self.subTest(notebook=notebook), self.assertRaises(ValueError):
                exec(compile(configuration, notebook, 'exec'),
                     {'spark': spark, 'dbutils': SimpleNamespace(widgets=widgets)})
            self.assertEqual(spark.calls, [])


if __name__ == '__main__':
    unittest.main()
