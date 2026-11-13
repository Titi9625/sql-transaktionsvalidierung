"""Exercise the handler locally with the same CSV fixtures used in Azure."""

import json
from pathlib import Path
import unittest
from unittest.mock import Mock

import azure.functions as func

from azure_demo.function_app import validate_bank_csv


FIXTURES = Path(__file__).resolve().parents[1] / "testdaten" / "batch_scenarios"


class BankUploadTests(unittest.TestCase):
    def run_handler(self, scenario: str, filename: str) -> dict:
        incoming = Mock(spec=func.InputStream)
        incoming.name = f"incoming/{filename}"
        incoming.read.return_value = (FIXTURES / scenario / "bank.csv").read_bytes()
        report = Mock(spec=func.Out)

        handler = validate_bank_csv.build().get_user_function()
        handler(incoming, report)

        report.set.assert_called_once()
        return json.loads(report.set.call_args.args[0])

    def test_valid_bank_upload_produces_pass_report(self):
        result = self.run_handler("02_updated", "bank.csv")
        self.assertEqual(
            result, {"file": "bank.csv", "status": "passed", "rows_checked": 4}
        )

    def test_invalid_amount_produces_rejection_report(self):
        result = self.run_handler("03_broken_format", "bank-broken.csv")
        self.assertEqual(result["file"], "bank-broken.csv")
        self.assertEqual(result["status"], "rejected")
        self.assertIn("data row 4, column amount", result["reason"])


if __name__ == "__main__":
    unittest.main()
