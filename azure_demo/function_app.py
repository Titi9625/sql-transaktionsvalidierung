"""Validate new bank CSV uploads and save one JSON report per filename."""

import json
import logging

import azure.functions as func

from azure_demo.check_bank_file import check_bank_file


app = func.FunctionApp()


@app.blob_trigger(
    arg_name="bank_csv",
    path="incoming/{name}.csv",
    connection="BankStorage",
    source="EventGrid",
)
@app.blob_output(
    arg_name="report",
    path="results/{name}.csv.json",
    connection="BankStorage",
)
def validate_bank_csv(bank_csv: func.InputStream, report: func.Out[str]) -> None:
    result = check_bank_file(bank_csv.name, bank_csv.read())
    report.set(json.dumps(result, indent=2))
    logging.info("Bank CSV format check: %s (%s)", result["status"], result["file"])
