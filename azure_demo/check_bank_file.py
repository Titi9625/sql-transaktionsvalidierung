"""Check a bank CSV and print a result a storage job can save."""

import argparse
import json
from pathlib import Path

from python_import.csv_contract import CsvFormatError, read_csv_dataset


def check_bank_file(name: str, content: bytes) -> dict:
    """Use the existing bank format rules on bytes from a file or blob."""
    filename = Path(name).name
    try:
        dataset = read_csv_dataset(filename, "bank", content=content)
    except CsvFormatError as error:
        return {"file": filename, "status": "rejected", "reason": str(error)}
    return {"file": filename, "status": "passed", "rows_checked": len(dataset.rows)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bank_csv", type=Path)
    path = parser.parse_args().bank_csv
    try:
        content = path.read_bytes()
    except OSError as error:
        parser.exit(2, f"Could not open {path.name}: {error.strerror}\n")

    result = check_bank_file(path.name, content)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
