"""Package the bank CSV function and its shared checker for Azure deployment."""

import argparse
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


RUNTIME_FILES = {
    "azure_demo/function_app.py": "function_app.py",
    "azure_demo/host.json": "host.json",
    "azure_demo/requirements.txt": "requirements.txt",
    "azure_demo/check_bank_file.py": "azure_demo/check_bank_file.py",
    "python_import/csv_contract.py": "python_import/csv_contract.py",
}


def build_zip(repo: Path, output: Path) -> None:
    for source in RUNTIME_FILES:
        if not (repo / source).is_file():
            raise FileNotFoundError(f"Missing deployment input: {source}")

    with ZipFile(output, "x", compression=ZIP_DEFLATED) as archive:
        for source, target in RUNTIME_FILES.items():
            archive.write(repo / source, target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Path for a new deployment ZIP")
    output = parser.parse_args().output
    repo = Path(__file__).resolve().parents[1]

    try:
        build_zip(repo, output)
    except OSError as error:
        parser.exit(1, f"Could not package function: {error}\n")
    print(f"Deployment ZIP created: {output}")


if __name__ == "__main__":
    main()
