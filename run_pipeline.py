"""Run the CSV imports and dbt checks using one shared .env configuration.

Requires the raw tables and import_runs table to have been created already.
Each import commits independently; this is not one transaction for all steps.
"""

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parent


def execute_steps(steps):
    for number, (label, command) in enumerate(steps, start=1):
        print(f"\n[{number}/{len(steps)}] {label}", flush=True)
        try:
            subprocess.run(command, cwd=ROOT, check=True)
        except subprocess.CalledProcessError as error:
            print(f"Pipeline stopped: {label} failed (exit {error.returncode}).",
                  file=sys.stderr, flush=True)
            return 1
        except OSError:
            print(f"Pipeline stopped: could not start {label}.",
                  file=sys.stderr, flush=True)
            return 1

    print("\nPipeline completed successfully.", flush=True)
    return 0


def main():
    from dotenv import dotenv_values

    project = ROOT / "dbt_validation_pipeline"
    dbt = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    required = [
        ROOT / ".env", dbt, project / "dbt_project.yml",
        ROOT / "python_import" / "import_payment_csv_large.py",
        ROOT / "python_import" / "import_bank_csv_large.py",
        ROOT / "testdaten" / "sample_stripe_transactions_large.csv",
        ROOT / "testdaten" / "sample_bank_transactions_large.csv",
    ]
    for path in required:
        if not path.is_file():
            print(f"Required file missing: {path}", file=sys.stderr)
            return 1

    settings = dotenv_values(ROOT / ".env")
    for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD"):
        if not settings.get(key):
            print(f"Missing setting in .env: {key}", file=sys.stderr)
            return 1
    try:
        port = int(settings["DB_PORT"])
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        print("DB_PORT must be an integer from 1 to 65535.", file=sys.stderr)
        return 1

    # JSON is valid YAML. This safely quotes passwords and other string values.
    # The temporary dbt profile uses the same settings as the Python importers.
    profile = {
        "bachelorarbeit_validation": {
            "target": "dev",
            "outputs": {"dev": {
                "type": "postgres",
                "host": settings["DB_HOST"],
                "port": port,
                "dbname": settings["DB_NAME"],
                "user": settings["DB_USER"],
                "password": settings["DB_PASSWORD"],
                "schema": "public",
                "threads": 1,
                "connect_timeout": 10,
            }},
        }
    }

    with TemporaryDirectory(prefix="validation_dbt_") as profile_dir:
        (Path(profile_dir) / "profiles.yml").write_text(
            json.dumps(profile), encoding="utf-8"
        )
        dbt_options = ["--project-dir", str(project), "--profiles-dir", profile_dir]
        steps = [
            ("Check dbt connection", [str(dbt), "debug", *dbt_options]),
            ("Import payment CSV", [sys.executable, str(required[3])]),
            ("Import bank CSV", [sys.executable, str(required[4])]),
            ("Build dbt models and run tests", [str(dbt), "build", *dbt_options]),
        ]
        return execute_steps(steps)


if __name__ == "__main__":
    raise SystemExit(main())
