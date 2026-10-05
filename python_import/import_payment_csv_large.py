from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import dotenv_values

from import_logging import start_import, finish_import, fail_import


BASE_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = BASE_DIR / "testdaten" / "sample_stripe_transactions_large.csv"
ENV_PATH = BASE_DIR / ".env"


def to_int(value):
    if pd.isna(value) or str(value).strip() == "":
        return None
    return int(float(value))


def main():
    settings = dotenv_values(ENV_PATH)

    connection = psycopg2.connect(
        host=settings["DB_HOST"],
        port=settings["DB_PORT"],
        dbname=settings["DB_NAME"],
        user=settings["DB_USER"],
        password=settings["DB_PASSWORD"],
    )

    run_id = None

    try:
        run_id = start_import(
            connection,
            CSV_PATH.name,
            "raw_payment_transactions",
        )

        df = pd.read_csv(CSV_PATH)

        insert_sql = """
            INSERT INTO public.raw_payment_transactions (
                provider_transaction_id, created, currency,
                amount, fee, net, status, type,
                reporting_category, source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        inserted_rows = 0

        with connection.cursor() as cursor:
            cursor.execute(
                "TRUNCATE TABLE public.raw_payment_transactions;"
            )

            for _, row in df.iterrows():
                cursor.execute(
                    insert_sql,
                    (
                        row["provider_transaction_id"],
                        row["created"],
                        row["currency"],
                        to_int(row["amount"]),
                        to_int(row["fee"]),
                        to_int(row["net"]),
                        row["status"],
                        row["type"],
                        row["reporting_category"],
                        row["source"],
                    ),
                )
                inserted_rows += 1

        finish_import(connection, run_id, inserted_rows)
        connection.commit()

    except Exception as error:
        if run_id is not None:
            fail_import(connection, run_id, error)
        else:
            connection.rollback()
        raise

    finally:
        connection.close()

    print(
        f"Payment CSV imported successfully. "
        f"Rows inserted: {inserted_rows}. Run ID: {run_id}"
    )


if __name__ == "__main__":
    main()