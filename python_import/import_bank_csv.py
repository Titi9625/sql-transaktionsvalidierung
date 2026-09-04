from pathlib import Path
import os
from decimal import Decimal

import pandas as pd
import psycopg2
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]
CSV_PATH = BASE_DIR / "04_Daten (Data)" / "Beispieldaten (Sample Data)" / "sample_bank_transactions.csv"
ENV_PATH = Path(__file__).resolve().parent / ".env"


def to_decimal(value):
    if pd.isna(value) or str(value).strip() == "":
        return None
    return Decimal(str(value).replace(",", "."))


def main():
    load_dotenv(ENV_PATH)

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    cursor = connection.cursor()

    cursor.execute("TRUNCATE TABLE raw_bank_transactions;")

    insert_sql = """
        INSERT INTO raw_bank_transactions (
            bank_transaction_id,
            booking_date,
            reference_text,
            amount,
            currency,
            counterparty
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    inserted_rows = 0

    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (
                row["bank_transaction_id"],
                row["booking_date"],
                row["reference_text"],
                to_decimal(row["amount"]),
                row["currency"],
                row["counterparty"],
            ),
        )
        inserted_rows += 1

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Bank CSV imported successfully. Rows inserted: {inserted_rows}")


if __name__ == "__main__":
    main()