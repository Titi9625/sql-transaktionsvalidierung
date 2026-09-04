from pathlib import Path
import csv
from datetime import date, timedelta


BASE_DIR = Path(__file__).resolve().parents[2]
SAMPLE_DATA_DIR = BASE_DIR / "04_Daten (Data)" / "Beispieldaten (Sample Data)"

PAYMENT_FILE = SAMPLE_DATA_DIR / "sample_stripe_transactions_large.csv"
BANK_FILE = SAMPLE_DATA_DIR / "sample_bank_transactions_large.csv"


def payment_row(
    transaction_id,
    created,
    currency,
    amount,
    fee,
    net,
    status="available",
    transaction_type="charge",
    reporting_category="charge",
    source=None
):
    return {
        "provider_transaction_id": transaction_id,
        "created": created,
        "currency": currency,
        "amount": amount,
        "fee": fee,
        "net": net,
        "status": status,
        "type": transaction_type,
        "reporting_category": reporting_category,
        "source": source or f"ch_{transaction_id.replace('txn_', '')}",
    }


def bank_row(bank_id, booking_date, reference_text, amount, currency="EUR", counterparty="Stripe Payments Europe"):
    return {
        "bank_transaction_id": bank_id,
        "booking_date": booking_date,
        "reference_text": reference_text,
        "amount": amount,
        "currency": currency,
        "counterparty": counterparty,
    }


def make_amounts(index):
    amount = 5000 + (index * 137)
    fee = 150 + (index % 5) * 20
    net = amount - fee
    return amount, fee, net


def main():
    SAMPLE_DATA_DIR.mkdir(parents=True, exist_ok=True)

    payment_rows = []
    bank_rows = []

    start_date = date(2026, 7, 1)

    # 30 normal available EUR transactions
    for i in range(1, 31):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)
        amount, fee, net = make_amounts(i)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount=amount,
                fee=fee,
                net=net,
                status="available",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}",
            )
        )

    # 5 duplicate rows: txn_005 to txn_009 appear again
    for i in range(5, 10):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)
        amount, fee, net = make_amounts(i)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount=amount,
                fee=fee,
                net=net,
                status="available",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}_duplicate",
            )
        )

    # 3 missing amount cases
    for i in range(31, 34):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount="",
                fee=0,
                net="",
                status="available",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}",
            )
        )

    # 3 wrong currency cases
    for i in range(34, 37):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)
        amount, fee, net = make_amounts(i)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="usd",
                amount=amount,
                fee=fee,
                net=net,
                status="available",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}",
            )
        )

    # 3 pending transactions
    for i in range(37, 40):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)
        amount, fee, net = make_amounts(i)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount=amount,
                fee=fee,
                net=net,
                status="pending",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}",
            )
        )

    # 3 refunds
    for i in range(40, 43):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount=-2500,
                fee=0,
                net=-2500,
                status="available",
                transaction_type="refund",
                reporting_category="refund",
                source=f"re_{i:03d}",
            )
        )

    # 3 available payment transactions without bank match
    for i in range(43, 46):
        transaction_id = f"txn_{i:03d}"
        created = start_date + timedelta(days=i % 10)
        amount, fee, net = make_amounts(i)

        payment_rows.append(
            payment_row(
                transaction_id=transaction_id,
                created=created.isoformat(),
                currency="eur",
                amount=amount,
                fee=fee,
                net=net,
                status="available",
                transaction_type="charge",
                reporting_category="charge",
                source=f"ch_{i:03d}",
            )
        )

    # At this point: 30 + 5 + 3 + 3 + 3 + 3 + 3 = 50 payment rows

    # 25 matching bank rows for txn_001 to txn_025
    for i in range(1, 26):
        transaction_id = f"txn_{i:03d}"
        _, _, net = make_amounts(i)
        bank_rows.append(
            bank_row(
                bank_id=f"bank_{i:03d}",
                booking_date=(start_date + timedelta(days=(i % 10) + 1)).isoformat(),
                reference_text=f"STRIPE PAYOUT {transaction_id}",
                amount=f"{net / 100:.2f}",
                currency="EUR",
                counterparty="Stripe Payments Europe",
            )
        )

    # 5 amount mismatch cases for txn_026 to txn_030
    for i in range(26, 31):
        transaction_id = f"txn_{i:03d}"
        _, _, net = make_amounts(i)
        wrong_amount = (net / 100) - 7.50

        bank_rows.append(
            bank_row(
                bank_id=f"bank_{i:03d}",
                booking_date=(start_date + timedelta(days=(i % 10) + 1)).isoformat(),
                reference_text=f"STRIPE PAYOUT {transaction_id}",
                amount=f"{wrong_amount:.2f}",
                currency="EUR",
                counterparty="Stripe Payments Europe",
            )
        )

    # 5 bank transactions without matching payment transaction
    for i in range(900, 905):
        bank_rows.append(
            bank_row(
                bank_id=f"bank_{i}",
                booking_date=(start_date + timedelta(days=i % 10)).isoformat(),
                reference_text=f"STRIPE PAYOUT txn_{i}",
                amount="25.00",
                currency="EUR",
                counterparty="Stripe Payments Europe",
            )
        )

    # 3 PayPal examples, optional extension
    for i in range(1, 4):
        bank_rows.append(
            bank_row(
                bank_id=f"bank_pp_{i:03d}",
                booking_date=(start_date + timedelta(days=i)).isoformat(),
                reference_text=f"PAYPAL PAYOUT pp_{i:03d}",
                amount="60.00",
                currency="EUR",
                counterparty="PayPal Europe",
            )
        )

    # 2 unrelated bank transactions without extractable transaction id
    bank_rows.append(
        bank_row(
            bank_id="bank_fee_001",
            booking_date="2026-07-11",
            reference_text="BANK SERVICE FEE",
            amount="-5.00",
            currency="EUR",
            counterparty="Bank",
        )
    )

    bank_rows.append(
        bank_row(
            bank_id="bank_misc_001",
            booking_date="2026-07-12",
            reference_text="UNKNOWN TRANSFER",
            amount="18.00",
            currency="EUR",
            counterparty="Unknown",
        )
    )

    # At this point: 25 + 5 + 5 + 3 + 2 = 40 bank rows

    payment_fields = [
        "provider_transaction_id",
        "created",
        "currency",
        "amount",
        "fee",
        "net",
        "status",
        "type",
        "reporting_category",
        "source",
    ]

    bank_fields = [
        "bank_transaction_id",
        "booking_date",
        "reference_text",
        "amount",
        "currency",
        "counterparty",
    ]

    with PAYMENT_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=payment_fields)
        writer.writeheader()
        writer.writerows(payment_rows)

    with BANK_FILE.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=bank_fields)
        writer.writeheader()
        writer.writerows(bank_rows)

    print("Large test data created successfully.")
    print(f"Payment file: {PAYMENT_FILE}")
    print(f"Payment rows: {len(payment_rows)}")
    print(f"Bank file: {BANK_FILE}")
    print(f"Bank rows: {len(bank_rows)}")


if __name__ == "__main__":
    main()