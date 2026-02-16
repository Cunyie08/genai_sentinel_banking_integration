from faker import Faker
import random
from datetime import datetime, timedelta
from app.models.transaction import TransactionType, TransactionStatus

fake = Faker()

# Nigerian cities for transaction locations
NIGERIAN_CITIES = [
    "Lagos",
    "Abuja",
    "Port Harcourt",
    "Kano",
    "Ibadan",
    "Kaduna",
    "Benin City",
    "Enugu",
    "Jos",
    "Ilorin",
    "Aba",
    "Onitsha",
    "Warri",
    "Calabar",
    "Akure",
    "Abeokuta",
    "Maiduguri",
    "Zaria",
]

# Common merchants in Nigeria
NIGERIAN_MERCHANTS = [
    "Shoprite",
    "Spar",
    "Jumia",
    "Konga",
    "DStv",
    "GOtv",
    "MTN",
    "Airtel",
    "Glo",
    "9mobile",
    "PHCN/EKEDC",
    "Uber",
    "Bolt",
    "Chicken Republic",
    "Mr Biggs",
    "Domino's Pizza",
    "Filmhouse Cinemas",
    "Genesis Cinemas",
    "Slot",
    "Computer Village",
]


def generate_transaction_ref():
    """Generate unique transaction reference"""
    import uuid

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    unique_id = str(uuid.uuid4())[:6].upper()
    return f"TXN{timestamp}{unique_id}"


def generate_device_id():
    """Generate realistic device ID"""
    devices = ["iPhone", "Samsung", "Tecno", "Infinix", "Xiaomi", "Huawei"]
    return f"{random.choice(devices)}-{fake.uuid4()[:8]}"


def generate_transactions(customer_ids, num_transactions=5000):
    """Generate synthetic transaction data with fraud patterns"""
    transactions = []

    for i in range(num_transactions):
        customer_id = random.choice(customer_ids)
        transaction_type = random.choice(list(TransactionType))

        # Generate realistic amounts based on transaction type
        if transaction_type == TransactionType.DEPOSIT:
            amount = round(random.uniform(1000, 500000), 2)
        elif transaction_type == TransactionType.WITHDRAWAL:
            amount = round(random.uniform(500, 100000), 2)
        elif transaction_type == TransactionType.PAYMENT:
            amount = round(random.uniform(200, 50000), 2)
        elif transaction_type == TransactionType.TRANSFER:
            amount = round(random.uniform(1000, 200000), 2)
        else:  # REVERSAL
            amount = round(random.uniform(500, 50000), 2)

        # Determine if transaction should be flagged (10% suspicious)
        is_suspicious = random.random() < 0.1

        if is_suspicious:
            # Create suspicious patterns
            risk_score = random.randint(6, 10)
            fraud_reasons = [
                "Unusual transaction time (3 AM)",
                "Multiple transactions in short period",
                "Transaction from new location",
                "Amount exceeds daily limit",
                "Unusual merchant category",
            ]
            fraud_reason = random.choice(fraud_reasons)
            status = (
                TransactionStatus.FLAGGED
                if risk_score >= 8
                else TransactionStatus.COMPLETED
            )
        else:
            risk_score = random.randint(0, 5)
            fraud_reason = None
            status = random.choice(
                [TransactionStatus.COMPLETED, TransactionStatus.PENDING]
            )

        # Generate timestamp (within last 6 months)
        created_at = fake.date_time_between(start_date="-6M", end_date="now")

        transaction = {
            "transaction_ref": generate_transaction_ref(),
            "customer_id": customer_id,
            "transaction_type": transaction_type.value,
            "amount": amount,
            "currency": "NGN",
            "location": random.choice(NIGERIAN_CITIES),
            "device_id": generate_device_id(),
            "ip_address": fake.ipv4(),
            "merchant_name": (
                random.choice(NIGERIAN_MERCHANTS)
                if transaction_type == TransactionType.PAYMENT
                else None
            ),
            "status": status.value,
            "created_at": created_at,
            "completed_at": (
                created_at + timedelta(seconds=random.randint(1, 300))
                if status == TransactionStatus.COMPLETED
                else None
            ),
            "is_suspicious": risk_score,
            "fraud_reason": fraud_reason,
        }
        transactions.append(transaction)

    return transactions


if __name__ == "__main__":
    # Test generation
    test_customer_ids = [1, 2, 3, 4, 5]
    transactions = generate_transactions(test_customer_ids, 10)
    for txn in transactions[:3]:
        print(txn)
