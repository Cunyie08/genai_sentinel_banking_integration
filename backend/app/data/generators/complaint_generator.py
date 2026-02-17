from faker import Faker
import random
from datetime import datetime, timedelta
from app.models.complaint import ComplaintCategory, ComplaintStatus, ComplaintPriority

fake = Faker()

# Complaint templates for each category
COMPLAINT_TEMPLATES = {
    ComplaintCategory.ATM_DISPENSE_ERROR: [
        "ATM at {location} dispensed ₦{amount} less than requested",
        "Card was debited but ATM did not dispense cash at {location}",
        "ATM gave me torn notes at {location} branch",
        "Withdrawal failed but account was debited ₦{amount}",
    ],
    ComplaintCategory.CARD_ISSUES: [
        "My ATM card is not working at POS terminals",
        "Card was declined at {merchant} even though I have sufficient balance",
        "Lost my ATM card, need urgent replacement",
        "Card got stuck in ATM at {location}",
    ],
    ComplaintCategory.FRAUD_ALERT: [
        "Unauthorized transaction of ₦{amount} on my account",
        "I did not authorize the transfer to {account_number}",
        "Suspicious debit alert from unknown merchant",
        "Someone is using my card details online",
    ],
    ComplaintCategory.ACCOUNT_ACCESS: [
        "Cannot login to mobile app, password reset not working",
        "Forgot my transaction PIN, need to reset",
        "Account has been locked, cannot access funds",
        "Mobile app keeps crashing when I try to login",
    ],
    ComplaintCategory.TRANSACTION_DISPUTE: [
        "Transfer to {account_number} was not received by beneficiary",
        "Charged twice for same transaction at {merchant}",
        "Reversal not received after failed transaction",
        "Wrong amount debited for {merchant} payment",
    ],
    ComplaintCategory.LOAN_INQUIRY: [
        "Want to apply for personal loan, what are the requirements?",
        "My loan application was rejected, need clarification",
        "Loan repayment deduction is higher than agreed",
        "How can I increase my loan limit?",
    ],
    ComplaintCategory.GENERAL_INQUIRY: [
        "How do I upgrade my account to current account?",
        "What are the charges for international transfers?",
        "Need statement of account for the past 6 months",
        "How can I link my BVN to my account?",
    ],
    ComplaintCategory.TECHNICAL_ISSUE: [
        "USSD code *123# is not responding",
        "Cannot receive OTP for transactions",
        "App is showing error 'Service temporarily unavailable'",
        "Internet banking portal is very slow",
    ],
}

# Messy variations (typos, informal language, incomplete info)
MESSY_VARIATIONS = [
    lambda text: text.lower(),  # all lowercase
    lambda text: text.upper(),  # all uppercase
    lambda text: text.replace("₦", "N"),  # inconsistent currency
    lambda text: text + " pls help urgent!!!",  # urgency markers
    lambda text: text + " abeg",  # pidgin
    lambda text: text.replace(".", ""),  # missing punctuation
]


def generate_ticket_id():
    """Generate ticket ID in format TKT-YYYYMMDD-XXXX"""
    import uuid

    date_part = datetime.now().strftime("%Y%m%d")
    unique_part = str(uuid.uuid4())[:8].upper()
    return f"TKT-{date_part}-{unique_part}"


def generate_complaints(customer_ids, num_complaints=2000):
    """Generate synthetic complaint data with messy, realistic text"""
    complaints = []

    for i in range(num_complaints):
        customer_id = random.choice(customer_ids)
        category = random.choice(list(ComplaintCategory))

        # Select template and fill in details
        template = random.choice(COMPLAINT_TEMPLATES[category])
        description = template.format(
            location=random.choice(["Ikeja", "Lekki", "Surulere", "VI", "Abuja", "PH"]),
            amount=random.randint(5000, 50000),
            merchant=random.choice(["Shoprite", "Jumia", "Uber", "DStv"]),
            account_number=f"{random.randint(1000000000, 9999999999)}",
        )

        # Apply messy variations (30% chance)
        if random.random() < 0.3:
            description = random.choice(MESSY_VARIATIONS)(description)

        # Determine priority based on category
        if category in [
            ComplaintCategory.FRAUD_ALERT,
            ComplaintCategory.ATM_DISPENSE_ERROR,
        ]:
            priority = random.choice(
                [ComplaintPriority.HIGH, ComplaintPriority.CRITICAL]
            )
        elif category == ComplaintCategory.GENERAL_INQUIRY:
            priority = ComplaintPriority.LOW
        else:
            priority = ComplaintPriority.MEDIUM

        # Determine status (most are open or in progress for demo)
        status = random.choice(
            [
                ComplaintStatus.OPEN,
                ComplaintStatus.OPEN,
                ComplaintStatus.IN_PROGRESS,
                ComplaintStatus.RESOLVED,
                ComplaintStatus.ESCALATED,
            ]
        )

        # Generate timestamps
        created_at = fake.date_time_between(start_date="-3M", end_date="now")

        complaint = {
            "ticket_id": generate_ticket_id(),
            "customer_id": customer_id,
            "category": category.value,
            "subject": f"{category.value.replace('_', ' ').title()} - {random.choice(['Urgent', 'Help Needed', 'Issue', 'Problem'])}",
            "description": description,
            "assigned_department": None,  # Will be assigned by AI
            "assigned_agent": None,
            "priority": priority.value,
            "status": status.value,
            "resolution_notes": (
                "Resolved via phone call"
                if status == ComplaintStatus.RESOLVED
                else None
            ),
            "created_at": created_at,
            "updated_at": (
                created_at + timedelta(hours=random.randint(1, 48))
                if status != ComplaintStatus.OPEN
                else None
            ),
            "resolved_at": (
                created_at + timedelta(days=random.randint(1, 7))
                if status == ComplaintStatus.RESOLVED
                else None
            ),
        }
        complaints.append(complaint)

    return complaints


if __name__ == "__main__":
    # Test generation
    test_customer_ids = [1, 2, 3, 4, 5]
    complaints = generate_complaints(test_customer_ids, 10)
    for complaint in complaints[:3]:
        print(f"\nTicket: {complaint['ticket_id']}")
        print(f"Category: {complaint['category']}")
        print(f"Description: {complaint['description']}")
        print(f"Priority: {complaint['priority']}")
