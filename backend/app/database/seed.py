"""
Database seeding script
Populates the database with synthetic Nigerian banking data
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent.parent))

from app.database.connection import engine, Base, SessionLocal
from app.models.customer import Customer
from app.models.transaction import Transaction
from app.models.complaint import Complaint
from app.data.generators.customer_generator import generate_customer_data
from app.data.generators.transaction_generator import generate_transactions
from app.data.generators.complaint_generator import generate_complaints


def create_tables():
    """Create all database tables"""
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("✓ Tables created successfully")


def seed_customers(db, num_customers=1000):
    """Seed customer data"""
    print(f"\nGenerating {num_customers} customers...")
    customer_data = generate_customer_data(num_customers)

    customers = []
    for data in customer_data:
        customer = Customer(**data)
        customers.append(customer)

    db.bulk_save_objects(customers)
    db.commit()
    print(f"✓ {num_customers} customers added to database")

    # Return customer IDs for foreign key relationships
    return [c.id for c in db.query(Customer).all()]


def seed_transactions(db, customer_ids, num_transactions=5000):
    """Seed transaction data"""
    print(f"\nGenerating {num_transactions} transactions...")
    transaction_data = generate_transactions(customer_ids, num_transactions)

    transactions = []
    for data in transaction_data:
        transaction = Transaction(**data)
        transactions.append(transaction)

    db.bulk_save_objects(transactions)
    db.commit()
    print(f"✓ {num_transactions} transactions added to database")


def seed_complaints(db, customer_ids, num_complaints=2000):
    """Seed complaint data"""
    print(f"\nGenerating {num_complaints} complaints...")
    complaint_data = generate_complaints(customer_ids, num_complaints)

    complaints = []
    for data in complaint_data:
        complaint = Complaint(**data)
        complaints.append(complaint)

    db.bulk_save_objects(complaints)
    db.commit()
    print(f"✓ {num_complaints} complaints added to database")


def main():
    """Main seeding function"""
    print("=" * 60)
    print("SENTINEL BANK - DATABASE SEEDING")
    print("=" * 60)

    # Create tables
    create_tables()

    # Create database session
    db = SessionLocal()

    try:
        # Seed data
        customer_ids = seed_customers(db, num_customers=1000)
        seed_transactions(db, customer_ids, num_transactions=5000)
        seed_complaints(db, customer_ids, num_complaints=2000)

        # Print summary
        print("\n" + "=" * 60)
        print("DATABASE SEEDING COMPLETE!")
        print("=" * 60)
        print(f"Total Customers: {db.query(Customer).count()}")
        print(f"Total Transactions: {db.query(Transaction).count()}")
        print(f"Total Complaints: {db.query(Complaint).count()}")
        print("=" * 60)

    except Exception as e:
        print(f"\n❌ Error during seeding: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    main()
