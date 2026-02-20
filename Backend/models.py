from sqlalchemy import Column, Integer, String, Boolean, Date, DateTime, Numeric, Text, ForeignKey, CheckConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from Backend.database import Base
from datetime import datetime, timezone
import uuid

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    age = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Customer(Base):
    __tablename__ = "customers"

    customer_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    full_name = Column(String(200), nullable=False)
    gender = Column(String(20))
    age = Column(Integer, CheckConstraint('age >= 0'))
    date_of_birth = Column(Date)
    bvn = Column(String(20), unique=True)
    nin = Column(String(20), unique=True)
    phone_number = Column(String(20))
    telco_provider = Column(String(50))
    email = Column(String(255), unique=True)
    state_of_origin = Column(String(100))
    residential_state = Column(String(100))
    banking_branch = Column(String(100))
    onboarding_date = Column(Date)

    accounts = relationship("Account", back_populates="customer", cascade="all, delete-orphan")
    complaints = relationship("Complaint", back_populates="customer", cascade="all, delete-orphan")


class Account(Base):
    __tablename__ = "accounts"

    account_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.customer_id", ondelete="CASCADE"), nullable=False)
    account_number = Column(String(20), unique=True, nullable=False)
    account_type = Column(String(50))
    currency = Column(String(10))
    current_balance = Column(Numeric(18, 2), default=0.00)
    opened_date = Column(Date)

    customer = relationship("Customer", back_populates="accounts")
    transactions = relationship("Transaction", back_populates="account", cascade="all, delete-orphan")


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    transaction_reference_number = Column(String(50), unique=True, nullable=False)
    account_id = Column(UUID(as_uuid=True), ForeignKey("accounts.account_id", ondelete="CASCADE"), nullable=False)
    channel = Column(String(50))
    device_id = Column(String(100))
    counterparty_bank = Column(String(100))
    narration = Column(Text)
    transaction_type = Column(String(20))  # Constraint dropped in SQL
    amount = Column(Numeric(18, 2), nullable=False)
    currency = Column(String(10))
    transaction_balance = Column(Numeric(18, 2))
    transaction_status = Column(String(50))
    failure_reason = Column(Text)
    is_fraud_score = Column(Integer, CheckConstraint('is_fraud_score >= 0'))
    fraud_explainability_trace = Column(Text)
    merchant_category = Column(String(100))
    merchant_name = Column(String(150))
    salary_detected = Column(Boolean, default=False)
    car_loan_signal_score = Column(Numeric(5, 2))
    recommended_product = Column(String(150))
    transaction_timestamp = Column(DateTime)

    account = relationship("Account", back_populates="transactions")
    complaints = relationship("Complaint", back_populates="transaction")


class Complaint(Base):
    __tablename__ = "complaints"

    complaint_id = Column(String(50), primary_key=True)
    customer_id = Column(UUID(as_uuid=True), ForeignKey("customers.customer_id", ondelete="CASCADE"), nullable=False)
    linked_transaction_id = Column(UUID(as_uuid=True), ForeignKey("transactions.transaction_id", ondelete="SET NULL"))
    linked_reference = Column(String(50))
    department_code = Column(String(20))
    department_name = Column(String(150))
    priority_level = Column(String(20))
    sentiment = Column(String(50))
    complaint_channel = Column(String(50))
    assigned_agent_id = Column(String(50))
    complaint_timestamp = Column(DateTime)
    resolution_timestamp = Column(DateTime)
    resolution_time_hours = Column(Integer)
    sla_hours_limit = Column(Integer)
    sla_breach_flag = Column(Boolean, default=False)
    complaint_status = Column(String(50))
    fraud_related = Column(Boolean, default=False)
    complaint_text = Column(Text)
    complaint_narration = Column(Text)

    customer = relationship("Customer", back_populates="complaints")
    transaction = relationship("Transaction", back_populates="complaints")
