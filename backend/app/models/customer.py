from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.sql import func
from app.database.connection import Base


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    account_number = Column(String(10), unique=True, index=True, nullable=False)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True, index=True, nullable=False)
    phone = Column(String(20), nullable=False)

    # Nigerian-specific fields
    state = Column(String(50), nullable=False)  # Nigerian state
    lga = Column(String(100))  # Local Government Area
    address = Column(String(500))

    # Account details
    account_type = Column(String(50), nullable=False)  # Savings, Current, etc.
    balance = Column(Float, default=0.0)
    is_active = Column(Boolean, default=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    def __repr__(self):
        return f"<Customer {self.account_number}: {self.first_name} {self.last_name}>"
