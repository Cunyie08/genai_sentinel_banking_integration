from sqlalchemy import Column, Integer, String, DateTime, Float, ForeignKey, Enum as SQLEnum
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
import enum
from app.database.connection import Base

class TransactionType(str, enum.Enum):
    DEPOSIT = "deposit"
    WITHDRAWAL = "withdrawal"
    TRANSFER = "transfer"
    PAYMENT = "payment"
    REVERSAL = "reversal"

class TransactionStatus(str, enum.Enum):
    PENDING = "pending"
    COMPLETED = "completed"
    FAILED = "failed"
    FLAGGED = "flagged"

class Transaction(Base):
    __tablename__ = "transactions"
    
    id = Column(Integer, primary_key=True, index=True)
    transaction_ref = Column(String(50), unique=True, index=True, nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)
    
    # Transaction details
    transaction_type = Column(SQLEnum(TransactionType), nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String(3), default="NGN")
    
    # Metadata for fraud detection
    location = Column(String(200))  # City or GPS coordinates
    device_id = Column(String(100))
    ip_address = Column(String(50))
    merchant_name = Column(String(200))  # For payments
    
    # Status and timestamps
    status = Column(SQLEnum(TransactionStatus), default=TransactionStatus.PENDING)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))
    
    # Fraud flags
    is_suspicious = Column(Integer, default=0)  # 0-10 risk score
    fraud_reason = Column(String(500))
    
    def __repr__(self):
        return f"<Transaction {self.transaction_ref}: {self.transaction_type} ₦{self.amount}>"
