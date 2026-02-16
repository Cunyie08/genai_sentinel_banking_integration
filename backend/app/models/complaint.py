from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Enum as SQLEnum,
    Text,
)
from sqlalchemy.sql import func
import enum
from app.database.connection import Base


class ComplaintCategory(str, enum.Enum):
    ATM_DISPENSE_ERROR = "atm_dispense_error"
    CARD_ISSUES = "card_issues"
    FRAUD_ALERT = "fraud_alert"
    ACCOUNT_ACCESS = "account_access"
    TRANSACTION_DISPUTE = "transaction_dispute"
    LOAN_INQUIRY = "loan_inquiry"
    GENERAL_INQUIRY = "general_inquiry"
    TECHNICAL_ISSUE = "technical_issue"


class ComplaintStatus(str, enum.Enum):
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    RESOLVED = "resolved"
    ESCALATED = "escalated"
    CLOSED = "closed"


class ComplaintPriority(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Complaint(Base):
    __tablename__ = "complaints"

    id = Column(Integer, primary_key=True, index=True)
    ticket_id = Column(String(20), unique=True, index=True, nullable=False)
    customer_id = Column(Integer, ForeignKey("customers.id"), nullable=False)

    # Complaint details
    category = Column(SQLEnum(ComplaintCategory), nullable=False)
    subject = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)

    # Routing and assignment
    assigned_department = Column(String(100))  # IT, Operations, Fraud, etc.
    assigned_agent = Column(String(100))
    priority = Column(SQLEnum(ComplaintPriority), default=ComplaintPriority.MEDIUM)

    # Status tracking
    status = Column(SQLEnum(ComplaintStatus), default=ComplaintStatus.OPEN)
    resolution_notes = Column(Text)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    resolved_at = Column(DateTime(timezone=True))

    def __repr__(self):
        return f"<Complaint {self.ticket_id}: {self.category} - {self.status}>"
