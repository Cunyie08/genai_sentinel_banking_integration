from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# --- Auth Schemas ---

class UserBase(BaseModel):
    username: str
    email: EmailStr
    age: Optional[int] = None

class UserCreate(UserBase):
    password: str

class UserLogin(BaseModel):
    username: str # Can be email or username
    password: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class UserResponse(UserBase):
    id: int
    created_at: datetime
    
    class Config:
        from_attributes = True

# --- Chat Schemas ---

class ChatRequest(BaseModel):
    message: str
    thread_id: Optional[str] = None

# --- Onboarding Schemas ---

class OnboardingSubmission(BaseModel):
    answers: Dict[str, Any]

class OnboardingResponse(BaseModel):
    id: int
    user_id: int
    answers: Dict[str, Any]
    derived_profile: Optional[Dict[str, Any]] = None

    class Config:
        from_attributes = True

# --- Agent Schemas ---

class RoutingResponse(BaseModel):
    intent: str = Field(description="Why the model is routing to the department")
    department: str = Field(description="Banking department we are sending the complaint to")
    confidence: float = Field(ge=0, le=1, description="Confidence score between 0 and 1")
    reasoning: str = Field(description="How the model arrived at the decision")

class FraudResponse(BaseModel):
    intent: str = Field(description="Transaction Fraud Analysis")
    department: str = Field(description="Fraud Prevention")
    confidence: float = Field(ge=0, le=1, description="Confidence score between 0 and 1")
    reasoning: str = Field(description="How the model arrived at the decision")
