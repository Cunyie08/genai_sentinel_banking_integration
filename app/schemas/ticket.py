# This file contains the structure of the ticket


from typing import List, Optional, Literal
from pydantic import BaseModel, Field


# Define the contract for the router's output schema 
class RouterOutput(BaseModel):
    intent: Literal[    # To prevent the LLM from inventing/hallucinating.
        "Bank_Policy_Enquiry",
        "Security_Alert",
        "Financial_Growth_Advisory",
        "Transaction_Dispute",
        "General_support"
    ] = Field(description="The targeted banking department for the query")
    confidence: float = Field(description="How accurate the AI is, ranging from 0 to 1")
    urgency: bool = Field(description="Whether the issue requires immediate attention")


# Define the Global State of the entire Sentinnel Banking system
class AgentState(BaseModel):
    messages: List[str] = []
    customer_id: Optional[str] = None
    current_intent: Optional[str] = None
    risk_assessment: str = "Low" 
    confidence: float = 0.0
    is_anomaly_detected : bool = False 
    next_step: str = "triage" # Tells the graph where to go next