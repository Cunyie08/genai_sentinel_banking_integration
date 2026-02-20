from pydantic import BaseModel, Field

class RoutingResponse(BaseModel):
    intent: str = Field(description="Why the model is routing to the department")
    department: str = Field(description="Banking department we are sending the complaint to")
    confidence: float = Field(ge=0, le=1, description="Confidence score between 0 and 1")
    reasoning: str = Field(description="How the model arrived at the decision")

class FraudResponse(BaseModel):
    intent: str = Field(description="Why the model is routing to the department")
    department: str = Field(description="Banking department we are sending the complaint to")
    confidence: float = Field(ge=0, le=1, description="Confidence score between 0 and 1")
    reasoning: str = Field(description="How the model arrived at the decision") 