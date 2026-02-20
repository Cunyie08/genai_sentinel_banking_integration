import uvicorn
from fastapi import Depends, FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from Backend.middleware import get_current_user
from app.agents.dispatcher_agent import DispatcherAgent
from app.agents.sentinel_agent import SentinelAgent
from Backend.api import router 


app = FastAPI(version="1.0.0")
app.include_router(router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class DispatcherQuery(BaseModel):
    complaint_text: str

class SentinelQuery(BaseModel):
    transaction_details: Dict[str, Any]

@app.get("/")
def home():
    return {"Message": "Welcome to the Bank System AI Assistant"}

@app.post("/dispatcher")
async def run_dispatcher(query: DispatcherQuery, user: dict = Depends(get_current_user)):
    """
    Endpoint for the Dispatcher Agent.
    Routes complaints to the appropriate department.
    """
    agent = DispatcherAgent()
    result = await agent.run({"complaint_text": query.complaint_text})
    return result

@app.post("/sentinel")
async def run_sentinel(query: SentinelQuery, user: dict = Depends(get_current_user)):
    """
    Endpoint for the Sentinel Agent.
    Analyzes transactions for fraud risk.
    """
    agent = SentinelAgent()
    # SentinelAgent expects input_data to be the transaction details dictionary
    result = await agent.run(query.transaction_details)
    return result

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
