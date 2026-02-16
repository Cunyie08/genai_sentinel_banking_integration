# This contains the Intent + routing agent (LLM-based Dispatcher Agent)

from typing import Dict, Any
from app.agents.abstract_agent import BaseAgent
from app.prompts.dispatcher_prompt import Dispatcher_System_Prompt
from app.utils.llm_client import LLMClient
from app.utils.logger import ReasoningLogger


# Create a class that uses LLM to reason when a customer comaplaint is logged and routes it to the correct department

class DispatcherAgent(BaseAgent): 
    
    # Initialize DispatchAgent with LLM client
    def __init__(self):
        super().__init__(name="DispatcherAgent")

    # initialize LLM client (sampling)
        self.llm = LLMClient(model_name="gpt-4o-mini")

    # Analyze the customer complaint using LLM reasoning
    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """ 
        Args:
            input_data: Dict[str, Any]: Must contain the customer "complaint_text"
        
        Returns: 
            Dict[str, Any]: A structured routing decision
        """
        complaint_text: str = input_data.get("complaint_text", "")


        # Call LLM (hidden chain of Thoughts- internal reasoning)
        llm_response: Dict[str, Any] = self.llm.generate(
            system_prompt=Dispatcher_System_Prompt,
            user_input=complaint_text
        )

        # Include agent metadata
        llm_response["agent"] = self.name

        # Log reasoning summary 
        ReasoningLogger.log(
                agent_name=self.name,
                payload={
                    "intent": llm_response.get("intent"),
                    "department": llm_response.get("department"),
                    "confidence": llm_response.get("confidence"),
                    "reasoning": llm_response.get("reasoning")
                }
            )
        
        return llm_response