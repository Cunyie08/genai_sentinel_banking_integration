# This module abstracts LLM calls to ensure agents do not depend on specific provider implementation

from typing import Dict, Any

# create a class for the LLM client Interface
class LLMClient:

    # Initialize the LLM client
    def __init__(self, model_name: str): # Identifies the LLM to use
        
        self.model_name = model_name
    
# Generate a response from the LLM

    def generate(self, system_prompt: str, user_input: str) -> Dict[str,Any]: # Placeholder till the API call is plugged in

        """ 
        Args:
            system_prompt(str): System instructions
            user_input(str): User compalint text

        Returns:
            Dict[str, Any]: parsed JSON response
        """

        # Sample response (this allows the system to end-to-end)
        return {
            "intent": "fraud_suspected",
            "department": "FRAUD_TEAM",
            "confidence": 0.90,
            "reasoning": "Transaction appears unauthorized based on complaint."
        }