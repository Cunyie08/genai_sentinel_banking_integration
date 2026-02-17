# This contains the Fraud/risk aassessment

# stub (to be reviewed)
from typing import Dict, Any
from app.agents.abstract_agent import BaseAgent
from app.utils.logger import ReasoningLogger


# Create a class that assess fraud/risk and explains why transaction was flagged
class SentinelAgent(BaseAgent):

    # Initialize the agent
    def __init(self):
        super().__init__(name="SentinelAgent")
    
    # Perform basic fraud assessment
    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:

        """
        Args: Dispatcher output
        
        Returns: Fraud Assessment result
        """
    
        results = {
        "agent": self.name,
        "risk_level": "high",
        "decision": "transaction_flagged",
        "reasoning": "Stub: fraud logic to be implemented in Week 2",
        "original_dispatch": input_data
        }

        # log Sentinel decision
        ReasoningLogger.log(
            agent_name=self.name,
            payload= {
                "risk_level": results["risk_level"],
                "decision": results["decision"],
                "reasoning": results["reasoning"]
            }
        )

        return results