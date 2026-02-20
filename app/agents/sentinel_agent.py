# This contains the Fraud/risk assessment

# stub (to be reviewed)
from typing import Dict, Any
from app.agents.abstract_agent import BaseAgent
from app.settings import OPENAI_API_KEY, GEMINI_API_KEY
from app.utils.logger import ReasoningLogger
from app.utils.schemas import FraudResponse
from app.utils.llm_client import LLMClient
from openai import RateLimitError, AsyncOpenAI
from google import genai
from app.prompts.sentinel_prompt import Sentinel_System_Prompt
import asyncio
# Create a class that assess fraud/risk and explains why transaction was flagged

class SentinelAgent(BaseAgent):
    # Initialize the agent
    def __init__(self):
        super().__init__(name="SentinelAgent")
    
        self.openai_llm = LLMClient(
                client=AsyncOpenAI(api_key=OPENAI_API_KEY),
                model_name="gpt-4o",
                response_schema=FraudResponse
            )

        self.gemini_llm = LLMClient(
            client=genai.Client(api_key=GEMINI_API_KEY),
            model_name="gemini-2.5-flash",
            response_schema=FraudResponse
        )
    
    # Perform basic fraud assessment
    async def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:

        """
        Args: Dispatcher output
        
        Returns: Fraud Assessment result
        """
        transaction_details = input_data
        try:
            llm_response = await self.openai_llm.generate(
                system_prompt=Sentinel_System_Prompt,
                user_input=f"Input data: {transaction_details}.Please assess the risk level of this transaction."
            )

        except RateLimitError:
            print("OpenAI rate limited. Falling back to Gemini...")

            llm_response = await self.gemini_llm.generate(
                system_prompt=Sentinel_System_Prompt,
                user_input=f"Input data: {transaction_details}.Please assess the risk level of this transaction."
            )

        result = llm_response.model_dump()
        result["agent"] = self.name

        ReasoningLogger.log(
            agent_name=self.name,
            payload=result
        )

        return result

async def main():
    sentinel = SentinelAgent()
    features = ["transaction_id","transaction_reference_number","account_id","channel","device_id","counterparty_bank","narration","transaction_type","amount","currency","transaction_balance","transaction_status","failure_reason","is_fraud_score","fraud_explainability_trace","merchant_category","merchant_name","salary_detected","car_loan_signal_score","recommended_product","transaction_timestamp"]
    data = ["TXN001","REF001","ACC001","channel","device_id","counterparty_bank","narration","transaction_type","amount","currency","transaction_balance","transaction_status","failure_reason","is_fraud_score","fraud_explainability_trace","merchant_category","merchant_name","salary_detected","car_loan_signal_score","recommended_product","transaction_timestamp"]
    input_data_zip = dict(zip(features, data))
    input_data = {features[i]: data[i] for i in range(len(features))}
    result = await sentinel.run(input_data)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())