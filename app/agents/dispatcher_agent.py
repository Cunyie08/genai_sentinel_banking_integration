from typing import Dict, Any
from openai import AsyncOpenAI
from google import genai
from openai import RateLimitError
from app.agents.abstract_agent import BaseAgent
from app.prompts.dispatcher_prompt import Dispatcher_System_Prompt
from app.utils.logger import ReasoningLogger
from app.utils.schemas import RoutingResponse
from app.utils.llm_client import LLMClient
import asyncio
from app.settings import OPENAI_API_KEY, GEMINI_API_KEY

class DispatcherAgent(BaseAgent):

    def __init__(self):
        super().__init__(name="DispatcherAgent")

        self.openai_llm = LLMClient(
            client=AsyncOpenAI(api_key=OPENAI_API_KEY),
            model_name="gpt-4o",
            response_schema=RoutingResponse
        )

        self.gemini_llm = LLMClient(
            client=genai.Client(api_key=GEMINI_API_KEY),
            model_name="gemini-2.5-flash",
            response_schema=RoutingResponse
        )



    async def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        complaint_text: str = input_data.get("complaint_text", "")

        try:
            llm_response = await self.openai_llm.generate(
                system_prompt=Dispatcher_System_Prompt,
                user_input=complaint_text
            )

        except RateLimitError:
            print("OpenAI rate limited. Falling back to Gemini...")

            llm_response = await self.gemini_llm.generate(
                system_prompt=Dispatcher_System_Prompt,
                user_input=complaint_text
            )

        result = llm_response.model_dump()
        result["agent"] = self.name

        ReasoningLogger.log(
            agent_name=self.name,
            payload=result
        )

        return result

async def main():
    dispatcher = DispatcherAgent()
    complaint_text = """I transferred money to a friend of mine who in turn
    complained that he hasn't been credited. Please look into this"""
    result = await dispatcher.run(input_data={"complaint_text": complaint_text})
    print(result)

if __name__ == "__main__":
    asyncio.run(main())