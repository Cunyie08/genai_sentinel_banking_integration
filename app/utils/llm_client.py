import asyncio
from openai import AsyncOpenAI
from google import genai




import asyncio
from openai import AsyncOpenAI
from google import genai




class LLMClient:
    def __init__(self, client, model_name: str, response_schema, max_concurrent: int = 5):
        self.client = client
        self.model_name = model_name
        self.response_schema = response_schema
        self._semaphore = asyncio.Semaphore(max_concurrent)

# Genrate a response from the LLM

    async def generate(self, system_prompt: str, user_input: str):
        async with self._semaphore:
            try:
                if isinstance(self.client, AsyncOpenAI):
                    result = await asyncio.wait_for(
                        self.client.beta.chat.completions.parse(
                            model=self.model_name,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_input}
                            ],
                            response_format=self.response_schema,
                        ),
                        timeout=30,
                    )
                    return result.choices[0].message.parsed
                elif isinstance(self.client, genai.Client):
                    response = await self.client.aio.models.generate_content(
                        model=self.model_name,
                        contents=user_input,
                        config={
                            "system_instruction": system_prompt,
                            "response_mime_type": "application/json",
                            "response_schema": self.response_schema,
                        })
                    return response.parsed

            except asyncio.TimeoutError:
                raise RuntimeError("LLM request timed out")