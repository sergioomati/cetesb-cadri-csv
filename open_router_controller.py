import os
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class OpenRouterController():
    def __init__(self):

        openrouter_api_key = os.getenv('OPENROUTER_API_KEY')

        if not openrouter_api_key:
            print("[WARN] OPENROUTER_API_KEY not found in environment variables.")
            print("[WARN] AI functionality may not work properly.")
        else:
            print("[INFO] OpenRouter API key found in environment variables")
            
        self.client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=openrouter_api_key,
        )
        self.models = {
            'cost-optimized': 'google/gemini-2.5-flash',
            'flagship': 'google/gemini-2.5-pro',
            'reasoning': 'google/gemini-2.5-flash',
            'batch': 'google/gemini-2.5-flash',
            'deepseek': 'deepseek/deepseek-chat-v3.1',
            'free-gemini': 'google/gemini-2.0-flash-exp:free',
            'gpt-test': 'openai/gpt-oss-120b',
            'grok-4-fast-free': 'x-ai/grok-4-fast:free',
            'gpt-4o-mini': 'openai/gpt-4o-mini',
            'gpt-5-mini': 'openai/gpt-5-mini'
        }
        self.responses = []


    def direct_chat_completion(self, system_prompt: str, message: str, custom_id: str, model='batch'):
        try:
            completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": "https://equitas.com.br",
                    "X-Title": "Credit Guide ETL",
                },
                model=self.models[model],
                messages=[
                    {
                        "role": "system",
                        "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": message
                    }
                ],
                temperature=0.1,
            )
            
            response_content = completion.choices[0].message.content
            
            self.responses.append({
                'custom_id': custom_id,
                'model': self.models[model],
                'content': response_content
            })
            
            print(f'[INFO] {custom_id} | Chat completion succeeded')
            return response_content
            
        except Exception as e:
            print(f'[ERROR] {custom_id} | Chat completion failed: {str(e)}')
            return None  


    def single_request(self, system_prompt, message, model='cost-optimized', temperature=0.1):
        try:
            completion = self.client.chat.completions.create(
                extra_headers={
                    "HTTP-Referer": "https://equitas.com.br",
                    "X-Title": "Credit Guide ETL",
                },
                model=self.models[model],
                messages=[
                    {
                        "role": "system", 
                        "content": system_prompt
                    },
                    {
                        "role": "user",
                        "content": message
                    }
                ],
                temperature=temperature
            )
            
            return completion.choices[0].message.content
            
        except Exception as e:
            print(f'[ERROR] Single request failed: {str(e)}')
            return None