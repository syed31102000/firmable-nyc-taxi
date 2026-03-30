import requests
from agent.config import OLLAMA_URL, OLLAMA_MODEL

def call_ollama(prompt: str, system: str) -> str:
    response = requests.post(
        OLLAMA_URL,
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "system": system,
            "stream": False,
            "options": {
                "temperature": 0.1
            }
        },
        timeout=180
    )
    response.raise_for_status()
    data = response.json()
    return data["response"].strip()