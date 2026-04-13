# bridge.client.llama
import os
import time
import subprocess
import requests
from bound.plane.emitter import get_logger

log = get_logger('client.llama')

## GLOBAL CONFIG
MODEL_HF = os.getenv("LLAMA_MODEL_HF", "ggml-org/gemma-3-1b-it-GGUF")
MODEL_NAME = os.getenv("LLAMA_MODEL_NAME", "gemma-3-1b-it-Q4_K_M.gguf")
SERVER_PORT = int(os.getenv("LLAMA_PORT", "8080"))

SERVER_URL = f"http://localhost:{SERVER_PORT}/v1/chat/completions"
HEALTH_URL = f"http://localhost:{SERVER_PORT}/health"

LLAMA_SERVER_CMD = [
    "llama-server",
    "-hf", MODEL_HF,
    "--port", str(SERVER_PORT),
]

## LLM INFRA LAYER
class LLMClient:
    """
    Infra layer:
    - llama-server lifecycle
    - HTTP transport
    - no structural logic
    """
    def __init__(self):
        self.model_name = MODEL_NAME
        self.server_url = SERVER_URL
        self.health_url = HEALTH_URL
        self._process = None

    def is_alive(self) -> bool:
        try:
            r = requests.get(self.health_url, timeout=2)
            return r.status_code == 200
        except Exception:
            log.error('[error] is_alive fail')
            return False

    def ensure_server(self):
        if self.is_alive():
            return

        self._process = subprocess.Popen(
            LLAMA_SERVER_CMD,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        for _ in range(15):
            if self.is_alive():
                return
            time.sleep(1)

        self._process.terminate()
        raise RuntimeError("llama-server failed to start")

    def chat(self, system_prompt: str, user_prompt: str, timeout: int = 30) -> str:
        self.ensure_server()

        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        r = requests.post(self.server_url, json=payload, timeout=timeout)
        r.raise_for_status()

        data = r.json()
        return data["choices"][0]["message"]["content"]

if __name__ == "__main__":
    client = LLMClient()
    try:
        log.info("Starting LLM Client test...")
        system_msg = "You are a concise assistant."
        user_msg = "Hello, tell me a short joke about robots."
        print(f"\n[Requesting to {MODEL_NAME}...]")
        
        response = client.chat(system_msg, user_msg)
        print(f"Response:\n{response}")
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    except Exception as e:
        log.error(f"Test failed: {e}")
    finally:
        if client._process:
            log.info("Terminating llama-server...")
            client._process.terminate()