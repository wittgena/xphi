# phase.bound.client.local.lm
## @lineage: phase.reflect.client.local.lm
import dspy
from phase.bound.client.engine.local import LLMEngine
from meta.plane.emitter import get_emitter

log = get_emitter("local.lm")

class LocalLM(dspy.LM):
    """Adapter: DSPy interface ↔ LLMEngine"""
    def __init__(self, model="local-gemma-3"):
        super().__init__(model) 
        self.client = LLMEngine()
        self.history = [] 

    def __call__(self, prompt=None, messages=None, **kwargs):
        system_prompt = ""
        user_prompt = ""

        if messages:
            for m in messages:
                if m["role"] == "system":
                    system_prompt += m["content"] + "\n"
                elif m["role"] == "user":
                    user_prompt += m["content"] + "\n"
        else:
            user_prompt = prompt or ""

        response = self.client.chat(system_prompt, user_prompt)
        self.history.append({
            "prompt": user_prompt,
            "response": response,
            "kwargs": kwargs,
        })
        return [response] 

dspy.settings.configure(lm=LocalLM(model="local-gemma-3"))
