# exam.dspy.flow
import dspy
from bridge.plane.emitter import get_logger
from bridge.client.llama import LLMClient

log = get_logger("dspy.flow")

class LlamaDSPyLM(dspy.LM):
    """Adapter: DSPy interface ↔ LLMClient"""
    ## 모델 식별자를 받을 수 있도록 파라미터 추가 (기본값 설정)
    def __init__(self, model="local-llama"):
        super().__init__(model) 
        self.client = LLMClient()
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

dspy.settings.configure(lm=LlamaDSPyLM(model="my-llama-3"))

class BasicQA(dspy.Signature):
    """주어진 질문에 대해 간결하고 정확하게 답변"""
    question = dspy.InputField(desc="사용자의 질문")
    answer = dspy.OutputField(desc="간결한 답변")

class CoTPipeline(dspy.Module):
    def __init__(self):
        super().__init__()
        self.generate_answer = dspy.ChainOfThought(BasicQA)

    def forward(self, question):
        return self.generate_answer(question=question)

if __name__ == "__main__":
    pipeline = CoTPipeline()
    question = "블랙홀의 사건의 지평선 너머에서 빛이 빠져나올 수 없는 이유는?"
    result = pipeline(question=question)
    rationale = getattr(result, "rationale", None)
    log.info(f"[Q] {question}\n")

    if rationale:
        log.info("[Rationale]")
        log.info(rationale)

    log.info("[Answer]")
    log.info(result.answer)