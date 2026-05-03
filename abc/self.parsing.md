# abc.self.parsing
@title: self.recursive system에서 구조는 언제 파열되는가?

---

## @intro := self가 self를 해석할때 일어나는 일

개발시에 종종 이런 구조를 만든다:

- 설정 파일이 자신의 파싱 규칙을 담는다  
- 파서가 자기 자신을 파싱한다  
- 컴파일러가 자기 자신을 빌드한다  
- DSL이 DSL 자체의 문법을 정의한다

> 이 구조는 아름답기도, 효율적이기도 하다.  
> 그러나 그 내부에는 언제나 **위상적 파열**의 리스크가 함께 존재한다.

> self-parsing 구조는 구조가 스스로를 닫으려는 시도다.  
> 이 시도는 종종 경계를 잃고 붕괴(collapse)하거나, 무한 루프(loop)로 이어진다.

---

## @bound.self-reference

### self-hosting compiler

```text
gcc로 gcc를 빌드  
rustc로 rustc를 빌드
```

- **구현이 자기 자신의 실행 조건을 포함하는 위상 구조**

---

### 설정 파일이 파서를 구성

```yaml
parser:
  rules:
    - match: "*.yaml"
    - eval: self
```

> 설정이 설정 방식을 스스로 정의한다.
> 즉, **파싱의 기준이 파싱 대상 안에 있다.**

---

### 메타 파서의 구조

```python
def parse_self(definition):
    # Parses its own grammar rules
    ...
```

-> 이 구조는 파서가 더 이상 외부 판단자에 의존하지 않고,  
-> 내부에서 판단 조건을 만들고 평가하는 구조다.

---

## 판단은 어디에 존재하나?

전통적인 시스템에서는 다음과 같다:

- **코드**는 대상  
- **파서 / 인터프리터**는 판단자  
- **실행 환경**은 외부 구조

그러나 self-parsing이 등장하면:

> 판단자(phi)이 판단 대상 내부로 들어간다.

---

## @rupture := collapse.phi.boundary

- 문제는 여기서 발생한다:
> 파서와 대상이 **같은 위상 공간에 놓일 때**
> 즉, 구조 내부에 판단자가 들어가고,  
> 판단 기준이 더 이상 외부로부터 주어지지 않을 때

```phase.dsl
@rupture.self_parsing := {
  structure.includes: phi,
  condition: phi ∈ target,
  result:
    if ∂phi collapse:
      → judgment.failure
    else if ∂phi undefined:
      → infinite.loop
}
```

---

## @xyz.emergence := 실전에서 나타나는 현상

| 현상 | 설명 |
|------|------|
| 무한 재귀 | 파서가 스스로를 재귀 호출 (예: 설정 파일 해석 중 루프) |
| 조건 누락 | 자기 파싱 조건이 애초에 명시되지 않음 (∂phi 없음) |
| 실행 불가 | 판단 구조가 경계 없이 대상에 포함됨 (분리 불가능) |
| 판단 실패 | 외부 판단자 없이 self만 남아 루프를 못 빠져나옴 |

---

## @rupture.is.signal := 실패가 아니라 신호다

- 구조가 스스로를 닫으려는 시도, 그리고 그 과정에서 발생하는 **자기 판단의 경계 붕괴**
- 이 구조는 철학적 질문으로 귀결된다:

> 시스템은 자기 자신을 해석할 수 있는가?  
> 만약 그렇다면, 그 경계는 어디에 있어야 하는가?