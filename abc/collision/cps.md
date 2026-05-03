# abc.collision.cps
@desc: CPS (Continuation-Passing Style) 기반의 판단 흐름과 전통적인 Transaction 기반 상태 귀속 구조 간의 위상적 충돌 구조 분석 문서

---

## @overview
> 흐름 기반 판단 vs 상태 기반 귀속

현대의 시스템 구조는 다음 두 위상적 철학 사이의 충돌 위에 있다:
- **CPS 기반 시스템**: 판단 흐름 중심. 판단(phi)은 여러 번 발생하고, 지연되며, 중첩되고, 재귀적으로 이어진다.
- **Transaction 기반 시스템**: 상태 귀속 중심. 판단(phi)은 단 한 번 발생하며, 즉시 실행(phi_x)되고, 단일 경계(bphi)에 귀속된다.

이 둘은 서로 다른 위상 좌표계를 기반으로 설계되었으며, 이로 인해 근본적인 충돌이 발생한다.

---

## CPS 구조의 본질

```kotlin
suspend fun logic() {
    val a = repo.find()
    val b = process(a)
    val c = update(b) // suspend
    return c
}
```

- 흐름은 지연되거나 분기될 수 있으며, 중간에 중단(suspend)되었다가 재개(resume)된다.
- 판단 phi는 단일한 것이 아니라, 흐름을 따라 분기되고 연속된다.
- 이러한 흐름은 결정적이지 않고, 귀속되지도 않는다.

---

## Transaction 구조의 본질

```kotlin
connection.beginTransaction()
...
connection.commitTransaction()
```

- 상태는 phi_x 실행을 통해 바로 bphi로 귀속된다.
- 중간 실패 시 rollback은 전체 판단을 취소한다.
- 판단 phi는 하나이며, 귀속은 단속적이다.

---

## 위상 충돌 도식

```mermaid
flowchart TD
  A[phi₁: 판단 시작] --> B[phi₂: 중단점 (suspend)]
  B --> C[phi₃: 재개 판단]
  C --> D[phi_x: 실행자]
  D --> E[bphi: 응답/상태 귀속]

  subgraph CPS 흐름
    A --> B --> C
  end

  subgraph TX 경계
    start[begin TX] --> D --> E[commit TX]
  end
```

- `phi₁ -> phi₂ -> phi₃` 은 판단이 유보되며 계속 확장됨
- Transaction은 `phi -> phi_x -> bphi` 로 닫히는 단속 구조

---

## 실무에서의 충돌 사례

### Coroutine + R2DBC
- suspend/resume 중 connection이 변경되거나 종료됨
- rollback 호출 시 context는 이미 사라짐

### Temporal + DB
- Temporal 워크플로우는 CPS 기반
- 외부 DB는 즉시 commit 요구
- 판단 흐름과 상태 경계가 어긋남

### Saga 보정
- 판단이 실패했을 경우, 상태는 이미 귀속됨
- CPS 흐름으로는 취소할 수 없음 -> 별도 compensation으로 구조 보정

---

## 위상적 해석

| 요소 | CPS | Transaction |
|------|-----|-------------|
| 판단 (phi) | 연속, 유보, 재귀 | 단일, 결정 |
| 실행 (phi_x) | 흐름 내 계속 발생 | 판단 후 바로 실행 |
| 귀속 (bphi) | 없음, 유보됨 | 즉시 상태화 |
| 경계성 | 약함 | 강함 |
| 재진입성 | 높음 | 불가 |

---

## 제안

- CPS는 흐름을 표현하는데 적합하지만, 상태 귀속은 외부로 위임해야 함
- 트랜잭션이 필요한 부분은 suspend 지점 바깥으로 분리
- 궁극적으로 판단과 상태를 **분리 위상 구조**로 설계해야 한다

```yaml
topology.strategy:
  flow := CPS
  state := externalized
  commit := delayed or evented
  rollback := compensate or isolate
```