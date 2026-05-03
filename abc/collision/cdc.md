# abc.collision.cdc
@desc: Spring Event / Domain Event / CDC(Event Log)가 혼재할 때 발생하는 **경계(bound) 충돌**과 **이벤트 의미·시간·판단의 inversion**

---

## @prepos

현대 Spring 기반 MSA에서 다음 요소들이 동시에 존재하는 경우가 많다:

- Spring Application Event
- Domain Event
- CDC Event (Kafka / Debezium)
- Distributed Lock
- Transaction (@Transactional)
- Async Execution

> 이 조합이 왜 **급속도로 복잡성을 폭발**시키며,
> 왜 많은 경우 **이론적으로도 완전한 해결이 불가능한 상태 공간**으로 진입하는지 검토

---

## @overview

- 서로 다른 이벤트는 **의미 레벨이 다르다**
- 이벤트마다 **시간축(time semantics)** 이 다르다
- 판단 시점이 **코드 -> 플랫폼 -> 소비자**로 inversion 된다
- 이 경계들은 합성되지 않는다
- 결과적으로 시스템은 **추론 불가능 영역**으로 진입한다

---

## Event 계층 분류

| Event 유형     | 의미       | 보장            |
| ------------ | -------- | ------------- |
| Spring Event | 코드 실행 사실 | 없음            |
| Domain Event | 비즈니스 사실  | 제한적           |
| CDC Event    | DB 상태 변화 | 강함(Commit 이후) |

- 이름은 같지만 **서로 다른 층위의 사실**
- 동일한 추상화로 다룰 수 없음

---

## 시간 경계(Time Bound)의 충돌

### Spring Transaction Time

```text
begin → execute → commit/rollback
```

> thread + DB 중심
> scope가 명확함

---

### Event Dispatch Time

- sync event: 호출 중
- async event: scheduler 의존

> 실행 시점 불확정

---

### CDC Log Time

- DB commit 이후
- Kafka append 시점

> 가장 강한 시간 경계

---

### 결과

- 동일한 “사건”이

> 서로 다른 시간에
> 서로 다른 순서로
> 서로 다른 의미로 관측됨

---

## 판단 위치의 Inversion

### 기존 판단 구조

```text
Application Code
```

- 단일 판단 주체
- 국소적 불변성 유지 가능

---

### Event 기반 판단 구조

```text
N개의 Event Listener / Consumer
```

- 판단 복제
> 판단 비용 N배 증가

---

## Distributed Lock과 Event의 충돌

### Lock Time vs Transaction Time

- Lock lease ≠ Transaction scope
- 실패 시 복구 규칙 상이

> 일관된 상태 정의 불가

---

### CDC 관측의 결정성

- CDC는 commit만 본다
- lock 상태는 외부에 노출되지 않음

---

## Async 결합 시 상태 공간 폭발

### 시간 은닉

- 실행 지연
- 재시도 지연
- 순서 비결정성

---

### 실패 경로 조합 폭증

- Spring retry
- Async retry
- Consumer retry
- Replay

> 장애는 간헐적이며 재현 불가

---

## 왜 해결이 불가능해지는가

### 경계 모델의 비합성성

| 요소          | 경계 모델      |
| ----------- | ------------- |
| Transaction | DB / Thread   |
| Event       | Dispatcher    |
| CDC         | Log           |
| Lock        | External Time |
| Async       | Scheduler     |

> 공통 기준점 없음

---

### 인간 추론 한계 초과

- 상태 수 기하급수 증가
- 테스트 공간 폭발
- 로그로도 재현 불가

---

## 구조적 귀결

- 이벤트 수가 늘수록 안정성 ↓
- 의미 레벨이 섞일수록 판단 비용 ↑
- Lock/Async는 문제를 완화하지 않고 증폭

> 이는 설계 미숙이 아니라, 경계 충돌의 필연적 결과

---

## 실무적 함의

- Event 계층을 명시적으로 분리할 것
- Domain Event는 명확한 의미 단위로 제한
- CDC는 데이터 통합 경계로만 사용
- Lock과 Async는 최소화

---

## @super.position

> 이벤트가 많아서 복잡해진 것이 아니라,
> 서로 다른 경계가 중첩되어 존재하기에, 
> 존재의 여부를 외부 관찰로는 판별할수 없다. 
