# NodeSentinel 배포 — WorkStore 내구성과 단일 writer

상태: 초안
작성일: 2026-09-23
범위: `deploy/03-nodesentinel.yaml`의 WorkStore 영속화(PVC), 단일 writer 계약, 환경별 입력, 증명된 지원 범위와 미증명 경로
비범위: 이미지 빌드, Harbor 설정, NodeVault/NodeKit 연동, HA 구성, 운영 마이그레이션 절차

이 문서는 `deploy/03-nodesentinel.yaml`이 참조하는 대상이다. issue #2(SQLite WorkStore를 PVC로
영속화해 queue 내구성 보장)의 배포 측 계약을 기록한다.

---

## 1. 왜 emptyDir이 문제였나

WorkStore는 embedded SQLite 데이터베이스이며 validation job queue의 **유일한** 기록이다.
이전 manifest는 이것을 `emptyDir`에 두었다. `emptyDir`은 Pod 수명에 묶여 있어 Pod 삭제·재스케줄·
노드 장애에서 사라진다. 즉 rollout 한 번마다 queued/leased/running 상태의 작업이 통째로 없어졌고,
이것은 복구 가능한 실패가 아니라 조용한 데이터 손실이었다.

`PersistentVolumeClaim`으로 바꾸면 볼륨이 Pod보다 오래 살아남아 교체 후에도 queue가 이어진다.

## 2. 데이터베이스 "디렉터리"를 마운트한다 — 파일이 아니라

PVC는 `NODESENTINEL_DB_PATH`가 가리키는 파일이 들어있는 **디렉터리**를 잡는다.

SQLite는 rollback journal을 데이터베이스 파일의 **형제**로 쓴다. 크래시 복구는 journal이 자신이
속한 데이터베이스와 함께 살아남아야만 성립한다. 데이터베이스 파일만 (`subPath`로) 마운트하면
journal은 컨테이너 파일시스템에 남고, Pod 교체 시 둘의 짝이 깨져 일관성이 무너진다.

그래서 `test/k8s/manifest_contract_test.go`의 `TestNodeSentinelDurabilityContract`는
`data` volumeMount에 `subPath`가 붙는 것을 실패로 고정하고, `NODESENTINEL_DB_PATH`가 마운트
경로 **안**에 있는지 검사한다.

WAL 도입은 이 변경의 요구사항이 아니다. 현재 rollback journal 의미론을 그대로 유지한다.

## 3. 단일 writer 계약 — 튜닝 값이 아니다

`replicas: 1`과 `strategy.type: Recreate`는 성능 조정 항목이 아니라 **정확성 제약**이다.

- 하나의 ReadWriteOnce 볼륨 위의 embedded SQLite는 두 Pod가 동시에 열어서는 안 된다.
- 기본값 `RollingUpdate`는 나가는 Pod가 아직 볼륨을 쥐고 있는 동안 교체 Pod를 **먼저** 띄운다.
  단일 RWO claim에서는 rollout이 멈추거나, 공유 파일시스템에서는 두 프로세스가 같은
  데이터베이스를 잡는다.
- `Recreate`는 반대로 이전 Pod를 **내린 뒤** 새 Pod를 띄운다(직렬화된 교체).

`accessModes: [ReadWriteOnce]`도 같은 이유다. many-writer 모드는 두 번째 Pod가 같은 SQLite
데이터베이스를 열 수 있게 만든다.

이 세 값은 manifest contract test가 고정한다. 되돌리면 테스트가 실패한다.

## 4. fsGroup — 볼륨을 실제로 쓸 수 있게 만드는 것

컨테이너는 `readOnlyRootFilesystem`과 non-root uid로 돈다. 즉 마운트된 볼륨이 **유일한**
쓰기 가능 경로다. 그런데 새로 프로비저닝된 PV는 `root:root` 소유다.

`fsGroup`이 없으면 컨테이너는 정상 기동한 다음 첫 쓰기에서 맨 EACCES로 실패한다. 더 나쁜 것은
SQLite가 이것을 `unable to open database file`로 표면화한다는 점이다 — 디렉터리도, 소유권도,
고칠 설정 이름도 언급하지 않는 메시지라 바깥에서 디버깅해야 한다.

두 가지로 대응한다.

1. `securityContext.fsGroup: 65532` — kubelet이 볼륨을 이 GID로 chgrp하고 setgid로 마운트하며
   컨테이너의 supplementary group에 추가한다. 접근 권한이 **그룹**을 통해 주어지므로 이미지가
   어떤 uid로 돌든 성립한다. 이 repo는 이미지를 빌드하지 않아 uid를 단정할 수 없으므로
   `runAsUser` 고정과 일부러 짝짓지 않았다.
   `fsGroupChangePolicy: OnRootMismatch`는 최상위 디렉터리 소유권이 이미 맞으면 재귀 chown을
   건너뛰어, queue가 커져도 재시작이 느려지지 않게 한다.
2. `pkg/work/sqlite.New`의 `checkDirWritable` — 기동 시 디렉터리에 실제 쓰기 probe를 해보고,
   실패하면 경로를 적시하고 fsGroup을 가리키는 진단으로 바꿔 던진다.

## 5. 환경별 입력

`deploy/03-nodesentinel.yaml`의 PVC에서 환경마다 조정하는 값은 셋이다.

| 입력 | 기본값 | 조정 기준 |
|---|---|---|
| `storageClassName` | **생략** (클러스터 기본 StorageClass 사용) | 기본 StorageClass가 없거나 특정 class를 고정해야 하는 환경에서 명시한다. |
| `resources.requests.storage` | `1Gi` | job row 기준의 작업 queue 크기다. artifact 보관용이 아니다. 보존 기간이 긴 환경에서 올린다. |
| `accessModes` | `ReadWriteOnce` | 조정 대상이 아니다. §3의 정확성 제약이다. |

> **`storageClassName`을 빈 문자열로 두지 말 것.** Kubernetes에서 `""`은 "미설정"이 아니라
> 동적 프로비저닝을 **끄는** 의미이며, claim이 영원히 Pending으로 남는다. 클러스터 기본값을
> 쓰려면 필드를 **생략**한다. manifest contract test가 이 차이를 검사한다.

별도 환경 overlay 디렉터리는 두지 않았다. 현재 `deploy/`는 평평한 manifest 집합이고, 환경 차이는
위 세 입력뿐이므로 주석과 이 표로 문서화하는 편이 존재하지 않는 overlay 구조를 새로 만드는 것보다
정확하다.

## 6. 지원 범위 — 증명된 것

직렬화된 교체(`Recreate` + `replicas: 1` + RWO)가 보호하는 것은 **정상 rollout**이다.
이 범위에서 다음이 성립한다.

- Pod 교체를 가로질러 queued/leased/running 작업이 살아남는다.
- 교체 중 두 Pod가 동시에 데이터베이스를 열지 않는다.
- 만료된 lease를 회수한 attempt가, 아직 돌고 있는 이전 attempt의 K8s Job **옆에** 두 번째 Job을
  만들지 않는다(§7).

## 7. 실행 identity — lease 회수가 중복 실행을 만들지 않는다

내구성만으로는 부족하다. `LeaseJob`은 **모든** lease에서 attempt를 올리며, 여기에는 단순히
만료된 lease를 회수하는 경우도 포함된다. K8s Job 이름이 attempt에서 파생되던 동안에는,
자기 K8s Job은 죽지 않은 채 heartbeat만 끊긴 worker(크래시·eviction·network partition·
실행 중 Pod 교체)가 있을 때 다음 lease가 **다른** 이름을 계산해 두 번째 Job을 만들었다.
하나의 논리적 validation이 두 개의 물리적 실행으로 갈라진 것이다.

그래서 Job 이름은 attempt가 아니라 **durable execution identity**(`work.Job.ExecutionID`,
`Store.EnsureExecution`이 발급/승계)에서 파생된다.

- 이전 실행이 아직 terminal로 관측되지 않았으면 같은 ID를 **승계(adopt)**하고, 새로 만들지 않고
  **관측**한다.
- 실행이 terminal로 관측된 뒤에야(그리고 retry policy가 별도로 허용해야) 새 ID를 발급한다.

관측 결과가 **불확실한** 경로(K8s API 오류, 실행 중 context 만료, 삭제 실패)에서는 identity를
일부러 non-terminal로 남긴다. 다음 attempt가 같은 Job을 다시 관측하게 하는 쪽이, 아직 돌고 있을지
모르는 실행 옆에 새 Job을 세우는 쪽보다 안전한 실패 방향이기 때문이다.

## 8. 미증명 경로 — 이 구성이 증명하지 **않는** 것

아래는 이 변경의 지원 범위 **밖**이며, 위 계약을 근거로 주장해서는 안 된다.

- **Fencing이 아니다.** `Recreate`와 RWO는 network partition이나 force-detach에서 writer를
  격리하지 못한다. split-brain 상황에서 stranded writer가 생길 수 있고, 이 구성은 그것을 막지
  않는다.
- **수동 중복 실행.** 누군가 `replicas`를 올리거나 두 번째 Pod를 직접 띄우면 단일 writer 가정이
  깨진다. manifest contract test는 repo 안의 회귀만 잡고, 클러스터에서의 수동 변경은 잡지 못한다.
- **HA가 아니다.** `replicas: 1`은 의도된 것이다. 노드 장애 시 재스케줄까지 가용성 공백이 있다.
- **RWO의 다중 노드 의미론.** RWO는 노드 단위 보장이다. 일부 CSI 드라이버에서 같은 노드의 여러
  Pod가 동시에 마운트할 수 있다. 단일 writer는 `replicas: 1`과 `Recreate`가 함께 지켜야 한다.

## 9. 검증 상태

- manifest contract(`TestNodeSentinelDurabilityContract`)와 실행 identity 회귀 테스트
  (`pkg/worker/maxattempts_test.go`)는 이 repo의 CI에서 실행된다.
- **실제 클러스터에서의 Pod 교체 복구 실측은 이 변경에서 NOT VERIFIED다.** disposable
  클러스터 환경이 이 실행 환경에 없어 수행하지 못했다. CI PASS를 실측 복구 증거로 대체하지
  않는다. 필요한 capability는 disposable(운영 아님) 클러스터에 대한 배포·Pod 삭제·PVC 프로비저닝
  권한이다.
