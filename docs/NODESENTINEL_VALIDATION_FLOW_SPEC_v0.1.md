# NodeSentinel Validation Flow Spec v0.1

상태: 초안
작성일: 2026-06-02
범위: NodeSentinel 단독 설계 — validation flow, dry-run/smoke-run, observed profile, toolprofile/security referrer, validationHash, client 노출
비범위: NodeVault 이미지 빌드, NodeKit authoring UX, DagEdit DAG 편집, JUMI/Executor 실행 상세

---

## 1. 한 줄 정의

NodeSentinel은 NodeVault가 Harbor에 push한 Tool 이미지를 대상으로, 실제 sample fixture를 사용한
K8s Job 실행을 통해 재현 가능한 validation evidence를 생성하고 NodeVault Index에 저장하는
K8s data-plane observation record producer다.

ToolScanRecord / ToolCheckRecord / ObservedToolFunctionProfile의 생산 책임자.
이 Records는 NodeVault Index에 저장되고(primary), Harbor에 referrer mirror가 선택적으로 붙는다(secondary).
NodeVault가 이 Records를 바탕으로 Certification 여부를 결정하고 NodePalette에 노출할 stable projection을 생성한다.

이 문서는 다음 분산된 문서들의 관련 내용을 통합한다:
- `NodeSentinel_Validation_Data_Plane_설계_v0.1.md`
- `NodeVault_Reproducible_Tool_Authoring_업그레이드_설계_v0.6.1.md`
- `OBSERVED_PROFILE_SPEC.md`
- `SECURITY_SCAN_SPEC.md`

---

## 2. 전체 검증 흐름 (L1 → L5-b)

```
[NodeKit]  — K8s 외부, 데스크탑 클라이언트
  L1: DockGuard WASM 정적 검증
      latest 태그 차단, digest 미고정 차단, package version 미고정 차단
  BuildRequest 생성 (DockerfileContent 포함)
        ↓ gRPC :50051

[NodeVault]  — K8s 외부, seoy 호스트 바이너리
  L2: podbridge5 in-process 이미지 빌드
      Harbor push
      toolspec referrer 첨부 (application/vnd.nodevault.toolspec.v1+json)
      catalog/index 기본 기록 (lifecycle_phase=Pending)
  EnqueueValidationWork → NodeSentinel ingress
        ↓ gRPC enqueue

[NodeSentinel]  — K8s 내부 Pod
  L3: K8s dry-run Job (manifest 검증, 실제 실행 없음)
  L4: K8s smoke-run Job (컨테이너 기동 확인, timeout 5분)
  L5-a: sample fixture 기반 functional validation
        observedIoProfile 수집
        observedResourceProfile 수집
        contractCheck (선언 vs 관측 비교)
        validationHash 계산 (성공 시에만)
        ToolCheckRecord / ObservedToolFunctionProfile → NodeVault Index (primary)
        toolprofile referrer → Harbor (secondary mirror)
  L5-b: trivy-operator VulnerabilityReport 조회 (병렬)
        ToolScanRecord → NodeVault Index (primary)
        security referrer → Harbor (secondary mirror)
  NodeVault에 결과 통보
        ↓

[NodeVault Index]
  ToolCheckRecord + ToolScanRecord 수신
  CertifiedToolImageRecord / CertifiedToolFunctionImageRecord 생성
  ToolFunctionCatalogEntry / FunctionCompatibilityRecord 생성

[Harbor]
  image artifact (primary)
  toolprofile referrer mirror / security referrer mirror (secondary)
        ↓

[NodePalette]
  NodeVault Index stable projection (CertifiedToolImageRecord / ToolFunctionCatalogEntry) 읽기
  NodePalette 노출
        ↓

[DagEdit]
  toolFunctionSpecDigest 기반 RunnerNode 구성
  observedProfile 기반 UI badge 표시
```

---

## 3. 역할 경계

### NodeSentinel이 하는 것

- K8s Job으로 smoke-run / functional validation 실행
- sample fixture 파일을 실제 input으로 사용한 tool 실행
- observed I/O profile 수집 (파일 존재 여부, 개수, 크기)
- observed resource profile 수집 (CPU, 메모리, 실행 시간)
- infra failure와 application failure 분리
- validationHash 계산 (성공한 functional validation에 대해서만)
- **ToolCheckRecord** (toolSpecDigest × imageDigest) 생성 → NodeVault Index 저장
- **ToolScanRecord** 생성 → NodeVault Index 저장
- **ObservedToolFunctionProfile** 생성 → NodeVault Index 저장
- toolprofile referrer / security referrer → Harbor mirror (secondary)
- trivy-operator VulnerabilityReport CRD 조회
- client-facing status / badge summary 집계
- NodeVault에 결과 통보 (Certification 여부는 NodeVault 결정)

### NodeSentinel이 하지 않는 것

- Dockerfile authoring
- L1 static validation
- 이미지 빌드
- Harbor push (이미지 자체)
- casHash 결정 또는 변경
- dataCasHash 결정 또는 변경
- imageDigest 최종 권위
- NodeVault catalog identity 재정의
- DagEdit 파이프라인 그래프 구성

### 스토리지 권한 분리 (핵심 결정)

**NodeVault Index (primary store):**
```
NodeVault    → 모든 Spec / Record / Certification / Catalog 쓰기 (primary authority)
NodeSentinel → ToolCheckRecord / ToolScanRecord / ObservedToolFunctionProfile 쓰기
NodePalette  → read only
```

**Harbor (secondary mirror):**
```
NodeVault    → image push 권한만
NodeSentinel → referrer mirror push 권한
               toolprofile referrer  (L5-a ToolCheckRecord의 mirror)
               security referrer     (L5-b ToolScanRecord의 mirror)
NodePalette  → read only (optional)
```

NodeVault Index에 Record가 저장된다는 것 = NodeSentinel의 관측이 완료되었다는 의미.
NodeVault는 이 Records를 바탕으로 CertifiedToolImageRecord를 생성하고 NodePalette에 노출한다.
NodeVault는 이미지를 올릴 뿐, 어떤 validation metadata도 Harbor에 직접 기록하지 않는다.

### NodeVault와의 handoff 시점

NodeVault가 Harbor image push를 완료한 시점이 handoff다.
그 이전(이미지 빌드, Harbor image push)은 NodeVault 책임이고,
그 이후(모든 referrer 작성, dry-run, profiling, security scan)는 NodeSentinel 책임이다.

현재 transition 모델: NodeVault가 kubeconfig로 L3/L4를 직접 수행.
장기 방향: L3~L5 전체를 NodeSentinel로 이전.

---

## 4. WorkStore & Job 모델

### 4.1 Job 요청 필드

```
job_id                  string   UUID, 중복 방지
artifact_kind           string   "tool" 또는 "data"
image_repository        string   Harbor image repository
image_digest            string   sha256:... 형식
stable_ref              string   tool_name@version
tool_name               string
version                 string
cas_hash                string   NodeVault가 부여한 casHash
requested_actions       []string smoke_run / profile / security_scan
requested_fixture_set   string   사용할 sample fixture set 식별자
created_at              time
```

### 4.2 Job 상태 머신

```
queued
  └─ LeaseJob() → leased
       └─ Heartbeat() → running
            ├─ CompleteJob() → succeeded
            └─ FailJob(retryable=true) → queued  (재시도)
            └─ FailJob(retryable=false) → failed
```

보조 필드: `attempt`, `lease_owner`, `lease_until`, `last_error`, `result_summary`, `updated_at`

### 4.3 requested_actions 의미

| action | 내용 |
|--------|------|
| `smoke_run` | L3 dry-run + L4 smoke-run Job 실행 |
| `profile` | L5-a functional validation + observedProfile 수집 |
| `security_scan` | L5-b trivy-operator VulnerabilityReport 수집 |

### 4.4 Store 인터페이스 (백엔드 교체 가능)

```go
type Store interface {
    CreateJob(ctx context.Context, req JobRequest) (*Job, error)
    LeaseJob(ctx context.Context, worker string, ttl time.Duration) (*Job, error)
    Heartbeat(ctx context.Context, jobID, worker string, ttl time.Duration) error
    CompleteJob(ctx context.Context, jobID, worker, resultSummary string) error
    FailJob(ctx context.Context, jobID, worker, lastError string, retryable bool) error
    GetJob(ctx context.Context, jobID string) (*Job, error)
    ListJobs(ctx context.Context, status Status) ([]*Job, error)
    Close() error
}
```

현재 구현: SQLite (`pkg/work/sqlite/store.go`) — 단일 Pod 임시 구현.
향후 교체 후보: PostgreSQL, Redis-backed queue, NATS, Kubernetes CRD-backed store.
비즈니스 로직은 Store 인터페이스만 참조하며 SQLite를 직접 알지 않는다.

---

## 4-b. Tool Image 구조 및 nan 주입

### Tool Image 구성 요소

모든 tool image는 다음 4가지를 포함한다:

```
[Tool Image = bwa@sha256:...]
  ├── base image          (ubuntu:22.04@sha256:..., digest-pinned)
  ├── tool                (bwa 0.7.17 설치됨)
  ├── execution script    (run.sh / R script / command 구문)
  └── nan                 (/usr/local/bin/nan — runtime shim)
```

### nan 주입 방식

nan은 NodeVault L2 빌드 과정에서 **레이어로 주입**된다.
사용자는 Dockerfile에 nan을 명시하지 않는다. 플랫폼이 자동으로 주입한다.

```
1단계: 사용자 Dockerfile로 이미지 빌드
       (base image + tool + execution script)

2단계: nan 바이너리 레이어 추가
       buildah copy <container> /path/to/nan /usr/local/bin/nan

3단계: Harbor push → 최종 imageDigest 확보
       이 digest가 casHash 계산의 기준
```

### nan 버전 정책 (Option C — freeze)

```
등록 시점의 nan 버전이 해당 이미지에 고정된다.
새 등록 → 그 시점의 최신 nan 사용
기존 이미지 → 등록 시점 nan 버전 유지 (재현성 보장)
nan 치명적 버그 → 관리자가 해당 tool 재등록 선택 (강제 아님)
```

nan 버전은 toolspec referrer payload에 기록한다:
```json
{
  "toolspec": {
    "nanVersion": "v0.1.5",
    ...
  }
}
```

### nan의 실행 역할

K8s Job 내에서 nan이 사용자 script의 실제 실행 주체다:

```
entrypoint: /usr/local/bin/nan run --contract /jumi/node-contract.json -- <execution script>
               │
               ├── inputs materialization (sample fixture 파일 준비)
               ├── exec(<execution script>)  ← 자식 프로세스로 실행
               ├── outputs 수집 (digest, size)
               └── ArtifactManifest 작성 → /jumi/output-manifest.json

NodeSentinel이 ArtifactManifest를 읽어 observedIoProfile 구성
```

nan은 sidecar가 아니라 이미지 안에 baked-in된 단일 바이너리다.
ArtifactManifest schema는 nan 버전 간 backward compatible하게 유지한다.

---

## 5. gRPC EnqueueValidationWork

### 5.1 NodeVault → NodeSentinel 호출

NodeVault는 NodeSentinel에 worker RPC를 직접 호출하지 않는다.
enqueue 전용 ingress API 하나만 호출한다. 작업은 비동기로 처리된다.

```
NodeVault
  → gRPC EnqueueValidationWork
  → nodesentinel.apps.<base-domain>
  → shared Cilium Gateway
  → NodeSentinel GRPCRoute
  → NodeSentinel Service
  → NodeSentinel ingress handler
  → CreateJob() → WorkStore
  → 즉시 반환 (비동기)
```

### 5.2 EnqueueValidationWork 요청 필드 (초안)

```protobuf
message EnqueueValidationWorkRequest {
  string artifact_kind         = 1;
  string image_repository      = 2;
  string image_digest          = 3;
  string stable_ref            = 4;
  string tool_name             = 5;
  string version               = 6;
  string cas_hash              = 7;
  repeated string actions      = 8;  // smoke_run, profile, security_scan
  string fixture_set           = 9;
}

message EnqueueValidationWorkResponse {
  string job_id = 1;
}
```

### 5.3 Cilium GRPCRoute 배치 표준

```yaml
hostname: nodesentinel.apps.<base-domain>
backendRefs:
  - name: nodesentinel
    port: 50052
```

---

## 6. L3/L4 실행 모델

### 6.1 실행 namespace

```
nodevault-smoke
```

### 6.2 L3 dry-run (manifest 검증)

- K8s Job을 dry-run 모드로 제출 (`--dry-run=server`)
- 실제 컨테이너 실행 없음
- K8s API schema 검증만 수행
- 실패 시: job status = failed, retryable=false (manifest 문제)

### 6.3 L4 smoke-run

- K8s Job 실제 실행
- 기본 timeout: **5분**
- 컨테이너가 기동되고 exit 0으로 종료되면 통과
- sample fixture 없이 기본 command만 실행
- 실패 분류:
  - OOMKilled, timeout, scheduling failure → infra-level failure, retryable=true
  - exit code != 0 → application-level failure, retryable=false

### 6.4 최소 K8s 권한

```yaml
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "delete", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
  - apiGroups: ["aquasecurity.github.io"]
    resources: ["vulnerabilityreports"]
    verbs: ["get", "list", "watch"]
```

---

## 7. L5-a: Functional Validation / Profiling

### 7.1 sample fixture 기반 실행

L5-a는 실제 sample 파일을 input으로 사용하여 tool을 실행한다.
smoke-run과의 차이: L4는 기동 확인, L5-a는 실제 데이터 처리 확인.

`requested_fixture_set` 필드가 사용할 sample fixture set을 지정한다.
fixture set에는 input 파일, expected output 기준, command/script override가 포함된다.

### 7.2 기본 timeout

**30분**. tool별 또는 fixture set별 override 가능.

timeout 발생 시:
- `validationStatus: "infra_failed"`
- `validationHash`: 미생성
- `observedResourceProfile.timeout: true`, `observedResourceProfile.timeoutSeconds`: 기록

### 7.3 observedIoProfile

실행 후 output 경로에서 관측한 결과를 기록한다.

```json
{
  "validationRunId": "dryrun-001",
  "inputs": [
    {
      "port": "reads",
      "fileCount": 2,
      "nonEmpty": true
    }
  ],
  "outputs": [
    {
      "port": "alignment",
      "paths": ["/out/alignment.sam"],
      "exists": true,
      "count": 1,
      "totalBytes": 98765,
      "nonEmpty": true,
      "typeDetection": {
        "status": "not_performed",
        "detectedType": "unknown"
      }
    }
  ]
}
```

초기 구현 범위:
- 파일 존재 여부, 개수, 크기, nonEmpty 여부만 확인
- FASTQ/SAM/BAM/VCF 형식 자동 판별: 미수행
- BAM header 검사, VCF normalization 비교: 미수행
- semantic equivalence 검증: 미수행

### 7.4 observedResourceProfile

```json
{
  "validationRunId": "dryrun-001",
  "executionEnvironment": {
    "type": "k8s_pod",
    "platform": "linux/amd64",
    "containerRuntime": "containerd",
    "cpuLimit": "2",
    "memoryLimit": "4Gi",
    "nodeClass": "unknown",
    "cpuModel": "unknown"
  },
  "durationSeconds": 18.4,
  "cpu": {
    "peakMillicores": 1300,
    "avgMillicores": 800
  },
  "memory": {
    "peakBytes": 734003200
  },
  "disk": {
    "readBytes": 120000000,
    "writeBytes": 98000000
  },
  "exitCode": 0
}
```

**중요**: resource 관측값은 반드시 실행 환경 정보(`executionEnvironment`)와 함께 저장한다.
환경이 다르면 관측값도 달라지므로, 환경 없는 수치는 의미가 없다.

UI 표시 문구:
> 샘플 dry-run에서 관측된 값입니다. 운영 리소스 보장값이 아닌 sizing 참고값입니다.

### 7.5 contractCheck

선언된 PortSpec과 관측된 I/O를 비교한다.

```json
{
  "allOutputsPresent": true,
  "comparatorResult": "pass",
  "details": []
}
```

### 7.6 resourceRecommendation

observedResourceProfile 기반 K8s resource 권장값 생성.

```json
{
  "requestCpu": "500m",
  "requestMemory": "256Mi",
  "limitCpu": "2000m",
  "limitMemory": "1Gi"
}
```

생성 규칙: peak 관측값에 버퍼 비율 적용 (초안: CPU x1.5, 메모리 x2).

---

## 8. validationHash 정책 / ToolCheckRecord 관계

### 8.0 ToolSpec v0.5.1 모델에서의 위치

ToolSpec v0.5.1 통합 모델에서 validationHash는 ToolCheckRecord의 핵심 필드다.

```
ToolCheckRecord
  key:
    toolSpecDigest: sha256:...    (계약 identity)
    imageDigest: sha256:...       (artifact identity)
  result: passed
  validationHash: sha256:...      (재현성 증명 hash — 성공 시에만)
```

ToolCheckRecord는 toolSpecDigest × imageDigest 교차점에 붙는다 (M:N 관계).
ToolCheckRecord는 NodeVault Index에 저장되며, Harbor toolprofile referrer에 mirror될 수 있다.

### 8.1 정의

```
validationHash = successful functional validation의 재현성 증명 hash
              = ToolCheckRecord.validationHash
```

같은 tool + 같은 sample data + 같은 script → 같은 validationHash.
환경 차이(노드, 시간, 리소스 수치)는 hash에 영향을 주지 않는다.

### 8.2 생성 조건

**successful functional validation에 대해서만 생성한다.**

infra-level failure인 경우 validationHash를 생성하지 않는다.

### 8.3 hash 포함 항목

```
validationRun.mode                    (dry-run / smoke / profiler)
subject.digest                        (imageDigest)
validationRun.executionSpec.type      (bash / rscript / python / command / java)
validationRun.executionSpec.scriptDigest    (스크립트 내용 SHA256)
validationRun.executionSpec.scriptVersion   (관리자 명시 버전)
validationRun.executionSpec.interpreter     (Rscript / python3 / bash 등)
validationRun.executionSpec.interpreterVersion  (4.3.1 등)
validationRun.sampleDataRefs          (각 파일의 digest 목록)
validationRun.command
validationRun.exitCode                (0)
observedIoProfile 결정론적 요약
  - 각 port별 exists, count, nonEmpty
  - comparatorResult
contractCheck.allOutputsPresent
contractCheck.comparatorResult
```

execution spec은 bash script로 한정하지 않는다.
interpreter 버전이 다르면 validationHash가 달라진다.
스크립트 버전은 scriptDigest(content-addressed)와 scriptVersion(명시적) 둘 다 기록한다.

### 8.4 hash 제외 항목

```
peakCpuMillicores               (노드/부하 의존)
avgCpuMillicores
peakMemoryBytes                 (환경 의존)
durationSeconds                 (실행 시간)
diskReadBytes, diskWriteBytes   (환경 의존)
node name, cpuModel             (인프라 식별 정보)
raw stdout / stderr             (대용량, 비결정론적 가능)
timestamp
```

### 8.5 infra-level failure 목록 (validationHash 미생성)

```
OOMKilled
timeout (30분 기본)
node eviction
pod scheduling failure
image pull failure
registry pull error
SIGTERM / SIGKILL
cluster/network/storage transient failure
```

### 8.6 application-level failure (validationHash 미생성, 기본)

```
command exit code != 0
contract mismatch (expected output 없음)
expected output missing
```

예외: expected-failure fixture를 명시적으로 정의한 경우 validationHash 생성 허용 (옵션).

---

## 9. toolprofile referrer 명세 / ToolSpec 모델 매핑

### 9.0 ToolSpec v0.5.1 모델 매핑

toolprofile referrer는 ToolCheckRecord + ObservedToolFunctionProfile의 Harbor mirror다.

```
NodeVault Index (primary)        Harbor (secondary mirror)
ToolCheckRecord               →  toolprofile referrer payload
ObservedToolFunctionProfile   →  toolprofile referrer payload의 일부
```

primary store(NodeVault Index)가 source of truth다.
Harbor referrer는 portability / 외부 감사용 mirror다.
Certification과 NodePalette 노출은 NodeVault Index를 기준으로 판단한다.

### 9.1 기본 정보

```
artifactType: application/vnd.nodevault.toolprofile.v1+json
subject: Harbor 이미지 manifest (sha256:IMAGE_DIGEST)
```

### 9.2 성공 케이스 payload

```json
{
  "artifactType": "application/vnd.nodevault.toolprofile.v1+json",
  "subject": {
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "digest": "sha256:IMAGE_DIGEST"
  },
  "profile": {
    "casHash": "sha256:TOOLDEFINITION_CAS_HASH",
    "validationHash": "sha256:VALIDATION_HASH",
    "validationStatus": "succeeded",
    "validationRun": {
      "mode": "dry-run",
      "runnerScriptDigest": "sha256:SCRIPT_DIGEST",
      "sampleDataRefs": [
        { "port": "reads", "uri": "s3://...", "digest": "sha256:..." }
      ],
      "command": "bwa mem ref.fa reads.fastq",
      "exitCode": 0
    },
    "observedIoProfile": {
      "inputs": [
        { "port": "reads", "fileCount": 2, "nonEmpty": true }
      ],
      "outputs": [
        { "port": "alignment", "count": 1, "nonEmpty": true, "comparatorResult": "pass" }
      ]
    },
    "observedResourceProfile": {
      "executionEnvironment": {
        "type": "k8s_pod",
        "platform": "linux/amd64",
        "cpuLimit": "2",
        "memoryLimit": "4Gi"
      },
      "peakCpuMillicores": 1200,
      "peakMemoryMiB": 512,
      "durationSeconds": 42,
      "diskReadMiB": 180,
      "diskWriteMiB": 95
    },
    "contractCheck": {
      "allOutputsPresent": true,
      "comparatorResult": "pass",
      "details": []
    },
    "resourceRecommendation": {
      "requestCpu": "500m",
      "requestMemory": "256Mi",
      "limitCpu": "2000m",
      "limitMemory": "1Gi"
    },
    "profileStatus": "observed"
  }
}
```

### 9.3 infra-level failure 케이스 payload

```json
{
  "artifactType": "application/vnd.nodevault.toolprofile.v1+json",
  "subject": {
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "digest": "sha256:IMAGE_DIGEST"
  },
  "profile": {
    "casHash": "sha256:TOOLDEFINITION_CAS_HASH",
    "validationHash": null,
    "validationStatus": "infra_failed",
    "failureReason": "timeout",
    "observedResourceProfile": {
      "timeout": true,
      "timeoutSeconds": 1800
    },
    "profileStatus": "inconclusive"
  }
}
```

### 9.4 profileStatus 값

| 값 | 의미 |
|----|------|
| `observed` | 성공, validationHash 있음 |
| `inconclusive` | infra-level failure, 재시도 가능 |
| `failed` | application-level failure |
| `pending` | 아직 실행 전 |

### 9.5 Retention

- latest 3개 유지
- GC candidate 표시 (즉시 삭제 안 함)
- 임상·운영 evidence가 붙은 profile artifact는 manual review 대상

---

## 10. L5-b: Security Scan / ToolScanRecord

### 10.0 ToolSpec v0.5.1 모델 매핑

security referrer는 ToolScanRecord의 Harbor mirror다.

```
NodeVault Index (primary)   Harbor (secondary mirror)
ToolScanRecord           →  security referrer payload
```

ToolScanRecord의 key: imageDigest × scannerName × dbDigest

### 10.1 trivy-operator 통합 방향

NodeSentinel은 trivy를 직접 실행하지 않는다.
`trivy-operator`가 생성한 `VulnerabilityReport` CRD를 읽는 aggregator 역할을 한다.

```
trivy-operator
  → nodevault-security namespace에서 동작
  → Harbor image scan 완료 시 VulnerabilityReport CR 생성

NodeSentinel security worker
  → VulnerabilityReport CR 조회
  → CVE summary 추출 + reportDigest 계산
  → security referrer payload 생성
  → Harbor에 security referrer 첨부
  → index.Entry.SecurityScanDigest 갱신
```

### 10.2 security referrer 명세

```
artifactType: application/vnd.nodevault.security.v1+json
```

```json
{
  "artifactType": "application/vnd.nodevault.security.v1+json",
  "subject": {
    "mediaType": "application/vnd.oci.image.manifest.v1+json",
    "digest": "sha256:IMAGE_DIGEST"
  },
  "security": {
    "scanner": "trivy",
    "scannerVersion": "0.50.0",
    "source": "trivy-operator",
    "reportKind": "VulnerabilityReport",
    "reportDigest": "sha256:VULNERABILITY_REPORT_DIGEST",
    "scanTime": "2026-06-02T00:00:00Z",
    "summary": {
      "critical": 0,
      "high": 2,
      "medium": 5,
      "low": 12,
      "unknown": 0
    },
    "misconfiguration": {
      "high": 0,
      "medium": 1,
      "low": 3
    },
    "secretExposure": { "count": 0 },
    "policy": {
      "mode": "record_only",
      "result": "warning",
      "activeGate": false,
      "evaluatedAt": "2026-06-02T00:00:00Z"
    }
  }
}
```

### 10.3 기본 정책

```
mode: record_only
```

- security scan 결과 기록 = 기본
- NodePalette UI badge 표시 = 기본
- lifecycle_phase=Active 전환 차단 = 옵션 (기본은 꺼짐)
- `activeGate: true` + critical/high 위반 시에만 차단 가능

### 10.4 scan freshness

- 유효 기간: **30일**
- 만료 시 `integrity_health = Partial`
- reconcile loop에서 만료 감지 → 재스캔 트리거

### 10.5 Retention

- latest 3개 또는 최근 30일 유지
- GC candidate 표시 (즉시 삭제 안 함)

---

## 11. 실패 분류

| 분류 | 예시 | validationHash | retryable |
|------|------|----------------|-----------|
| infra-level | OOMKilled, timeout, scheduling failure, image pull failure, eviction, API unreachability, SIGKILL | 미생성 | true |
| application-level | exit code != 0, contract mismatch, expected output missing | 미생성 (기본) | false |
| 성공 | exit 0 + contractCheck pass | 생성 | — |

infra-level failure는 환경 문제이므로 재시도가 의미 있다.
application-level failure는 tool 또는 fixture 문제이므로 그대로 재시도해도 같은 결과가 나온다.

---

## 12. Certification 전이 (ToolSpec v0.5.1 모델)

### 12.0 lifecycle_phase → CertifiedToolImageRecord

ToolSpec v0.5.1 모델에서 lifecycle_phase=Active는 CertifiedToolImageRecord.promotion.status = active로 대체된다.

```
lifecycle_phase=Active (구 모델)
→ CertifiedToolImageRecord.promotion.status = active (신 모델)
```

### 12.1 Certification 조건

다음 조건이 모두 충족되어야 NodeVault가 CertifiedToolImageRecord를 생성한다:

```
ToolCheckRecord(toolSpecDigest, imageDigest).result = passed
ToolScanRecord(imageDigest, scannerName, dbDigest).result = passed
  (policy.mode = record_only인 경우 위반이 있어도 통과)
```

### 12.2 SecurityBlocked 조건

```
ToolScanRecord.policy.activeGate = true
AND (critical > 0 OR high > 허용 임계값)
```

이 경우 CertifiedToolImageRecord를 생성하지 않는다.
NodeKit Admin Review 화면에서 관리자가 정책 예외를 수동으로 승인할 수 있다.

### 12.3 전이 통보

NodeSentinel → NodeVault Index에 ToolCheckRecord / ToolScanRecord 저장.
NodeVault가 저장된 Records를 바탕으로 CertifiedToolImageRecord 생성 여부를 결정한다.
NodeSentinel은 직접 Certification을 결정하지 않는다.

---

## 13. client 노출 (NodePalette → DagEdit)

### 13.1 NodeVault Index stable projection이 단일 진실의 원천

NodePalette는 NodeVault Index의 stable projection을 읽는다.
Raw Record / Raw Spec을 직접 읽지 않는다.
NodeVault가 Certification을 통해 생성한 ToolFunctionCatalogEntry / CertifiedToolImageRecord만 소비한다.

```
[NodeVault]
  이미지 빌드 → Harbor push
  ToolBuildRecord / ToolImageRecord → NodeVault Index

[NodeSentinel]
  ToolCheckRecord / ObservedToolFunctionProfile → NodeVault Index (primary)
  ToolScanRecord → NodeVault Index (primary)
  toolprofile / security referrer → Harbor (secondary mirror)

[NodeVault]
  CertifiedToolImageRecord 생성
  ToolFunctionCatalogEntry / FunctionCompatibilityRecord 생성

[NodePalette] — 독립 repo, NodeVault Index 기반 stable projection 읽기
  NodeVault Index에서 CertifiedToolImageRecord / ToolFunctionCatalogEntry 조회
  Harbor referrer는 optional (external audit / portability용)
```

### 13.2 NodeVault Index Records와 Harbor referrer mirror 대응

| NodeVault Index (primary) | Harbor referrer mirror (secondary) | 작성 주체 | 의미 |
|--------------------------|-------------------------------------|----------|------|
| ResolvedToolSpec | toolspec referrer | NodeVault | 등록 시점 declared metadata |
| ToolCheckRecord / ObservedToolFunctionProfile | toolprofile referrer | NodeSentinel | dry-run 통과 evidence |
| ToolScanRecord | security referrer | NodeSentinel | 취약성 스캔 결과 |

### 13.3 NodePalette 노출 조건

```
CertifiedToolImageRecord 존재 (toolSpecDigest × platform)
  AND promotion.status = active
  AND ToolScanRecord.policy.activeGate = false  (또는 위반 없음)
```

이 조건을 NodeVault Index stable projection API로 평가한다.
Harbor referrer를 직접 조회하지 않는다 (Harbor는 optional mirror).

### 13.4 NodePalette API 응답 예시

```json
{
  "casHash": "sha256:existing-cas",
  "stableRef": "bwa@0.7.17",
  "display": { "label": "BWA 0.7.17", "category": "Alignment" },
  "inputs": [ { "name": "reads", "format": "FASTQ", "shape": "pair" } ],
  "outputs": [ { "name": "alignment", "format": "SAM" } ],
  "observedProfileDigest": "sha256:toolprofile-referrer-digest",
  "validationHash": "sha256:validation-hash",
  "securityScanDigest": "sha256:security-referrer-digest",
  "validationStatus": "succeeded",
  "securityStatus": "pass",
  "resourceRecommendation": {
    "requestCpu": "500m",
    "requestMemory": "256Mi",
    "limitCpu": "2000m",
    "limitMemory": "1Gi"
  }
}
```

### 13.5 DagEdit UI badge

| 조건 | badge |
|------|-------|
| `validationHash` 있음 + `securityStatus=pass` | **Verified** |
| `validationHash` 있음 + `securityStatus=warning` | **Security Warning** |
| `activeGate=true` + 위반 | **Security Blocked** (NodePalette 미노출) |
| `toolprofile referrer` 없음 | **Unverified** |
| `validationHash` 없음 | **No Dry-run Profile** |
| `security referrer` 없음 | **Security Not Scanned** |

### 13.6 DagEdit RunnerNode 저장 방식

```json
{
  "nodeType": "runner",
  "toolFunctionSpecDigest": "sha256:...",
  "selectedPlatform": { "os": "linux", "arch": "amd64" },
  "stableRef": "bwa@0.7.17",
  "portMetadata": {
    "observedProfileDigest": "sha256:...",
    "validationHash": "sha256:..."
  }
}
```

authoring pin은 `toolFunctionSpecDigest`. `stableRef`는 UI 표시용 snapshot.
실행 시: toolFunctionSpecDigest + platform → CertifiedToolFunctionImageRecord → imageDigest → JUMI.
파이프라인 저장 후 tool이 retract되어도 toolFunctionSpecDigest로 계약을 추적할 수 있다.

### 13.7 NodePalette 독립 repo 구조

현재 위치: `NodeVault/cmd/palette/` + `pkg/catalogrest/` (같은 go.mod)
목표 위치: `github.com/HeaInSeo/NodePalette` (독립 repo)

NodePalette의 유일한 외부 의존성:
- Harbor OCI API (referrer 조회)
- DockGuard 정책 없음, NodeVault gRPC 없음, NodeSentinel gRPC 없음

---

## 14. ValidateService / NodeSentinel 경계 (전환 정책)

### 14.1 현재 상태 (전환기)

ValidateService는 NodeVault 측에 존재하는 전환기 API boundary다.
초기 구현에서는 NodeVault가 일부 check/scan 로직을 직접 수행할 수 있다.

```
[전환기 Option A]
NodeVault exposes ValidateService
  → internally calls NodeSentinel adapter
  → ToolCheckRecord / ToolScanRecord를 NodeVault Index에 저장

[전환기 Option B]
NodeSentinel exposes validation API directly
  → NodeVault receives Records
  → NodeVault stores in Index and decides Certification
```

### 14.2 목표 상태 (target)

```
NodeSentinel = validation / dry-run / smoke-run / observation producer (target execution owner)
NodeVault = record store / certification / promotion / catalog index owner
```

ToolScanRecord / ToolCheckRecord / ToolFunctionCheckRecord / ObservedToolFunctionProfile의
생산 책임은 NodeSentinel을 목표 상태로 둔다.

### 14.3 전환 원칙

```
ValidateService API는 즉시 삭제하지 않는다.
전환기 compatibility layer로 유지할 수 있다.
하지만 target execution owner는 NodeSentinel이다.
```

---

## 15. 단계적 구현 순서

### Phase 1 — WorkStore (완료)

- `pkg/work/store.go`: Store 인터페이스 정의
- `pkg/work/sqlite/store.go`: SQLite 구현
- `pkg/work/sqlite/store_test.go`: CreateJob, LeaseJob, Heartbeat, CompleteJob, FailJob 테스트
- `cmd/nodesentinel/main.go`: placeholder

### Phase 1 나머지 — ingress gRPC

- `EnqueueValidationWork` proto 정의
- NodeSentinel gRPC 서버 기동 (`cmd/nodesentinel/main.go` 실제 구현)
- NodeVault 측 enqueue 호출 추가
- GRPCRoute 배포 YAML

### Phase 2 — smoke-run worker

- worker goroutine (LeaseJob loop)
- L3 dry-run Job 제출 + 결과 수집
- L4 smoke-run Job 제출 + 결과 수집
- infra-level / application-level failure 분류
- CompleteJob / FailJob 처리

### Phase 3 — functional validation + toolprofile referrer

- sample fixture 로드
- L5-a Job 실행 + observedIoProfile 수집
- observedResourceProfile 수집
- validationHash 계산
- toolprofile referrer payload 생성
- Harbor에 OCI referrer 첨부 (sori 연동)
- NodeVault에 observedProfileDigest 통보

### Phase 4 — security scan + security referrer

- trivy-operator VulnerabilityReport CRD 조회
- security referrer payload 생성
- Harbor에 OCI referrer 첨부
- NodeVault에 securityScanDigest 통보
- policy gate 평가 → lifecycle_phase Active 전환 통보

---

## 16. 관련 문서

| 문서 | 경로 | 내용 |
|------|------|------|
| ToolSpec integrated | `platform-docs/TOOLSPEC_INTEGRATED_v0.5.1.md` | ToolSpec v0.5.1 통합 명세 — Record 모델, Certification, casHash 마이그레이션 |
| Platform Architecture Decisions | `platform-docs/PLATFORM_ARCHITECTURE_DECISIONS_v0.1.md` | 플랫폼 핵심 결정사항 |
| Platform Component Map | `platform-docs/PLATFORM_COMPONENT_MAP_v0.1.md` | 전체 컴포넌트 역할/위치/통신 |
| observed profile spec | `NodeVault/docs/OBSERVED_PROFILE_SPEC.md` | toolprofile referrer 상세 명세 |
| security scan spec | `NodeVault/docs/SECURITY_SCAN_SPEC.md` | security referrer 상세 명세 |
| WorkStore 구현 | `NodeSentinel/pkg/work/` | 현재 SQLite 구현 |
