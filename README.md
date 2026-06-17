# NodeSentinel

NodeVault가 Harbor에 push한 Tool 이미지를 대상으로 K8s 데이터플레인 검증을 수행하는 에이전트.
NodeVault로부터 `EnqueueValidationWork` gRPC 호출로 작업을 수신하고, L3 → L4 → L5-a → L5-b 순서로 검증을 실행한 뒤 결과를 NodeVault REST로 제출한다.

→ 전체 플랫폼 구성 및 end-to-end 흐름: [NodeVault README](https://github.com/HeaInSeo/NodeVault/blob/main/README.md)

---

## 아키텍처 경계

NodeSentinel은 **검증 에이전트**다.

- 이미지 빌드: NodeVault 책임
- Harbor 인증 관리: NodeVault 책임
- Catalog 등록 및 인증 결정: NodeVault `certification.Service` 책임
- NodeSentinel은 K8s Job 실행과 결과 POST만 담당한다.

---

## 검증 단계

| 단계 | 설명 |
|------|------|
| **L3** | K8s Job dry-run — manifest admission 검증 (API server가 거부하는 spec 사전 차단) |
| **L4** | K8s smoke run — 컨테이너를 실제 기동해 정상 종료 여부 확인 |
| **L5-a** | functional validation Job 실행 → `validationHash` 계산 → `POST /v1/validation/check-records` |
| **L5-b** | trivy-operator `VulnerabilityReport` 조회 → `POST /v1/validation/scan-records` |

L3 또는 L4가 실패하면 L5 단계는 실행되지 않는다.
각 단계 결과는 `WorkStore`(SQLite)에 기록되고, 최종 결과가 NodeVault에 제출된다.

---

## 전체 구조

```
NodeVault
    │  EnqueueValidationWork (gRPC)
    ▼
NodeSentinel (이 프로젝트)
    │
    ├── pkg/ingress      — gRPC IngressService: 작업 수신 → WorkStore 저장
    ├── pkg/work/sqlite  — WorkStore: SQLite 기반 작업 큐 및 상태 추적
    ├── pkg/worker       — 검증 실행: L3/L4 K8s Job + L5-a/L5-b 결과 분류
    └── pkg/vaultclient  — NodeVault REST 클라이언트: check-records / scan-records POST
    │
    ├── L3: K8s Job dry-run
    ├── L4: K8s smoke run
    ├── L5-a: functional validation → validationHash → POST /v1/validation/check-records
    └── L5-b: trivy-operator VulnerabilityReport → POST /v1/validation/scan-records

NodeVault
    └── certification.Service: check+scan 평가 → CertifiedToolImageRecord 생성
```

---

## 주요 패키지

| 패키지 | 역할 |
|--------|------|
| `pkg/ingress` | gRPC `IngressService` 구현 — `EnqueueValidationWork` 수신 및 WorkStore 저장 |
| `pkg/worker` | 검증 실행 루프 — L3/L4 K8s Job 생성·조회, L5-a/L5-b 실행 및 결과 분류 |
| `pkg/work/sqlite` | SQLite 기반 `WorkStore` — 작업 큐 FIFO, 상태 전이(pending → running → done) |
| `pkg/vaultclient` | NodeVault REST 클라이언트 — `POST /v1/validation/check-records`, `POST /v1/validation/scan-records` |

---

## 환경 변수

| 변수 | 기본값 | 설명 |
|------|--------|------|
| `NODEVAULT_API_ADDR` | `http://nodevault.nodevault-system.svc:8082` | NodeVault REST 주소 (결과 제출) |
| `SMOKE_NAMESPACE` | `nodevault-smoke` | L4 smoke run Job 실행 네임스페이스 |
| `NODESENTINEL_GRPC_ADDR` | `:50052` | gRPC 서버 바인딩 주소 |
| `KUBECONFIG` | `~/.kube/config` | 로컬 실행 전용; Pod는 ServiceAccount 사용 |

---

## 빌드 및 실행

### 사전 조건

| 도구 | 용도 |
|------|------|
| Go 1.25.5 이상 | 빌드 |
| kubectl / in-cluster ServiceAccount | L3/L4 K8s Job 제어 |
| trivy-operator | L5-b `VulnerabilityReport` CRD 필요 |

### 빌드

```bash
go build ./...
```

### 테스트

```bash
go test -race ./...
```

### 실행 (로컬 디버깅)

```bash
NODEVAULT_API_ADDR=http://localhost:8082 \
SMOKE_NAMESPACE=nodevault-smoke \
go run ./cmd/nodesentinel
```

Kubernetes 배포는 NodeVault `deploy/` 매니페스트와 함께 동일 네임스페이스(`nodevault-system`)에 배포한다.

---

## CI (GitHub Actions)

`.github/workflows/ci.yml` 구성:

| Job | 내용 |
|-----|------|
| `lint` | golangci-lint (zero-warning) |
| `build` | go build + go vet |
| `test` | -race -cover |
| `vuln-scan` | govulncheck (continue-on-error) |

---

## 관련 프로젝트

| 프로젝트 | 역할 |
|----------|------|
| [`NodeVault`](https://github.com/HeaInSeo/NodeVault) | 이미지 빌드·등록·인증 — NodeSentinel 작업 enqueue 및 결과 수신 |
| [`NodePalette`](https://github.com/HeaInSeo/NodePalette) | 인증 tool 팔레트 REST 서비스 |
| [`NodeKit`](https://github.com/HeaInSeo/NodeKit) | C# 어드민 UI — ToolDefinition 편집 및 BuildRequest gRPC 전송 |
| [`DockGuard`](https://github.com/HeaInSeo/DockGuard) | OPA/Rego Dockerfile 정책 + .wasm 번들 빌드 |
