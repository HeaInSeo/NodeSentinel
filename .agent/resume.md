# RESUME — NodeSentinel L5 증거 실체화 (functional observation · scan provenance)

`작업 ID` NS-L5-EVIDENCE · `repo` HeaInSeo/NodeSentinel (한 저장소만) · `base` origin/main `bc2d7fb`
`branch/worktree` **ns-l5-evidence** = 이 worktree. **커밋만. push·PR·머지 금지.** 완료 후 오퍼레이터에게 보고하면 오퍼레이터가 push/PR/머지 판정.
`실행 지침` **이 파일 + 저장소 코드만으로 작업한다. 전역 Notion 검색·웹 조사 금지.** 필요한 설계 결정은 아래에 원문으로 있다.

## 설계 에이전트 결정 (정본, 원문)
> NodeSentinel이 L3~L5 실행·증거의 소유자다. **L5-a는 실제 command/output/resource 관측을 기반으로 해야 하며 allOutputsPresent와 validationHash를 추정해서는 안 된다.** L5-b는 scanner/version뿐 아니라 **platform, DB digest, scanned-at provenance를 보존**한다.

연결: 정본 §21.27.3 O1/O2 · 헌법 §1.10(관측하지 않은 것을 기록하지 않는다) · gap-register `#21` · OBS-8. 이 작업이 NodeVault "L4-only Active 인증 차단" 게이트를 언블록한다.

## 현재 결함 (코드로 확인됨 — 여기서 출발, R8)
`pkg/worker/l5a.go` 139~151:
- 주석이 이미 자백한다: "⚠ 이 validationHash는 검증 증거가 아니다 … ImageDigest의 다른 표현일 뿐이다. 아래 allOutputsPresent도 관측이 아니다."
- `validationHash := computeValidationHash(job.ImageDigest, command, exitCode)` — **관측이 아니라 입력의 해시**.
- 151행 `allOutputsPresent: true` — **상수**(관측 안 하고 true 박음). = §1.10이 정확히 겨누는 「상수는 결측보다 나쁘다」.
`pkg/worker/l5b.go`: `Scanner`/`ScannerVersion`은 보존하나 **platform · DB digest · scanned-at provenance**가 빠져 있는지 확인하라.

## 할 일
### L5-a — 추정 제거, 실제 관측 기반 또는 not-observed
1. **`allOutputsPresent`를 상수 `true`로 박지 마라.** 실제 실행에서 선언된 output들이 관측됐는지를 **실측**하거나(관측 장치가 있으면), **관측 수단이 없으면 not-observed/unknown**으로 남긴다(추정 금지 — R5·§1.10). 절대 관측 없이 true로 두지 마라.
2. **`validationHash`**를 「검증 증거」로 기록하지 마라. 실제 command/output/resource 관측 기반이 아니면, 그 필드를 검증 증거처럼 제출하지 말고 not-observed로 구분하거나(§1.10) OBS-8 재정의가 선행이면 그 상태를 명시.
3. ⚠ **실제 관측(output/resource capture) 메커니즘이 저장소에 아직 없으면**, 임의로 관측 파이프라인을 크게 신설하지 말고 **① 가짜 상수를 제거해 not-observed로 정직하게 남기고 ② 무엇이 선행(관측 장치)인지 보고**(R6/DESIGN_GAP). 「미구현은 괜찮다 — 미구현을 구현된 것처럼 표시하는 것만 고친다」(R1).
### L5-b — provenance 보존
`platform` · `DB digest`(vuln DB digest) · `scanned-at`을 L5-b 증거(ToolScanRecord 등)에 **additive로 보존**. 기존 scanner/version 유지. 관측 안 한 provenance는 추정하지 말고 not-available.

## 하지 말 것
- 관측하지 않은 값을 상수/추정으로 기록(R5·§1.10). true·"passed"·0 「일단 넣기」 금지.
- 관측 장치가 완비되기 전 관측 단계를 「켜서」 `/bin/sh -c true` 결과가 allOutputsPresent:true로 기록되게 만들지 마라(정본 O4 계열).
- 저장소 경계 넘기(R7) — NodeSentinel만. NodeVault/proto 계약 변경 필요하면 멈추고 보고(별건).
- push·PR·머지·`--admin`. 대형 리팩터 임의 신설(관측 파이프라인 등은 선행 판정 필요).
- 전역 Notion 검색·웹 조사.

## 검증 (R4)
- 신규/회귀 테스트: allOutputsPresent가 **관측 없이는 true가 되지 않음**(현재 상수 동작이 수정 전 실패→후 통과하도록), validationHash가 검증증거로 오제출 안 됨, L5-b provenance(platform/dbDigest/scannedAt) 보존. 수정 전 실패 먼저 확인.
- `go build ./...` · `go test ./pkg/worker/... ./pkg/vaultclient/...` · `go vet` · `golangci-lint`(있으면) 실행·출력 원문. 환경: 필요시 `unset GOROOT; PATH=/usr/local/go/bin`, gpgme 회피 build tag. 못 하는 건 「불가」로 명시(R5).

## 완료 보고 (오퍼레이터에게)
1. l5a.go에서 allOutputsPresent/validationHash를 어떻게 처리했는지(관측 기반 vs not-observed) + 관측 장치 부재 시 선행 판정.
2. l5b.go provenance additive 추가 위치.
3. 변경 파일·diff·커밋 해시.
4. 테스트 수정 전 실패→후 통과 원문, build/test/vet/lint 출력.
5. 범위 밖(NodeVault proto 등)이라 손대지 않은 것 · 「하지 말 것」 참은 것 · R6 지점.

막히면 R6로 멈추고 보고(가짜로 채우지 말 것).

---
# FIXUP-1 (CI Lint) — firstNestedStr unused bool return
`상태` PR #21 오픈됨. CI Lint가 **딱 1건 신규 지적**: `pkg/worker/l5b.go:186:77 firstNestedStr - result 1 (bool) is never used (unparam)`.
(나머지 Lint 지적 main.go:166/170·store.go:216/703 와 govulncheck 실패는 **기존 부채**로 main도 red·비-required — 이번 PR과 무관, 손대지 마라 R2.)

## 할 일 (이것만)
`firstNestedStr`의 반환을 `(string, bool)` → `string` 하나로 바꾼다.
- `pkg/worker/l5b.go:186` 시그니처: `func firstNestedStr(obj map[string]interface{}, paths ...[]string) string {`
- 본문: 매치 시 `return value`, 끝에서 `return ""` (bool 제거).
- 호출부 2곳: `s.DBDigest, _ = firstNestedStr(...)` → `s.DBDigest = firstNestedStr(...)` (l5b.go:166), `s.ScannedAt, _ = firstNestedStr(...)` → `s.ScannedAt = firstNestedStr(...)` (l5b.go:172).
## 검증
`golangci-lint run ./pkg/worker/...`(또는 repo make lint)에서 l5b.go unparam 사라짐 확인. `go build`·`go test ./pkg/worker/...` 여전히 green. 커밋만(push 금지) 후 보고.

---
# FIXUP-2 (설계 결정 A) — L5-b provenance를 실측 필드로 축소
`근거` 설계 결정 A: "stock trivy-operator report에서 실제 관측되는 scanner/name/version과 scanned_at만 보존. platform/db_digest synthetic lookup과 테스트는 제거하고 두 필드는 not-observed/omitted. 실제 platform/db_digest는 후속 scanner-produced evidence envelope에서 수집." (§1.10 — 합성키로 관측을 가장하지 않는다.)

## 할 일 (이것만, NodeSentinel repo)
1. `pkg/worker/l5b.go`: **platform·db_digest 파싱 제거** — `s.Platform, _ = nestedStr(obj,"report","artifact","platform")` 및 `s.DBDigest = firstNestedStr(...)` 블록 삭제. `trivyVulnSummary`의 `Platform`·`DBDigest` 필드 삭제. **`ScannedAt` 파싱·`Scanner`·`ScannerVersion`은 유지.**
2. `pkg/vaultclient/client.go`: `SubmitScanRecordRequest`의 `Platform`·`DBDigest` 필드 삭제(후속 envelope packet에서 재도입). **`ScannedAt`·`Scanner`·`ScannerVersion`은 유지.**
3. l5b 제출부(SubmitScanRecordRequest 생성)에서 Platform/DBDigest 대입 제거.
4. **테스트**: `l5b_test.go`·`l5b_integration_test.go`에서 **platform/dbDigest 합성키 주입과 그 assertion 제거**. scanned_at·scanner/version assertion은 유지(실측 필드). firstNestedStr이 scannedAt에만 쓰이면 유지, 안 쓰이면 정리.
5. FIXUP-1(firstNestedStr string 반환)은 이미 적용됨 — 유지.

## 하지 말 것
- platform/db_digest를 다른 임의 소스에서 끌어오지 마라(후속 envelope 설계 몫). not-observed/omitted로 둔다.
- L5-a(not-observed) 부분·scanned_at·scanner/version 건드리지 마라. 다른 파일 「이왕이면」 금지(R2).
- push·머지 금지(오퍼레이터가 함).

## 검증
`unset GOROOT; export PATH=/usr/local/go/bin`, 태그 3개+`-buildvcs=false`.
- `go build`·`go test ./pkg/worker/... ./pkg/vaultclient/...` green. `golangci-lint run ./pkg/worker/... ./pkg/vaultclient/...` 0(안 돌면 R5 명시).
- 제거 후 남은 provenance는 scanned_at·scanner/version뿐이고 platform/db_digest 관련 코드·테스트 0건임을 grep로 확인.
## 보고
변경 파일·diff·커밋 해시, build/test/lint 출력, platform/db_digest 잔존 0 확인. 커밋만.
