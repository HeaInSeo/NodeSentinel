.PHONY: verify ci fmt fmt-check mod-tidy-check lint lint-fix lint-config buf-lint test test-unit test-k8s coverage coverage-check build vet vuln proto

LOCALBIN ?= $(CURDIR)/bin
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
GOLANGCI_LINT_VERSION ?= v2.11.3
GOVULNCHECK ?= $(LOCALBIN)/govulncheck
COVERAGE_THRESHOLD ?= 70.0
SERVICE_PACKAGES = $(shell go list -f '{{if or .TestGoFiles .XTestGoFiles}}{{.ImportPath}}{{end}}' ./... | grep -v '/protos/' | grep -v '/test/k8s')
K8S_TEST_PACKAGES = ./test/k8s/...
PROTOC    ?= protoc
PROTO_SRC ?= ./protos

verify: fmt-check mod-tidy-check lint-config lint build vet test-unit test-k8s coverage-check

ci: verify

fmt:
	go fmt ./...

fmt-check:
	@test -z "$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*'))" || { \
		echo "gofmt is required for:"; \
		gofmt -l $$(find . -name '*.go' -not -path './vendor/*'); \
		exit 1; \
	}

mod-tidy-check:
	@before="$$(mktemp)"; after="$$(mktemp)"; \
	git diff -- go.mod go.sum > "$$before"; \
	go mod tidy; \
	git diff -- go.mod go.sum > "$$after"; \
	if ! diff -u "$$before" "$$after"; then \
		rm -f "$$before" "$$after"; \
		exit 1; \
	fi; \
	rm -f "$$before" "$$after"

$(GOLANGCI_LINT):
	@mkdir -p "$(LOCALBIN)"
	@test -x "$(GOLANGCI_LINT)" || bash -c '\
		set -euo pipefail; \
		OS="$$(uname | tr A-Z a-z)"; \
		ARCH="$$(uname -m)"; \
		case "$$ARCH" in x86_64) ARCH=amd64 ;; aarch64|arm64) ARCH=arm64 ;; *) echo "unsupported arch: $$ARCH"; exit 1 ;; esac; \
		VER="$(GOLANGCI_LINT_VERSION)"; \
		VER="$${VER#v}"; \
		FILE="golangci-lint-$$VER-$$OS-$$ARCH.tar.gz"; \
		URL="https://github.com/golangci/golangci-lint/releases/download/$(GOLANGCI_LINT_VERSION)/$$FILE"; \
		SUM_URL="https://github.com/golangci/golangci-lint/releases/download/$(GOLANGCI_LINT_VERSION)/golangci-lint-$$VER-checksums.txt"; \
		TMP="$$(mktemp -d)"; \
		curl -fsSL "$$URL" -o "$$TMP/lint.tgz"; \
		curl -fsSL "$$SUM_URL" -o "$$TMP/checksums.txt"; \
		EXPECTED="$$(awk -v f="$$FILE" "\$$2==f{print \$$1}" "$$TMP/checksums.txt")"; \
		ACTUAL="$$(sha256sum "$$TMP/lint.tgz" | awk "{print \$$1}")"; \
		if [ -z "$$EXPECTED" ] || [ "$$EXPECTED" != "$$ACTUAL" ]; then echo "checksum mismatch for $$FILE"; exit 1; fi; \
		tar -xzf "$$TMP/lint.tgz" -C "$$TMP"; \
		cp "$$TMP/golangci-lint-$$VER-$$OS-$$ARCH/golangci-lint" "$(GOLANGCI_LINT)"; \
		chmod +x "$(GOLANGCI_LINT)"; \
		rm -rf "$$TMP"'

lint: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --config=.golangci.yml ./...

lint-fix: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) run --config=.golangci.yml --fix ./...

lint-config: $(GOLANGCI_LINT)
	$(GOLANGCI_LINT) config verify --config=.golangci.yml

$(GOVULNCHECK):
	@mkdir -p "$(LOCALBIN)"
	GOBIN="$(LOCALBIN)" go install golang.org/x/vuln/cmd/govulncheck@latest

# ── Proto lint (Buf) ─────────────────────────────────────────────────────────
buf-lint:
	@command -v buf >/dev/null 2>&1 || { \
		echo "ERROR: buf is required; install it from https://buf.build/docs/installation/" >&2; \
		exit 1; \
	}
	buf lint

build:
	go build ./...

vet:
	go vet ./...

test:
	go test -v -race ./...

test-unit:
	go test -v -race -shuffle=on -count=1 $(SERVICE_PACKAGES)

test-k8s:
	go test -v -count=1 $(K8S_TEST_PACKAGES)

coverage:
	@printf "mode: atomic\n" > coverage.out
	@set -e; \
	i=0; \
	for pkg in $(SERVICE_PACKAGES); do \
		i=$$((i + 1)); \
		profile="coverage.$$i.out"; \
		go test -v -race -covermode=atomic -coverprofile="$$profile" "$$pkg"; \
		tail -n +2 "$$profile" >> coverage.out; \
		rm -f "$$profile"; \
	done
	go tool cover -func=coverage.out | tail -1

coverage-check: coverage
	@coverage="$$(go tool cover -func=coverage.out | awk '/^total:/ { gsub(/%/, "", $$3); print $$3 }')"; \
	awk -v got="$$coverage" -v want="$(COVERAGE_THRESHOLD)" 'BEGIN { \
		if (got + 0 < want + 0) { \
			printf("coverage %.1f%% below required %.1f%%\n", got, want); exit 1 \
		} \
		printf("coverage %.1f%% >= %.1f%%\n", got, want) \
	}'

vuln: $(GOVULNCHECK)
	$(GOVULNCHECK) ./...

# ── proto 코드 생성 ───────────────────────────────────────────────────────────
# NodeVault와 동일한 컨벤션: .pb.go / _grpc.pb.go를 .proto와 같은 디렉토리에 생성.
proto:
	$(PROTOC) --proto_path=$(PROTO_SRC) \
	  --go_out=$(PROTO_SRC) --go_opt=paths=source_relative \
	  --go-grpc_out=$(PROTO_SRC) --go-grpc_opt=paths=source_relative \
	  $(shell find $(PROTO_SRC) -name '*.proto')
