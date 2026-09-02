# offload-ingest — build, test and cross-compile the ingest load generator.

BINARY      := loadtest
PKG         := ./cmd/loadtest
MODULE      := github.com/offloadintelligence/offload-ingest
BIN_DIR     := bin
DIST_DIR    := dist
COVER_FILE  := coverage.out

VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT      ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
BUILD_DATE  ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(BUILD_DATE)

GO       ?= go
GOFLAGS  := -trimpath
COMPOSE  := docker-compose -f deployments/docker-compose.yml

# Cross-compilation matrix. linux/amd64 is the deployment target; the rest are
# for running the generator from a workstation.
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.DEFAULT_GOAL := build
.PHONY: help build run endpoints capture validate-routes compare-schemas verify-feeds install clean \
	licensetool keygen license dashboard production simulation \
        test test-race test-short cover bench \
        fmt vet lint tidy verify check \
        build-linux-amd64 cross release \
        docker-build docker-push compose-up compose-down compose-logs compose-ps

## help: list the available targets
help:
	@echo "offload-ingest targets:"
	@sed -n 's/^## \(.*\)/  \1/p' $(MAKEFILE_LIST) | sort

# --- build ----------------------------------------------------------------

## build: compile the binary for the host platform into ./bin
build:
	@mkdir -p $(BIN_DIR)
	CGO_ENABLED=0 $(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$(BINARY) $(PKG)
	@echo "built $(BIN_DIR)/$(BINARY) ($(VERSION))"

## run: build and run a short dry-run load test
run: build
	./$(BIN_DIR)/$(BINARY) -dry-run -duration 10s -stats-every 2s

## capture: refresh the captured provider responses (needs keys)
capture:
	$(GO) run ./cmd/schematool capture

## validate-routes: call every registered route against the live API (needs a key)
validate-routes:
	$(GO) run ./cmd/schematool routes

## compare-schemas: diff generated payloads against the captured responses
compare-schemas:
	$(GO) run ./cmd/schematool schemas

## verify-feeds: both provider checks — payload shape and route validity
verify-feeds: compare-schemas validate-routes

## endpoints: list every upstream endpoint the generators cover
endpoints: build
	./$(BIN_DIR)/$(BINARY) -endpoints

## install: install the binary into GOBIN
install:
	CGO_ENABLED=0 $(GO) install $(GOFLAGS) -ldflags '$(LDFLAGS)' $(PKG)

## clean: remove build artifacts and coverage output
clean:
	rm -rf $(BIN_DIR) $(DIST_DIR) $(COVER_FILE) coverage.html
	$(GO) clean -cache -testcache

# --- test -----------------------------------------------------------------

## test: run the full test suite
test:
	$(GO) test ./... -count=1

## test-race: run the suite under the race detector
test-race:
	$(GO) test ./... -race -count=1

## test-short: skip long-running tests
test-short:
	$(GO) test ./... -short -count=1

## cover: run tests with coverage and write coverage.html
cover:
	$(GO) test ./... -covermode=atomic -coverprofile=$(COVER_FILE) -count=1
	$(GO) tool cover -html=$(COVER_FILE) -o coverage.html
	@$(GO) tool cover -func=$(COVER_FILE) | tail -1

## bench: run benchmarks without running tests
bench:
	$(GO) test ./... -run '^$$' -bench . -benchmem

# --- quality --------------------------------------------------------------

## fmt: format all Go source
fmt:
	$(GO) fmt ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## lint: run golangci-lint if it is installed
lint:
	@command -v golangci-lint >/dev/null 2>&1 \
		&& golangci-lint run ./... \
		|| echo "golangci-lint not installed; skipping (brew install golangci-lint)"

## tidy: sync go.mod and go.sum
tidy:
	$(GO) mod tidy

## verify: verify module dependencies against go.sum
verify:
	$(GO) mod verify

## check: fmt, vet, lint and the race suite — run this before pushing
check: fmt vet lint test-race

# --- cross-compilation ----------------------------------------------------

## build-linux-amd64: cross-compile the deployment target into ./dist
build-linux-amd64:
	@mkdir -p $(DIST_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
		-o $(DIST_DIR)/$(BINARY)-linux-amd64 $(PKG)
	@echo "built $(DIST_DIR)/$(BINARY)-linux-amd64"

## cross: cross-compile every platform in the matrix into ./dist
cross:
	@mkdir -p $(DIST_DIR)
	@for platform in $(PLATFORMS); do \
		os=$${platform%%/*}; arch=$${platform##*/}; \
		ext=""; [ "$$os" = "windows" ] && ext=".exe"; \
		out="$(DIST_DIR)/$(BINARY)-$$os-$$arch$$ext"; \
		echo "  $$os/$$arch -> $$out"; \
		CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch \
			$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o "$$out" $(PKG) || exit 1; \
	done
	@echo "cross-compiled $(words $(PLATFORMS)) platforms into $(DIST_DIR)/"

## release: cross-compile everything, then tar each binary with a checksum
release: clean cross
	@cd $(DIST_DIR) && for f in $(BINARY)-*; do \
		tar -czf "$$f.tar.gz" "$$f"; \
	done && shasum -a 256 *.tar.gz > SHA256SUMS
	@echo "release artifacts in $(DIST_DIR)/"

# --- docker ---------------------------------------------------------------

## docker-build: build the linux/amd64 container image
docker-build:
	docker buildx build \
		--platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-f deployments/Dockerfile \
		-t offload-ingest/$(BINARY):$(VERSION) \
		-t offload-ingest/$(BINARY):latest \
		--load .

## docker-push: build and push the linux/amd64 image (set REGISTRY=...)
docker-push:
	@test -n "$(REGISTRY)" || { echo "set REGISTRY, e.g. make docker-push REGISTRY=ghcr.io/offloadintelligence"; exit 1; }
	docker buildx build \
		--platform linux/amd64 \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg BUILD_DATE=$(BUILD_DATE) \
		-f deployments/Dockerfile \
		-t $(REGISTRY)/$(BINARY):$(VERSION) \
		-t $(REGISTRY)/$(BINARY):latest \
		--push .

## compose-up: start Kafka, Kafka UI and the load generator
compose-up:
	VERSION=$(VERSION) COMMIT=$(COMMIT) BUILD_DATE=$(BUILD_DATE) $(COMPOSE) up -d --build

## compose-down: stop the stack and remove its volumes
compose-down:
	$(COMPOSE) down -v

## compose-logs: follow the load generator's logs
compose-logs:
	$(COMPOSE) logs -f loadtest

## compose-ps: show the status of the stack
compose-ps:
	$(COMPOSE) ps

# --- licensing ---------------------------------------------------------------
# The private signing key never leaves the build system. keygen writes it once;
# everything after that consumes only the public half.

## keygen: generate the Ed25519 signing pair into ./keys (run once)
keygen:
	@go run ./cmd/licensetool keygen -out keys

## fingerprint: print this machine's hardware fingerprint, for pinning a licence
fingerprint:
	@go run ./cmd/licensetool fingerprint

## license: issue a development licence pinned to this machine
license:
	@go run ./cmd/licensetool sign \
		-key keys/license.priv \
		-tenant $(or $(TENANT),dev-venue) \
		-venue "$(or $(VENUE),Development Venue)" \
		-tier $(or $(TIER),free) \
		-sports $(or $(SPORTS),nfl,ncaaf,ncaab,nba,soccer,afl,rugby,cricket,tennis,golf,ufc,mma,nascar,f1) \
		-regions $(or $(REGIONS),global) \
		-fingerprint $$(go run ./cmd/licensetool fingerprint) \
		-days $(or $(DAYS),365) \
		-out license.key

## verify-license: check license.key the way the binary does
verify-license:
	@OFFLOAD_LICENSE_PUBKEY=$$(cat keys/license.pub) \
		go run ./cmd/licensetool verify -license license.key

# --- running -----------------------------------------------------------------

## simulation: generated payloads, no upstream quota spent
simulation:
	@OFFLOAD_LICENSE_PUBKEY=$$(cat keys/license.pub) \
		go run ./cmd/loadtest -mode simulation -dry-run \
			-dashboard-addr $(or $(DASHBOARD),:8090) \
			-metrics-addr $(or $(METRICS),:9102) \
			-duration $(or $(DURATION),60s)

## production: live API-Sports ingest, paced by the licence tier
production:
	@OFFLOAD_LICENSE_PUBKEY=$$(cat keys/license.pub) \
		go run ./cmd/loadtest -mode production -dry-run \
			-dashboard-addr $(or $(DASHBOARD),:8090) \
			-metrics-addr $(or $(METRICS),:9102) \
			-duration $(or $(DURATION),120s)

## dashboard: run in simulation with the operator page open indefinitely
dashboard:
	@OFFLOAD_LICENSE_PUBKEY=$$(cat keys/license.pub) \
		go run ./cmd/loadtest -mode simulation -dry-run \
			-dashboard-addr $(or $(DASHBOARD),:8090) \
			-metrics-addr $(or $(METRICS),:9102) -duration 0
