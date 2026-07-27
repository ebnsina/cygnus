CYGNUS_DATABASE_URL ?= postgres://cygnus:cygnus@localhost:5433/cygnus?sslmode=disable
export CYGNUS_DATABASE_URL

GO      ?= go
BIN     := bin
SPIKE   := $(BIN)/spike

.DEFAULT_GOAL := help

## help: list available targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /' | column -t -s ':'

## db-up: start PostgreSQL and wait until it is ready
db-up:
	docker compose up -d --wait postgres

## db-down: stop PostgreSQL, preserving data
db-down:
	docker compose down

## db-reset: destroy PostgreSQL including its volume, then start fresh
db-reset:
	docker compose down -v
	docker compose up -d --wait postgres

## db-shell: open psql against the dev database
db-shell:
	docker compose exec postgres psql -U cygnus -d cygnus

## build: compile the spike CLI
build:
	$(GO) build -o $(SPIKE) ./cmd/spike

## migrate: apply the schema
migrate: build
	$(SPIKE) migrate

## test: run tests with the race detector
test:
	$(GO) test -race -count=1 ./...

## vet: run go vet
vet:
	$(GO) vet ./...

## fmt: format all Go source
fmt:
	$(GO) fmt ./...

## tidy: tidy and verify module dependencies
tidy:
	$(GO) mod tidy
	$(GO) mod verify

## check: everything CI runs
check: vet build test

## bench: drain benchmark over 100k jobs
bench: build
	$(SPIKE) bench -jobs 100000 -workers 25 -batch 100

## bench-large: drain benchmark over 1M jobs, the Phase 0 throughput gate
bench-large: build
	$(SPIKE) bench -jobs 1000000 -workers 50 -batch 200

## bench-latency: steady-rate insert-to-fetch latency
bench-latency: build
	$(SPIKE) bench -mode latency -rate 2000 -duration 30s

## explain: assert the fetch query still uses its index
explain: build
	$(SPIKE) explain

## listen: verify LISTEN/NOTIFY across all driver backends
listen: build
	$(SPIKE) listen -backend all

## clean: remove build output
clean:
	rm -rf $(BIN)

.PHONY: help db-up db-down db-reset db-shell build migrate test vet fmt tidy check \
        bench bench-large bench-latency explain listen clean
