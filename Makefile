# Contrail: flight telemetry pipeline
#
# `make demo` is the entry point for a fresh clone: it runs the ingester
# against recorded fixtures, with no credentials, broker, or Docker required.

SHELL := /bin/bash
.DEFAULT_GOAL := help

INGESTER_DIR := ingester
FIXTURES_DIR := fixtures

# A Western Europe window that contains most of the default regions, so replay
# yields genuinely different aircraft counts per region rather than a uniform
# slice of one recording.
FIXTURE_BOX ?= lamin=45.0&lomin=-6.0&lamax=56.0&lomax=16.0
FIXTURE_POLLS ?= 6
FIXTURE_INTERVAL ?= 15

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

## ---------------------------------------------------------------- ingester

.PHONY: demo
demo: ## Run the ingester on recorded fixtures (no credentials needed)
	cd $(INGESTER_DIR) && CONTRAIL_FIXTURES_DIR=../$(FIXTURES_DIR) \
		CONTRAIL_SOURCE=replay CONTRAIL_SINK=stdout \
		go run ./cmd/ingester

.PHONY: run
run: ## Run the ingester against the live API into Kafka
	cd $(INGESTER_DIR) && CONTRAIL_SOURCE=live CONTRAIL_SINK=kafka \
		go run ./cmd/ingester

.PHONY: run-replay
run-replay: ## Ingest recorded fixtures into Kafka
	cd $(INGESTER_DIR) && CONTRAIL_FIXTURES_DIR=../$(FIXTURES_DIR) \
		CONTRAIL_SOURCE=replay CONTRAIL_SINK=kafka \
		CONTRAIL_KAFKA_BROKERS=localhost:19092 CONTRAIL_REPLAY_LOOP=false \
		CONTRAIL_REPLAY_INTERVAL=20ms \
		CONTRAIL_REGIONS='[{"name":"benelux","box":{"LatMin":50.0,"LonMin":3.0,"LatMax":53.5,"LonMax":7.5}}]' \
		go run ./cmd/ingester

## ---------------------------------------------------------------- pipeline

.PHONY: venv
venv: ## Create the Python virtualenv and install the pipeline
	cd pipeline && uv venv && uv pip install -e ".[dev,orchestration,api]"

.PHONY: sink
sink: ## Consume Kafka into Parquet and ClickHouse
	cd pipeline && CONTRAIL_BATCH_SIZE=2000 CONTRAIL_BATCH_TIMEOUT_SECONDS=5 \
		.venv/bin/python -m contrail.cli --max-batches 5

.PHONY: transform
transform: ## Build and test the dbt models
	cd pipeline/transform && DBT_PROFILES_DIR=. ../.venv/bin/dbt deps
	cd pipeline/transform && DBT_PROFILES_DIR=. ../.venv/bin/dbt build

.PHONY: dagster
dagster: ## Launch the Dagster UI
	cd pipeline && .venv/bin/dagster dev -m orchestration.definitions

.PHONY: api
api: ## Serve the FastAPI read layer
	cd api && ../pipeline/.venv/bin/uvicorn main:app --reload --port 8000

.PHONY: web
web: ## Serve the Next.js dashboard
	cd web && npm run dev

.PHONY: lint-py
lint-py: ## Lint and type check the Python pipeline
	cd pipeline && .venv/bin/ruff check . && .venv/bin/ruff format --check . \
		&& .venv/bin/mypy contrail

.PHONY: test-py
test-py: ## Run the Python test suite
	cd pipeline && .venv/bin/pytest

.PHONY: up
up: ## Start the local stack
	docker compose up -d

.PHONY: down
down: ## Stop the stack and remove volumes
	docker compose down -v

.PHONY: test
test: ## Run the Go test suite
	cd $(INGESTER_DIR) && go test ./... -count=1

.PHONY: test-verbose
test-verbose: ## Run the Go test suite with per-test output
	cd $(INGESTER_DIR) && go test ./... -count=1 -v

.PHONY: cover
cover: ## Report Go test coverage per package
	cd $(INGESTER_DIR) && go test ./... -count=1 -cover

.PHONY: lint
lint: ## Format check and vet
	cd $(INGESTER_DIR) && test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }
	cd $(INGESTER_DIR) && go vet ./...

.PHONY: build
build: ## Build the ingester binary
	cd $(INGESTER_DIR) && go build -o ../bin/ingester ./cmd/ingester

## ---------------------------------------------------------------- fixtures

.PHONY: record-fixtures
record-fixtures: ## Re-record replay fixtures from the live API (uses credits)
	@mkdir -p $(FIXTURES_DIR)
	@echo "Recording $(FIXTURE_POLLS) polls at $(FIXTURE_INTERVAL)s intervals…"
	@for i in $$(seq 1 $(FIXTURE_POLLS)); do \
		n=$$(printf "%03d" $$i); \
		curl -sS -m 60 -o "$(FIXTURES_DIR)/states-$$n.json" \
			"https://opensky-network.org/api/states/all?$(FIXTURE_BOX)"; \
		echo "  states-$$n.json: $$(python3 -c "import json,sys;d=json.load(open('$(FIXTURES_DIR)/states-$$n.json'));print(len(d['states']),'vectors')")"; \
		[ $$i -lt $(FIXTURE_POLLS) ] && sleep $(FIXTURE_INTERVAL) || true; \
	done

.PHONY: fixture-stats
fixture-stats: ## Report duplicate rate and staleness across recorded fixtures
	@python3 scripts/fixture_stats.py $(FIXTURES_DIR)

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf bin
