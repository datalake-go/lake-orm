.PHONY: test integration-test e2e-test docker-up docker-down lint example bench build tidy help

GO ?= go

help:
	@echo "Targets:"
	@echo "  make test              Run unit tests (no external deps)"
	@echo "  make integration-test  Run adjacent integration tests (-tags=integration); uses testcontainers"
	@echo "  make e2e-test          Run black-box teste2e suite (-tags=e2e); requires docker-compose stack"
	@echo "  make docker-up         Bring up Spark Connect + SeaweedFS + Nessie"
	@echo "  make docker-down       Tear down the stack"
	@echo "  make lint              go vet + go build"
	@echo "  make example           Run examples/basic against the stack"
	@echo "  make bench             Run benchmark harness"
	@echo "  make build             go build ./..."
	@echo "  make tidy              go mod tidy"

build:
	$(GO) build ./...

test:
	$(GO) test -race ./...

# Adjacent integration tests — one per driver/format/backend. Use
# testcontainers so CI runners only need Docker, not a pre-built
# docker-compose stack.
integration-test:
	$(GO) test -tags=integration -race ./...

# Black-box teste2e against the docker-compose stack. Expects the
# caller to have run `make docker-up` and exported DORM_SPARK_URI /
# DORM_NESSIE_URI / DORM_S3_DSN; teste2e skips cleanly if unset.
e2e-test:
	DORM_SPARK_URI=$${DORM_SPARK_URI:-sc://localhost:15002} \
	DORM_NESSIE_URI=$${DORM_NESSIE_URI:-http://localhost:19120/api/v1} \
	DORM_S3_DSN=$${DORM_S3_DSN:-'s3://dorm-local/lake?endpoint=http://localhost:8333&path_style=true&access_key=dorm&secret_key=dorm'} \
	$(GO) test -tags=e2e -race -v ./teste2e/...

docker-up:
	docker compose up -d
	@echo ""
	@echo "SeaweedFS:     http://localhost:8333 (creds: dorm/dorm, bucket: dorm-local)"
	@echo "Nessie:        http://localhost:19120/api/v1"
	@echo "Spark Connect: sc://localhost:15002  (may take ~60s on first run while JARs resolve)"
	@echo ""
	@echo "Examples and teste2e will pick these up automatically."

docker-down:
	docker compose down -v

lint:
	$(GO) vet ./...
	$(GO) build ./...

example:
	$(GO) run ./examples/basic

bench:
	$(GO) test -bench=. -benchmem -run=^$$ ./...

tidy:
	$(GO) mod tidy
