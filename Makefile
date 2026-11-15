.PHONY: fmt vet lint test test-workflow test-history test-chaos test-load test-integration test-python build docker-build helm-lint helm-template check

GO ?= go
PYTHON ?= python3
DOCKER ?= docker
HELM ?= helm

fmt:
	gofmt -s -w .

vet:
	$(GO) vet ./...

lint:
	golangci-lint run --timeout=5m

test:
	$(GO) test -v -race ./test/unit/...

test-workflow:
	$(GO) test -v -race ./test/workflow/...

test-history:
	$(GO) test -v -race -run "Test.*History" ./test/workflow/...

test-chaos:
	$(GO) test -v -race -run "TestChaos_(WorkerDrop|SubprocessKill|CanaryDrain|RateLimiterBound)" ./test/chaos/...

# Non-race load & memory ceiling verification (P99 < 35 ms, 10 GB multipart, < 256 MiB heap)
test-load:
	$(GO) test -v -run "TestChaos_(Load|MemoryCeiling|SustainedPresign)" ./test/chaos/...

test-integration:
	$(GO) test -v -race ./test/integration/...

test-python:
	$(PYTHON) -m pytest workers/python/tests

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags="-s -w" -o bin/coordinator ./cmd/coordinator

docker-build:
	$(DOCKER) build -f deployments/docker/Dockerfile -t rag-ingestion-coordinator:local .

helm-lint:
	$(HELM) lint deployments/helm

helm-template:
	$(HELM) template rag-ingestion-coordinator deployments/helm --set rollout.enabled=false > /dev/null
	$(HELM) template rag-ingestion-coordinator deployments/helm --set rollout.enabled=true --set monitoring.serviceMonitor.enabled=true --set monitoring.prometheusRule.enabled=true > /dev/null

check: fmt vet lint test test-workflow test-history test-chaos test-load build

