GO       ?= go
BUF      ?= $(GO) run github.com/bufbuild/buf/cmd/buf@v1.50.0
BIN      := bin
PKGS     := ./...

.PHONY: all build test test-short race chaos lint proto vet fmt cluster cluster-down image clean

all: vet test build

build:
	$(GO) build -trimpath -o $(BIN)/dtaskd ./cmd/dtaskd
	$(GO) build -trimpath -o $(BIN)/dtaskctl ./cmd/dtaskctl

test:
	$(GO) test $(PKGS) -count=1

test-short:
	$(GO) test -short $(PKGS) -count=1

# needs cgo (gcc), on windows install mingw-w64
race:
	CGO_ENABLED=1 $(GO) test -race $(PKGS) -count=1

# the fault-injection suites on their own, repeated to shake out timing luck
chaos:
	CGO_ENABLED=1 $(GO) test -race ./internal/raft ./internal/node -run 'Random|Chaos|Partition|Zombie|Crash' -count=3 -timeout 20m

vet:
	$(GO) vet $(PKGS)

fmt:
	gofmt -l -w cmd internal pkg

lint:
	$(BUF) lint
	@command -v golangci-lint >/dev/null && golangci-lint run || echo "golangci-lint not installed, skipped"

proto:
	$(BUF) generate

image:
	docker build -t dtask:dev .

cluster:
	docker compose -f deploy/docker-compose.yml up --build -d

cluster-down:
	docker compose -f deploy/docker-compose.yml down -v

clean:
	rm -rf $(BIN)
