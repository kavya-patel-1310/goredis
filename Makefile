BINARY := bin/goredis
PKG    := ./...

.PHONY: all build run test race cover fuzz bench bench-go lint docker clean

all: lint test build

build:            ## Build the server binary
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/goredis

run: build        ## Run with AOF enabled and the dashboard on :8080
	$(BINARY) -appendonly -dir ./data

test:             ## Unit + integration tests
	go test -count=1 $(PKG)

race:             ## Tests with the race detector
	go test -race -count=1 $(PKG)

cover:            ## Coverage report
	go test -coverprofile=coverage.out $(PKG) && go tool cover -func=coverage.out | tail -1

fuzz:             ## Fuzz the RESP parser for 60s
	go test -run=^$$ -fuzz=FuzzReadCommand -fuzztime=60s ./internal/resp

bench:            ## redis-benchmark: GoRedis vs redis-server (needs redis-tools)
	./scripts/bench.sh

bench-go:         ## Go micro-benchmarks
	go test -run=^$$ -bench=. -benchmem ./internal/...

lint:
	go vet $(PKG)
	@test -z "$$(gofmt -l .)" || (echo "gofmt needed:"; gofmt -l .; exit 1)

docker:
	docker build -t goredis:latest .

clean:
	rm -rf bin data coverage.out
