# RaftKV. `make help` lists targets.
GO      ?= go
RUNS    ?= 500
DURATION?= 5s
PARALLEL?= 0
BIN     := bin

.PHONY: help build test race lint chaos chaos-replay bench cluster cluster-stop proto clean

help:
	@echo "build        build all binaries into ./bin"
	@echo "test         go test ./... (short mode for the long seeded suites)"
	@echo "race         full test suite with -race (what CI runs)"
	@echo "lint         go vet + staticcheck"
	@echo "chaos        RUNS=500 DURATION=5s seeded chaos runs checked by Porcupine"
	@echo "chaos-replay SEED=N replay one chaos run verbosely"
	@echo "bench        full benchmark protocol (benchmarks/run.sh)"
	@echo "cluster      5-node cluster with docker compose"
	@echo "proto        regenerate protobuf/gRPC code"

build:
	$(GO) build -o $(BIN)/ ./cmd/...

test:
	$(GO) test -short ./...

race:
	$(GO) test -race ./...

lint:
	$(GO) vet ./...
	staticcheck ./...

chaos:
	$(GO) test -race -count=1 -timeout 0 -run 'TestChaos$$' ./chaos -runs=$(RUNS) -duration=$(DURATION) -parallel-runs=$(PARALLEL) -v

chaos-replay:
	$(GO) test -race -count=1 -run 'TestChaos$$' ./chaos -seed=$(SEED) -duration=$(DURATION) -v

bench: build
	./benchmarks/run.sh

cluster:
	docker compose -f deploy/docker-compose.yml up --build -d

cluster-stop:
	docker compose -f deploy/docker-compose.yml down -v

# Needs protoc (or `python3 -m grpc_tools.protoc`), protoc-gen-go and
# protoc-gen-go-grpc on PATH. The generated code is committed.
PROTOC ?= protoc
proto:
	$(PROTOC) -I proto --go_out=. --go_opt=module=github.com/shreyans-chowdry/raftkv \
		--go-grpc_out=. --go-grpc_opt=module=github.com/shreyans-chowdry/raftkv proto/raftkv.proto

clean:
	rm -rf $(BIN)
