GO ?= go
PYTHON ?= python3
BUF ?= buf

.PHONY: all build vet test race python-test verify buf-install proto cluster stop clean

all: verify

build:
	$(GO) build ./...

vet:
	$(GO) vet ./...

test:
	$(GO) test ./...

race:
	$(GO) test -race ./...

python-test:
	$(PYTHON) -m pytest prototype/

verify: build vet test race python-test

buf-install:
	$(GO) install github.com/bufbuild/buf/cmd/buf@v1.75.0

proto:
	$(BUF) lint
	$(BUF) generate
	mkdir -p gen/python/bitacora/v1
	$(PYTHON) -m grpc_tools.protoc -I proto --python_out=gen/python --grpc_python_out=gen/python proto/bitacora/v1/bitacora.proto
	$(GO) test ./gen/go/...

cluster:
	mkdir -p data/dev
	$(GO) build -o data/dev/bitacorad ./cmd/bitacorad
	$(PYTHON) scripts/dev/cluster.py start

stop:
	$(PYTHON) scripts/dev/cluster.py stop

clean:
	rm -rf data/dev gen
