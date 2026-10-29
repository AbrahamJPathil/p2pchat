BINARY := bin/p2pchat
NAME   ?= alice
RELAY  ?= localhost:7835

.PHONY: build run-relay run-peer test vet clean

build:
	go build -o $(BINARY) ./cmd/p2pchat

run-relay:
	go run ./cmd/p2pchat relay

run-peer:
	go run ./cmd/p2pchat peer --relay $(RELAY) --name $(NAME)

test:
	go test ./...

vet:
	go vet ./...

clean:
	rm -rf bin
