all: test

test: build
	go vet ./...
	go test ./...

build: generate

generate:
	go generate ./...

deps:
	go get -u golang.org/x/tools/cmd/stringer
