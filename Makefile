all: test

test: build
	go vet ./...
	go run honnef.co/go/tools/cmd/staticcheck ./...
	go clean -testcache
	CARAVAN_DEBUG=0 go test ./...
	go clean -testcache
	CARAVAN_DEBUG=1 go test ./...

build: generate

generate:
	go generate ./...
