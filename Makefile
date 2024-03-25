.PHONY: test
test:
	go test -v -race ./... -count=1

.PHONY: run
run:
	go run cmd/main.go
