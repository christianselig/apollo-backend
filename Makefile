BREW_PREFIX  ?= $(shell brew --prefix)
DATABASE_URL ?= "postgres://$(USER)@localhost/apollo_test?sslmode=disable"
REDIS_URL    ?= "redis://localhost:6379"

test:
	@DATABASE_URL=$(DATABASE_URL) REDIS_URL=$(REDIS_URL) go test -race -timeout 1s ./...

test-setup: $(BREW_PREFIX)/bin/migrate
	migrate -path migrations/ -database $(DATABASE_URL) up

build:
	@go build ./cmd/apollo

lint:
	@golangci-lint run

$(BREW_PREFIX)/bin/migrate:
	@brew install golang-migrate

.PHONY: all build deps lint test
