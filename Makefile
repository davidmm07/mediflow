MODULES := common gateway services/auth-service services/doctor-service \
           services/patient-service services/appointment-service services/notification-service

PACT_DIR       := $(CURDIR)/pacts
PACT_BROKER    ?= http://localhost:9292
PACT_USER      ?= pact
PACT_PASSWORD  ?= pact
GIT_COMMIT     ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo dev)
GIT_BRANCH     ?= $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

# ------------------------------------------------------------------ building

.PHONY: tidy
tidy: ## go mod tidy every module in the workspace
	@for m in $(MODULES); do (cd $$m && go mod tidy) || exit 1; done

.PHONY: build
build: ## Compile every module
	@for m in $(MODULES); do (cd $$m && go build ./...) || exit 1; done

.PHONY: vet
vet: ## go vet every module
	@for m in $(MODULES); do (cd $$m && go vet ./...) || exit 1; done

.PHONY: test
test: ## Run unit tests (no Pact FFI needed)
	@for m in $(MODULES); do (cd $$m && go test ./...) || exit 1; done

.PHONY: check
check: vet test ## vet + unit tests

# --------------------------------------------------------------------- pact
# The Pact suites are behind the `pact` build tag because they need the
# native FFI library; `make test` stays installable-free and fast.

# The library must land in /tmp, /opt/pact/lib or /usr/local/lib — those are
# the only paths pact-go's cgo directive searches at link time, and anywhere
# else fails with "cannot find -lpact_ffi". LD_LIBRARY_PATH does not help:
# it affects loading at runtime, not linking. The installer defaults to
# /usr/local/lib, so run this with sudo if that is not writable by your user.
.PHONY: pact-install
pact-install: ## Download the Pact FFI native library into /usr/local/lib
	cd services/appointment-service && \
	  go run github.com/pact-foundation/pact-go/v2 install

.PHONY: pact-consumer
pact-consumer: ## Run consumer-side tests; regenerates ./pacts
	cd services/appointment-service && go test -tags=pact -count=1 ./internal/doctorclient/
	cd services/notification-service && go test -tags=pact -count=1 ./internal/events/

.PHONY: pact-provider
pact-provider: ## Verify every provider against the generated pacts
	cd services/doctor-service && go test -tags=pact -count=1 ./internal/api/
	cd services/appointment-service && go test -tags=pact -count=1 ./internal/booking/
	cd services/auth-service && go test -tags=pact -count=1 ./internal/api/

.PHONY: pact
pact: pact-consumer pact-provider ## Full contract suite: generate then verify

.PHONY: pact-publish
pact-publish: ## Publish the generated pacts to the local broker
	docker run --rm --network host \
	  -v $(PACT_DIR):/pacts \
	  pactfoundation/pact-cli:latest \
	  publish /pacts \
	    --consumer-app-version $(GIT_COMMIT) \
	    --branch $(GIT_BRANCH) \
	    --broker-base-url $(PACT_BROKER) \
	    --broker-username $(PACT_USER) \
	    --broker-password $(PACT_PASSWORD)

.PHONY: can-i-deploy
can-i-deploy: ## Ask the broker whether appointment-service is safe to release
	docker run --rm --network host \
	  pactfoundation/pact-cli:latest \
	  broker can-i-deploy \
	    --pacticipant appointment-service \
	    --version $(GIT_COMMIT) \
	    --to-environment production \
	    --broker-base-url $(PACT_BROKER) \
	    --broker-username $(PACT_USER) \
	    --broker-password $(PACT_PASSWORD)

# ---------------------------------------------------------------- local stack

.PHONY: up
up: ## Start the whole stack (Keycloak, Redpanda, Mongo, Pact Broker, services)
	docker compose up -d --build

.PHONY: down
down: ## Stop the stack, keeping volumes
	docker compose down

.PHONY: clean
clean: ## Stop the stack and delete its volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail the application services' logs
	docker compose logs -f gateway auth-service doctor-service patient-service appointment-service notification-service

.PHONY: smoke
smoke: ## Run the end-to-end smoke test against a running stack
	./scripts/smoke-test.sh
