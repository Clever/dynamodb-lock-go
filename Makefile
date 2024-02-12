include golang.mk

.PHONY: all test install_deps
SHELL := /bin/bash
PKGS := $(shell go list ./... | grep -v /vendor)

all: test 

# this test relies on a local DynamoDB instance. You can run it with this command:
#     CID=$(docker run -d -p 8000:8000 amazon/dynamodb-local)
# If the test fails, you can use localhost:8800 as a live dynamo endpoint for debugging
# If you need to rerun the test, clean up the dynamo table by running:
#     docker restart $CID
# When you are satisfied with testing:
#     docker stop $CID
integration-test:
	INTEGRATION_TEST=true go test -v ./... -count 1 -timeout 0

$(PKGS): golang-test-all-deps
	$(call golang-test-all-strict,$@)

test: install_deps $(PKGS) integration-test 

install_deps:
	go mod vendor