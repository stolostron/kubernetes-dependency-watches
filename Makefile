# Copyright Contributors to the Open Cluster Management project

PWD := $(shell pwd)
LOCAL_BIN ?= $(PWD)/bin

# Keep an existing GOPATH, make a private one if it is undefined
GOPATH_DEFAULT := $(PWD)/.go
export GOPATH ?= $(GOPATH_DEFAULT)
GOBIN_DEFAULT := $(GOPATH)/bin
export GOBIN ?= $(GOBIN_DEFAULT)
export PATH := $(LOCAL_BIN):$(GOBIN):$(PATH)
TESTARGS_DEFAULT := -v
export TESTARGS ?= $(TESTARGS_DEFAULT)

include build/common/Makefile.common.mk

############################################################
# clean section
############################################################

.PHONY: clean
clean:
	-rm bin/*

############################################################
# lint section
############################################################

.PHONY: fmt
fmt:

.PHONY: lint
lint:

############################################################
# test section
############################################################

.PHONY: test
test: e2e-dependencies envtest
	 KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" $(GINKGO) $(TESTARGS) ./...

.PHONY: test-coverage
test-coverage: TESTARGS = -v --json-report=report_unit.json --cover --covermode=atomic --coverprofile=coverage.out
test-coverage: test

.PHONY: gosec
gosec:
