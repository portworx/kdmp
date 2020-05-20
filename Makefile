DOCKER_HUB_REPO?=portworx
DOCKER_HUB_KDMP_UNITTEST_IMAGE?=px-kdmp-unittest
DOCKER_HUB_KDMP_TAG?=latest

KDMP_UNITTEST_IMG=$(DOCKER_HUB_REPO)/$(DOCKER_HUB_KDMP_UNITTEST_IMAGE):$(DOCKER_HUB_KDMP_TAG)

RELEASE_VER := v0.1.0
BUILD_DATE  := $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')
BASE_DIR    := $(shell git rev-parse --show-toplevel)
GIT_SHA     := $(shell git rev-parse HEAD)
BIN         := $(BASE_DIR)/bin

export GO111MODULE=on
export GOFLAGS = -mod=vendor

ifndef PKGS
	PKGS := $(shell GOFLAGS=-mod=vendor go list ./... 2>&1 | grep -v 'go: ' | grep -v 'github.com/portworx/kdmp/vendor' | grep -v versioned | grep -v 'pkg/apis/v1')
endif

GO_FILES := $(shell find . -name '*.go' | grep -v 'vendor' | \
                                   grep -v '\.pb\.go' | \
                                   grep -v '\.pb\.gw\.go' | \
                                   grep -v 'externalversions' | \
                                   grep -v 'versioned' | \
                                   grep -v 'generated')

.DEFAULT_GOAL: all
.PHONY: test

all: pretest test

test:
	sudo docker run --rm -it -v ${GOPATH}:/go: $(KDMP_UNITTEST_IMG) make unittest

test-container:
	@echo "Building container: docker build --tag $(KDMP_UNITTEST_IMG) -f Dockerfile.unittest ."
	sudo docker build --tag $(KDMP_UNITTEST_IMG) -f Dockerfile.unittest .


unittest:
	echo "mode: atomic" > coverage.txt
	for pkg in $(PKGS); do \
		go test -v -tags unittest -coverprofile=profile.out -covermode=atomic $(BUILD_OPTIONS) $${pkg} || exit 1; \
		if [ -f profile.out ]; then \
			cat profile.out | grep -v "mode: atomic">> coverage.txt; \
			rm profile.out; \
		fi; \
	done

lint:
	GO111MODULE=off go get -u golang.org/x/lint/golint
	for file in $(GO_FILES); do \
        golint $${file}; \
        if [ -n "$$(golint $${file})" ]; then \
            exit 1; \
        fi; \
        done

vet:
	go vet $(PKGS)
	go vet -tags unittest $(PKGS)
	#go vet -tags integrationtest github.com/portworx/kdmp/test/integration_test


staticcheck:
	GO111MODULE=off go get -u honnef.co/go/tools/cmd/staticcheck
	staticcheck $(PKGS)
	#staticcheck -tags integrationtest test/integration_test/*.go
	staticcheck -tags unittest $(PKGS)


errcheck:
	GO111MODULE=off go get -u github.com/kisielk/errcheck
	errcheck -ignoregenerated -verbose -blank $(PKGS)
	errcheck -ignoregenerated -verbose -blank -tags unittest $(PKGS)
	#errcheck -ignoregenerated -verbose -blank -tags integrationtest github.com/portworx/kdmp/test/integration_test


check-fmt:
	bash -c "diff -u <(echo -n) <(gofmt -l -d -s -e $(GO_FILES))"

do-fmt:
	gofmt -s -w $(GO_FILES)

gocyclo:
	go get -u github.com/fzipp/gocyclo
	gocyclo -over 15 $(GO_FILES)

pretest: check-fmt lint vet errcheck staticcheck

codegen:
	@echo "Generating CRD"
	@hack/update-codegen.sh

vendor-sync:
	go mod tidy
	go mod vendor

build:
	go build -o ${BIN}/kdmp -ldflags="-s -w \
	-X github.com/portworx/kdmp/pkg/version.gitVersion=${RELEASE_VER} \
	-X github.com/portworx/kdmp/pkg/version.gitCommit=${GIT_SHA} \
	-X github.com/portworx/kdmp/pkg/version.buildDate=${BUILD_DATE}" \
	./cmd/kdmp
