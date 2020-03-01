#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname ${BASH_SOURCE})/..
CODEGEN_PKG=${CODEGEN_PKG:-$(cd ${SCRIPT_ROOT}; ls -d -1 ./vendor/k8s.io/code-generator 2>/dev/null || echo ../code-generator)}

# generate the code with:
${CODEGEN_PKG}/generate-groups.sh \
        all \
  github.com/portworx/kdmp/pkg/client \
        github.com/portworx/kdmp/pkg/apis \
  "kdmp:v1alpha1" \
  --go-header-file ${SCRIPT_ROOT}/hack/custom-boilerplate.go.txt
