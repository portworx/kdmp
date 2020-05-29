# E2E tests for the kdmp controller

## Setup

Test suit is running on a kubernetes cluster so needs to provied a `KUBECONFIG` environment variable:

```
$ KUBECONFIG=/path/to/a/file go test -tags e2e ./test/e2e
```