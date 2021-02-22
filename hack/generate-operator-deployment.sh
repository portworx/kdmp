#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

IMAGE_TAG=${RELEASE_VER:-latest}
DEPLOYMENT_DIR=./deploy

cat << EOF > ${DEPLOYMENT_DIR}/deployment.yaml
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kdmp-operator
  namespace: kube-system
  labels:
    name: kdmp-operator
spec:
  strategy:
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 1
    type: RollingUpdate
  replicas: 1
  selector:
    matchLabels:
      name: kdmp-operator
  template:
    metadata:
      labels:
        name: kdmp-operator
    spec:
      containers:
      - name: kdmp-operator
        image: portworx/kdmp:${IMAGE_TAG}
        imagePullPolicy: Always
        resources:
          requests:
            cpu: 0.5
            memory: 200Mi
          limits:
            cpu: 1
            memory: 500Mi
      serviceAccountName: kdmp-operator
EOF
