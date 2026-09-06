#!/bin/bash
set -ex

export DEFAULT_TEST_IP="$(minikube -p minikube ip 2>/dev/null)"
mvn test -Prun-integration-tests