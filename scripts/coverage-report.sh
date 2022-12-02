#!/usr/bin/env bash

export CGO_ENABLED=1

DEFAULT_THRESHOLD=90

declare -A SPECIFIC_THRESHOLDS

# unit test thresholds
SPECIFIC_THRESHOLDS[gitlab.com/cameron_w20/serverless-mapreduce]=0
SPECIFIC_THRESHOLDS[gitlab.com/cameron_w20/serverless-mapreduce/test]=0
SPECIFIC_THRESHOLDS[gitlab.com/cameron_w20/serverless-mapreduce/controller]=88

go clean -testcache

RESULT="PASS"

# unit tests
packages=$(go list ./... | grep -v /test/)
for p in $packages; do
    coverage="$(go test -cover "$p" | awk '{print $(NF-2)}' | grep -Eo '^[0-9]+' || true)"
    if [ -z "$coverage" ]; then
        coverage=0
    fi
    if [ ${SPECIFIC_THRESHOLDS[$p]} ]; then
        THRESHOLD=${SPECIFIC_THRESHOLDS[$p]}
    else
        THRESHOLD=$DEFAULT_THRESHOLD
    fi
    (( "$coverage" >= THRESHOLD )) || (echo "FAIL: package $p has ${coverage}% unit test coverage (expected $THRESHOLD)" && false)
    (( "$coverage" >= THRESHOLD )) || RESULT="FAIL"
    (( "$coverage" >= THRESHOLD )) && (echo "PASS: package $p has ${coverage}% unit test coverage")
done

echo $RESULT
[ "$RESULT" = "PASS" ] || exit 1
