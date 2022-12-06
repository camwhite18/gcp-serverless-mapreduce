#!/usr/bin/env bash

export CGO_ENABLED=1
go clean -testcache
go test -timeout 120s --race --short ./...
