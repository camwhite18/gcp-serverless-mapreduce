#!/usr/bin/env bash

export CGO_ENABLED=1
go test -timeout 120s --race --short ./...
