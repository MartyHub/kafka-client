#!/usr/bin/env sh

set -eux

test=

if [[ ! -z "$@" ]]; then
  test="-run $@"
fi

go test -coverprofile cover.out -short -timeout 30s -v ${test}
