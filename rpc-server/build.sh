#!/usr/bin/env bash
RUN_NAME="demo.im.rpc"

mkdir -p output/bin
cp script/* output/
chmod +x output/bootstrap.sh

go get github.com/go-sql-driver/mysql
go get github.com/avast/retry-go

if [ "$IS_SYSTEM_TEST_ENV" != "1" ]; then
    go build -o output/bin/${RUN_NAME}
else
    go test -c -covermode=set -o output/bin/${RUN_NAME} -coverpkg=./...
fi
