# This is a toy project and still in early development stage. Please do not use it in any serious stuff.

# baddb
A service that simulates DynamoDB with inconsistent reads, throughput limits, and partial batch get/write operations.

## Install
```shell
go install github.com/ocowchun/baddb/cli/baddb
```

## Usage
```shell
# run baddb server on port 9527, if not specified, the default port is 9527
baddb --port 9527 
```
