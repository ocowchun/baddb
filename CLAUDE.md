# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

baddb is a DynamoDB-compatible service simulator written in Go that emulates AWS DynamoDB behavior with configurable inconsistent reads, throughput limits, and partial batch operations. It's designed for testing applications that interact with DynamoDB by providing controlled failure scenarios.

**Primary Goal**: Behavioral accuracy over performance - simulate DynamoDB behavior as closely as possible for testing purposes.

## Development Commands

### Building and Running
```bash
# Build the CLI tool
go build -o baddb ./cli/baddb

# Run the server (default port 9527)
go run ./cli/baddb/main.go
go run ./cli/baddb/main.go --port 8080

# Install as CLI tool
go install github.com/ocowchun/baddb/cli/baddb
```

### Testing
```bash
# Run all tests
go test ./...

# Run specific package tests
go test ./ddb/...
go test ./integration/...

# Run a single test
go test ./integration -run TestBatchWriteItemBehavior

# Run tests with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Integration Testing Setup
```bash
# Start DynamoDB Local (required for integration tests)
docker-compose up -d

# Run integration tests (requires DynamoDB Local on port 8000)
go test ./integration/...
```

### Linting and Formatting
```bash
# Format code
go fmt ./...

# Run go vet
go vet ./...

# Tidy dependencies
go mod tidy
```

## Architecture Overview

### Core Components

**HTTP Server Layer** (`server/`)
- `server.go`: HTTP handlers that implement the DynamoDB JSON protocol
- Handles AWS signature validation, request routing, and error formatting
- Translates HTTP requests to internal service calls

**Service Layer** (`ddb/`)
- `service.go`: Main business logic implementing DynamoDB operations (1100+ lines, identified for refactoring to improve maintainability)
- Coordinates between HTTP layer and storage layer
- Manages table metadata and enforces DynamoDB constraints

**Storage Layer** (`ddb/storage/`)
- `storage.go`: SQLite-based storage engine
- `tuple.go`: Handles eventual consistency simulation with time-based tuples
- `primary_key.go`: Key management and serialization

**Request Processing** (`ddb/request/`)
- Builder pattern for constructing validated requests from AWS inputs
- Separate builders for Get, Put, Update, Delete operations

**Expression System** (`expression/`)
- `lexer/`: Tokenizes DynamoDB expressions
- `parser/`: Parses condition and update expressions into AST
- `ast/`: Abstract syntax tree for expression evaluation

**Core Data Types** (`ddb/core/`)
- `attribute.go`: DynamoDB AttributeValue implementation and conversion
- `entry.go`: Internal data representation with path operations
- `table.go`: Table metadata and schema management

### Key Design Principles

**Behavioral Accuracy**: All behavior should match AWS DynamoDB as closely as possible. When in doubt, test against real DynamoDB and replicate the exact behavior, including error messages and edge cases.

**Maintainability Focus**: Code organization and clarity are prioritized over performance optimizations. The service.go file is identified for refactoring into smaller, focused components.

**Testing Strategy**: 
- Integration tests compare baddb behavior against DynamoDB Local
- Each DynamoDB operation has dedicated test files in `integration/`
- Test utilities in `integration/util.go` provide shared setup and comparison functions

**Error Handling**: Custom error types match AWS DynamoDB exceptions exactly, with centralized error translation in `server/server.go`

## Special Configuration Features

### Simulating Inconsistent Reads
Configure delay via special metadata table:
```bash
aws dynamodb put-item \
    --table-name baddb_table_metadata \
    --item '{"tableName": {"S": "YourTable"}, "tableDelaySeconds": {"N": "60"}}' \
    --endpoint-url http://localhost:9527
```

### Simulating Throughput Limits
Configure unprocessed requests:
```bash
aws dynamodb put-item \
    --table-name baddb_table_metadata \
    --item '{"tableName": {"S": "YourTable"}, "unprocessedRequests": {"N": "5"}}' \
    --endpoint-url http://localhost:9527
```

## Development Notes

**Maintainability Priority**: `ddb/service.go` contains 1100+ lines and should be refactored into focused services (table, item, batch, query, transaction operations) for better maintainability.

**Test Dependencies**: Integration tests require DynamoDB Local running on port 8000. Use `docker-compose up -d` to start it.

**Port Configuration**: Default port is 9527. Integration tests use port 8080 for baddb and 8000 for DynamoDB Local.

**Storage Backend**: Uses SQLite with in-memory database for simplicity.

**Behavioral Fidelity**: When implementing new features or fixing bugs, always verify behavior against actual DynamoDB. Error messages, status codes, and edge case handling should match exactly.

## Implementation Status

Implements core DynamoDB operations with varying completeness:
- **Full**: CreateTable, DeleteTable, GetItem, PutItem, UpdateItem, DeleteItem, Query, Scan, BatchGetItem, BatchWriteItem, TransactWriteItems
- **Partial**: ListTables (no paging), DescribeTable
- **Missing**: TransactGetItems, UpdateTable, most advanced AWS-specific features

Refer to README.md for detailed feature support matrix.