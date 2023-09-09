# Distributed Token Manager System

A distributed system implementation using the Read-Impose-Write-All (RIW) protocol to ensure consistency across replicas.

## Overview

This system maintains a collection of tokens, with each token being replicated across a set of nodes. To ensure consistency when reading and writing these tokens, the system uses the RIW protocol.

## Features

- **Distributed Reads and Writes**: The system supports reading and writing tokens in a distributed environment.
- **Read-Modify-Write All Protocol**: Ensures consistency across replicas using the RIW protocol.
- **Connection Pooling**: Efficiently manages gRPC connections to other nodes using a connection pool.
- **Concurrent Access**: Uses Go routines for concurrent access to replicas and a mutex to protect shared data.

## Getting Started

### Prerequisites

- Go (at least version 1.15)
- gRPC and Protocol Buffers

### Installation and Setup

1. Run the following commands
\```bash
cd grpc-token-manager
go get -v ./...
protoc --go_out=pkg --go_opt=paths=source_relative --go-grpc_out=pkg --go-grpc_opt=paths=source_relative api/token.proto
\```

### Running

1. Start the server:
\```bash
go run main.go
\```

## Usage

Check script/ for examples
