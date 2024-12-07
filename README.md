# TSO

![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/rleungx/tso/go.yml)
![Codecov](https://img.shields.io/codecov/c/github/rleungx/tso)
![GitHub License](https://img.shields.io/github/license/rleungx/tso)
[![Go Reference](https://pkg.go.dev/badge/github.com/rleungx/tso.svg)](https://pkg.go.dev/github.com/rleungx/tso)

TSO (Timestamp Oracle) is a distributed timestamp allocation service that provides globally unique and monotonically increasing timestamps. It is primarily used in distributed systems for transaction ordering, causal consistency, and similar scenarios.

## Features
- High Availability: Supports multi-node deployment with automatic failover
- High Performance: Batch request processing, supports high concurrency scenarios
- Security: Supports TLS encrypted communication
- Easy to Use: Provides HTTP/gRPC interface and client SDK

## Quick Start

### Prerequisites

- Go 1.23 or later
- libprotoc 3.8.0 or later

### Clone repository
```bash
   git clone https://github.com/yourusername/tso.git
   cd tso
```

### Build project
```bash
   make
```

### Start server
```bash
# Start with default configuration
./bin/tso-server
```

### HTTP interface
```bash
# Request example
curl -X GET http://127.0.0.1:7788/timestamp?count=10

# Response example
{"timestamp":{"physical":1732898843667,"logical":10},"count":10}
```

### gRPC interface
Details can be found in [example](https://github.com/rleungx/tso/blob/main/examples/grpc_example.go)

```bash
go run examples/grpc_example.go

# Response example
2024-11-30 00:49:47.845	INFO	Received timestamp	{"timestamp": 1732898987817, "logical": 1}
2024-11-30 00:49:47.845	INFO	Received timestamp	{"timestamp": 1732898987817, "logical": 2}
2024-11-30 00:49:47.845	INFO	Received timestamp	{"timestamp": 1732898987817, "logical": 3}
...
```

## Contributing
Contributions are welcome! Please open an issue or submit a pull request.

## License
This project is licensed under the Apache License 2.0. See the [LICENSE](./LICENSE) file for details.
