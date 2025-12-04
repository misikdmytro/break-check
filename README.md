# break-check

A high-performance, distributed rate limiter service built with Rust and gRPC. Uses a sliding window algorithm with Redis as the storage backend to provide accurate, scalable rate limiting for modern applications.

## Features

- **Sliding Window Algorithm** - Accurate rate limiting that smooths traffic spikes across window boundaries
- **gRPC API** - High-performance protocol buffers interface
- **Policy-Based Configuration** - Flexible rate limiting with pattern matching (exact and prefix)
- **Redis-Backed** - Distributed, persistent storage with atomic operations
- **Health Checks** - Built-in gRPC health check service for orchestration and monitoring
- **Graceful Shutdown** - Proper cleanup and connection handling
- **Comprehensive Testing** - Unit tests, integration tests, and property-based testing

## Quick Start

### Prerequisites

- Rust 1.70+ (2024 edition)
- Docker and Docker Compose

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd break-check

# Start Redis using Docker Compose
docker-compose up -d

# Build the project
cargo build --release

# Run the server
cargo run --release
```

The server will start on `[::]:50051` by default.

## Configuration

Configuration is managed through `config/config.toml`:

```toml
[server]
address = "[::]:50051"              # Server bind address
redis_url = "redis://127.0.0.1/"    # Redis connection URL
redis_timeout_ms = 200              # Redis operation timeout

[default_policy]
max_tokens = 10                     # Default tokens per window
window_secs = 60                    # Default window duration

[[policies]]
pattern = "user.login"              # Resource pattern
type = "exact"                      # Match type: "exact" or "prefix"
max_tokens = 5                      # Maximum tokens in window
window_secs = 60                    # Window duration in seconds
priority = 100                      # Higher priority = checked first
```

### Policy Matching

Policies are matched in the following order:

1. **Exact matches** with highest priority
2. **Prefix matches** with highest priority
3. Lower priority policies
4. **Default policy** as fallback

Example patterns:

- `user.login` (exact) - matches only "user.login"
- `user.` (prefix) - matches "user.login", "user.register", etc.
- `api.public.` (prefix) - matches all public API endpoints

## Development

### Running Tests

```bash
# Run all tests
cargo test

# Run unit tests
cargo test --lib

# Run integration tests (requires Redis)
cargo test --test acquire

# Run with logging
RUST_LOG=debug cargo test
```

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Protocol buffers are automatically compiled via build.rs
```
