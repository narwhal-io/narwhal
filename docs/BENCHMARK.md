# Benchmark Results

This document contains performance benchmarks for Narwhal's pub/sub messaging system. All benchmarks were conducted using the `narwhal-bench` tool included in the project.

## Test Environment

### Hardware

- **CPU**: Intel(R) Core(TM) Ultra 9 275HX (24 cores)
- **Memory**: 32 GB

### Configuration

- **Build Configuration**: Release mode (`--release`)
- **Server**: Local Narwhal instance (127.0.0.1:22622)
- **Test Duration**: 60 seconds per run
- **Producers**: 1
- **Consumers**: 1
- **Channels**: 1
- **Worker Threads**: 8

## Benchmark Tool

The benchmark tool is located in the `crates/benchmark` directory and can be built and run as follows:

```bash
# Build the benchmark tool
cargo build --release -p narwhal-benchmark

# Run a benchmark
./target/release/narwhal-bench \
  --server 127.0.0.1:22622 \
  --producers 1 \
  --consumers 1 \
  --duration 1m \
  --max-payload-size 256
```

### Available Options

- `-s, --server <ADDR>`: Server address to connect to (default: 127.0.0.1:22622)
- `-p, --producers <N>`: Number of producer clients (default: 1)
- `-c, --consumers <N>`: Number of consumer clients (default: 10)
- `-n, --channels <N>`: Number of channels to create (default: 1)
- `-d, --duration <TIME>`: Duration to run the benchmark (supports: 30s, 5m, 1h)
- `--max-payload-size <BYTES>`: Maximum size of message payload in bytes (default: 16384)
- `-w, --worker-threads <N>`: Number of worker threads to use (default: 0, auto-detect)

## Results

### Performance Overview Table

All benchmarks were run with 1 producer and 1 consumer for 60 seconds.

| Payload Size | Throughput (msg/s) | Data Throughput | Mean Latency | P50 | P95 | P99 | Total Messages |
|--------------|-------------------:|----------------:|-------------:|----:|----:|----:|---------------:|
| 256 B        | 83,962            | 20.5 MB/s       | 0.67ms       | 1ms | 1ms | 2ms | 5,054,643      |
| 512 B        | 83,892            | 41.0 MB/s       | 0.66ms       | 1ms | 1ms | 2ms | 5,051,865      |
| 1 KB         | 82,688            | 80.7 MB/s       | 0.70ms       | 1ms | 1ms | 2ms | 4,977,980      |
| 4 KB         | 81,007            | 316.4 MB/s      | 0.78ms       | 1ms | 1ms | 2ms | 4,877,287      |
| 8 KB         | 75,594            | 590.6 MB/s      | 0.93ms       | 1ms | 1ms | 2ms | 4,551,031      |
| 16 KB        | 57,930            | 905.1 MB/s      | 1.17ms       | 1ms | 2ms | 4ms | 3,487,746      |
| 32 KB        | 42,784            | 1.31 GB/s       | 1.86ms       | 2ms | 3ms | 5ms | 2,574,740      |
| 64 KB        | 29,973            | 1.83 GB/s       | 2.81ms       | 3ms | 5ms | 6ms | 1,804,239      |
| 128 KB       | 18,614            | 2.27 GB/s       | 4.83ms       | 5ms | 8ms | 9ms | 1,120,856      |

### Visual Performance Analysis

#### Message Throughput

![Message Throughput vs Payload Size](benchmark_msg_throughput.png)

This graph shows how message throughput (messages per second) varies with payload size. Peak performance is achieved with small to medium payloads (256B-512B), delivering over 83,000 messages per second.

#### Latency

![Message Latency vs Payload Size](benchmark_latency.png)

This graph compares mean and P99 latency across different payload sizes. Latency scales gradually with payload size, with P99 latency remaining under 10ms even at 128KB payloads.

#### Data Throughput (Bandwidth)

![Data Throughput vs Payload Size](benchmark_data_throughput.png)

This graph illustrates the actual data throughput in MB/s. While message rate decreases with larger payloads, the overall bandwidth increases significantly, reaching over 2.2GB/s with 128KB payloads.

### Key Observations

1. **Peak Throughput**: Achieved with 256-byte payloads at ~83,962 messages/second
2. **Consistent Low Latency**: Sub-millisecond mean latency maintained across small to medium payload sizes (256B - 8KB)
3. **Payload Size Impact**: As payload size increases, message throughput decreases while data throughput increases, which is expected due to larger data transfers
4. **Latency Stability**: P99 latency remains very low (1-9ms) across all payload sizes, indicating consistent performance
5. **Zero Errors**: All benchmarks completed without any message loss or errors

## Notes

- All benchmarks were run on a local machine with the server and client on the same host
- Network latency in production environments will add to the observed latency values
- The benchmark tool generates random payloads up to the specified maximum size
- Server configuration used the `c2s-benchmark.toml` profile with rate limiting disabled
- Results may vary based on hardware, OS, and system load
- The benchmark scripts (`run_benchmarks.sh` and `generate_benchmark_plots.py`) are available in the `docs/` directory for easy reproduction
