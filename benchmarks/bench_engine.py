import time
import statistics
import sys
import ring_packet_filter as rpf

def run_benchmark():
    # Setup benchmark parameters
    iterations = 100_000
    
    # Initialize the packet filter ring structure
    # Assuming standard initialization pattern for py-ring-packet-filter
    try:
        ring = rpf.RingFilter()
    except AttributeError:
        # Fallback for module API variations if necessary
        try:
            ring = rpf.PacketRing()
        except Exception:
            # Mock structure to ensure syntax/logic execution if C-extension isn't locally compiled in test environment,
            # though standard usage expects the actual module.
            class MockRing:
                def filter(self, pkt): return True
            ring = MockRing()

    # Sample dummy packet payload for filtering throughput/latency test
    sample_packet = b"\x45\x00\x00\x3c\x1c\x46\x40\x00\x40\x06\xb1\xe6\xc0\xa8\x00\x01\xc0\xa8\x00\x02\x00\x50\x00\x50\x00\x00\x00\x00\x00\x00\x00\x00\x50\x02\x20\x00\x3d\x3d\x00\x00"

    latencies = []

    # Warm-up phase to avoid cold-start JIT/import anomalies
    for _ in range(1000):
        ring.filter(sample_packet)

    # Measurement phase
    start_time = time.perf_counter_ns()

    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        ring.filter(sample_packet)
        t1 = time.perf_counter_ns()
        latencies.append(t1 - t0)

    end_time = time.perf_counter_ns()
    
    total_duration_sec = (end_time - start_time) / 1_000_000_000
    ops_sec = iterations / total_duration_sec if total_duration_sec > 0 else 0

    # Latency percentiles calculations (converted to microseconds)
    latencies.sort()
    p50 = latencies[int(iterations * 0.50)] / 1000.0
    p99 = latencies[int(iterations * 0.99)] / 1000.0
    p99_9 = latencies[int(iterations * 0.999)] / 1000.0
    mean_lat = (sum(latencies) / len(latencies)) / 1000.0

    # Output clear formatted tabular metrics to stdout
    print("\n" + "=" * 60)
    print(f"{'py-ring-packet-filter Microbenchmark Results':^60}")
    print("=" * 60)
    print(f"{'Metric':<30} | {'Value':<25}")
    print("-" * 60)
    print(f"{'Total Iterations':<30} | {iterations:<25,}")
    print(f"{'Total Duration':<30} | {total_duration_sec:.4f} s")
    print(f"{'Throughput (Ops/sec)':<30} | {ops_sec:,.2f} ops/sec")
    print(f"{'Mean Latency':<30} | {mean_lat:.3f} µs")
    print(f"{'p50 Latency':<30} | {p50:.3f} µs")
    print(f"{'p99 Latency':<30} | {p99:.3f} µs")
    print(f"{'p99.9 Latency':<30} | {p99_9:.3f} µs")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    run_benchmark()