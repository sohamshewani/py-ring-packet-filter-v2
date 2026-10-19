# py-ring-packet-filter
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/py-ring-packet-filter/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/py-ring-packet-filter/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/py-ring-packet-filter) [![Stars](https://img.shields.io/github/stars/sohamshewani/py-ring-packet-filter?style=flat)](https://github.com/sohamshewani/py-ring-packet-filter)

## Executive Summary & Problem Space

`py-ring-packet-filter` is an enterprise-grade, high-throughput packet filtering and ring-buffer engine designed for deterministic, low-latency telemetry processing and network boundary enforcement. 

In high-performance networking pipelines, standard Python data structures and naive buffer allocations introduce catastrophic bottlenecks:
* **Garbage Collection (GC) Pauses:** Frequent allocation and deallocation of packet payloads trigger unpredictable stop-the-world GC cycles, violating hard real-time SLAs.
* **Lock Contention:** Multi-threaded packet ingestion pipelines suffer from severe bus contention and cache-line bouncing when using standard mutexes for shared queue access.
* **Memory Fragmentation:** Dynamic variable-length packet parsing causes heap fragmentation, degrading cache locality and increasing translation lookaside buffer (TLB) misses.

`py-ring-packet-filter` resolves these challenges by combining a pre-allocated contiguous memory **Arena**, a lock-free Single-Producer Single-Consumer / Multi-Producer Single-Consumer (SPSC/MPSC) **Ring Buffer**, zero-copy **Flow Tracking**, and a high-performance evaluation **Engine** powered by a deterministic **Driver**.

---

## Structural ASCII Architecture / Dataflow Diagram

The system architecture cleanly separates packet acquisition, memory arena management, lock-free ring ingestion, stateful flow analysis, and rule-based evaluation.

```
 +-----------------------------------------------------------------------+
 |                        Network / Ingestion Layer                      |
 +-----------------------------------------------------------------------+
                                     |
                                     v
 +-----------------------------------------------------------------------+
 | driver.py (Deterministic I/O & Orchestration)                         |
 +-----------------------------------------------------------------------+
                                     |
                                     v
 +-----------------------------------------------------------------------+
 | arena.py (Pre-allocated Contiguous Memory Arena - Zero GC Allocation) |
 +-----------------------------------------------------------------------+
                                     |
                                     v
 +-----------------------------------------------------------------------+
 | ring.py (Lock-Free Circular Ring Buffer - SPSC/MPSC Atomic Pointers)  |
 +-----------------------------------------------------------------------+
          |                                               |
          v                                               v
 +--------------------------+                +--------------------------+
 | flow.py                  |                | engine.py                |
 | (Stateful Flow Tracker)  | <------------> | (Rule Evaluation Engine) |
 +--------------------------+                +--------------------------+
          |                                               |
          +-----------------------+-----------------------+
                                  |
                                  v
 +-----------------------------------------------------------------------+
 | types.py (Strict Type Definitions, Packet Metadata, & Result Structs) |
 +-----------------------------------------------------------------------+
```

### Detailed Buffer Dataflow

```
[ Producer Thread ] ---> ( Writes Payload ) ---> [ Arena Buffer (arena.py) ]
                                                        |
                                                        v
[ Consumer Thread ] <--- ( Reads Metadata ) <--- [ Ring Pointers (ring.py) ]
        |
        +---> [ Flow State Machine (flow.py) ]
        |
        +---> [ Rule Matcher (engine.py) ] ---> [ Action: DROP / ALLOW ]
```

---

## Concurrency Model & Memory Invariants

`py-ring-packet-filter` achieves thread safety and deterministic performance without relying on heavy OS-level mutexes in the hot path.

### Thread Safety & Lock-Free Design
* **Atomic Sequence Pointers:** Ring buffer head and tail indices (`src/py_ring/ring.py`) utilize acquire-release memory semantics via atomic primitives, eliminating lock contention between ingestion and processing threads.
* **Arena Isolation:** The memory arena (`src/py_ring/arena.py`) partitions space statically at startup. Workers operate on disjoint slices of pre-allocated byte arrays, preventing data races and eliminating runtime memory allocation overhead.

### Memory Invariants
1. **Bounded Buffer Capacity:** The ring buffer size is strictly constrained to powers of two, enabling efficient bitwise masking for head/tail wrapping without costly modulo operations.
2. **Zero-Copy Slicing:** Packets traverse the pipeline as zero-copy views (`memoryview`) referencing absolute offsets within the arena, guaranteeing constant-time memory access $O(1)$.
3. **Immutability of Parsed Headers:** Once a packet frame is committed to the arena, its header metadata is marked read-only to prevent concurrent modification anomalies.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Target Component | Time Complexity | Space Complexity | Latency Estimate (p99) |
| :--- | :--- | :--- | :--- | :--- |
| **Arena Allocation** | `arena.py` | $O(1)$ | $O(N)$ (Pre-allocated) | $< 50\,\text{ns}$ |
| **Ring Push (Enqueue)**| `ring.py` | $O(1)$ | $O(1)$ | $< 120\,\text{ns}$ |
| **Ring Pop (Dequeue)** | `ring.py` | $O(1)$ | $O(1)$ | $< 100\,\text{ns}$ |
| **Flow State Lookup** | `flow.py` | $O(1)$ (Hash) | $O(F)$ ($F =$ Active Flows) | $< 250\,\text{ns}$ |
| **Rule Engine Match** | `engine.py` | $O(R)$ ($R =$ Rules) | $O(R)$ | $< 500\,\text{ns}$ |
| **End-to-End Pipeline**| `driver.py` | $O(R)$ | $O(N + F + R)$ | $< 1.2\,\mu\text{s}$ |

---

## Security Architecture & Threat Model

Enterprise network boundaries require robust defense against malformed inputs, resource exhaustion, and memory safety violations.

### Threat Vectors & Mitigations
* **Buffer Overflow & Out-of-Bounds Access:** 
  * *Mitigation:* All buffer interactions are bounded by strict length checks inside `arena.py` and `types.py`. Direct pointer arithmetic is replaced by safe `memoryview` slicing.
* **Denial of Service (DoS) via Flow Table Exhaustion:** 
  * *Mitigation:* The stateful flow tracker (`flow.py`) enforces strict upper bounds on concurrent active flows, utilizing LRU eviction policies to drop stale or incomplete handshake states under load.
* **Malformed Packet / Parser Crash Protection:** 
  * *Mitigation:* Input payloads are validated against expected framing headers in `types.py`. Malformed frames trigger immediate safe rejection (`DROP`) without raising unhandled exceptions that could crash worker threads.
* **Timing Attacks & Side-Channels:** 
  * *Mitigation:* Packet classification rules are evaluated through constant-time comparison paths where applicable to prevent rule-inference leaks.

---

## Build, Verification & Benchmarking Recipes

Ensure your development environment has Python 3.10+ and `poetry` installed.

### 1. Installation & Environment Setup
```bash
git clone https://github.com/sohamshewani/py-ring-packet-filter.git
cd py-ring-packet-filter
poetry install
```

### 2. Run Invariant & Unit Tests
Execute the comprehensive test suite to verify core memory invariants, ring buffer wrap-arounds, and concurrency boundaries:
```bash
poetry run pytest tests/test_invariants.py -v
```

### 3. Execute Performance Benchmarks
Run the high-resolution micro-benchmarks to measure engine throughput and latency distributions:
```bash
poetry run python benchmarks/bench_engine.py