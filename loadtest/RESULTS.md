# Load test results

Measured with [`loadgen.py`](loadgen.py). Raw reports are in [`results/`](results/).

## Setup, and what these numbers do and don't cover

- **Machine**: one 8-core Windows 11 laptop running **both** the server and the
  load generator, so they compete for the same cores.
- **Detector**: rules-only mode (`MODSTREAM_SCORER=none`). These runs measure the
  serving architecture: connections, batching, queueing, fan-out and persistence.
  They do **not** measure transformer inference cost; see "With the ML model" below.
- **Infrastructure**: in-memory broker and SQLite. The Redis/Postgres deployment
  passes the CI integration tests (real Redis 7 + Postgres 16) but is not benchmarked here.
- **Windows caveats**: asyncio's IOCP loop is the slowest event-loop backend
  (Linux containers get uvloop), and Windows timers have ~15 ms granularity,
  which inflates small waits such as the 5 ms batching window.

> These runs used rules v1. Rules v2 ([evaluation](../evaluation/README.md)) is ~2.4x slower per
> message (0.13 ms vs 0.055 ms), a small share of the ~0.45 ms per-message serving cost; re-run
> pending.

## Synchronous path: WebSocket `analyze` round trips

| Streams | Offered load | Server processes | Answered | Errors | Throughput | p50 | p95 | p99 |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 200 | 1,000 msg/s | 1 | 100% | 0 | 966 msg/s | 42 ms | 62 ms | 90 ms |
| 1,000 | 2,000 msg/s | 1 | 100% | 0 | 1,964 msg/s | 40 ms | 83 ms | 198 ms |
| 1,000 | 5,000 msg/s | 4 | 100% | 0 | **4,836 msg/s** | 28 ms | 50 ms | 72 ms |
| 2,000 | 8,000 msg/s | 4 | 100% | 0 | **7,772 msg/s** | 52 ms | 86 ms | 165 ms |

## Asynchronous path: publish → queue → worker → store → fan-out

Each message is persisted and delivered to every live subscriber (10 subscribers,
so 10 deliveries per message).

| Streams | Offered load | Processed | Fan-out deliveries | Dropped | p50 end-to-end | p99 end-to-end |
|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,000 msg/s | 995 msg/s | 9,955/s | 0 | 85 ms | 275 ms |
| 2,000 | 2,000 msg/s | 1,366 msg/s (saturated) | 13,660/s | 0 | 5.6 s (queueing) | 11.2 s |

In the saturated run the queue absorbed the excess and drained afterwards:
**all 39,840 published messages were stored, none lost, none dead-lettered.**
That's the purpose of the durable queue. The fix for the saturation is more
worker processes, which needs Redis so the workers share a queue.

## What profiling changed

The first 1,000-stream × 5 msg/s run collapsed: **635 msg/s, 79,417 rejected
requests, p99 18 s**. Load shedding kept the process alive, but capacity was far too
low. Profiling under load found three causes:

| Finding | Fix | Effect |
|---|---|---|
| The rule layer ran ~150 separate regexes per message | One compiled alternation, longest-first | 7.8k → 29.6k texts/s per core (3.8×) |
| `dataclasses.asdict` deep-copied every result | Hand-written `to_dict` | Removed ~36 µs per message |
| WebSocket per-message deflate compressed tiny JSON frames | Disabled (server + client) | Removed zlib from the hot path |
| Each event was JSON-encoded once per subscriber | Encode once, share the bytes | Fan-out throughput +26% per process |
| One Python process saturates one core (GIL) | Scale out: N processes / replicas | 1,964 → 7,772 msg/s with 4 processes |

Running several processes also surfaced two real bugs, both now fixed:
concurrent schema creation raced at startup, and per-process random session keys
would have broken sessions behind a load balancer.

## With the ML model

With Detoxify (BERT) enabled, model inference becomes the bottleneck, not the
serving layer. That's what the architecture is built around:

- the **micro-batcher** turns many concurrent requests into one forward pass,
- the **worker pool** scales independently of API nodes (add replicas, or GPUs),
- the next roadmap phase (ONNX Runtime + int8 quantization) targets per-batch
  inference cost directly.

Next measurement to add: per-batch inference latency on CPU vs ONNX vs GPU.

## Reproduce

```bash
# terminal 1: server (rules-only, 4 processes)
MODSTREAM_SECRET_KEY=dev MODSTREAM_SCORER=none python -m modstream api --workers 4
# terminal 2
python loadtest/loadgen.py analyze --streams 2000 --rate 4 --duration 20 --procs 4
```

The pipeline mode with the in-memory broker needs a single server process
(`--workers 1`); with `MODSTREAM_REDIS_URL` set, any number of API and worker processes
share one queue.
