# Roadmap: from demo app to a streaming moderation platform

Goal: moderate **thousands of concurrent message streams** in real time, with
infrastructure that scales horizontally and can be measured.

## Why the v2 architecture can't do this

| Bottleneck | Effect at scale |
|---|---|
| Flask + thread per request | Every open SSE/stream connection pins a thread: 8 threads ≈ 8 live viewers |
| Inference inside the request | Each message runs its own forward pass; no batching across users |
| SQLite + per-connection polling | One writer; every viewer polls the DB every second |
| State inside one process | Can't add a second server: cache, stats and streams aren't shared |

## Target architecture

```
 producers ──HTTP / WebSocket──►  API nodes (FastAPI + uvicorn, stateless, N replicas)
                                   │  sync path:  /analyze, WS frames ─► MicroBatcher ─► model
                                   │  async path: /messages ─► XADD ──┐
                                   │                                  ▼
                                   │                 Redis Stream  cs:messages
                                   │                 (consumer group, at-least-once)
                                   │                                  │
                                   │              workers (N replicas): batch read → model
                                   │              → bulk insert → publish → XACK  (DLQ on failure)
                                   │                                  │
 viewers ◄──SSE / WebSocket── Hub ◄──── Redis Stream  cs:events ◄─────┘
                                   │
                         Postgres (SQLAlchemy async) · Redis counters for stats
                         Prometheus /metrics → Grafana
```

## Phases

- [ ] **1. Async core**: FastAPI + uvicorn (ASGI), Pydantic schemas, auto OpenAPI docs,
      typed settings, and an asyncio **micro-batcher** that merges concurrent requests
      into one model forward pass.
- [ ] **2. Streaming pipeline**: Broker abstraction (Redis Streams in production,
      in-memory for dev), ingestion API, worker pool with consumer groups,
      crash recovery (`XAUTOCLAIM`), dead-letter queue, a per-process fan-out hub for
      thousands of SSE/WebSocket subscribers, and resumable streams.
- [ ] **3. Storage**: SQLAlchemy 2.0 async (Postgres in production, SQLite locally),
      bulk inserts from workers, and Redis atomic counters replacing aggregate queries.
- [ ] **4. Observability**: Prometheus metrics (throughput, batch size, p50/p95/p99
      latency, queue lag, connected clients), JSON logs, liveness/readiness probes,
      and a provisioned Grafana dashboard.
- [ ] **5. Full local stack + load testing**: docker compose (api, worker, redis,
      postgres, prometheus, grafana); a load generator for thousands of concurrent
      streams; published benchmark numbers.
- [ ] **6. Fast inference**: ONNX Runtime export + int8 quantization; benchmark
      against PyTorch.
- [ ] **7. LLM cascade**: send only "needs review" items to an LLM judge; track cost per
      1k messages and accuracy per tier.
- [ ] **8. Multi-tenant API**: API keys, Redis token-bucket rate limiting, signed
      (HMAC) webhooks for verdicts.
- [ ] **9. Cloud deploy**: Kubernetes manifests, KEDA autoscaling of workers on stream
      lag, an HPA for the API, images pushed to GHCR from CI, a live public demo.
