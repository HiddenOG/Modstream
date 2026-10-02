"""Load generator: many concurrent message streams against a Modstream node.

Modes
-----
analyze   Each stream is a WebSocket sending ``analyze`` frames; measures the
          round trip on the synchronous (micro-batched) path.
pipeline  Each stream publishes to a channel on the asynchronous path while
          subscribers receive verdicts; measures enqueue -> verdict delivered.

All connections are opened first; the clock starts once every stream is
connected, so connection setup isn't counted in throughput.

    python loadtest/loadgen.py analyze  --streams 1000 --rate 5 --duration 30
    python loadtest/loadgen.py pipeline --streams 2000 --rate 2 --subscribers 50 --duration 30
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import math
import multiprocessing
import random
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime

import websockets

TEXTS = [
    "good luck on the exam tomorrow!", "who's coming to practice tonight?", "gg everyone, great match",
    "can someone send the notes", "that exam was stupid hard", "you're such a loser", "ur an 1d10t",
    "see you on monday", "this playlist is fire", "nobody likes you, just leave", "omw, 5 minutes",
]


@dataclass
class Run:
    total: int
    duration: float
    connected: int = 0
    failed: int = 0
    all_connected: asyncio.Event = field(default_factory=asyncio.Event)
    go: asyncio.Event = field(default_factory=asyncio.Event)
    deadline: float = math.inf

    def arrived(self, ok: bool) -> None:
        if ok:
            self.connected += 1
        else:
            self.failed += 1
        if self.connected + self.failed == self.total:
            self.all_connected.set()

    @property
    def running(self) -> bool:
        return time.monotonic() < self.deadline


@dataclass
class Stats:
    latencies: list[float] = field(default_factory=list)
    sent: int = 0
    received: int = 0
    errors: int = 0
    dropped: int = 0

    def summary(self) -> dict:
        lat = sorted(self.latencies)

        def pct(p):
            return round(lat[min(len(lat) - 1, int(p / 100 * len(lat)))] * 1000, 1) if lat else None

        return {"p50": pct(50), "p95": pct(95), "p99": pct(99),
                "mean": round(statistics.fmean(lat) * 1000, 1) if lat else None}


async def open_ws(url: str, run: Run):
    ws = None
    for attempt in range(5):
        try:
            ws = await websockets.connect(url, max_queue=None, ping_interval=None, open_timeout=60, compression=None)
            break
        except Exception:  # noqa: BLE001
            await asyncio.sleep(0.25 * (attempt + 1))
    run.arrived(ws is not None)
    await run.go.wait()
    return ws


async def analyze_stream(url: str, rate: float, run: Run, stats: Stats):
    ws = await open_ws(url, run)
    if ws is None:
        return
    pending: dict[str, float] = {}

    async def reader():
        async for raw in ws:
            msg = json.loads(raw)
            started = pending.pop(msg.get("ref"), None)
            if msg["type"] == "result" and started is not None:
                stats.received += 1
                stats.latencies.append(time.perf_counter() - started)
            elif msg["type"] == "error":
                stats.errors += 1

    read_task = asyncio.create_task(reader())
    await asyncio.sleep(random.random() / rate)  # de-synchronize streams
    n = 0
    try:
        while run.running:
            pending[str(n)] = time.perf_counter()
            await ws.send(json.dumps({"type": "analyze", "ref": str(n), "text": random.choice(TEXTS), "record": False}))
            stats.sent += 1
            n += 1
            await asyncio.sleep(1 / rate)
        await asyncio.sleep(1)  # let in-flight replies land
    finally:
        read_task.cancel()
        await ws.close()


async def publish_stream(url: str, rate: float, run: Run, stats: Stats, channel: str):
    ws = await open_ws(url, run)
    if ws is None:
        return

    async def drain_acks():
        async for _ in ws:
            pass

    read_task = asyncio.create_task(drain_acks())
    await asyncio.sleep(random.random() / rate)
    try:
        while run.running:
            frame = {"type": "publish", "channel": channel, "author": "load", "text": random.choice(TEXTS)}
            await ws.send(json.dumps(frame))
            stats.sent += 1
            await asyncio.sleep(1 / rate)
    finally:
        read_task.cancel()
        await ws.close()


async def subscriber(url: str, run: Run, stats: Stats, grace: float):
    ws = await open_ws(url, run)
    if ws is None:
        return
    await ws.send(json.dumps({"type": "subscribe"}))
    try:
        while time.monotonic() < run.deadline + grace:
            try:
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=0.5))
            except TimeoutError:
                continue
            if msg["type"] == "event" and msg["event"]["type"] == "message":
                stats.received += 1
                enqueued = datetime.fromisoformat(msg["event"]["data"]["enqueued_at"]).timestamp()
                stats.latencies.append(max(0.0, time.time() - enqueued))
            elif msg["type"] == "lagged":
                stats.dropped += msg["dropped"]
    finally:
        await ws.close()


async def run_slice(args, streams: int, n_subs: int, offset: int) -> dict:
    """Run one process's share of the load and return raw counters and latency samples."""
    run = Run(total=streams + n_subs, duration=args.duration)
    producers, consumers = Stats(), Stats()
    grace = 5.0

    tasks = []
    for i in range(streams):
        if args.mode == "analyze":
            tasks.append(analyze_stream(args.url, args.rate, run, producers))
        else:
            tasks.append(publish_stream(args.url, args.rate, run, producers, f"room-{(offset + i) % 500}"))
    tasks += [subscriber(args.url, run, consumers, grace) for _ in range(n_subs)]
    runners = [asyncio.create_task(t) for t in tasks]

    opened = time.monotonic()
    await run.all_connected.wait()
    print(f"[slice {offset}] connected {run.connected} in {time.monotonic() - opened:.1f}s ({run.failed} failed)")
    run.deadline = time.monotonic() + args.duration
    run.go.set()
    while run.running:
        await asyncio.sleep(0.5)
    await asyncio.wait(runners, timeout=grace + 5)
    for r in runners:
        r.cancel()
    with contextlib.suppress(Exception):
        await asyncio.gather(*runners, return_exceptions=True)
    return {
        "connected": run.connected, "failed": run.failed,
        "sent": producers.sent, "answered": producers.received, "errors": producers.errors,
        "delivered": consumers.received, "dropped": consumers.dropped,
        "producer_latencies": producers.latencies, "consumer_latencies": consumers.latencies,
    }


def _slice_entry(job):
    args, streams, n_subs, offset = job
    return asyncio.run(run_slice(args, streams, n_subs, offset))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("mode", choices=["analyze", "pipeline"])
    p.add_argument("--url", default="ws://127.0.0.1:8000/api/v1/ws")
    p.add_argument("--streams", type=int, default=500, help="concurrent producer connections (total)")
    p.add_argument("--rate", type=float, default=5, help="messages per second per stream")
    p.add_argument("--subscribers", type=int, default=10, help="pipeline mode: live subscribers (total)")
    p.add_argument("--duration", type=float, default=20)
    p.add_argument("--procs", type=int, default=1, help="client processes (one Python process tops out ~2-3k msg/s)")
    p.add_argument("--out", help="also write the JSON report here")
    args = p.parse_args()

    n_subs = args.subscribers if args.mode == "pipeline" else 0
    procs = max(1, min(args.procs, args.streams))
    jobs = [(args, args.streams // procs + (i < args.streams % procs), n_subs // procs + (i < n_subs % procs),
             i * (args.streams // procs)) for i in range(procs)]
    print(f"{args.mode}: {args.streams} streams x {args.rate}/s"
          f"{f' + {n_subs} subscribers' if n_subs else ''} for {args.duration}s across {procs} process(es)")
    if procs == 1:
        parts = [_slice_entry(jobs[0])]
    else:
        with multiprocessing.get_context("spawn").Pool(procs) as pool:
            parts = pool.map(_slice_entry, jobs)

    total = {k: sum(part[k] for part in parts) for k in parts[0] if not k.endswith("latencies")}
    producer_lat = Stats(latencies=[x for part in parts for x in part["producer_latencies"]])
    consumer_lat = Stats(latencies=[x for part in parts for x in part["consumer_latencies"]])

    if args.mode == "analyze":
        report = {
            "mode": "analyze (synchronous, micro-batched)",
            "streams": total["connected"], "rate_per_stream": args.rate, "duration_s": args.duration,
            "sent": total["sent"], "answered": total["answered"], "errors": total["errors"],
            "throughput_msg_s": round(total["answered"] / args.duration, 1),
            "round_trip_ms": producer_lat.summary(),
        }
    else:
        per_sub = total["delivered"] / max(n_subs, 1)
        report = {
            "mode": "pipeline (queue -> workers -> fan-out)",
            "streams": args.streams, "subscribers": n_subs, "rate_per_stream": args.rate,
            "duration_s": args.duration, "published": total["sent"],
            "verdicts_per_subscriber": round(per_sub),
            "throughput_msg_s": round(per_sub / args.duration, 1),
            "fanout_deliveries_s": round(total["delivered"] / args.duration, 1),
            "dropped_for_slow_subscribers": total["dropped"],
            "end_to_end_ms": consumer_lat.summary(),
        }
    report["client_processes"] = procs
    report["connect_failures"] = total["failed"]
    print(json.dumps(report, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(report, f, indent=2)


if __name__ == "__main__":
    main()
