"""Live-mode background producer for the orders demo.

`just orders-live` starts a runserver whose page polls a JSON snapshot ~1×/sec.
Behind it, a single daemon thread — the *producer* — drives the whole rakaia
pipeline in real time:

    random order  ─append→  orders stream  ─replay→  OrderSummary projection

Every ~0.5–1.5 s it invents a random order (and, now and then, a loyalty-bonus
event), appends it to the in-memory stream, then **incrementally replays** only
the new events (`replay(start_seq=…)`) through the *same* versioned handlers the
static demo uses. Form submissions from the browser are funnelled through the
same thread via a queue.

**Why one thread owns the store.** rakaia's in-memory `StreamStore` is a
process-wide singleton but is *not* safe for a background writer appending while
request threads read it (its internal lock is asyncio, not cross-thread). So the
producer thread is the *only* code that ever touches the store: the web views
read the durable `OrderSummary` rows (sqlite, already concurrency-safe) and a
lock-guarded ring buffer of recent events. A form POST doesn't append directly —
it enqueues the order for the producer to append on its next tick.
"""

from __future__ import annotations

import json
import random
import threading
import time
from collections import deque
from queue import Empty, Queue
from typing import Any

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.store import get_store
from rakaia.replay import replay

STREAM = "orders"

# Bounded log of what the producer has appended, newest first — the "stream" the
# live page shows next to the projection. Read by web requests under `_lock`.
_RECENT_MAX = 40

_SKUS = [
    ("TEA-EARL", "6.00"),
    ("TEA-GREEN", "5.00"),
    ("MUG-CERAMIC", "12.00"),
    ("KETTLE-STEEL", "40.00"),
    ("SPOON-SET", "9.00"),
    ("HONEY-JAR", "7.50"),
]
_STATUSES = ["PAID", "PAID", "PAID", "PENDING", "CANCELLED"]  # weighted toward PAID


class LiveProducer:
    """Owns the orders stream and materialises it live. Singleton via `_producer`."""

    def __init__(self) -> None:
        self._rng = random.Random()
        self._queue: Queue[dict[str, Any]] = Queue()
        self._lock = threading.Lock()
        self._recent: deque[dict[str, Any]] = deque(maxlen=_RECENT_MAX)
        self._thread: threading.Thread | None = None
        self._started = False
        # Absolute event index already replayed into OrderSummary, so each tick
        # replays only the tail (replay(start_seq=self._replayed)).
        self._replayed = 0
        self._seq = 0  # total events appended so far
        self._order_counter = 2000  # live order ids start at ORD-2001
        # order_ids the producer has placed, so a bonus can target a real order
        # (lands) or a fabricated one (op="update" no-op — no phantom row).
        self._placed_paid: list[str] = []

    # -- lifecycle ---------------------------------------------------------
    def ensure_started(self) -> None:
        """Idempotently start the producer (called lazily by the live views)."""
        with self._lock:
            if self._started:
                return
            self._started = True
        # Fresh slate so the live page fills from empty on each server start.
        from orders.models import OrderSummary

        store = get_store()
        store.delete(STREAM)
        store.create(STREAM)
        OrderSummary.objects.all().delete()
        self._thread = threading.Thread(
            target=self._run, name="orders-live-producer", daemon=True
        )
        self._thread.start()

    def submit(self, order: dict[str, Any]) -> None:
        """Enqueue a browser-submitted order for the producer to append."""
        self._queue.put(order)

    # -- snapshot for the web view ----------------------------------------
    def recent(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._recent)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {"events": self._seq, "running": self._started}

    # -- producer loop -----------------------------------------------------
    def _run(self) -> None:
        while True:
            time.sleep(self._rng.uniform(0.5, 1.5))
            batch: list[dict[str, Any]] = []
            # User submissions first (drain the queue), then one auto order.
            while True:
                try:
                    batch.append(self._queue.get_nowait())
                except Empty:
                    break
            batch.append(self._invent_event())
            try:
                self._append_and_replay(batch)
            except Exception as exc:  # keep the loop alive on any hiccup
                self._log_note(f"producer error: {exc}")

    def _append_and_replay(self, batch: list[dict[str, Any]]) -> None:
        store = get_store()
        start = self._seq
        for event in batch:
            store.append(STREAM, json.dumps(event).encode("utf-8"))
            self._seq += 1
            # Track placed PAID orders (invented *or* browser-submitted) so a
            # later bonus can target a real row; do this before _note_event so
            # its applied/no-op label matches.
            if event.get("kind") != "loyalty_bonus" and event.get("status") == "PAID":
                self._placed_paid.append(event["order_id"])
            self._note_event(event)
        # Incremental: replay only the events appended since last time. The
        # effects (update_or_create / update) are idempotent, so this converges.
        replay(
            store=store,
            stream_path=STREAM,
            executor=DjangoExecutor(),
            start_seq=start,
        )
        self._replayed = self._seq

    # -- event invention ---------------------------------------------------
    def _invent_event(self) -> dict[str, Any]:
        # ~1 in 4 ticks is a loyalty bonus (the op="update" showcase); the rest
        # are fresh orders.
        if self._placed_paid and self._rng.random() < 0.25:
            return self._invent_bonus()
        return self._invent_order()

    def _invent_order(self) -> dict[str, Any]:
        self._order_counter += 1
        order_id = f"ORD-{self._order_counter}"
        status = self._rng.choice(_STATUSES)
        n_items = self._rng.randint(1, 3)
        items = []
        for _ in range(n_items):
            sku, price = self._rng.choice(_SKUS)
            items.append(
                {"sku": sku, "quantity": self._rng.randint(1, 5), "price": price}
            )
        return {
            "schema_version": 2,
            "order_id": order_id,
            "status": status,
            "currency": "USD",
            "items": items,
        }

    def _invent_bonus(self) -> dict[str, Any]:
        # 70% target a real placed order (bonus lands); 30% target a fabricated
        # order that was never placed (op="update" is a clean no-op — no row is
        # minted, which is the whole point).
        if self._rng.random() < 0.7:
            order_id = self._rng.choice(self._placed_paid)
        else:
            order_id = f"ORD-GHOST-{self._rng.randint(100, 999)}"
        return {
            "schema_version": 2,
            "kind": "loyalty_bonus",
            "order_id": order_id,
            "bonus": self._rng.choice([10, 25, 50, 100]),
        }

    # -- recent-events feed ------------------------------------------------
    def _note_event(self, event: dict[str, Any]) -> None:
        seq = self._seq - 1
        if event.get("kind") == "loyalty_bonus":
            oid = event["order_id"]
            applied = oid in self._placed_paid
            entry = {
                "seq": seq,
                "kind": "bonus",
                "order_id": oid,
                "detail": f"+{event['bonus']} pts",
                "outcome": "applied" if applied else "no-op — no such order",
            }
        else:
            entry = {
                "seq": seq,
                "kind": "order",
                "order_id": event["order_id"],
                "detail": f"{len(event['items'])} item(s)",
                "outcome": event["status"],
            }
        with self._lock:
            self._recent.appendleft(entry)

    def _log_note(self, msg: str) -> None:
        with self._lock:
            self._recent.appendleft(
                {
                    "seq": self._seq,
                    "kind": "note",
                    "order_id": "",
                    "detail": msg,
                    "outcome": "",
                }
            )


# Process-wide singleton — mirrors the store's own singleton lifetime.
_producer = LiveProducer()


def get_producer() -> LiveProducer:
    return _producer
