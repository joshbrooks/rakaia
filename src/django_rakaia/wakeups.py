"""Wake waiting readers with Postgres `LISTEN`/`NOTIFY` instead of polling (#290).

Every write to a stream sends `NOTIFY rakaia_streams, '<path>'` inside its own
transaction, which Postgres delivers only when that transaction commits and
drops if it rolls back. Each process serving reads holds one listening
connection per database alias, in a thread of its own, and hands each
notification to the event loop that is waiting on that path.

Both halves are off unless `RAKAIA_NOTIFY_READERS` is true, and do nothing on a
database other than Postgres. While the listener is starting or reconnecting,
`event_for` answers None and the server falls back to polling. A notification
that never arrives (a writer without the setting, a listening connection that
died without an error) costs a reader the rest of its wait: the server reads
once more when a wait ends, so the message is late, not lost.

Works with psycopg 3 (3.2 or later, for `notifies(timeout=)`) and psycopg2.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import select
import threading
from collections.abc import Iterable
from typing import Any

from django.conf import settings
from django.db import connections

logger = logging.getLogger(__name__)

CHANNEL = "rakaia_streams"

# How long the listening thread blocks before checking whether it was stopped.
_LISTEN_TICK_SECONDS = 1.0
# How long it waits before connecting again after losing the connection.
_RECONNECT_SECONDS = 1.0


def notify_readers_enabled(using: str | None) -> bool:
    """Whether writes on `using` notify and reads on it listen."""
    if not getattr(settings, "RAKAIA_NOTIFY_READERS", False):
        return False
    return connections[using or "default"].vendor == "postgresql"


def notify_readers(using: str | None, paths: Iterable[str]) -> None:
    """Send one notification per path, delivered when the current transaction
    commits. Call it inside the write's transaction, on the write's alias."""
    unique = sorted(set(paths))
    if not unique or not notify_readers_enabled(using):
        return
    with connections[using or "default"].cursor() as cursor:
        for path in unique:
            cursor.execute("SELECT pg_notify(%s, %s)", [CHANNEL, path])


class Listener:
    """One listening connection for one alias, waking waiters on one event loop.

    The connection lives in a daemon thread. Each notification names a path;
    the thread asks the loop to set that path's event and drop it, so the next
    wait takes a fresh one. Losing the connection with an error wakes every
    waiter, so each reads again, and answers None until it is back. A
    connection that dies silently is not noticed (see the module docstring).
    """

    def __init__(self, using: str, loop: asyncio.AbstractEventLoop) -> None:
        self.using = using
        self.loop = loop
        self.listening = False
        self._events: dict[str, asyncio.Event] = {}
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run, name=f"rakaia-listen-{using}", daemon=True
        )
        self._thread.start()

    def event_for(self, path: str) -> asyncio.Event | None:
        """The event set on the next change to `path`; None while not listening.

        Call it on the listener's loop, before reading, so a change landing
        between the read and the wait has already set it.
        """
        if not self.listening:
            return None
        event = self._events.get(path)
        if event is None:
            event = self._events[path] = asyncio.Event()
        return event

    def stop(self) -> None:
        self._stopped.set()
        self._thread.join(timeout=5)

    # -- on the loop -------------------------------------------------------

    def _wake(self, path: str) -> None:
        event = self._events.pop(path, None)
        if event is not None:
            event.set()

    def _set_listening(self, value: bool) -> None:
        self.listening = value
        if not value:
            events, self._events = self._events, {}
            for event in events.values():
                event.set()

    # -- on the thread -----------------------------------------------------

    def _call_on_loop(self, fn: Any, *args: Any) -> bool:
        try:
            self.loop.call_soon_threadsafe(fn, *args)
        except RuntimeError:  # the loop is closed: nobody is waiting any more
            self._stopped.set()
            return False
        return True

    def _run(self) -> None:
        while not self._stopped.is_set():
            conn: Any = None
            try:
                conn = self._connect()
                conn.autocommit = True
                with conn.cursor() as cursor:
                    cursor.execute(f"LISTEN {CHANNEL}")
                self._call_on_loop(self._set_listening, True)
                self._receive(conn)
            except Exception:
                if not self._stopped.is_set():
                    logger.warning(
                        "rakaia: lost the notification connection on %r; "
                        "readers poll until it is back",
                        self.using,
                        exc_info=True,
                    )
            finally:
                self._call_on_loop(self._set_listening, False)
                if conn is not None:
                    with contextlib.suppress(Exception):
                        conn.close()
            self._stopped.wait(_RECONNECT_SECONDS)

    def _connect(self) -> Any:
        """A new driver connection with the alias's settings, outside Django's
        own connection handling, so nothing else ever uses or closes it."""
        wrapper = connections[self.using]
        return wrapper.get_new_connection(wrapper.get_connection_params())

    def _receive(self, conn: Any) -> None:
        """Hand each notification to the loop until stopped or disconnected."""
        if callable(getattr(conn, "notifies", None)):  # psycopg 3
            while not self._stopped.is_set():
                for note in conn.notifies(timeout=_LISTEN_TICK_SECONDS):
                    if not self._call_on_loop(self._wake, note.payload):
                        return
            return
        while not self._stopped.is_set():  # psycopg2
            ready, _, _ = select.select([conn], [], [], _LISTEN_TICK_SECONDS)
            if not ready:
                continue
            conn.poll()
            while conn.notifies:
                note = conn.notifies.pop(0)
                if not self._call_on_loop(self._wake, note.payload):
                    return


_listeners: dict[tuple[str, int], Listener] = {}
_listeners_lock = threading.Lock()


def listener_for(using: str | None) -> Listener | None:
    """This process's listener for `using` on the running loop, started on
    first use; None when notifications are off for that alias."""
    if not notify_readers_enabled(using):
        return None
    alias = using or "default"
    loop = asyncio.get_running_loop()
    key = (alias, id(loop))
    with _listeners_lock:
        listener = _listeners.get(key)
        if listener is None or listener.loop is not loop:
            listener = _listeners[key] = Listener(alias, loop)
        return listener


def stop_listeners() -> None:
    """Stop every listener in this process and close its connection."""
    with _listeners_lock:
        listeners = list(_listeners.values())
        _listeners.clear()
    for listener in listeners:
        listener.stop()
