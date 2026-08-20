"""Scramble the letters of the catalog over and over, to put the SSE fan-out
under load.

A human editing the right-hand pane produces one event every few seconds. That
tells you the wiring is connected; it tells you nothing about what happens when
events arrive faster than a browser can paint. This command is the other end of
that range: N saves as fast as the stack will take them, every one of them a
`StreamEvent` fanned out to `translations:{langcode}` and pushed down every open
EventSource.

By default the saves go through the running server's `update_translation` view
over HTTP, exactly as the editor pane does. That matters more than it looks:
live delivery happens through the channel layer, and under `just polyglot-dev`
that layer is `InMemoryChannelLayer` — in-memory to *one process*. A management
command that wrote to the database directly would append perfectly good events
that no connected browser would ever see, because the runserver process never
hears about them. Posting to the server puts the save inside the process the
browsers are attached to, so the demo works under `polyglot-dev` (in-memory) and
`polyglot-serve` (channels-redis) alike.

`--direct` is the escape hatch: save via the ORM, no server required. The events
are still durable, so a browser that connects afterwards replays them from
`Last-Event-ID` — but nothing arrives live under an in-memory channel layer.

Usage:

    # 1000 scrambles against the dev server, flat out
    uv run python manage.py stress_translations

    # slow enough to watch a single string thrash
    uv run python manage.py stress_translations --delay 0.25 --lang pt

Originals are restored on the way out (including after Ctrl-C) unless you pass
`--no-restore`.
"""

from __future__ import annotations

import itertools
import random
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from http.cookiejar import CookieJar
from typing import Any

from django.core.management.base import BaseCommand, CommandError
from django.db import connections

from polyglot.models import Translatable
from polyglot.strings import DEFAULT_LANG, LANGUAGES

_CSRF_INPUT = re.compile(r'name="csrfmiddlewaretoken"\s+value="([^"]+)"')


def scramble(text: str, rng: random.Random) -> str:
    """Shuffle the letters inside each whitespace-separated word.

    Word boundaries are kept so the result still reads as text of the right
    shape — the same number of words, the same length — which is what makes a
    live update obvious in the browser rather than just noisy.

    A short or repetitive word can shuffle back to itself; the caller re-rolls
    the whole string rather than looping forever on `"ho"`.
    """
    return " ".join(
        "".join(rng.sample(word, len(word))) if len(word) > 1 else word
        for word in text.split(" ")
    )


def scramble_differently(text: str, rng: random.Random, attempts: int = 8) -> str:
    """`scramble`, retried until the result actually differs from the input.

    Saving an unchanged value would still emit an event — `post_save` does not
    care whether anything changed — but a browser repainting the identical
    string looks like a dropped update, which is the one thing this command is
    meant to make visible.
    """
    for _ in range(attempts):
        candidate = scramble(text, rng)
        if candidate != text:
            return candidate
    return text


class _Poster:
    """POSTs translation updates to a running polyglot server.

    Holds the cookie jar (Django's CSRF cookie is set on the first GET) and the
    `csrfmiddlewaretoken` scraped out of the editor form, so each save is byte
    for byte the request the browser's Save button makes.
    """

    def __init__(self, base_url: str, langcode: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(CookieJar())
        )
        landing = f"{self.base_url}/?lang={langcode}"
        try:
            with self.opener.open(landing, timeout=10) as response:
                body = response.read().decode("utf-8", "replace")
        except urllib.error.URLError as exc:
            raise CommandError(
                f"Could not reach {landing} ({exc}).\n"
                "Start the server first (`just polyglot-dev`), point at it with "
                "--url, or run with --direct to write straight to the database."
            ) from exc
        match = _CSRF_INPUT.search(body)
        if match is None:
            raise CommandError(
                f"No csrfmiddlewaretoken in the page at {landing} — is that the "
                "polyglot landing page?"
            )
        self.csrf_token = match.group(1)

    def save(self, pk: int, msgstr: str) -> None:
        data = urllib.parse.urlencode(
            {"msgstr": msgstr, "csrfmiddlewaretoken": self.csrf_token}
        ).encode()
        request = urllib.request.Request(
            f"{self.base_url}/translations/{pk}/update/",
            data=data,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": self.base_url + "/",
            },
        )
        with self.opener.open(request, timeout=10):
            pass


class Command(BaseCommand):
    help = "Scramble translations repeatedly to load-test the SSE fan-out."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "-n",
            "--iterations",
            type=int,
            default=1000,
            help="Number of saves to perform (default: 1000).",
        )
        parser.add_argument(
            "--lang",
            default=DEFAULT_LANG,
            choices=[code for code, _ in LANGUAGES],
            help=f"Language to scramble (default: {DEFAULT_LANG}).",
        )
        parser.add_argument(
            "--delay",
            type=float,
            default=0.0,
            help=(
                "Seconds to sleep between saves (default: 0.0, i.e. as fast as "
                "the stack allows). Try 0.25 to watch individual updates land."
            ),
        )
        parser.add_argument(
            "--msgid",
            help="Scramble only this msgid, instead of picking one at random.",
        )
        parser.add_argument(
            "--url",
            default="http://localhost:8001",
            help="Base URL of the running polyglot server (default: %(default)s).",
        )
        parser.add_argument(
            "--direct",
            action="store_true",
            help=(
                "Save via the ORM instead of posting to the server. No server "
                "needed, but under an in-memory channel layer no connected "
                "browser sees the events live."
            ),
        )
        parser.add_argument(
            "--no-restore",
            action="store_true",
            help="Leave the scrambled text in the database when the run ends.",
        )
        parser.add_argument(
            "--seed",
            type=int,
            help="Seed the RNG, to make a run reproducible.",
        )
        parser.add_argument(
            "-c",
            "--concurrency",
            type=int,
            default=1,
            help=(
                "Number of writers to run in parallel (default: 1). One writer "
                "spends most of each save waiting on a disk flush, so the "
                "single-threaded number measures latency, not capacity."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        langcode: str = options["lang"]
        iterations: int = options["iterations"]
        delay: float = options["delay"]
        concurrency: int = max(1, options["concurrency"])

        rows = Translatable.objects.filter(langcode=langcode, deleted__isnull=True)
        if options["msgid"]:
            rows = rows.filter(msgid=options["msgid"])
        # `msgstr` is nullable and the editor allows blanks; a row with nothing
        # in it has no letters to scramble, so it is not a useful target.
        targets = [(row.pk, row.msgstr) for row in rows if row.msgstr]
        if not targets:
            raise CommandError(
                f"No translations to scramble for lang={langcode}"
                f"{' msgid=' + options['msgid'] if options['msgid'] else ''}. "
                "Load the landing page once to seed the catalog."
            )

        originals = dict(targets)
        # The scramble compounds: each pass shuffles the value the *last* pass
        # produced, so a string a browser stopped tracking visibly diverges
        # instead of being re-synced by the next update. Held in memory rather
        # than re-read per iteration — a read-before-write would add a query to
        # every save and, under --concurrency, a lost-update race that would be
        # measuring itself.
        latest = dict(originals)
        latest_lock = threading.Lock()

        url: str = options["url"]
        direct: bool = options["direct"]
        # One _Poster per writer: each owns a cookie jar and an HTTP connection,
        # which urllib does not make safe to share across threads.
        posters = [] if direct else [_Poster(url, langcode) for _ in range(concurrency)]
        mode = "ORM (direct)" if direct else f"HTTP {url}"
        self.stdout.write(
            f"Scrambling {len(targets)} string(s) in '{langcode}' {iterations} "
            f"times via {mode}, {concurrency} writer(s). Ctrl-C to stop."
        )

        counter = itertools.count(1)
        failures: list[str] = []
        stop = threading.Event()
        started = time.monotonic()

        def save(pk: int, msgstr: str, worker: int) -> None:
            if direct:
                row = Translatable.objects.get(pk=pk)
                row.msgstr = msgstr
                row.save()  # post_save -> create_stream_event -> SSE
            else:
                posters[worker].save(pk, msgstr)

        def writer(worker: int) -> int:
            # Per-thread RNG: `random.Random` is not thread-safe, and a shared
            # one would make --seed meaningless anyway once threads interleave.
            seed = options["seed"]
            rng = random.Random(None if seed is None else f"{seed}:{worker}")
            done_here = 0
            try:
                while not stop.is_set():
                    n = next(counter)
                    if n > iterations:
                        return done_here
                    pk = targets[rng.randrange(len(targets))][0]
                    with latest_lock:
                        source = latest[pk]
                    scrambled = scramble_differently(source, rng)
                    try:
                        save(pk, scrambled, worker)
                    except Exception as exc:  # noqa: BLE001
                        # A stress run reports what broke under load; it does not
                        # abort on the first "database is locked".
                        failures.append(f"{type(exc).__name__}: {exc}")
                        continue
                    with latest_lock:
                        latest[pk] = scrambled
                    done_here += 1
                    if n % 100 == 0:
                        rate = n / max(time.monotonic() - started, 1e-9)
                        self.stdout.write(f"  {n}/{iterations}  ({rate:.0f} saves/s)")
                    if delay:
                        time.sleep(delay)
            finally:
                # Each thread opened its own connection under --direct; leaving
                # them behind holds SQLite locks open past the run.
                connections.close_all()
            return done_here

        try:
            if concurrency == 1:
                done = writer(0)
            else:
                with ThreadPoolExecutor(concurrency) as pool:
                    done = sum(pool.map(writer, range(concurrency)))
        except KeyboardInterrupt:
            stop.set()
            done = next(counter) - 1
            self.stdout.write(self.style.WARNING("\nInterrupted."))

        elapsed = time.monotonic() - started
        self.stdout.write(
            self.style.SUCCESS(
                f"{done} saves in {elapsed:.1f}s "
                f"({done / max(elapsed, 1e-9):.0f}/s), "
                f"{done} events appended to translations:{langcode}."
            )
        )
        if failures:
            counts = Counter(failures)
            self.stdout.write(self.style.ERROR(f"{len(failures)} save(s) failed:"))
            for message, count in counts.most_common(5):
                self.stdout.write(self.style.ERROR(f"  {count}x {message[:120]}"))

        if options["no_restore"]:
            self.stdout.write("Leaving scrambled text in place (--no-restore).")
            return
        # Restoring is itself a save per row, so the browsers watch the page
        # snap back — a last, deliberate burst that proves the connection
        # survived the flood.
        for pk, original in originals.items():
            save(pk, original, 0)
        self.stdout.write(f"Restored {len(originals)} original string(s).")
