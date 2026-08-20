"""One JSON shape for an event, shared by every surface that emits JSON.

`message_of` is the single definition for surfaces that can carry bytes — the
store's `read()`, and anything downstream of it. Surfaces that emit *JSON*
cannot: `StreamMessage.data` is bytes, so they publish the stored
`data`/`payload_encoding` pair and let the consumer run `decode_payload`
itself. #153 made the primitives behind that single (`event_label`,
`payload_fields`, `decode_payload`) but left each surface assembling its own
dict, and three of them drifted — including on which timestamp they publish.

`event_view` is the JSON-wire counterpart to `message_of`: one assembly, so the
next surface is correct by construction rather than by having been reviewed.
"""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.event_message import event_view_of_entry as event_view
from django_rakaia.models import Stream, StreamEntry

pytestmark = pytest.mark.django_db


def _one_entry(path: str = "s", payload: bytes = b'{"n": 1}', **append_kwargs):
    store = DjangoStreamStore()
    store.create(path)
    store.append(path, payload, **append_kwargs)
    return StreamEntry.objects.select_related("event").get(stream__stream_id=path)


class TestTheShape:
    def test_it_publishes_the_stored_pair_not_decoded_bytes(self):
        entry = _one_entry()
        view = event_view(entry)
        # JSON cannot carry bytes; the consumer inverts the pair itself.
        assert view["data"] == {"n": 1}
        assert "payload_encoding" not in view  # omitted when None, as before

    def test_a_non_json_body_carries_its_encoding(self):
        from rakaia import AppendOptions

        entry = _one_entry(
            "bin",
            b"\xff\xfe\x00binary",
            options=AppendOptions(content_type="application/octet-stream"),
        )
        view = event_view(entry)
        assert view["payload_encoding"] == "base64"
        assert view["data"] == "//4AYmluYXJ5"

    def test_a_labelless_append_reports_no_label_not_the_sentinel(self):
        view = event_view(_one_entry())
        assert view["event_type"] == ""

    def test_a_real_label_survives(self):
        from rakaia import AppendOptions

        entry = _one_entry(options=AppendOptions(label="update"))
        assert event_view(entry)["event_type"] == "update"

    def test_identity_keys_are_opt_in(self):
        entry = _one_entry()
        assert set(event_view(entry)) == {"event_type", "created_at", "data"}
        assert "id" in event_view(entry, event_id=True)
        assert "offset" in event_view(entry, offset=True)
        assert "stream_id" in event_view(entry, stream_id=True)


class TestItAgreesWithTheCanonicalReader:
    """The property the whole exercise is for: one event, one description.

    `read()` and a JSON surface differ in *how* they carry the payload — bytes
    versus the stored pair — and in nothing else. Where they name the same fact
    they must name it identically.
    """

    def test_the_timestamp_is_the_one_read_reports(self):
        # The live divergence this closes. `frame_event` published the *event's*
        # `created_at`; `message_of` uses the *entry's* for
        # `StreamMessage.timestamp`, and that is the correct one — a message is
        # per-stream, so a fan-out has one transport time per stream that
        # received it.
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"n": 1}')
        entry = StreamEntry.objects.select_related("event").get()

        from datetime import datetime

        view_ts = datetime.fromisoformat(event_view(entry)["created_at"]).timestamp()
        assert view_ts == store.read("s")[0][0].timestamp

    def test_the_label_is_the_one_read_reports(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"n": 1}')
        entry = StreamEntry.objects.select_related("event").get()

        assert event_view(entry)["event_type"] == store.read("s")[0][0].label

    def test_the_payload_round_trips_to_what_read_returns(self):
        from django_rakaia.event_message import decode_payload
        from rakaia import AppendOptions

        store = DjangoStreamStore()
        store.create("bin")
        store.append(
            "bin",
            b"\xff\xfe\x00binary",
            AppendOptions(content_type="application/octet-stream"),
        )
        entry = StreamEntry.objects.select_related("event").get()

        view = event_view(entry)
        assert decode_payload(view["data"], view.get("payload_encoding")) == (
            store.read("bin")[0][0].data
        )

    def test_a_fan_out_reports_a_timestamp_per_stream(self):
        # Why the entry's column cannot be collapsed into the event's: one event,
        # two entries, two transport times — and each JSON view must report its
        # own, matching that stream's `read()`.
        from django_rakaia.django_store import write_enveloped_event

        a = Stream.objects.create(stream_id="a")
        b = Stream.objects.create(stream_id="b")
        _event, entries = write_enveloped_event([a, b], {"n": 1})

        views = [event_view(e) for e in entries]
        assert views[0]["created_at"] != views[1]["created_at"]

        store = DjangoStreamStore()
        from datetime import datetime

        for path, view in zip(("a", "b"), views, strict=True):
            assert (
                datetime.fromisoformat(view["created_at"]).timestamp()
                == store.read(path)[0][0].timestamp
            )


class TestTheThreeSurfacesNowAgree:
    """At the surfaces, not at the helper.

    Testing `event_view` proves the assembly is right; it does not prove the
    three callers use it. The divergence this issue is about was invisible to the
    suite precisely because nothing asserted what a *frame* or an *API response*
    reports — reverting `frame_event` to the event's timestamp left all 637
    Django tests green. These attach at the surfaces instead.
    """

    def _stream_with_one_event(self, path: str = "s"):
        store = DjangoStreamStore()
        store.create(path)
        store.append(path, b'{"n": 1}')
        return store

    def test_the_channel_frame_reports_what_read_reports(self):
        from django_rakaia.channels_signals import frame_event

        store = self._stream_with_one_event()
        entry = StreamEntry.objects.select_related("event").get()

        from datetime import datetime

        frame = frame_event(entry)
        assert (
            datetime.fromisoformat(frame["created_at"]).timestamp()
            == store.read("s")[0][0].timestamp
        )
        assert frame["event_type"] == store.read("s")[0][0].label

    def test_the_frame_and_the_stream_api_agree_on_one_event(self):
        # The two surfaces a consumer is most likely to compare: a live push and
        # a poll of the same stream. They disagreed on `created_at` before this.
        import json

        from django.contrib.auth import get_user_model
        from django.test import Client
        from django.urls import reverse

        from django_rakaia.channels_signals import frame_event

        user = get_user_model().objects.create_user(username="d", password="pw")
        client = Client()
        client.force_login(user)
        # The test app emits a stream event on every `auth.User` save, including
        # the `last_login` update `force_login` performs, so clear those first.
        StreamEntry.objects.all().delete()
        Stream.objects.all().delete()

        self._stream_with_one_event()
        entry = StreamEntry.objects.select_related("event").get()
        frame = frame_event(entry)

        response = client.get(reverse("django_rakaia:stream_events_api", args=["s"]))
        assert response.status_code == 200, response.status_code
        api_event = json.loads(response.content)["events"][0]

        assert api_event["created_at"] == frame["created_at"]
        assert api_event["event_type"] == frame["event_type"]
        assert api_event["data"] == frame["data"]
        # Same identity too: both name the event, and the same offset.
        assert api_event["id"] == frame["id"]
        assert api_event["offset"] == frame["offset"]

    def test_the_dashboard_listing_agrees_too(self):
        # `recent_events` feeds a template rather than a JSON response, so it is
        # asserted through the response context. It was the one caller whose
        # `created_at` no mutation could reach — the cross-surface test above
        # covers the per-stream API, not the index.
        from django.contrib.auth import get_user_model
        from django.test import Client
        from django.urls import reverse

        from django_rakaia.channels_signals import frame_event

        user = get_user_model().objects.create_user(username="d2", password="pw")
        client = Client()
        client.force_login(user)
        StreamEntry.objects.all().delete()
        Stream.objects.all().delete()

        self._stream_with_one_event()
        entry = StreamEntry.objects.select_related("event").get()
        frame = frame_event(entry)

        response = client.get(reverse("django_rakaia:streams_index"))
        assert response.status_code == 200
        listed = response.context["recent_events"][0]

        assert listed["created_at"] == frame["created_at"]
        assert listed["event_type"] == frame["event_type"]
        assert listed["data"] == frame["data"]
        # This surface names the stream rather than the event — the one shape
        # difference between the three, and deliberate.
        assert listed["stream_id"] == "s"
        assert "id" not in listed
