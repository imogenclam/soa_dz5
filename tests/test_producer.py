import json
import unittest
from collections import defaultdict, deque
from datetime import UTC, date, datetime, timedelta
from io import BytesIO
from pathlib import Path
from uuid import UUID

from confluent_kafka import KafkaError, KafkaException
from fastavro import schemaless_reader
from pydantic import ValidationError

from producer.events import MovieEvent, encode_event
from producer.generate import iter_history
from producer.kafka import Publisher


EXAMPLE = {
    "event_id": "072a9a46-e5f2-4e50-8d17-6a2b17573421",
    "user_id": "alice",
    "movie_id": "arrival",
    "event_type": "VIEW_PAUSED",
    "timestamp": "2026-04-03T18:05:12.193+03:00",
    "device_type": "TV",
    "session_id": "evening",
    "progress_seconds": 912,
}


class DeliveryQueue:
    def __init__(self, error=None):
        self.error = error
        self.pending = deque()
        self.delivered = []

    def produce(self, topic, *, key, value, on_delivery):
        self.pending.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        topic, key, value, callback = self.pending.popleft()
        if self.error is None:
            self.delivered.append((topic, key, value))
        callback(self.error, None)


class ProducerTest(unittest.TestCase):
    def test_confluent_record_decodes_using_the_published_schema(self):
        event = MovieEvent.model_validate(EXAMPLE)
        packet = encode_event(event, 528)
        self.assertEqual(packet[:5], b"\x00\x00\x00\x02\x10")
        schema_file = Path(__file__).resolve().parents[1] / "schemas" / "movie-event.avsc"
        schema = json.loads(schema_file.read_text(encoding="utf-8"))
        record = schemaless_reader(BytesIO(packet[5:]), schema)
        self.assertEqual(
            record,
            EXAMPLE | {
                "event_id": UUID(EXAMPLE["event_id"]),
                "timestamp": datetime(2026, 4, 3, 15, 5, 12, 193000, UTC),
            },
        )
        self.assertEqual(event.timestamp.utcoffset(), timedelta(0))

    def test_view_progress_is_required_nonnegative_and_fits_avro_int(self):
        for kind in ("VIEW_STARTED", "VIEW_PAUSED", "VIEW_RESUMED", "VIEW_FINISHED"):
            for value in (None, -1, 2**31, True, 1.5):
                with self.subTest(kind=kind, value=value), self.assertRaises(ValidationError):
                    MovieEvent.model_validate(EXAMPLE | {"event_type": kind, "progress_seconds": value})
        for kind in ("LIKED", "SEARCHED"):
            data = EXAMPLE | {"event_type": kind}
            del data["progress_seconds"]
            self.assertIsNone(MovieEvent.model_validate(data).progress_seconds)

    def test_rejects_invalid_identifiers_types_and_timezone(self):
        for field, value in (
            ("event_id", "broken"), ("user_id", " "), ("session_id", ""),
            ("event_type", "STOP"), ("device_type", "PHONE"),
            ("timestamp", "2026-04-03T18:05:12"), ("timestamp", 1760000000),
            ("timestamp", "0001-01-01T00:00:00+12:00"),
            ("timestamp", "9999-01-01T00:00:00Z"), ("timestamp", "1969-12-31T23:59:59Z"),
        ):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                MovieEvent.model_validate(EXAMPLE | {field: value})

    def test_publish_waits_for_delivery_and_partitions_by_user(self):
        broker = DeliveryQueue()
        event = MovieEvent.model_validate(EXAMPLE)
        Publisher(broker, 19).publish_batch([event])
        self.assertEqual(broker.delivered, [("movie-events", b"alice", encode_event(event, 19))])

    def test_delivery_failure_reaches_the_caller(self):
        broker = DeliveryQueue(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE))
        with self.assertLogs("producer.kafka", level="ERROR"), self.assertRaises(KafkaException):
            Publisher(broker, 19).publish_batch([MovieEvent.model_validate(EXAMPLE)])

    def test_history_is_repeatable_and_sessions_form_real_watch_sequences(self):
        end = date(2026, 5, 19)
        events = list(iter_history(end, days=14, users_per_day=12, seed=1))
        ids = [event.event_id for event in events]
        self.assertEqual(ids, [e.event_id for e in iter_history(end, days=14, users_per_day=12, seed=1)])
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual({e.timestamp.date() for e in events}, {end - timedelta(days=i) for i in range(14)})
        self.assertEqual([e.timestamp for e in events], sorted(e.timestamp for e in events))

        sessions = defaultdict(list)
        visits = defaultdict(set)
        for event in events:
            sessions[event.session_id].append(event)
            visits[event.user_id].add(event.timestamp.date())
        finishes = 0
        for session in sessions.values():
            watching = [e for e in session if e.event_type.startswith("VIEW_")]
            kinds = [e.event_type for e in watching]
            self.assertEqual(kinds[0], "VIEW_STARTED")
            if "VIEW_RESUMED" in kinds:
                self.assertLess(kinds.index("VIEW_PAUSED"), kinds.index("VIEW_RESUMED"))
            if "VIEW_FINISHED" in kinds:
                self.assertEqual(kinds[-1], "VIEW_FINISHED")
                finishes += 1
            progress = [e.progress_seconds for e in watching]
            self.assertTrue(all(a < b for a, b in zip(progress, progress[1:])))
        self.assertGreater(finishes, 0)
        self.assertLess(finishes, len(sessions))
        self.assertTrue({"LIKED", "SEARCHED"}.issubset({e.event_type for e in events}))
        for age in (1, 7):
            mature = [days for days in visits.values() if min(days) + timedelta(days=age) <= end]
            returned = sum(min(days) + timedelta(days=age) in days for days in mature)
            self.assertGreater(returned, 0)
            self.assertLess(returned, len(mature))
