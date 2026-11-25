import json
import os
import time
import unittest
from contextlib import closing
from datetime import UTC, date, datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import NAMESPACE_URL, uuid4, uuid5

import boto3
import psycopg
from botocore.config import Config
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from analytics.export import export_day, upload_object


def api(url, method="GET", payload=None):
    body = None if payload is None else json.dumps(payload).encode()
    request = Request(url, body, {"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def ch(sql):
    request = Request(os.environ["CLICKHOUSE_URL"], (sql + " FORMAT JSONEachRow").encode(), {
        "X-ClickHouse-User": os.environ["CLICKHOUSE_USER"],
        "X-ClickHouse-Key": os.environ["CLICKHOUSE_PASSWORD"],
    })
    with urlopen(request, timeout=15) as response:
        return [json.loads(line) for line in response]


def open_s3():
    return closing(boto3.client(
        "s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1",
        aws_access_key_id=os.environ["S3_ACCESS_KEY"], aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    ))


def read_archive(storage, location):
    obj = storage.get_object(Bucket=location["bucket"], Key=location["key"])
    with obj["Body"] as stream:
        return json.load(stream)


def fixture_day():
    # берём отдельную неделю, чтобы тест не зависел от фильмов и зрителей в демо
    for offset in range(0, 3650, 10):
        day = date(2001, 1, 10) + timedelta(days=offset)
        count = ch(f"""SELECT count() AS n FROM cinema.events
            WHERE toDate(timestamp) BETWEEN toDate('{day}') - 1 AND toDate('{day}') + 7
              AND NOT startsWith(user_id, 'test-{day}-')""")[0]["n"]
        if int(count) == 0:
            return day
    raise AssertionError("не нашлось свободной недели для теста")


def event(day, age, user, movie, action, minute, progress=None):
    stamp = datetime.combine(day + timedelta(days=age), datetime.min.time(), UTC) + timedelta(hours=12, minutes=minute)
    prefix = f"test-{day}"
    return {
        "event_id": str(uuid5(NAMESPACE_URL, f"{prefix}/{age}/{user}/{action}/{minute}")),
        "user_id": f"{prefix}-{user}", "movie_id": movie, "event_type": action,
        "timestamp": stamp.isoformat(), "device_type": "TV",
        "session_id": f"{prefix}-{user}-{age}", "progress_seconds": progress,
    }


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.day = fixture_day()
        cases = [
            (-1, "a", "luna", "SEARCHED", 0, None),
            (0, "a", "luna", "VIEW_STARTED", 0, 0),
            (0, "a", "luna", "VIEW_PAUSED", 1, 30),
            (0, "a", "luna", "VIEW_RESUMED", 2, 31),
            (0, "a", "luna", "VIEW_FINISHED", 3, 120),
            (0, "b", "luna", "VIEW_STARTED", 0, 0),
            (0, "c", "mars", "VIEW_STARTED", 0, 0),
            (0, "c", "mars", "VIEW_FINISHED", 1, 60),
            (0, "d", "mars", "SEARCHED", 0, None),
            (1, "a", "mars", "VIEW_STARTED", 0, 0),
            (1, "c", "mars", "VIEW_STARTED", 0, 0),
            (1, "b", "luna", "LIKED", 1, None),
            (6, "c", "mars", "VIEW_STARTED", 0, 0),
            (7, "b", "mars", "VIEW_STARTED", 0, 0),
        ]
        cls.events = [event(cls.day, *case) for case in cases]
        # по свежему поиску видно, что события дошли именно в этом прогоне
        cls.events[0]["event_id"] = str(uuid4())
        cls.url = "http://producer:8000/events"
        cls.recalculate = f"http://analytics:8000/recalculate/{cls.day}"
        cls.export = f"http://analytics:8000/export/{cls.day}"
        for item in [*cls.events, cls.events[4]]:
            receipt = api(cls.url, "POST", item)
            if receipt["event_id"] != item["event_id"]:
                raise AssertionError(f"неожиданный ответ продюсера: {receipt}")

        event_ids = ", ".join(f"'{item['event_id']}'" for item in cls.events)
        deadline = time.monotonic() + 75
        while True:
            cls.raw = ch(f"""SELECT event_id, user_id, movie_id, event_type,
                toUnixTimestamp64Milli(timestamp) AS milliseconds, device_type, session_id, progress_seconds
                FROM cinema.events FINAL WHERE toDate(timestamp) BETWEEN toDate('{cls.day}') - 1
                AND toDate('{cls.day}') + 7 AND event_id IN ({event_ids})""")
            if len(cls.raw) == len(cls.events):
                break
            if time.monotonic() >= deadline:
                raise AssertionError(f"ClickHouse получил {len(cls.raw)} из {len(cls.events)} событий")
            time.sleep(0.5)
        cls.calculation = api(cls.recalculate, "POST")

    def metrics(self):
        with psycopg.connect(os.environ["POSTGRES_DSN"]) as db:
            rows = db.execute(
                "SELECT metric, dimension, value FROM daily_metrics WHERE day = %s", (self.day,)
            ).fetchall()
        return {(name, dimension): value for name, dimension, value in rows}

    def test_all_event_fields_survive_kafka(self):
        actual = {row["event_id"]: row for row in self.raw}
        for original in self.events:
            expected = dict(original)
            stamp = datetime.fromisoformat(expected.pop("timestamp"))
            row = dict(actual[original["event_id"]])
            self.assertEqual(int(row.pop("milliseconds")), int(stamp.timestamp() * 1000))
            self.assertEqual(row, expected)

    def test_invalid_event_is_rejected(self):
        for change in ({"event_type": "BROKEN"}, {"progress_seconds": -1}, {"progress_seconds": None}):
            with self.subTest(change=change), self.assertRaises(HTTPError) as error:
                api(self.url, "POST", {**self.events[1], **change})
            self.assertEqual(error.exception.code, 422)

    def test_registry_and_kafka_replication(self):
        schema = api("http://schema-registry:8081/subjects/movie-events-value/versions/latest")
        self.assertGreaterEqual(schema["version"], 1)
        self.assertEqual({f["name"] for f in json.loads(schema["schema"])["fields"]}, set(self.events[0]))
        admin = AdminClient({"bootstrap.servers": "kafka1:9092,kafka2:9092,kafka3:9092"})
        topic = admin.list_topics("movie-events", timeout=10).topics["movie-events"]
        self.assertGreaterEqual(len(topic.partitions), 3)
        for part in topic.partitions.values():
            self.assertEqual(len(set(part.replicas)), 3)
        resource = ConfigResource(ResourceType.TOPIC, "movie-events")
        config = admin.describe_configs([resource])[resource].result(timeout=10)
        self.assertEqual(config["min.insync.replicas"].value, "2")

    def test_daily_metrics_and_repeat_calculation(self):
        before = self.metrics()
        self.assertEqual(self.calculation["processed_events"], 8)
        self.assertEqual(before["dau", ""], 4)
        self.assertEqual(before["avg_watch_seconds", ""], 90)
        self.assertAlmostEqual(before["conversion", ""], 2 / 3)
        self.assertEqual(before["movie_views", "luna"], 2)
        self.assertEqual(before["movie_views", "mars"], 1)
        for age, expected in enumerate([1, 2/3, 0, 0, 0, 0, 1/3, 1/3]):
            self.assertAlmostEqual(before["retention", str(age)], expected)
        second = api(self.recalculate, "POST")
        self.assertEqual(second["metric_rows"], len(before))
        self.assertEqual(self.metrics(), before)
        rows = ch(f"SELECT metric, dimension, value FROM cinema.daily_metrics WHERE day = '{self.day}'")
        self.assertEqual(len(rows), len(before))
        self.assertEqual({(r["metric"], r["dimension"]): r["value"] for r in rows}, before)

    def test_s3_object_matches_postgres_and_is_overwritten(self):
        first = api(self.export, "POST")
        self.assertEqual(first, api(self.export, "POST"))
        self.assertEqual(first["key"], f"daily/{self.day}/aggregates.json")
        with open_s3() as storage:
            keys = storage.list_objects_v2(Bucket=first["bucket"], Prefix=f"daily/{self.day}/")["Contents"]
            self.assertEqual([item["Key"] for item in keys], [first["key"]])
            document = read_archive(storage, first)
        self.assertEqual(document["day"], str(self.day))
        self.assertIsNotNone(datetime.fromisoformat(document["computed_at"]).tzinfo)
        self.assertEqual({(r["metric"], r["dimension"]): r["value"] for r in document["metrics"]}, self.metrics())

    def test_recalculation_refreshes_the_archived_snapshot(self):
        location = api(self.export, "POST")
        latest = api(self.recalculate, "POST")
        export_day(self.day)
        with open_s3() as storage:
            archived = read_archive(storage, location)
        self.assertEqual(datetime.fromisoformat(archived["computed_at"]), datetime.fromisoformat(latest["computed_at"]))

    def test_first_upload_creates_the_bucket(self):
        bucket = "cinema-test-" + uuid4().hex
        key = "daily/example.json"
        upload_object(bucket, key, b'{"views": 2}')
        with open_s3() as storage:
            try:
                self.assertEqual(read_archive(storage, {"bucket": bucket, "key": key}), {"views": 2})
            finally:
                storage.delete_object(Bucket=bucket, Key=key)
                storage.delete_bucket(Bucket=bucket)

    def test_grafana_runs_every_panel_query(self):
        root = "http://grafana:3000"
        dashboard = api(f"{root}/api/dashboards/uid/cinema-analytics")["dashboard"]
        panels = [panel for panel in dashboard["panels"] if panel.get("targets")]
        self.assertGreaterEqual(len(panels), 3)
        start = datetime.combine(self.day, datetime.min.time(), UTC)
        cohort = None
        for panel in panels:
            request = {
                "from": str(int(start.timestamp() * 1000)),
                "to": str(int((start + timedelta(days=1)).timestamp() * 1000) - 1),
                "queries": panel["targets"],
            }
            with self.subTest(panel=panel["title"]):
                result = api(f"{root}/api/ds/query", "POST", request)["results"]["A"]
                self.assertNotIn("error", result)
                self.assertTrue(result["frames"], result)
                frame = result["frames"][0]
                self.assertTrue(frame["data"]["values"][0], panel["title"])
                fields = [field["name"] for field in frame["schema"]["fields"]]
                if "День 7" in fields:
                    cohort = dict(zip(fields, frame["data"]["values"]))
        self.assertIsNotNone(cohort, "не нашлась панель когорт D0–D7")
        for age, expected in enumerate([1, 2/3, 0, 0, 0, 0, 1/3, 1/3]):
            self.assertAlmostEqual(cohort[f"День {age}"][0], expected)
