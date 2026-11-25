import json
import logging
import os
import time
from contextlib import closing
from datetime import UTC, timedelta

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from .db import connect_pg


log = logging.getLogger(__name__)


class MetricsMissing(LookupError):
    pass


def upload_object(bucket, key, data):
    config = Config(
        connect_timeout=3, read_timeout=10,
        retries={"mode": "standard", "total_max_attempts": 2},
        s3={"addressing_style": "path"},
    )
    client = boto3.session.Session().client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://s3:8333"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "cinema_admin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "cinema_storage_local"),
        region_name="us-east-1",
        config=config,
    )
    with closing(client) as s3:
        upload = {"Bucket": bucket, "Key": key, "Body": data, "ContentType": "application/json; charset=utf-8"}
        try:
            s3.put_object(**upload)
            return
        except ClientError as error:
            if error.response["Error"]["Code"] != "NoSuchBucket":
                raise
        try:
            s3.create_bucket(Bucket=bucket)
        except ClientError as error:
            if error.response["Error"]["Code"] == "BucketAlreadyExists":
                s3.head_bucket(Bucket=bucket)
            elif error.response["Error"]["Code"] != "BucketAlreadyOwnedByYou":
                raise
        s3.put_object(**upload)


def export_day(day, *, manual=False):
    started = time.monotonic()
    log.info("export started day=%s", day)
    try:
        with connect_pg() as conn:
            if manual:
                if not conn.execute("SELECT 1 FROM public.daily_metrics WHERE day = %s LIMIT 1", (day,)).fetchone():
                    raise MetricsMissing(day)
                conn.execute(
                    """INSERT INTO public.daily_exports (day) VALUES (%s)
                       ON CONFLICT (day) DO UPDATE SET revision = daily_exports.revision + 1, exported_at = NULL""",
                    (day,),
                )
            conn.execute("SELECT pg_advisory_lock('public.daily_exports'::regclass::int, %s)", (day.toordinal(),))
            with conn.transaction():
                conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                job = conn.execute(
                    "SELECT revision, exported_at FROM public.daily_exports WHERE day = %s", (day,),
                ).fetchone()
                if not job or (not manual and job[1] is not None):
                    return None
                rows = conn.execute(
                    """SELECT metric, dimension, value, computed_at FROM public.daily_metrics
                       WHERE day = %s ORDER BY metric, dimension""",
                    (day,),
                ).fetchall()
                if not rows:
                    raise MetricsMissing(day)
            computed_at = rows[0][3].astimezone(UTC).isoformat(timespec="milliseconds")
            metrics = [{"metric": name, "dimension": dim, "value": val} for name, dim, val, _ in rows]
            payload = {"day": day.isoformat(), "computed_at": computed_at, "metrics": metrics}
            data = json.dumps(payload, ensure_ascii=False, allow_nan=False, indent=2).encode("utf-8")
            bucket = os.getenv("S3_BUCKET", "movie-analytics")
            key = f"daily/{day.isoformat()}/aggregates.json"
            upload_object(bucket, key, data)
            conn.execute(
                "UPDATE public.daily_exports SET exported_at = clock_timestamp() WHERE day = %s AND revision = %s",
                (day, job[0]),
            )
    except MetricsMissing:
        raise
    except Exception:
        log.exception("export failed day=%s elapsed=%.2fs", day, time.monotonic() - started)
        raise
    log.info("export finished day=%s rows=%d bytes=%d elapsed=%.2fs", day, len(rows), len(data), time.monotonic() - started)
    return {"day": day, "bucket": bucket, "key": key, "metric_rows": len(rows)}


def plan_cycle(today):
    yesterday = today - timedelta(days=1)
    with connect_pg() as conn, conn.transaction():
        # после простоя добавляем и те дни, которые успели пропустить
        conn.execute(
            """WITH next_day AS (
                   SELECT coalesce(max(day) + 1, %s::date) AS first_day
                   FROM public.daily_exports WHERE scheduled
               )
               INSERT INTO public.daily_exports (day, scheduled)
               SELECT dates.day::date, true
               FROM next_day, LATERAL generate_series(
                   next_day.first_day::timestamp, %s::date::timestamp, interval '1 day'
               ) AS dates(day)
               ON CONFLICT (day) DO UPDATE SET scheduled = true, revision = daily_exports.revision + 1,
                                               exported_at = NULL""",
            (yesterday, yesterday),
        )
        outstanding = conn.execute(
            """SELECT day FROM public.pending_recalculations
               UNION
               SELECT job.day FROM public.daily_exports AS job
               WHERE job.scheduled AND job.exported_at IS NULL AND NOT EXISTS (
                   SELECT 1 FROM public.daily_metrics AS m WHERE m.day = job.day
                     AND m.computed_at >= ((job.day + 1)::timestamp AT TIME ZONE 'UTC')
               )"""
        ).fetchall()
    recent = {today - timedelta(days=i) for i in range(15)}
    return sorted(recent.union(day for day, in outstanding), reverse=True)


def export_pending(stop):
    started = time.monotonic()
    try:
        with connect_pg() as conn:
            jobs = conn.execute(
                """SELECT job.day FROM public.daily_exports AS job
                   WHERE job.exported_at IS NULL AND EXISTS (
                       SELECT 1 FROM public.daily_metrics AS m WHERE m.day = job.day
                         AND (NOT job.scheduled OR m.computed_at >= ((job.day + 1)::timestamp AT TIME ZONE 'UTC'))
                   )
                   ORDER BY job.day"""
            ).fetchall()
    except Exception:
        log.exception("could not read the export queue")
        return
    completed = failures = 0
    for day, in jobs:
        if stop.is_set():
            break
        try:
            if export_day(day):
                completed += 1
        except Exception:
            # задача останется в postgres до успешной выгрузки
            failures += 1
    if jobs:
        log.info("export cycle finished files=%d failed=%d elapsed=%.2fs", completed, failures, time.monotonic() - started)
