import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime

from .db import clickhouse, connect_pg, select_clickhouse


log = logging.getLogger(__name__)

DAILY_SQL = """
SELECT
    movie_id,
    grouping(movie_id) AS daily_total,
    count() AS event_count,
    uniqExact(user_id) AS dau,
    countIf(event_type = 'VIEW_STARTED') AS starts,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(starts, 0) AS conversion,
    avgOrNullIf(progress_seconds, event_type = 'VIEW_FINISHED') AS avg_watch_seconds
FROM cinema.events FINAL
WHERE toDate(timestamp) = {day:Date}
GROUP BY GROUPING SETS ((), (movie_id))
HAVING daily_total = 1 OR starts > 0
"""

RETENTION_SQL = """
SELECT
    dateDiff('day', {day:Date}, toDate(timestamp)) AS age,
    uniqExact(user_id) AS users
FROM cinema.events
WHERE toDate(timestamp) BETWEEN {day:Date} AND least(addDays({day:Date}, 7), {today:Date})
  AND event_type IN ('VIEW_STARTED', 'VIEW_PAUSED', 'VIEW_RESUMED', 'VIEW_FINISHED')
  AND user_id IN (
      SELECT user_id
      FROM cinema.first_views
      WHERE user_id IN (
          SELECT user_id FROM cinema.events
          WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
      )
      GROUP BY user_id
      HAVING min(first_day) = {day:Date}
  )
GROUP BY age
"""


@dataclass(frozen=True)
class Metric:
    name: str
    dimension: str
    value: float | None


def compute(day, today):
    total = {"event_count": 0, "dau": 0, "avg_watch_seconds": None, "conversion": None}
    metrics = []
    for row in select_clickhouse(DAILY_SQL, day=day):
        if row["daily_total"]:
            total = row
        else:
            metrics.append(Metric("movie_views", row["movie_id"], row["starts"]))
    for name in ("dau", "avg_watch_seconds", "conversion"):
        metrics.append(Metric(name, "", total[name]))

    visits = {}
    if total.get("starts", 0) and day <= today:
        visits = {row["age"]: row["users"] for row in select_clickhouse(RETENTION_SQL, day=day, today=today)}
    cohort_size = visits.get(0, 0)
    elapsed_days = (today - day).days
    for age in range(8):
        value = None
        if cohort_size and age <= elapsed_days:
            value = visits.get(age, 0) / cohort_size
        metrics.append(Metric("retention", str(age), value))
    return total["event_count"], metrics


def replace_clickhouse(day, metrics, computed_at):
    day_text = day.isoformat()
    ch_time = computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows = (
        {"day": day_text, "metric": m.name, "dimension": m.dimension, "value": m.value, "computed_at": ch_time}
        for m in metrics
    )
    data = "\n".join(json.dumps(row, ensure_ascii=False, allow_nan=False) for row in rows).encode("utf-8")
    params = {"day": day}
    drop = "ALTER TABLE cinema.daily_metrics_stage DROP PARTITION {day:Date}"
    clickhouse(drop, params=params)
    clickhouse("INSERT INTO cinema.daily_metrics_stage FORMAT JSONEachRow", data=data)
    clickhouse(
        "ALTER TABLE cinema.daily_metrics REPLACE PARTITION {day:Date} FROM cinema.daily_metrics_stage",
        params=params,
    )
    clickhouse(drop, params=params)


def snapshot_matches(day, row_count, computed_at):
    snapshot = select_clickhouse(
        """SELECT count() AS rows, min(toUnixTimestamp64Milli(computed_at)) AS oldest,
                  max(toUnixTimestamp64Milli(computed_at)) AS newest
           FROM cinema.daily_metrics WHERE day = {day:Date}""",
        day=day,
    )[0]
    stamp = round(computed_at.timestamp() * 1000)
    return snapshot["rows"] == row_count and snapshot["oldest"] == snapshot["newest"] == stamp


def calculate_once(day, scheduled):
    with connect_pg() as conn:
        # замок живёт до закрытия соединения, в том числе между транзакциями
        conn.execute("SELECT pg_advisory_lock('public.daily_metrics'::regclass::int, %s)", (day.toordinal(),))
        recovering = conn.execute(
            """INSERT INTO public.pending_recalculations (day) VALUES (%s)
               ON CONFLICT DO NOTHING RETURNING day""", (day,),
        ).fetchone() is None

        now = datetime.now(UTC)
        computed_at = now.replace(microsecond=now.microsecond // 1000 * 1000)
        event_count, metrics = compute(day, now.date())
        with conn.transaction(), conn.cursor() as cursor:
            previous = cursor.execute(
                "SELECT metric, dimension, value, computed_at FROM public.daily_metrics WHERE day = %s", (day,),
            ).fetchall()
            changed = {row[:3] for row in previous} != {(m.name, m.dimension, m.value) for m in metrics}
            previous_at = previous[0][3] if previous else None
            # вчерашний снимок должен хотя бы раз обновиться после полуночи UTC
            needs_closing = previous_at is not None and previous_at.astimezone(UTC).date() <= day < now.date()
            can_reuse = scheduled and not recovering and not changed and not needs_closing
            if can_reuse and snapshot_matches(day, len(metrics), previous_at):
                computed_at = previous_at
            else:
                cursor.execute("DELETE FROM public.daily_metrics WHERE day = %s", (day,))
                cursor.executemany(
                    """INSERT INTO public.daily_metrics (day, metric, dimension, value, computed_at)
                       VALUES (%s, %s, %s, %s, %s)""",
                    [(day, m.name, m.dimension, m.value, computed_at) for m in metrics],
                )
                cursor.execute(
                    """UPDATE public.daily_exports SET revision = revision + 1, exported_at = NULL
                       WHERE day = %s""",
                    (day,),
                )
                replace_clickhouse(day, metrics, computed_at)
            cursor.execute("DELETE FROM public.pending_recalculations WHERE day = %s", (day,))
    return {"day": day, "processed_events": event_count, "metric_rows": len(metrics), "computed_at": computed_at}


def recalculate(day: date, *, scheduled=False):
    started = time.monotonic()
    log.info("recalculation started day=%s", day)
    for attempt in range(1, 4):
        try:
            result = calculate_once(day, scheduled)
        except Exception:
            log.exception("recalculation failed day=%s attempt=%d elapsed=%.2fs", day, attempt, time.monotonic() - started)
            if attempt == 3:
                raise
            time.sleep(2 ** (attempt - 1))
        else:
            log.info(
                "recalculation finished day=%s events=%d metrics=%d elapsed=%.2fs",
                day, result["processed_events"], result["metric_rows"], time.monotonic() - started,
            )
            return result
