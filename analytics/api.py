import asyncio
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from typing import Annotated

from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel

from .aggregate import recalculate
from .db import clickhouse, connect_pg
from .export import MetricsMissing, export_day, export_pending, plan_cycle


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)
Day = Annotated[date, Path(ge=date(1970, 1, 1), le=date(2149, 6, 6), description="Дата по UTC в формате ГГГГ-ММ-ДД")]


def scheduled_cycles(stop, interval):
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="s3-export") as exporter:
        export_task = None
        while not stop.is_set():
            started = time.monotonic()
            total_events = total_metrics = finished_days = failed_days = 0
            log.info("aggregation cycle started")
            try:
                for day in plan_cycle(datetime.now(UTC).date()):
                    if stop.is_set():
                        break
                    try:
                        result = recalculate(day, scheduled=True)
                    except Exception:
                        failed_days += 1
                        continue
                    total_events += result["processed_events"]
                    total_metrics += result["metric_rows"]
                    finished_days += 1
            except Exception:
                log.exception("could not plan aggregation cycle")
            log.info(
                "aggregation cycle finished days=%d failed=%d events=%d metrics=%d elapsed=%.2fs",
                finished_days, failed_days, total_events, total_metrics, time.monotonic() - started,
            )
            if not stop.is_set() and (export_task is None or export_task.done()):
                export_task = exporter.submit(export_pending, stop)
            stop.wait(interval)


@asynccontextmanager
async def lifespan(_app):
    interval = int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "300"))
    if interval <= 0:
        raise ValueError("AGGREGATION_INTERVAL_SECONDS must be positive")
    stop = threading.Event()
    worker = threading.Thread(target=scheduled_cycles, args=(stop, interval), daemon=True, name="scheduler")
    worker.start()
    try:
        yield
    finally:
        stop.set()
        await asyncio.to_thread(worker.join, 5)


app = FastAPI(title="Аналитика онлайн-кинотеатра", version="1.0", lifespan=lifespan)


class Recalculation(BaseModel):
    day: date
    processed_events: int
    metric_rows: int
    computed_at: datetime


class Export(BaseModel):
    day: date
    bucket: str
    key: str
    metric_rows: int


@app.get("/health", summary="Проверить подключение к ClickHouse и PostgreSQL")
def health():
    try:
        clickhouse("SELECT 1 FROM cinema.daily_metrics LIMIT 0")
        with connect_pg() as conn:
            conn.execute("SELECT 1 FROM public.daily_metrics LIMIT 0")
    except Exception as error:
        log.warning("storage health check failed: %s", error)
        raise HTTPException(503, "Нет подключения к хранилищу агрегатов") from error
    return {"status": "ok"}


@app.post("/recalculate/{day}", response_model=Recalculation, summary="Пересчитать метрики за день")
def recalculate_date(day: Day):
    """Рассчитывает метрики по событиям из ClickHouse и сохраняет результат в ClickHouse и PostgreSQL."""
    try:
        return recalculate(day)
    except Exception as error:
        raise HTTPException(503, "Не удалось пересчитать метрики") from error


@app.post("/export/{day}", response_model=Export, summary="Выгрузить готовые метрики в S3")
def export_date(day: Day):
    """Выгружает в S3 метрики из PostgreSQL. Если повторить запрос за ту же дату, сервис обновит существующий файл."""
    try:
        return export_day(day, manual=True)
    except MetricsMissing as error:
        raise HTTPException(404, "За эту дату ещё нет рассчитанных метрик") from error
    except Exception as error:
        raise HTTPException(503, "Не удалось выгрузить метрики") from error
