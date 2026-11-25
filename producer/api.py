import logging
from contextlib import asynccontextmanager
from uuid import UUID

from confluent_kafka import KafkaException
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from producer.events import MovieEvent
from producer.kafka import Publisher


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO)
    app.state.publisher = Publisher.connect()
    try:
        yield
    finally:
        app.state.publisher.close()


app = FastAPI(title="Movie Service", lifespan=lifespan)


class Receipt(BaseModel):
    event_id: UUID


@app.post("/events", status_code=201, response_model=Receipt, summary="Отправить событие в Kafka")
def create_event(event: MovieEvent, request: Request):
    """Возвращает event_id, когда Kafka подтвердит запись."""
    try:
        request.app.state.publisher.publish_batch([event])
    except (KafkaException, TimeoutError) as error:
        raise HTTPException(status_code=503, detail="подтверждение kafka не получено") from error
    return Receipt(event_id=event.event_id)


@app.get("/health", summary="Проверить готовность топика к записи")
def health(request: Request):
    try:
        ready = request.app.state.publisher.ready()
    except KafkaException:
        ready = False
    if not ready:
        raise HTTPException(status_code=503, detail="топик пока недоступен")
    return {"status": "ok"}
