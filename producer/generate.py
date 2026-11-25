import argparse
import logging
from collections.abc import Iterator
from datetime import UTC, date, datetime, time, timedelta
from itertools import batched
from random import Random
from uuid import NAMESPACE_URL, uuid5

from producer.events import DeviceType, EventType, MovieEvent
from producer.kafka import Publisher


log = logging.getLogger(__name__)
FILMS = (
    ("arrival", 116), ("the-matrix", 136), ("spirited-away", 125),
    ("interstellar", 169), ("ratatouille", 111), ("moon", 97),
    ("the-grand-budapest-hotel", 100), ("the-truman-show", 103),
)
RETURN_CHANCE = (1.0, .65, .48, .39, .33, .28, .24, .23, .19, .15, .14, .12, .11, .10)


def session_events(user_id: str, day: date, rng: Random) -> Iterator[MovieEvent]:
    session_id = str(uuid5(NAMESPACE_URL, f"cinema/session/{user_id}/{day}"))
    movie, minutes = rng.choices(FILMS, weights=(20, 17, 14, 12, 9, 7, 6, 5))[0]
    device = rng.choice(tuple(DeviceType))
    start = datetime.combine(day, time(8), UTC) + timedelta(seconds=rng.randrange(12 * 3600))

    def event(kind, timestamp, progress=None):
        return MovieEvent(
            event_id=uuid5(NAMESPACE_URL, f"cinema/event/{session_id}/{kind}"),
            user_id=user_id, movie_id=movie, event_type=kind, timestamp=timestamp,
            device_type=device, session_id=session_id, progress_seconds=progress,
        )

    if rng.random() < .45:
        yield event(EventType.SEARCHED, start - timedelta(seconds=rng.randint(8, 100)))
    yield event(EventType.VIEW_STARTED, start, 0)

    finished = rng.random() < .68
    extra_seconds = 0
    if rng.random() < .50:
        pause_at = rng.randint(90, minutes * 45)
        yield event(EventType.VIEW_PAUSED, start + timedelta(seconds=pause_at), pause_at)
        if finished or rng.random() < .40:
            extra_seconds = rng.randint(50, 900)
            resume_at = pause_at + 1
            yield event(EventType.VIEW_RESUMED, start + timedelta(seconds=resume_at + extra_seconds), resume_at)

    if finished:
        duration = minutes * 60
        ended = start + timedelta(seconds=duration + extra_seconds)
        yield event(EventType.VIEW_FINISHED, ended, duration)
        if rng.random() < .37:
            yield event(EventType.LIKED, ended + timedelta(seconds=rng.randint(2, 25)))


def iter_history(end_date: date, days: int, users_per_day: int, seed: int) -> Iterator[MovieEvent]:
    first_day = end_date - timedelta(days=days - 1)
    for index in range(days):
        day = first_day + timedelta(days=index)
        events = []
        for age in range(min(index + 1, len(RETURN_CHANCE))):
            cohort = day - timedelta(days=age)
            for slot in range(1, users_per_day + 1):
                user_id = f"demo-{seed}-{cohort:%Y%m%d}-{slot:02d}"
                rng = Random(f"{user_id}/{day}")
                if rng.random() < RETURN_CHANCE[age]:
                    events.extend(session_events(user_id, day, rng))
        yield from sorted(events, key=lambda e: e.timestamp)


def positive_int(value):
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("нужно целое число от 1")
    return number


def main():
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    parser = argparse.ArgumentParser(description="сгенерировать историю киносеансов")
    parser.add_argument("--days", type=positive_int, default=14)
    parser.add_argument("--users-per-day", type=positive_int, default=12)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--end-date", type=date.fromisoformat, default=yesterday)
    args = parser.parse_args()
    if args.end_date > yesterday:
        parser.error("последняя дата должна быть завершившимся днём по utc")

    logging.basicConfig(level=logging.INFO)
    publisher = Publisher.connect()
    total = 0
    try:
        events = iter_history(args.end_date, args.days, args.users_per_day, args.seed)
        for batch in batched(events, 250):
            publisher.publish_batch(batch)
            total += len(batch)
    finally:
        publisher.close()
    log.info("доставлено %s событий за %s дней, по %s включительно", total, args.days, args.end_date)


if __name__ == "__main__":
    main()
