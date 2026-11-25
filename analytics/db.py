import json
import os
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import psycopg


def connect_pg():
    dsn = os.getenv("POSTGRES_DSN", "postgresql://cinema:cinema_local@postgres:5432/cinema")
    return psycopg.connect(dsn, connect_timeout=5, autocommit=True)


def clickhouse(sql, *, params=None, data=b""):
    settings = {"wait_end_of_query": 1, "output_format_json_quote_64bit_integers": 0}
    settings.update({f"param_{key}": str(value) for key, value in (params or {}).items()})
    base = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123").rstrip("/")
    request = Request(
        f"{base}/?{urlencode(settings)}",
        data=sql.encode("utf-8") + b"\n" + data,
        headers={
            "X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "cinema"),
            "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "cinema_local"),
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=60) as response:
            return response.read()
    except HTTPError as error:
        detail = error.read(3000).decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"ClickHouse HTTP {error.code}: {detail}") from error


def select_clickhouse(sql, **params):
    result = clickhouse(f"{sql}\nFORMAT JSONEachRow", params=params)
    return [json.loads(line) for line in result.splitlines()]
