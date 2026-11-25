import json
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from producer.events import SCHEMA_TEXT


SUBJECT = "movie-events-value"
SCHEMA_BODY = {"schemaType": "AVRO", "schema": SCHEMA_TEXT}


def registry_request(method, path, body):
    base_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    media_type = "application/vnd.schemaregistry.v1+json"
    request = Request(
        base_url.rstrip("/") + path,
        method=method,
        data=json.dumps(body).encode(),
        headers={"Content-Type": media_type, "Accept": media_type},
    )
    try:
        with urlopen(request, timeout=5) as response:
            return json.load(response)
    except HTTPError as error:
        if 400 <= error.code < 500 and error.code not in (408, 429):
            detail = error.read().decode(errors="replace")
            raise ValueError(f"schema registry вернул {error.code}: {detail}") from error
        raise


def registered_schema():
    return registry_request("POST", f"/subjects/{SUBJECT}?normalize=true", SCHEMA_BODY)


def register_schema():
    registry_request("PUT", f"/config/{SUBJECT}", {"compatibility": "BACKWARD_TRANSITIVE"})
    registry_request("POST", f"/subjects/{SUBJECT}/versions?normalize=true", SCHEMA_BODY)
    return registered_schema()
