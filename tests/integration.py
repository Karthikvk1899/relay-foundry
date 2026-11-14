"""Real Postgres, Kafka, publisher, consumer, sender and HTTP receiver smoke test."""
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request
import uuid

API = os.environ["API_URL"]
RECEIVER = os.environ["RECEIVER_URL"]
ADMIN = os.environ["API_KEY"]


def request(base, path, method="GET", body=None, token=None, headers=None):
    payload = json.dumps(body).encode() if body is not None else None
    values = {"Content-Type": "application/json", **(headers or {})}
    if token:
        values["Authorization"] = f"Bearer {token}"
    with urllib.request.urlopen(urllib.request.Request(base + path, data=payload, method=method, headers=values), timeout=10) as response:
        return json.loads(response.read())


def eventually(fn, deadline=35):
    end = time.monotonic() + deadline
    while time.monotonic() < end:
        result = fn()
        if result:
            return result
        time.sleep(0.35)
    raise AssertionError("condition not reached before deadline")


tenant = "ci-" + uuid.uuid4().hex[:10]
key = request(API, "/admin/tenants", "POST", {"id": tenant}, ADMIN)["api_key"]
target = RECEIVER + "/hook"
endpoint = request(API, "/v1/endpoints", "POST", {"url": target}, key)
assert endpoint["signing_secret"]
idem = str(uuid.uuid4())
body = {"type": "order.created", "data": {"order_id": "integration-1"}}
event = request(API, "/v1/events", "POST", body, key, {"Idempotency-Key": idem})
assert event["accepted"] and not event["duplicate"]
duplicate = request(API, "/v1/events", "POST", body, key, {"Idempotency-Key": idem})
assert duplicate["id"] == event["id"] and duplicate["duplicate"]


def success():
    rows = request(API, "/v1/deliveries", token=key)
    return next((d for d in rows if d["event_id"] == event["id"] and d["status"] == "delivered"), None)


delivery = eventually(success)
history = request(API, "/v1/deliveries/" + delivery["id"], token=key)
assert history["attempts"] == 1 and history["attempt_history"][0]["http_status"] == 204
received = request(RECEIVER, "/")["received"]
match = next(x for x in received if x["event_id"] == event["id"])
signed = f'{match["timestamp"]}.{match["event_id"]}.'.encode() + match["body"].encode()
expected = "v1=" + hmac.new(endpoint["signing_secret"].encode(), signed, hashlib.sha256).hexdigest()
assert hmac.compare_digest(match["signature"], expected)

request(RECEIVER, "/state", "POST", {"fail": True})
second = request(API, "/v1/events", "POST", {"type": "order.updated", "data": {"order_id": "integration-2"}},
                 key, {"Idempotency-Key": str(uuid.uuid4())})


def failed_attempt():
    rows = request(API, "/v1/deliveries", token=key)
    return next((d for d in rows if d["event_id"] == second["id"] and d["attempts"] >= 1 and d["last_status"] == 503), None)


retried = eventually(failed_attempt)
request(RECEIVER, "/state", "POST", {"fail": False})
eventually(lambda: next((d for d in request(API, "/v1/deliveries", token=key)
                         if d["id"] == retried["id"] and d["status"] == "delivered" and d["attempts"] >= 2), None))
print("PASS: acceptance, idempotency, Kafka fanout, signed delivery, retry, tenant API")
