"""Three independently restartable loops. Kafka and HTTP are at-least-once boundaries."""
import json
import logging
import random
import signal
import time
import uuid

import httpx
from confluent_kafka import Consumer, Producer, KafkaException, TopicPartition
from psycopg.rows import dict_row

from relay.config import settings
from relay.db import close_pool, connection, open_pool
from relay.security import canonical_body, signature, validate_target

log = logging.getLogger(__name__)
running = True


def stop(_signal, _frame):
    global running
    running = False


def publish_batch(producer: Producer) -> int:
    """Keep row locks until broker acknowledgment; a crash may publish twice."""
    count = 0
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""SELECT o.id,e.id AS event_id,e.tenant_id,e.event_type,e.body FROM outbox o
                JOIN events e ON e.id=o.event_id WHERE o.published_at IS NULL
                ORDER BY o.created_at FOR UPDATE OF o SKIP LOCKED LIMIT 50""")
            for row in cur.fetchall():
                result = []
                producer.produce(settings.kafka_topic, key=str(row["event_id"]),
                                 value=json.dumps({"id": str(row["event_id"]), "tenant_id": row["tenant_id"]}).encode(),
                                 on_delivery=lambda error, _message: result.append(error))
                producer.flush(10)
                if result != [None]:
                    raise RuntimeError("Kafka publish acknowledgment missing or failed")
                cur.execute("UPDATE outbox SET published_at=now() WHERE id=%s", (row["id"],))
                count += 1
    return count


def ingest_message(raw: bytes) -> None:
    message = json.loads(raw)
    event_id, tenant_id = uuid.UUID(message["id"]), message["tenant_id"]
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Never trust a tenant ID from Kafka without checking the stored event.
            cur.execute("SELECT id FROM events WHERE id=%s AND tenant_id=%s", (event_id, tenant_id))
            if cur.fetchone() is None:
                raise ValueError("Kafka message refers to an unknown event")
            cur.execute("SELECT id FROM endpoints WHERE tenant_id=%s AND enabled=true", (tenant_id,))
            for endpoint in cur.fetchall():
                cur.execute("""INSERT INTO deliveries(id,tenant_id,event_id,endpoint_id)
                    VALUES (%s,%s,%s,%s) ON CONFLICT (event_id,endpoint_id) DO NOTHING""",
                    (uuid.uuid4(), tenant_id, event_id, endpoint["id"]))


def claim_delivery():
    token = uuid.uuid4()
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""WITH candidate AS (
                SELECT id FROM deliveries WHERE
                (status='pending' AND next_at<=now()) OR (status='leased' AND lease_until<now())
                ORDER BY next_at,id FOR UPDATE SKIP LOCKED LIMIT 1)
                UPDATE deliveries d SET status='leased',lease_until=now()+interval '30 seconds',lease_token=%s
                FROM candidate c WHERE d.id=c.id RETURNING d.id,d.attempts,d.lease_token,d.event_id,d.endpoint_id""",
                (token,))
            return cur.fetchone()


def retry_delay(attempt: int, jitter: float) -> float:
    return min(3600, 2 ** min(attempt, 10)) + jitter


def deliver_one(client: httpx.Client, item: dict) -> None:
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""SELECT e.event_type,e.body,p.url,p.secret,p.enabled FROM events e
                JOIN endpoints p ON p.id=%s WHERE e.id=%s""", (item["endpoint_id"], item["event_id"]))
            target = cur.fetchone()
    status, error = None, None
    body = canonical_body(str(item["event_id"]), target["event_type"], target["body"])
    try:
        if not target["enabled"]:
            raise ValueError("endpoint disabled")
        validate_target(target["url"])
        timestamp = int(time.time())
        response = client.post(target["url"], content=body, follow_redirects=False, headers={
            "Content-Type": "application/json", "User-Agent": "RelayFoundry/0.1",
            "X-Relay-Event-Id": str(item["event_id"]),
            "X-Relay-Delivery-Id": str(item["id"]),
            "X-Relay-Timestamp": str(timestamp),
            "X-Relay-Signature": signature(target["secret"], timestamp, str(item["event_id"]), body),
        })
        status = response.status_code
        if not 200 <= status < 300:
            error = f"HTTP {status}"
    except (httpx.HTTPError, ValueError, OSError) as exc:
        error = str(exc)[:256]
    success = error is None
    attempt = item["attempts"] + 1
    terminal = attempt >= settings.max_attempts or status in (400, 401, 403, 404, 410, 413)
    delay = retry_delay(attempt, random.random())
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # A reclaimed lease fences stale workers from changing the outcome.
            cur.execute("""UPDATE deliveries SET attempts=%s,status=%s,lease_until=NULL,lease_token=NULL,
                next_at=now()+(%s * interval '1 second'),last_status=%s,last_error=%s,
                delivered_at=CASE WHEN %s THEN now() ELSE delivered_at END
                WHERE id=%s AND lease_token=%s RETURNING id""",
                (attempt, "delivered" if success else "failed" if terminal else "pending",
                 delay, status, error, success, item["id"], item["lease_token"]))
            if cur.fetchone():
                cur.execute("""INSERT INTO attempts(delivery_id,number,http_status,error)
                    VALUES (%s,(SELECT COALESCE(MAX(number),0)+1 FROM attempts WHERE delivery_id=%s),%s,%s)""",
                    (item["id"], item["id"], status, error))


def publisher_loop() -> None:
    producer = Producer({"bootstrap.servers": settings.kafka_bootstrap, "enable.idempotence": True,
                         "acks": "all", "message.timeout.ms": 10000})
    while running:
        try:
            if not publish_batch(producer):
                time.sleep(0.5)
        except (KafkaException, RuntimeError, OSError):
            log.exception("publisher failure; retrying")
            time.sleep(2)


def consumer_loop() -> None:
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": "relay-foundry-v1",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([settings.kafka_topic])
    dead_letter = Producer({"bootstrap.servers": settings.kafka_bootstrap, "acks": "all"})
    try:
        while running:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue
            try:
                ingest_message(msg.value())
                consumer.commit(message=msg, asynchronous=False)
            except (ValueError, json.JSONDecodeError) as exc:
                # Permanently malformed records must not block the partition forever.
                result = []
                dead_letter.produce("relay.invalid.v1", key=msg.key(), value=msg.value(),
                                    headers={"reason": str(exc)[:200]},
                                    on_delivery=lambda error, _message: result.append(error))
                dead_letter.flush(10)
                if result == [None]:
                    consumer.commit(message=msg, asynchronous=False)
                else:
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                log.warning("quarantined invalid Kafka message: %s", exc)
            except Exception:
                log.exception("ingest failed; partition will be retried after restart")
                # Never commit past a failed record in this partition.
                consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                time.sleep(2)
    finally:
        consumer.close()


def sender_loop() -> None:
    with httpx.Client(timeout=httpx.Timeout(8, connect=3), trust_env=False) as client:
        while running:
            try:
                item = claim_delivery()
                if item:
                    deliver_one(client, item)
                else:
                    time.sleep(0.25)
            except Exception:
                log.exception("sender failure; retrying")
                time.sleep(1)


def main(kind: str) -> None:
    logging.basicConfig(level=logging.INFO)
    settings.validate_security()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    open_pool()
    try:
        {"publisher": publisher_loop, "consumer": consumer_loop, "sender": sender_loop}[kind]()
    finally:
        close_pool()


if __name__ == "__main__":
    import sys
    main(sys.argv[1])
