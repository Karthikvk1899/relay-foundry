# Contracts and operations

## Event acceptance

`POST /v1/events` requires a tenant bearer key, `Idempotency-Key` (8–128 characters), and JSON `{ "type": "order.created", "data": { ... } }`. Returns 202 `{id, accepted, duplicate}`. Same tenant and key with identical type/data returns the original ID and `duplicate:true`; changed content returns 409. Input bodies are limited to 64 KiB. Accepted means committed in PostgreSQL, **not delivered**.

The outbox publisher acknowledges Kafka before setting `published_at`. If it crashes between those operations, it publishes again. The consumer inserts `(event_id, endpoint_id)` uniquely and commits Kafka offsets only after the database commit. Endpoints enabled when the consumer processes an event receive a delivery. Endpoint changes between acceptance and consumption can affect recipients; this version does not snapshot subscriptions at acceptance.

The sender claims due rows with `FOR UPDATE SKIP LOCKED`, a 30-second lease, and a fencing token. It times out HTTP after eight seconds, follows no redirects, retries 5xx/429/transport errors using capped exponential backoff with jitter, and fails after six attempts. Selected 4xx statuses fail immediately. A 2xx response marks delivered. A sender crash after HTTP success but before the PostgreSQL commit causes a duplicate delivery after lease expiry. Consumers must deduplicate event IDs; replay intentionally sends a new attempt with the same event ID and delivery ID. A failed delivery can be replayed with `POST /v1/deliveries/{id}/replay`.

Malformed or unknown Kafka records are copied to `relay.invalid.v1` before committing the original offset. The consumer retries transient database errors without advancing the partition. Inspect the invalid topic and repair producer contracts before reprocessing; there is no automatic DLT replay tool.

## Signing

Headers: `X-Relay-Event-Id`, `X-Relay-Delivery-Id`, `X-Relay-Timestamp` (Unix seconds), `X-Relay-Signature` (`v1=` plus lowercase hex HMAC-SHA256). The signed bytes are `timestamp + "." + event_id + "." + raw HTTP body`. The JSON body is `{id,type,data}` with deterministic key order. Signing secret is returned only on endpoint creation. Treat the headers and body as untrusted until validated. Use a freshness window and constant-time comparison on the receiving side.

## Runbook

- Check `GET /health/ready` for database reachability; Kafka/sender availability requires inspecting container logs and outbox/delivery age. This is not a full readiness probe for every dependency.
- If Kafka is unavailable, acceptance continues in PostgreSQL; the outbox backlog grows. Restore Kafka, then watch publisher logs and pending outbox count.
- If a receiver is unavailable, attempts progress to `failed`. Investigate its `attempt_history`, repair the receiver, then replay via API or console.
- PostgreSQL is the durable source of truth. Back it up and test restore before deployment. Monitor outbox age, due delivery age, lease expiries, failed deliveries, Kafka lag, disk use, and request errors. Set alerts against your SLOs.
- Schema initialization uses `CREATE TABLE IF NOT EXISTS`. Versioned migrations, retention/partitioning, key rotation, tenant quotas, and pagination beyond 100 rows are future production work.

## Deployment limits

Compose uses one Kafka broker, one PostgreSQL instance, plaintext infrastructure links, no backup, and no high availability. Local and CI tests do not establish throughput, multi-region behavior, live internet egress controls, or AWS deployment. Production use requires capacity tests, HA configuration, TLS/ACLs, network egress enforcement, secret rotation, backups, retention, and incident exercises. See [security](../SECURITY.md).
