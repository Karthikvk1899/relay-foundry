CREATE TABLE IF NOT EXISTS endpoints (
    id UUID PRIMARY KEY, tenant_id TEXT NOT NULL, url TEXT NOT NULL,
    secret TEXT NOT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, url)
);
CREATE TABLE IF NOT EXISTS tenants (
    id TEXT PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY, tenant_id TEXT NOT NULL, event_type TEXT NOT NULL,
    body JSONB NOT NULL, idempotency_key TEXT NOT NULL, body_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (tenant_id, id)
);
CREATE TABLE IF NOT EXISTS outbox (
    id UUID PRIMARY KEY, event_id UUID NOT NULL REFERENCES events(id),
    published_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox(created_at) WHERE published_at IS NULL;
CREATE TABLE IF NOT EXISTS deliveries (
    id UUID PRIMARY KEY, tenant_id TEXT NOT NULL,
    event_id UUID NOT NULL REFERENCES events(id), endpoint_id UUID NOT NULL REFERENCES endpoints(id),
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','leased','delivered','failed')),
    attempts INTEGER NOT NULL DEFAULT 0, next_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ, lease_token UUID, last_status INTEGER, last_error TEXT,
    delivered_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, endpoint_id)
);
CREATE INDEX IF NOT EXISTS deliveries_ready_idx ON deliveries(next_at) WHERE status IN ('pending','leased');
CREATE TABLE IF NOT EXISTS attempts (
    delivery_id UUID NOT NULL REFERENCES deliveries(id), number INTEGER NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    http_status INTEGER, error TEXT, PRIMARY KEY(delivery_id, number)
);
