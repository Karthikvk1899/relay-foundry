import hashlib
import json
import secrets
import uuid
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from psycopg.rows import dict_row

from relay.config import settings
from relay.db import close_pool, connection, open_pool
from relay.security import key_hash, validate_target


@asynccontextmanager
async def lifespan(_app: FastAPI):
    settings.validate_security()
    open_pool()
    with connection() as conn:
        conn.execute(open(__import__("pathlib").Path(__file__).with_name("schema.sql")).read())
        # The bootstrap credential can administer tenants; it is never a tenant key.
    yield
    close_pool()


app = FastAPI(title="Relay Foundry", version="0.1.0", lifespan=lifespan)


@app.exception_handler(ValueError)
async def invalid_value(_request: Request, exc: ValueError):
    return JSONResponse(status_code=422, content={"detail": str(exc)})


def credential(authorization: str = Header(default="")) -> str:
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "Bearer token required")
    return authorization[7:]


def tenant(token: str = Depends(credential)) -> str:
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id FROM tenants WHERE key_hash = %s", (key_hash(token),))
            row = cur.fetchone()
    if row is None:
        raise HTTPException(401, "invalid credential")
    return row["id"]


class TenantInput(BaseModel):
    id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,62}$")


@app.post("/admin/tenants", status_code=201)
def create_tenant(data: TenantInput, token: str = Depends(credential)):
    if not secrets.compare_digest(token, settings.api_key):
        raise HTTPException(403, "administrator credential required")
    key = secrets.token_urlsafe(40)
    with connection() as conn:
        try:
            conn.execute("INSERT INTO tenants(id,key_hash) VALUES (%s,%s)", (data.id, key_hash(key)))
        except Exception as exc:
            if getattr(exc, "sqlstate", None) == "23505":
                raise HTTPException(409, "tenant exists") from exc
            raise
    return {"id": data.id, "api_key": key}


class EndpointInput(BaseModel):
    url: str = Field(max_length=2048)


@app.post("/v1/endpoints", status_code=201)
def create_endpoint(data: EndpointInput, tenant_id: str = Depends(tenant)):
    validate_target(data.url)
    endpoint_id, secret = uuid.uuid4(), secrets.token_urlsafe(40)
    with connection() as conn:
        try:
            conn.execute("INSERT INTO endpoints(id,tenant_id,url,secret) VALUES (%s,%s,%s,%s)",
                         (endpoint_id, tenant_id, data.url, secret))
        except Exception as exc:
            if getattr(exc, "sqlstate", None) == "23505":
                raise HTTPException(409, "endpoint already exists") from exc
            raise
    return {"id": endpoint_id, "url": data.url, "signing_secret": secret}


@app.get("/v1/endpoints")
def list_endpoints(tenant_id: str = Depends(tenant)):
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT id,url,enabled,created_at FROM endpoints WHERE tenant_id=%s ORDER BY created_at DESC LIMIT 100",
                        (tenant_id,))
            return cur.fetchall()


class EventInput(BaseModel):
    type: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9._-]+$")
    data: dict


@app.post("/v1/events", status_code=202)
async def publish_event(data: EventInput, request: Request, idempotency_key: str = Header(min_length=8, max_length=128),
                        tenant_id: str = Depends(tenant)):
    raw = await request.body()
    if len(raw) > settings.max_payload_bytes:
        raise HTTPException(413, "payload too large")
    payload = json.dumps(data.data, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256((data.type + ":" + payload).encode()).hexdigest()
    event_id = uuid.uuid4()
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""INSERT INTO events(id,tenant_id,event_type,body,idempotency_key,body_hash)
                VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (tenant_id,idempotency_key) DO NOTHING RETURNING id""",
                (event_id, tenant_id, data.type, payload, idempotency_key, digest))
            inserted = cur.fetchone()
            if inserted:
                cur.execute("INSERT INTO outbox(id,event_id) VALUES (%s,%s)", (uuid.uuid4(), event_id))
            else:
                cur.execute("SELECT id,body_hash FROM events WHERE tenant_id=%s AND idempotency_key=%s",
                            (tenant_id, idempotency_key))
                existing = cur.fetchone()
                if existing["body_hash"] != digest:
                    raise HTTPException(409, "idempotency key belongs to a different event")
                event_id = existing["id"]
    return {"id": event_id, "accepted": True, "duplicate": not bool(inserted)}


@app.get("/v1/deliveries")
def list_deliveries(tenant_id: str = Depends(tenant), status: str | None = None):
    if status and status not in ("pending", "leased", "delivered", "failed"):
        raise HTTPException(422, "invalid status")
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""SELECT d.id,d.event_id,d.endpoint_id,d.status,d.attempts,d.next_at,
                d.last_status,d.last_error,d.created_at FROM deliveries d
                WHERE d.tenant_id=%s AND (%s IS NULL OR d.status=%s)
                ORDER BY d.created_at DESC LIMIT 100""", (tenant_id, status, status))
            return cur.fetchall()


@app.get("/v1/deliveries/{delivery_id}")
def delivery_detail(delivery_id: uuid.UUID, tenant_id: str = Depends(tenant)):
    with connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT * FROM deliveries WHERE id=%s AND tenant_id=%s", (delivery_id, tenant_id))
            row = cur.fetchone()
            if row is None:
                raise HTTPException(404, "delivery not found")
            cur.execute("SELECT number,started_at,http_status,error FROM attempts WHERE delivery_id=%s ORDER BY number",
                        (delivery_id,))
            row.pop("tenant_id")
            row["attempt_history"] = cur.fetchall()
            return row


@app.post("/v1/deliveries/{delivery_id}/replay", status_code=202)
def replay(delivery_id: uuid.UUID, tenant_id: str = Depends(tenant)):
    with connection() as conn:
        result = conn.execute("""UPDATE deliveries SET status='pending',attempts=0,next_at=now(),
            lease_until=NULL,lease_token=NULL,last_error=NULL WHERE id=%s AND tenant_id=%s AND status='failed'""",
            (delivery_id, tenant_id))
        if result.rowcount == 0:
            raise HTTPException(409, "only failed deliveries can be replayed")
    return {"id": delivery_id, "status": "pending"}


@app.get("/health/live")
def live():
    return {"status": "ok"}


@app.get("/health/ready")
def ready():
    with connection() as conn:
        conn.execute("SELECT 1")
    return {"status": "ok"}
