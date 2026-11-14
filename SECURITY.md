# Security policy

Please report suspected vulnerabilities privately through GitHub's private vulnerability reporting, if available, or contact the repository owner privately. Do not include live credentials or customer payloads in a public issue.

The Compose stack is a **loopback development environment**. It uses plaintext service connections and permits private target addresses to exercise its local receiver. Never expose this configuration to the internet. Production mode requires `PRODUCTION=true`, `ALLOW_PRIVATE_TARGETS=false`, and an exact `ALLOWED_HOSTS` list. Place the API and UI behind TLS, authenticate infrastructure connections, restrict egress to an approved proxy or firewall, and use managed secret storage.

Tenant API keys are stored as SHA-256 digests in PostgreSQL, returned once on creation, and sent as bearer tokens. The bootstrap administrator key is set with `API_KEY`; it must be separately protected and rotated. Endpoint HMAC secrets are stored in plaintext because the sender must sign requests; use encrypted storage and rotation for sensitive deployments. The browser console keeps its tenant key only in component memory, cleared on reload. A compromised browser session can still expose it.

The application rejects non-HTTPS targets and non-global DNS answers in the default mode, requires exact host allowlisting in production, and disables HTTP redirects. DNS can change between validation and connection: **network egress enforcement is mandatory** for SSRF protection. The receiver must check `X-Relay-Timestamp` for freshness, verify HMAC SHA-256 over `{timestamp}.{event_id}.{raw_body}`, compare signatures in constant time, and deduplicate `X-Relay-Event-Id`.

There has been no penetration test, live IdP integration, encrypted Kafka/PostgreSQL deployment, or production load/failover qualification. See [operations](docs/OPERATIONS.md).
