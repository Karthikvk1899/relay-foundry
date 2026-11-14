"""Controllable local-only webhook receiver for the Compose test profile."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

state = {"fail": False, "received": []}


class Handler(BaseHTTPRequestHandler):
    def respond(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.respond(200, state)

    def do_POST(self):
        if self.path == "/state":
            state["fail"] = json.loads(self.rfile.read(int(self.headers["Content-Length"]))) ["fail"]
            self.respond(200, state)
            return
        body = self.rfile.read(int(self.headers["Content-Length"]))
        state["received"].append({"body": body.decode(), "signature": self.headers.get("X-Relay-Signature"),
                                  "timestamp": self.headers.get("X-Relay-Timestamp"),
                                  "event_id": self.headers.get("X-Relay-Event-Id")})
        self.respond(503 if state["fail"] else 204, {})


ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
