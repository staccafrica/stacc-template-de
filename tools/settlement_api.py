"""A mock settlement-partner API for the Stacc Data Engineering project.

    python tools/settlement_api.py --ledger data/ledger.csv --port 8080

    GET /v1/settlements?limit=500&cursor=<opaque>      -> {"data": [...], "next_cursor": "<opaque>" | null}
    GET /health                                        -> {"ok": true}
    POST /admin/release                                -> publishes the held-back batches (see --hold-days)

It behaves like a real partner API so your ingestion has to cope with it:
    * cursor pagination (the cursor is opaque; never build one yourself),
    * --rate-limit N        answers 429 + Retry-After once a client exceeds N requests per second,
    * --fail-rate 0.05      answers 503 to 5% of requests, at random (retry with backoff),
    * --hold-days 7         withholds the last 7 days of the ledger until POST /admin/release, so
                            "seven days of batches arrived late" can be reproduced on demand.
Standard library only. Reads the CSV lazily, so a 10M-row ledger needs no extra memory.
"""
import argparse
import base64
import csv
import json
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MAX_LIMIT = 1000


class Ledger:
    def __init__(self, path, hold_days):
        self.path = path
        self.released = hold_days == 0
        self.hold_days = hold_days
        self.cutoff = None
        if hold_days:
            self.cutoff = self._last_created()
        self.lock = threading.Lock()

    def _last_created(self):
        # The ledger is written in time order, so the last row holds the latest timestamp.
        with open(self.path, "rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - 4096))
            last = handle.read().decode().strip().splitlines()[-1]
        return last.split(",")[6][:10]

    def _held(self, created_at):
        if self.released or not self.cutoff:
            return False
        day = created_at[:10]
        # hold back the final N calendar days of the ledger
        from datetime import date, timedelta
        limit = date.fromisoformat(self.cutoff) - timedelta(days=self.hold_days - 1)
        return date.fromisoformat(day) >= limit

    def page(self, offset, limit):
        rows, position = [], offset
        with open(self.path, newline="") as handle:
            header = handle.readline().strip().split(",")
            if offset:
                handle.seek(offset)
            for line in iter(handle.readline, ""):
                position = handle.tell()
                values = next(csv.reader([line]))
                record = dict(zip(header, values))
                if self._held(record["created_at"]):
                    return rows, None  # everything after this point is held back too
                rows.append(record)
                if len(rows) >= limit:
                    break
            else:
                return rows, None
        return rows, position


def encode(offset):
    return base64.urlsafe_b64encode(json.dumps({"o": offset}).encode()).decode()


def decode(cursor):
    return json.loads(base64.urlsafe_b64decode(cursor.encode()))["o"]


def make_handler(ledger, rate_limit, fail_rate):
    hits = {}
    rng = random.Random(7)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def send(self, status, body, headers=None):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/health":
                return self.send(200, {"ok": True})
            if url.path != "/v1/settlements":
                return self.send(404, {"error": "not found"})
            if rate_limit:
                now = int(time.time())
                hits[now] = hits.get(now, 0) + 1
                if hits[now] > rate_limit:
                    return self.send(429, {"error": "rate limited"}, {"Retry-After": "1"})
            if fail_rate and rng.random() < fail_rate:
                return self.send(503, {"error": "temporarily unavailable"})
            query = parse_qs(url.query)
            try:
                limit = min(int(query.get("limit", ["100"])[0]), MAX_LIMIT)
                offset = decode(query["cursor"][0]) if "cursor" in query else 0
            except (ValueError, KeyError, json.JSONDecodeError):
                return self.send(400, {"error": "bad cursor or limit"})
            rows, next_offset = ledger.page(offset, limit)
            self.send(200, {"data": rows, "next_cursor": encode(next_offset) if next_offset else None})

        def do_POST(self):
            if urlparse(self.path).path == "/admin/release":
                ledger.released = True
                return self.send(200, {"released": True})
            self.send(404, {"error": "not found"})

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ledger", default="data/ledger.csv")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--rate-limit", type=int, default=0, help="max requests per second per server (0 = off)")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="fraction of requests answered with 503")
    parser.add_argument("--hold-days", type=int, default=0, help="withhold the last N days until POST /admin/release")
    args = parser.parse_args()
    ledger = Ledger(args.ledger, args.hold_days)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(ledger, args.rate_limit, args.fail_rate))
    print(f"settlement API on http://{args.host}:{args.port}/v1/settlements (ledger: {args.ledger})")
    server.serve_forever()


if __name__ == "__main__":
    main()
