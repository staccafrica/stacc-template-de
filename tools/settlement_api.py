"""A local settlement-partner API for the Stacc Data Engineering project.

It serves the files from generate_ledger.py the way a real payments partner would. The contract is in
tools/openapi.yaml; the README has the short version.

    export STACC_API_KEY=sk_test_local_123       # any value; your client sends the same one
    python tools/settlement_api.py --ledger data/ledger.csv

    GET  /v1/transactions   payments, oldest first. Filters: created_from, created_to, updated_since, merchant_id, status
    GET  /v1/payouts        settlement batches. Filters: settlement_date_from, settlement_date_to, merchant_id
    GET  /health            {"status": "ok"} (no key needed)
    POST /admin/release     publishes the late batches held back by --hold-days (local only)

Every /v1 request needs `Authorization: Bearer <STACC_API_KEY>`. Lists use cursor pagination:
{"object": "list", "data": [...], "has_more": true, "next_cursor": "..."}. Errors look like
{"error": {"type": "...", "code": "...", "message": "...", "request_id": "req_..."}}.

It behaves like a real partner, so your client has to cope (the default `--profile realistic`):
    * a per-key rate limit with X-RateLimit-* headers, and 429 + Retry-After when you exceed it,
    * occasional 503s and the odd 500 (retry both with backoff), and a few slow responses,
    * once in a while a page repeats the last record of the page before (at-least-once delivery),
    * --hold-days 7 withholds the last 7 days, and shows older payments as they were then (pending,
      not yet refunded) until POST /admin/release, so "seven days arrived late" can be reproduced.
Use --profile calm while you build your first client: no failures, no limit, no delays.
Standard library only. Reads the CSV lazily, so a 10M-row ledger needs no extra memory.
"""
import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import random
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100
PROFILES = {
    # requests per second per key, burst, share of 503/500 answers, share of slow answers, share of repeated records
    "realistic": {"rate": 25, "burst": 50, "fail": 0.03, "slow": 0.01, "repeat": 0.01},
    "harsh": {"rate": 10, "burst": 15, "fail": 0.12, "slow": 0.05, "repeat": 0.05},
    "calm": {"rate": 0, "burst": 0, "fail": 0.0, "slow": 0.0, "repeat": 0.0},
}
INTEGER_FIELDS = {"amount_minor", "fee_minor", "net_minor", "gross_minor", "fees_minor", "txn_count"}


def parse_time(value, name):
    """Accepts 2024-03-01 or 2024-03-01T10:00:00Z. Returns an aware datetime."""
    try:
        if len(value) == 10:
            return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        raise ApiError(400, "invalid_request_error", "parameter_invalid", f"{name} must be a date (2024-03-01) or an ISO 8601 time (2024-03-01T10:00:00Z)")


def read_time(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


class ApiError(Exception):
    def __init__(self, status, kind, code, message, headers=None):
        super().__init__(message)
        self.status, self.kind, self.code, self.message, self.headers = status, kind, code, message, headers or {}


class CsvTable:
    """A CSV file in time order, read lazily, with a byte offset for the first row of each day."""

    def __init__(self, path, time_column):
        self.path, self.time_column = path, time_column
        with open(path, newline="") as handle:
            self.header = next(csv.reader([handle.readline()]))
        self.time_index = self.header.index(time_column)
        self.day_offsets = {}
        self.last_day = None
        with open(path, "rb") as handle:
            handle.readline()
            while True:
                offset = handle.tell()
                line = handle.readline()
                if not line:
                    break
                day = line.split(b",")[self.time_index][:10].decode()
                if day not in self.day_offsets:
                    self.day_offsets[day] = offset
                self.last_day = day

    def start_offset(self, since):
        """The first byte offset that can hold a row on or after `since` (a date string)."""
        days = sorted(day for day in self.day_offsets if day >= since)
        return self.day_offsets[days[0]] if days else None

    def rows(self, offset):
        """Yields (row_start_offset, next_offset, record) from `offset` onwards."""
        with open(self.path, newline="") as handle:
            if offset:
                handle.seek(offset)
            else:
                handle.readline()
            while True:
                start = handle.tell()
                line = handle.readline()
                if not line:
                    return
                values = next(csv.reader([line]))
                yield start, handle.tell(), dict(zip(self.header, values))


class Partner:
    """The partner's data, as visible right now (late batches can be held back)."""

    def __init__(self, ledger_path, payouts_path, hold_days):
        self.transactions = CsvTable(ledger_path, "created_at")
        self.payouts = CsvTable(payouts_path, "settlement_date") if payouts_path and os.path.exists(payouts_path) else None
        self.released = hold_days == 0
        self.horizon = None
        if hold_days and self.transactions.last_day:
            last = datetime.fromisoformat(self.transactions.last_day).replace(tzinfo=timezone.utc)
            self.horizon = last - timedelta(days=hold_days - 1)  # nothing on or after this moment is visible yet

    def visible_now(self):
        return None if self.released else self.horizon

    @staticmethod
    def as_of(record, moment):
        """The payment as the partner's system showed it at `moment`."""
        settled, reversed_at = read_time(record.get("settled_at")), read_time(record.get("reversed_at"))
        record = dict(record)
        if reversed_at and reversed_at >= moment:
            record.update(status="settled", reversed_at="")
            reversed_at = None
        if settled and settled >= moment:
            record.update(status="pending", settled_at="", fee_minor="0", net_minor=record["amount_minor"], settlement_id="")
            settled = None
        times = [t for t in [read_time(record["created_at"]), settled, reversed_at] if t]
        record["updated_at"] = max(times).strftime("%Y-%m-%dT%H:%M:%SZ") if times else record["updated_at"]
        return record


def to_json(record, kind):
    """CSV strings become the types a JSON API sends: integers, nulls, and no key for columns added later."""
    out = {"object": kind}
    for key, value in record.items():
        if key == "terminal_id" and value == "" and record.get("payment_method") == "card" and record.get("created_at", "") < "2024-07-01":
            continue  # this field did not exist before July 2024, so older payments do not have it
        if value == "":
            out[key] = None
        elif key in INTEGER_FIELDS:
            try:
                out[key] = int(value)
            except ValueError:
                out[key] = value
        else:
            out[key] = value
    return out


def encode_cursor(offset, filters):
    payload = json.dumps({"o": offset, "f": filters_hash(filters)}).encode()
    return "cur_" + base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_cursor(cursor, filters):
    try:
        raw = cursor.removeprefix("cur_")
        payload = json.loads(base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4)))
        offset, fingerprint = int(payload["o"]), payload["f"]
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        raise ApiError(400, "invalid_request_error", "cursor_invalid", "cursor is not one this API issued")
    if fingerprint != filters_hash(filters):
        raise ApiError(400, "invalid_request_error", "cursor_mismatch", "send the same filters with a cursor as with the request that returned it")
    return offset


def filters_hash(filters):
    return hashlib.sha256(json.dumps(filters, sort_keys=True).encode()).hexdigest()[:12]


class RateLimiter:
    """A token bucket per API key, like most partner APIs."""

    def __init__(self, rate, burst):
        self.rate, self.burst, self.buckets, self.lock = rate, burst, {}, threading.Lock()

    def take(self, key):
        if not self.rate:
            return {}
        with self.lock:
            now = time.monotonic()
            tokens, last = self.buckets.get(key, (self.burst, now))
            tokens = min(self.burst, tokens + (now - last) * self.rate)
            reset = int(time.time() + (self.burst - tokens) / self.rate) + 1
            if tokens < 1:
                self.buckets[key] = (tokens, now)
                wait = max(1, int((1 - tokens) / self.rate + 0.999))
                raise ApiError(429, "rate_limit_error", "rate_limited", f"too many requests: the limit is {self.rate} per second",
                               {"Retry-After": str(wait), "X-RateLimit-Limit": str(self.rate), "X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(reset)})
            self.buckets[key] = (tokens - 1, now)
            return {"X-RateLimit-Limit": str(self.rate), "X-RateLimit-Remaining": str(int(tokens - 1)), "X-RateLimit-Reset": str(reset)}


def make_handler(partner, api_key, profile, seed):
    limiter = RateLimiter(profile["rate"], profile["burst"])
    rng = random.Random(seed)
    rng_lock = threading.Lock()

    def chance(p):
        with rng_lock:
            return rng.random() < p

    class Handler(BaseHTTPRequestHandler):
        server_version = "StaccSettlements/2.3"
        request_id = "-"
        sys_version = ""

        def log_message(self, fmt, *args):
            print(f"{self.log_date_time_string()} {self.request_id} {fmt % args}")

        def send(self, status, body, headers=None):
            payload = json.dumps(body, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Request-Id", self.request_id)
            self.send_header("Stacc-Version", "2024-06-01")
            for key, value in (headers or {}).items():
                self.send_header(key, value)
            self.end_headers()
            self.wfile.write(payload)

        def fail(self, error):
            body = {"error": {"type": error.kind, "code": error.code, "message": error.message, "request_id": self.request_id}}
            self.send(error.status, body, error.headers)

        def do_GET(self):
            self.request_id = "req_" + os.urandom(8).hex()
            url = urlparse(self.path)
            if url.path == "/health":
                return self.send(200, {"status": "ok"})
            try:
                if url.path not in ("/v1/transactions", "/v1/settlements", "/v1/payouts"):
                    raise ApiError(404, "invalid_request_error", "resource_missing", f"no such endpoint: {url.path}")
                self.authenticate()
                headers = limiter.take(self.key_id)
                if chance(profile["slow"]):
                    time.sleep(rng.uniform(2, 6))
                if chance(profile["fail"]):
                    if chance(0.8):
                        raise ApiError(503, "api_error", "service_unavailable", "the service is temporarily unavailable, retry with backoff", headers)
                    raise ApiError(500, "api_error", "internal_error", "something went wrong on our side, retry with backoff", headers)
                query = {key: values[-1] for key, values in parse_qs(url.query).items()}
                if url.path == "/v1/payouts":
                    body = self.list_payouts(query)
                else:
                    body = self.list_transactions(query)
                self.send(200, body, headers)
            except ApiError as error:
                self.fail(error)

        def do_POST(self):
            self.request_id = "req_" + os.urandom(8).hex()
            if urlparse(self.path).path == "/admin/release":
                partner.released = True
                return self.send(200, {"released": True})
            self.fail(ApiError(404, "invalid_request_error", "resource_missing", "no such endpoint"))

        def authenticate(self):
            header = self.headers.get("Authorization", "")
            scheme, _, token = header.partition(" ")
            if scheme.lower() != "bearer" or not token:
                raise ApiError(401, "authentication_error", "api_key_missing", "send your key as Authorization: Bearer <key>", {"WWW-Authenticate": 'Bearer realm="settlements"'})
            if not hmac.compare_digest(token.strip(), api_key):
                raise ApiError(401, "authentication_error", "api_key_invalid", "that API key is not valid", {"WWW-Authenticate": 'Bearer realm="settlements"'})
            self.key_id = hashlib.sha256(token.encode()).hexdigest()[:8]

        def limit(self, query):
            try:
                limit = int(query.get("limit", DEFAULT_LIMIT))
            except ValueError:
                raise ApiError(400, "invalid_request_error", "parameter_invalid", "limit must be a whole number")
            if not 1 <= limit <= MAX_LIMIT:
                raise ApiError(400, "invalid_request_error", "parameter_invalid", f"limit must be between 1 and {MAX_LIMIT}")
            return limit

        def page(self, table, query, filters, start_day, keep, shape):
            limit = self.limit(query)
            if "cursor" in query:
                offset = decode_cursor(query["cursor"], filters)
            else:
                offset = table.start_offset(start_day) if start_day else 0
                if offset is None:
                    return {"object": "list", "data": [], "has_more": False, "next_cursor": None}
            data, last_start, next_offset = [], None, None
            for start, after, record in table.rows(offset):
                verdict = keep(record)
                if verdict == "stop":
                    next_offset = None
                    break
                next_offset = after
                if verdict:
                    data.append(shape(record))
                    last_start = start
                    if len(data) >= limit:
                        break
            else:
                next_offset = None
            # At-least-once delivery: now and then the next page starts with the last record of this one.
            if next_offset and last_start is not None and chance(profile["repeat"]):
                next_offset = last_start
            return {"object": "list", "data": data, "has_more": next_offset is not None,
                    "next_cursor": encode_cursor(next_offset, filters) if next_offset else None}

        def list_transactions(self, query):
            filters = {k: query[k] for k in ("created_from", "created_to", "updated_since", "merchant_id", "status") if k in query}
            created_from = parse_time(filters["created_from"], "created_from") if "created_from" in filters else None
            created_to = parse_time(filters["created_to"], "created_to") if "created_to" in filters else None
            updated_since = parse_time(filters["updated_since"], "updated_since") if "updated_since" in filters else None
            statuses = {"pending", "failed", "settled", "reversed", "charged_back"}
            if "status" in filters and filters["status"] not in statuses:
                raise ApiError(400, "invalid_request_error", "parameter_invalid", f"status must be one of {', '.join(sorted(statuses))}")
            if created_from and created_to and created_to <= created_from:
                raise ApiError(400, "invalid_request_error", "parameter_invalid", "created_to must be after created_from")
            horizon = partner.visible_now()

            def view(record):
                return partner.as_of(record, horizon) if horizon else record

            def keep(record):
                created = read_time(record["created_at"])
                if created is None:
                    return True
                if horizon and created >= horizon:
                    return "stop"  # the rest of the file has not "arrived" yet
                if created_to and created >= created_to:
                    return "stop"
                if created_from and created < created_from:
                    return False
                shown = view(record)
                if updated_since and (read_time(shown["updated_at"]) or created) < updated_since:
                    return False
                if "merchant_id" in filters and record["merchant_id"] != filters["merchant_id"]:
                    return False
                if "status" in filters and shown["status"] != filters["status"]:
                    return False
                return True

            start_day = created_from.date().isoformat() if created_from else None
            return self.page(partner.transactions, query, filters, start_day, keep, lambda record: to_json(view(record), "transaction"))

        def list_payouts(self, query):
            if not partner.payouts:
                raise ApiError(404, "invalid_request_error", "resource_missing", "no payouts file: run generate_ledger.py again")
            filters = {k: query[k] for k in ("settlement_date_from", "settlement_date_to", "merchant_id") if k in query}
            date_from = parse_time(filters["settlement_date_from"], "settlement_date_from").date().isoformat() if "settlement_date_from" in filters else None
            date_to = parse_time(filters["settlement_date_to"], "settlement_date_to").date().isoformat() if "settlement_date_to" in filters else None
            horizon = partner.visible_now()

            def keep(record):
                day = record["settlement_date"]
                if horizon and day >= horizon.date().isoformat():
                    return "stop"
                if date_to and day > date_to:
                    return "stop"
                if date_from and day < date_from:
                    return False
                return "merchant_id" not in filters or record["merchant_id"] == filters["merchant_id"]

            return self.page(partner.payouts, query, filters, date_from, keep, lambda record: to_json(record, "payout"))

    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ledger", default="data/ledger.csv")
    parser.add_argument("--payouts", default=None, help="default: payouts.csv next to the ledger")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--profile", choices=sorted(PROFILES), default="realistic")
    parser.add_argument("--rate-limit", type=int, default=None, help="override the profile: requests per second per key (0 = off)")
    parser.add_argument("--fail-rate", type=float, default=None, help="override the profile: share of requests answered 503/500")
    parser.add_argument("--hold-days", type=int, default=0, help="withhold the last N days until POST /admin/release")
    parser.add_argument("--seed", type=int, default=7, help="seed for the failures, so a run can be repeated")
    args = parser.parse_args()

    api_key = os.environ.get("STACC_API_KEY", "")
    if not api_key:
        parser.error("set STACC_API_KEY first, for example: export STACC_API_KEY=sk_test_local_123")
    profile = dict(PROFILES[args.profile])
    if args.rate_limit is not None:
        profile.update(rate=args.rate_limit, burst=args.rate_limit * 2)
    if args.fail_rate is not None:
        profile["fail"] = args.fail_rate
    payouts = args.payouts or os.path.join(os.path.dirname(args.ledger) or ".", "payouts.csv")
    print("indexing the ledger...", flush=True)
    partner = Partner(args.ledger, payouts, args.hold_days)
    server = ThreadingHTTPServer((args.host, args.port), make_handler(partner, api_key, profile, args.seed))
    held = f", last {args.hold_days} days held back" if args.hold_days else ""
    print(f"settlement API on http://{args.host}:{args.port}/v1/transactions (profile {args.profile}{held})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
