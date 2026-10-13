"""Generate a deterministic, synthetic payment-settlement dataset for the Stacc Data Engineering project.

Why synthetic: real settlement ledgers are confidential. This one is generated, so you can publish
the whole repository, and the same --seed always produces byte-identical files.

    python tools/generate_ledger.py --rows 1000000 --out data            # quick local run
    python tools/generate_ledger.py --rows 10000000 --out data           # the Spark-scale run

Outputs (CSV unless noted), written to --out:
    merchants.csv          merchant_id, name, category, country
    merchant_changes.csv   merchant_id, category, valid_from   (category re-classifications: your SCD Type 2 input)
    ledger.csv             txn_id, merchant_id, account_id, amount_minor, currency, status, created_at, settled_at
    events.jsonl           payment authorisation events (--events N), keyed by account_id: your Kafka input
Standard library only.
"""
import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, timezone

CATEGORIES = ["grocery", "fuel", "restaurants", "electronics", "travel", "utilities", "pharmacy", "fashion"]
COUNTRIES = ["NG", "GH", "KE", "ZA", "EG"]
STATUSES = [("settled", 0.93), ("reversed", 0.04), ("pending", 0.03)]
START = datetime(2024, 1, 1, tzinfo=timezone.utc)


def pick_status(rng):
    roll, total = rng.random(), 0.0
    for name, weight in STATUSES:
        total += weight
        if roll < total:
            return name
    return STATUSES[0][0]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--days", type=int, default=365, help="history length in days")
    parser.add_argument("--merchants", type=int, default=2_000)
    parser.add_argument("--accounts", type=int, default=200_000)
    parser.add_argument("--events", type=int, default=100_000, help="rows in events.jsonl (0 to skip)")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)

    categories = {}
    with open(os.path.join(args.out, "merchants.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["merchant_id", "name", "category", "country"])
        for i in range(1, args.merchants + 1):
            categories[i] = rng.choice(CATEGORIES)
            writer.writerow([f"M{i:05d}", f"Merchant {i:05d}", categories[i], rng.choice(COUNTRIES)])

    with open(os.path.join(args.out, "merchant_changes.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["merchant_id", "category", "valid_from"])
        for i in rng.sample(range(1, args.merchants + 1), k=max(1, args.merchants // 10)):
            new = rng.choice([c for c in CATEGORIES if c != categories[i]])
            when = START + timedelta(days=rng.randint(30, max(31, args.days - 30)))
            writer.writerow([f"M{i:05d}", new, when.date().isoformat()])

    span = args.days * 86_400
    with open(os.path.join(args.out, "ledger.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["txn_id", "merchant_id", "account_id", "amount_minor", "currency", "status", "created_at", "settled_at"])
        for n in range(1, args.rows + 1):
            created = START + timedelta(seconds=(n * span) // args.rows + rng.randint(0, 59))
            status = pick_status(rng)
            settled = "" if status == "pending" else (created + timedelta(hours=rng.randint(1, 72))).isoformat()
            # Heavy-tailed amounts (minor units): many small payments, a few large ones. A few hot merchants.
            amount = int(rng.lognormvariate(8.5, 1.1))
            merchant = int(args.merchants * rng.random() ** 2) + 1
            writer.writerow([f"T{n:010d}", f"M{merchant:05d}", f"A{rng.randint(1, args.accounts):07d}", amount, "NGN", status, created.isoformat(), settled])

    if args.events:
        with open(os.path.join(args.out, "events.jsonl"), "w") as handle:
            for n in range(1, args.events + 1):
                at = START + timedelta(seconds=(n * span) // args.events)
                handle.write(json.dumps({
                    "event_id": f"E{n:010d}",
                    "account_id": f"A{rng.randint(1, args.accounts):07d}",
                    "type": rng.choices(["authorization", "capture", "decline"], weights=[0.7, 0.25, 0.05])[0],
                    "amount_minor": int(rng.lognormvariate(8.5, 1.1)),
                    "at": at.isoformat(),
                }) + "\n")

    print(f"wrote {args.rows} ledger rows, {args.merchants} merchants"
          f"{f', {args.events} events' if args.events else ''} to {args.out}/ (seed {args.seed})")


if __name__ == "__main__":
    main()
