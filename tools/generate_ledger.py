"""Generate a realistic, synthetic payments dataset for the Stacc Data Engineering project.

Why synthetic: real settlement ledgers are confidential. This one is generated, so you can publish the whole
repository. It behaves like a pan-African payment processor's data: five countries and currencies, card, bank
transfer, USSD and mobile money, busy lunchtimes, evenings and paydays, next-business-day settlement with fees,
refunds and chargebacks that arrive days later, and the small defects a real partner feed carries.

The data is seeded from your GitHub username, so you get the same files every run and other learners get
different ones. Pass --seed 42 for the shared reference data.

    python tools/generate_ledger.py --rows 1000000 --out data            # the everyday run (~200 MB, a minute or two)
    python tools/generate_ledger.py --rows 10000000 --out data           # the Spark-scale run (~2 GB)

Outputs, written to --out:
    merchants.csv          the merchant list as it was on the first day
    merchant_changes.csv   every later change to a merchant, one full new version per row: your SCD Type 2 input
    ledger.csv             one row per payment, in its final state: the partner's system of record (served by the API)
    payouts.csv            one row per merchant settlement batch: what was paid out, for reconciliation
    events.jsonl           payment lifecycle events in the order they arrived: your Kafka input
Standard library only. Same seed, same files, on every machine.
"""
import argparse
import csv
import hashlib
import json
import math
import os
import random
import re
import subprocess
from datetime import date, datetime, timedelta, timezone

START = datetime(2024, 1, 1, tzinfo=timezone.utc)
# Columns a partner added part-way through the year. Rows before this date have no terminal_id.
TERMINAL_ID_FROM = datetime(2024, 7, 1, tzinfo=timezone.utc)

COUNTRIES = {
    # code: (currency, UTC offset hours, share of payments, typical payment in major units, cities with weights)
    "NG": ("NGN", 1, 0.46, 9000, [("Lagos", 45), ("Abuja", 18), ("Port Harcourt", 10), ("Ibadan", 9), ("Kano", 8), ("Enugu", 5), ("Benin City", 5)]),
    "KE": ("KES", 3, 0.18, 1800, [("Nairobi", 55), ("Mombasa", 18), ("Kisumu", 10), ("Nakuru", 9), ("Eldoret", 8)]),
    "GH": ("GHS", 0, 0.12, 180, [("Accra", 50), ("Kumasi", 25), ("Takoradi", 10), ("Tamale", 8), ("Cape Coast", 7)]),
    "ZA": ("ZAR", 2, 0.14, 320, [("Johannesburg", 40), ("Cape Town", 28), ("Durban", 15), ("Pretoria", 12), ("Gqeberha", 5)]),
    "EG": ("EGP", 2, 0.10, 650, [("Cairo", 55), ("Alexandria", 25), ("Giza", 12), ("Mansoura", 8)]),
}

# How people pay in each country.
METHODS = {
    "NG": [("card", 0.42), ("bank_transfer", 0.36), ("ussd", 0.16), ("mobile_money", 0.06)],
    "KE": [("mobile_money", 0.68), ("card", 0.26), ("bank_transfer", 0.06)],
    "GH": [("mobile_money", 0.55), ("card", 0.30), ("bank_transfer", 0.15)],
    "ZA": [("card", 0.76), ("bank_transfer", 0.20), ("mobile_money", 0.04)],
    "EG": [("card", 0.58), ("mobile_money", 0.27), ("bank_transfer", 0.15)],
}
CARD_BRANDS = {
    "NG": [("verve", 0.45), ("mastercard", 0.35), ("visa", 0.20)],
    "default": [("visa", 0.55), ("mastercard", 0.45)],
}
# Processor fee per method: (percent, cap in major units or None, fixed fee in major units)
FEES = {
    "card": (0.015, None, 0),
    "bank_transfer": (0.010, None, 0),
    "ussd": (0.0125, None, 0),
    "mobile_money": (0.0150, None, 0),
}
NG_CARD_CAP = 2000  # Nigerian card fees are capped at 2,000 naira

CATEGORIES = {
    # category: (share of merchants, typical-amount multiplier, business nouns)
    "grocery": (0.22, 1.0, ["Supermarket", "Mart", "Stores", "Provisions", "Fresh Market"]),
    "restaurants": (0.18, 0.7, ["Kitchen", "Grill", "Eatery", "Suya Spot", "Bistro", "Chop House"]),
    "fuel": (0.08, 1.4, ["Filling Station", "Petroleum", "Energy", "Oil & Gas"]),
    "fashion": (0.12, 2.2, ["Fashion House", "Boutique", "Fabrics", "Couture", "Styles"]),
    "electronics": (0.08, 5.5, ["Electronics", "Gadgets", "Phones & Accessories", "Digital World"]),
    "pharmacy": (0.08, 0.8, ["Pharmacy", "Chemists", "Pharmaceuticals", "Health Store"]),
    "travel": (0.05, 9.0, ["Travels & Tours", "Air Travel", "Bus Lines", "Logistics"]),
    "utilities": (0.06, 1.6, ["Power", "Telecom", "Water Services", "Internet"]),
    "education": (0.05, 6.0, ["Academy", "Schools", "Learning Centre", "College"]),
    "beauty": (0.08, 0.9, ["Salon", "Spa", "Beauty Lounge", "Barbers"]),
}
FOUNDERS = {
    "NG": ["Adebayo", "Chukwuma", "Ngozi", "Emeka", "Funke", "Tunde", "Aisha", "Ifeanyi", "Yetunde", "Musa", "Chiamaka", "Olumide", "Halima", "Obinna", "Bisi", "Mama Nkechi", "Iya Bose", "Alhaji Sani"],
    "KE": ["Wanjiru", "Otieno", "Kamau", "Achieng", "Mwangi", "Njeri", "Kiprono", "Wairimu", "Ochieng", "Muthoni"],
    "GH": ["Kofi", "Ama", "Kwame", "Akosua", "Yaw", "Efua", "Kojo", "Abena", "Nana", "Esi"],
    "ZA": ["Thabo", "Naledi", "Sipho", "Lerato", "Pieter", "Zanele", "Bongani", "Anele", "Johan", "Precious"],
    "EG": ["El Masry", "Nour", "Farouk", "Hassan", "Mostafa", "Salma", "Youssef", "Mariam", "Khaled", "Amira"],
}
PLACES = {
    "NG": ["Ikeja", "Lekki", "Surulere", "Wuse", "Garki", "Yaba", "Aba", "Onitsha", "Ogba", "Ajah"],
    "KE": ["Westlands", "Kilimani", "Karen", "Thika", "Rongai", "Kasarani"],
    "GH": ["Osu", "Labone", "Adabraka", "Tema", "Madina", "East Legon"],
    "ZA": ["Sandton", "Soweto", "Rosebank", "Khayelitsha", "Umhlanga", "Midrand"],
    "EG": ["Zamalek", "Heliopolis", "Maadi", "Nasr City", "Dokki", "Smouha"],
}
SUFFIXES = {
    "NG": ["Ltd", "Enterprises", "Ventures", "& Sons", "Nigeria Ltd", "Global Ltd", ""],
    "KE": ["Ltd", "Enterprises", "Kenya Ltd", "& Co", ""],
    "GH": ["Ltd", "Enterprise", "Ventures", "Ghana Ltd", ""],
    "ZA": ["(Pty) Ltd", "CC", "Holdings", "& Co", ""],
    "EG": ["Co.", "Group", "for Trade", "Egypt", ""],
}
RISK_TIERS = ["low", "medium", "high"]

# Share of payments in each local hour: quiet overnight, a lunch peak and a bigger evening peak.
HOURS = [0.6, 0.35, 0.2, 0.15, 0.15, 0.3, 0.9, 2.0, 3.4, 4.2, 4.8, 5.4, 6.6, 6.9, 5.6, 5.0, 5.2, 6.0, 7.2, 7.4, 6.4, 4.6, 2.8, 1.4]
# Monday to Sunday.
WEEKDAYS = [0.96, 0.94, 0.97, 1.0, 1.18, 1.22, 0.83]


def learner_seed():
    """A stable seed per learner: the owner of the repository's GitHub remote. Falls back to 42."""
    try:
        url = subprocess.run(["git", "remote", "get-url", "origin"], capture_output=True, text=True, timeout=5).stdout
        owner = re.search(r"github\.com[:/]([^/]+)/", url)
        if owner:
            return int.from_bytes(hashlib.sha256(owner.group(1).lower().encode()).digest()[:4], "big")
    except (OSError, subprocess.SubprocessError):
        pass
    return 42


def weighted(rng, pairs):
    roll, total = rng.random() * sum(w for _, w in pairs), 0.0
    for value, weight in pairs:
        total += weight
        if roll < total:
            return value
    return pairs[-1][0]


class Picker:
    """weighted() for the hot loop: the cumulative weights are built once."""

    def __init__(self, pairs):
        import itertools
        self.values = [value for value, _ in pairs]
        self.totals = list(itertools.accumulate(weight for _, weight in pairs))

    def __call__(self, rng):
        import bisect
        return self.values[min(len(self.values) - 1, bisect.bisect_right(self.totals, rng.random() * self.totals[-1]))]


def iso(moment):
    return moment.isoformat()[:19] + "Z" if moment else ""  # strftime is several times slower on Windows


def day_weight(day, index, days):
    """Relative volume for one calendar day: weekday pattern, paydays, December, and steady growth."""
    weight = WEEKDAYS[day.weekday()]
    if day.day >= 25 or day.day <= 2:
        weight *= 1.25  # salaries land at month end
    if day.month == 12:
        weight *= 1.2 + (0.5 if 15 <= day.day <= 24 else 0)  # Christmas shopping
    if (day.month, day.day) in {(1, 1), (12, 25), (12, 26)}:
        weight *= 0.55  # public holidays are quiet
    return weight * (1 + 0.35 * index / max(1, days))  # the processor grows through the year


def next_business_day(moment):
    day = moment.date() + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day


def build_merchants(rng, count, days):
    merchants, names = [], set()
    countries = [(code, info[2]) for code, info in COUNTRIES.items()]
    categories = [(name, info[0]) for name, info in CATEGORIES.items()]
    for i in range(1, count + 1):
        country = weighted(rng, countries)
        category = weighted(rng, categories)
        city = weighted(rng, COUNTRIES[country][4])
        noun = rng.choice(CATEGORIES[category][2])
        lead = rng.choice(FOUNDERS[country]) if rng.random() < 0.65 else rng.choice(PLACES[country])
        name = " ".join(part for part in [lead, noun, rng.choice(SUFFIXES[country])] if part)
        if name in names:
            name = f"{name} ({rng.choice(PLACES[country])})"
        if name in names:
            name = f"{name} {i}"
        names.add(name)
        # Most merchants were live before the year began; the rest onboard during it.
        onboarded = START - timedelta(days=rng.randint(30, 1500)) if rng.random() < 0.8 else START + timedelta(days=rng.randint(0, days - 30))
        # About one in ten merchants was put in the wrong category at signup; a later reclassification corrects it.
        listed = rng.choice([c for c in CATEGORIES if c != category]) if rng.random() < 0.1 else category
        merchants.append({
            "merchant_id": f"M{i:05d}", "name": name, "category": listed, "country": country, "city": city, "_true": category,
            "risk_tier": weighted(rng, [("low", 0.7), ("medium", 0.24), ("high", 0.06)]),
            "onboarded_at": iso(onboarded.replace(hour=rng.randint(8, 17), minute=rng.randint(0, 59))),
            "_onboarded": onboarded,
            # A few merchants take most of the volume.
            "_weight": 1 / (i ** 0.85),
        })
    return merchants


def write_merchants(rng, merchants, out, days):
    fields = ["merchant_id", "name", "category", "country", "city", "risk_tier", "onboarded_at"]
    with open(os.path.join(out, "merchants.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(fields)
        for merchant in merchants:
            writer.writerow([merchant[f] for f in fields])

    # Each change is a full new version of the merchant, the way a CDC feed or a partner export sends it.
    changes = []
    for merchant in rng.sample(merchants, k=max(1, len(merchants) // 6)):
        current = dict(merchant)
        when = max(START, merchant["_onboarded"]) + timedelta(days=rng.randint(20, 60))
        for _ in range(rng.choice([1, 1, 1, 2, 2, 3])):
            if when >= START + timedelta(days=days - 5):
                break
            options = [("risk_review", 0.6), ("rebrand", 0.15), ("relocated", 0.25)]
            if current["category"] != current["_true"]:
                options.append(("reclassified", 1.5))
            reason = weighted(rng, options)
            if reason == "reclassified":
                current["category"] = current["_true"]
            elif reason == "risk_review":
                current["risk_tier"] = rng.choice([t for t in RISK_TIERS if t != current["risk_tier"]])
            elif reason == "rebrand":
                current["name"] = f"{rng.choice(FOUNDERS[current['country']])} {rng.choice(CATEGORIES[current['category']][2])} {rng.choice(SUFFIXES[current['country']])}".strip()
            else:
                current["city"] = weighted(rng, [c for c in COUNTRIES[current["country"]][4] if c[0] != current["city"]])
            changed_at = when.replace(hour=rng.randint(7, 19), minute=rng.randint(0, 59), second=rng.randint(0, 59))
            changes.append([current["merchant_id"], current["name"], current["category"], current["country"], current["city"], current["risk_tier"], iso(changed_at), reason])
            when += timedelta(days=rng.randint(25, 120))
    changes.sort(key=lambda row: row[6])
    with open(os.path.join(out, "merchant_changes.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["merchant_id", "name", "category", "country", "city", "risk_tier", "valid_from", "change_reason"])
        writer.writerows(changes)
    return len(changes)


def fee_for(method, amount_minor, country):
    percent, _, fixed = FEES[method]
    fee = round(amount_minor * percent) + fixed * 100
    if method == "card" and country == "NG":
        fee = min(fee, NG_CARD_CAP * 100)
    return fee


def amount_for(rng, country, category):
    typical = COUNTRIES[country][3] * CATEGORIES[category][1]
    major = rng.lognormvariate(math.log(typical), 0.85)
    # Prices are usually round: whole units, often ending in 0 or 50.
    step = 50 if major > 2000 else 10 if major > 200 else 1
    major = max(step, round(major / step) * step)
    if rng.random() < 0.15:
        major = max(1, major - (1 if step == 1 else step // 10))  # the odd 4,990-style price
    return int(major * 100)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rows", type=int, default=1_000_000, help="payments in ledger.csv, before partner duplicates")
    parser.add_argument("--days", type=int, default=366, help="history length in days, from 2024-01-01 (2024 is a leap year)")
    parser.add_argument("--merchants", type=int, default=2_000)
    parser.add_argument("--accounts", type=int, default=250_000)
    parser.add_argument("--events", type=float, default=0.1, help="share of payments that also emit events.jsonl (0 to skip)")
    parser.add_argument("--seed", type=int, default=None, help="default: derived from your GitHub username")
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    if args.seed is None:
        args.seed = learner_seed()
    rng = random.Random(args.seed)
    defects = random.Random(args.seed + 1)  # separate stream, so defects never shift the clean data
    os.makedirs(args.out, exist_ok=True)
    end = START + timedelta(days=args.days)

    merchants = build_merchants(rng, args.merchants, args.days)
    change_count = write_merchants(rng, merchants, args.out, args.days)
    by_country = {code: [m for m in merchants if m["country"] == code] for code in COUNTRIES}
    pick_country = Picker([(code, info[2]) for code, info in COUNTRIES.items() if by_country[code]])
    pick_hour = Picker(list(enumerate(HOURS)))
    pick_method = {code: Picker(pairs) for code, pairs in METHODS.items()}
    pick_brand = {code: Picker(CARD_BRANDS.get(code, CARD_BRANDS["default"])) for code in COUNTRIES}

    # Spread the rows over the days by expected volume (largest remainder, so the total is exact).
    weights = [day_weight((START + timedelta(days=d)).date(), d, args.days) for d in range(args.days)]
    raw = [args.rows * w / sum(weights) for w in weights]
    per_day = [int(x) for x in raw]
    for d in sorted(range(args.days), key=lambda d: raw[d] - per_day[d], reverse=True)[: args.rows - sum(per_day)]:
        per_day[d] += 1

    # Cumulative merchant weights per country, so picking stays fast at 10M rows.
    import bisect
    cumulative = {}
    for code, group in by_country.items():
        running, totals = 0.0, []
        for merchant in group:
            running += merchant["_weight"]
            totals.append(running)
        cumulative[code] = totals

    def pick_merchant(country, moment):
        group, totals = by_country[country], cumulative[country]
        for _ in range(6):
            merchant = group[bisect.bisect_left(totals, rng.random() * totals[-1])]
            if merchant["_onboarded"] <= moment:
                return merchant
        return group[0]

    accounts_per_country = {code: max(1, int(args.accounts * info[2])) for code, info in COUNTRIES.items()}
    account_base, base = {}, 1
    for code in COUNTRIES:
        account_base[code] = base
        base += accounts_per_country[code]

    payouts = {}
    rand = rng.random

    def ri(low, high):  # rng.randint, about four times faster; matters at 10M rows
        return low + int(rand() * (high - low + 1))

    columns = ["txn_id", "reference", "merchant_id", "account_id", "amount_minor", "fee_minor", "net_minor", "currency",
               "payment_method", "card_brand", "status", "created_at", "settled_at", "reversed_at", "updated_at",
               "settlement_id", "terminal_id"]
    ledger = open(os.path.join(args.out, "ledger.csv"), "w", newline="")
    writer = csv.writer(ledger)
    writer.writerow(columns)
    events = open(os.path.join(args.out, "events.jsonl"), "w") if args.events else None
    event_buffer = []
    n = duplicates = event_count = 0
    pending_dupe = None

    for d in range(args.days):
        day_start = START + timedelta(days=d)
        moments = []
        for _ in range(per_day[d]):
            country = pick_country(rng)
            utc_hour = (pick_hour(rng) - COUNTRIES[country][1]) % 24
            moments.append((day_start + timedelta(hours=utc_hour, seconds=ri(0, 3599)), country))
        moments.sort(key=lambda item: item[0])

        for created, country in moments:
            n += 1
            merchant = pick_merchant(country, created)
            method = pick_method[country](rng)
            amount = amount_for(rng, country, merchant["category"])
            brand = pick_brand[country](rng) if method == "card" else ""
            # Failure rates differ by method: USSD and transfers time out more often than cards.
            fail_rate = {"card": 0.035, "bank_transfer": 0.05, "ussd": 0.08, "mobile_money": 0.04}[method]
            settled = reversed_at = None
            fee = 0
            if rng.random() < fail_rate:
                status = "failed"
            else:
                settle_day = next_business_day(created)
                settled = datetime(settle_day.year, settle_day.month, settle_day.day, tzinfo=timezone.utc) + timedelta(hours=ri(5, 9), minutes=ri(0, 59), seconds=ri(0, 59))
                fee = fee_for(method, amount, country)
                status = "settled"
                roll = rng.random()
                if roll < 0.018:
                    status, reversed_at = "reversed", settled + timedelta(days=ri(1, 20), hours=ri(0, 12))
                elif method == "card" and roll < 0.018 + 0.004:
                    status, reversed_at = "charged_back", settled + timedelta(days=ri(14, 75))
                # Anything not yet true by the end of the data stays in its earlier state.
                if reversed_at and reversed_at > end:
                    status, reversed_at = "settled", None
                if settled > end:
                    status, settled, fee = "pending", None, 0
            updated = max(t for t in [created, settled, reversed_at] if t)
            account_index = account_base[country] + int(accounts_per_country[country] * rng.random() ** 1.6)
            settlement_id = f"STL-{settled.year}{settled.month:02d}{settled.day:02d}-{merchant['merchant_id']}" if settled else ""
            terminal = f"POS{int(merchant['merchant_id'][1:]) * 4 + ri(0, 3):07d}" if method == "card" and created >= TERMINAL_ID_FROM else ""
            row = [f"T{n:010d}", f"{rng.choice(['ORD', 'INV', 'PAY', 'TX'])}-{ri(10_000, 9_999_999)}", merchant["merchant_id"],
                   f"A{account_index:07d}", amount, fee, amount - fee, COUNTRIES[country][0], method, brand, status,
                   iso(created), iso(settled), iso(reversed_at), iso(updated), settlement_id, terminal]
            if settled:
                key = (settlement_id, merchant["merchant_id"], COUNTRIES[country][0], settled.date())
                batch = payouts.setdefault(key, [0, 0, 0, settled])
                batch[0] += amount
                batch[1] += fee
                batch[2] += 1
                batch[3] = max(batch[3], settled)

            # Real partner feeds are slightly dirty. These defects are rare and stay the same for a given seed.
            roll = defects.random()
            if roll < 0.0008:
                row[7] = row[7].lower()  # a currency sent in lower case
            elif roll < 0.0012:
                row[3] = ""  # a payment with no account
            elif roll < 0.0014 and row[12]:
                offset = COUNTRIES[country][1]
                row[12] = (settled + timedelta(hours=offset)).strftime("%Y-%m-%dT%H:%M:%S") + f"+{offset:02d}:00"  # sent in local time
            writer.writerow(row)
            if pending_dupe is not None:
                writer.writerow(pending_dupe)  # the partner re-sent an earlier payment
                duplicates += 1
                pending_dupe = None
            if defects.random() < 0.0006:
                pending_dupe = row

            if events and rng.random() < args.events:
                event_count += 1
                txn = row[0]
                base = {"txn_id": txn, "merchant_id": merchant["merchant_id"], "account_id": row[3] or None,
                        "amount_minor": amount, "currency": COUNTRIES[country][0], "payment_method": method}
                authorised = created - timedelta(seconds=ri(1, 20))
                lifecycle = [("authorization", authorised), ("decline" if status == "failed" else "capture", created)]
                if settled:
                    lifecycle.append(("settlement", settled))
                if reversed_at:
                    lifecycle.append(("refund" if status == "reversed" else "chargeback", reversed_at))
                for kind, at in lifecycle:
                    # Most events arrive within a second; a few arrive minutes late, and some are delivered twice.
                    delay = rng.expovariate(2.0) if rng.random() > 0.01 else rng.uniform(60, 1800)
                    event = {"event_id": f"evt_{hashlib.sha1(f'{txn}{kind}'.encode()).hexdigest()[:16]}", "type": kind,
                             "at": iso(at), **base}
                    event_buffer.append((at + timedelta(seconds=delay), event))
                    if rng.random() < 0.008:
                        event_buffer.append((at + timedelta(seconds=delay + rng.uniform(1, 30)), event))

        # Write the events that have certainly arrived by the end of this day, in arrival order.
        if events:
            cutoff = day_start + timedelta(days=1) - timedelta(hours=1)
            event_buffer.sort(key=lambda item: item[0])
            ready = [item for item in event_buffer if item[0] < cutoff]
            event_buffer = [item for item in event_buffer if item[0] >= cutoff]
            for arrived, event in ready:
                if arrived <= end:
                    events.write(json.dumps({**event, "received_at": iso(arrived)}) + "\n")

    if pending_dupe is not None:
        writer.writerow(pending_dupe)
        duplicates += 1
    ledger.close()
    if events:
        for arrived, event in sorted(event_buffer, key=lambda item: item[0]):
            if arrived <= end:
                events.write(json.dumps({**event, "received_at": iso(arrived)}) + "\n")
        events.close()

    with open(os.path.join(args.out, "payouts.csv"), "w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["settlement_id", "merchant_id", "currency", "settlement_date", "gross_minor", "fees_minor", "net_minor", "txn_count", "paid_at"])
        for (settlement_id, merchant_id, currency, day), (gross, fees, count, last) in sorted(payouts.items(), key=lambda item: (item[0][3], item[0][0])):
            paid = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(hours=11)
            writer.writerow([settlement_id, merchant_id, currency, day.isoformat(), gross, fees, gross - fees, count, iso(max(paid, last))])

    print(f"wrote {args.rows} payments (+{duplicates} partner duplicates), {len(payouts)} payouts, {args.merchants} merchants "
          f"({change_count} changes){f', events for {event_count} payments' if events else ''} to {args.out}/ (seed {args.seed})")


if __name__ == "__main__":
    main()
