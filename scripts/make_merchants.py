"""Create the small merchant reference table (the 'dimension') used to enrich payments.

Only M1..M290 are written on purpose: the producer also sends M291..M300, so ten merchants
will NOT match in the join. That gives you a real data-quality case (category = 'UNKNOWN').
"""
import csv
import random
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reference" / "merchants.csv"

CATEGORIES = ["grocery", "fuel", "electronics", "travel", "restaurants", "pharmacy", "fashion", "gaming", "utilities", "jewellery"]
COUNTRIES = ["IN", "IN", "IN", "AE", "SG", "GB", "US"]
HIGH_RISK = {"gaming", "jewellery", "electronics"}


def main() -> None:
    rng = random.Random(7)  # fixed seed: the reference table is stable between runs
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["merchant_id", "merchant_name", "category", "country", "risk_tier"])
        for i in range(1, 291):
            cat = rng.choice(CATEGORIES)
            tier = "HIGH" if cat in HIGH_RISK and rng.random() < 0.6 else rng.choice(["LOW", "LOW", "MEDIUM"])
            w.writerow([f"M{i}", f"Merchant {i}", cat, rng.choice(COUNTRIES), tier])
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
