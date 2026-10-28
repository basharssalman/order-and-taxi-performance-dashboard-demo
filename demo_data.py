"""Generate fictional data without reading credentials or production datasets."""
from datetime import date, timedelta
from pathlib import Path
import math
import random

import pandas as pd

DATA_VERSION = "v4"
# Relative hourly demand: delivery peaks at lunch/dinner; taxis at commute times.
DELIVERY_HOUR_WEIGHTS = [1, 1, 0.5, 0.5, 0.5, 1, 2, 3, 4, 5, 7, 10,
                         12, 10, 7, 6, 8, 12, 16, 18, 14, 9, 5, 2]
TAXI_HOUR_WEIGHTS = [3, 2, 1, 1, 1, 2, 5, 10, 12, 8, 5, 5,
                     6, 6, 5, 6, 9, 13, 12, 9, 7, 6, 5, 4]


def ensure_demo_data(directory: Path):
    """Generate once; reuse across restarts until removed or version changes."""
    today = date.today()
    marker = directory / "generated.txt"
    if (marker.exists() and marker.read_text().startswith(f"{DATA_VERSION}:")
            and (directory / "trips.parquet").exists()
            and any((directory / "orders_by_year").glob("orders_*.parquet"))):
        return
    rng = random.Random(42)
    orders, trips = [], []
    cities = ["Harbor City", "Maple Heights", "Riverdale"]
    merchants = ["Cedar Kitchen", "Moonlight Pizza", "Orchard Market", "Sunny Cafe"]
    for offset in range(731):
        day = today - timedelta(days=731 - offset)
        demand = (1 + offset / 1800) * (1.2 if day.weekday() >= 4 else 0.9)
        demand *= 1 + 0.12 * math.sin(offset / 30)
        for city_index, city in enumerate(cities):
            for merchant, hour in ((merchant, hour) for merchant in merchants for hour in range(24)):
                total = max(1, round(
                    rng.randint(25, 80) * demand
                    * DELIVERY_HOUR_WEIGHTS[hour] / sum(DELIVERY_HOUR_WEIGHTS)
                ))
                # Sample individual outcomes instead of rounding small hourly
                # counts down to zero. Merchants have different demo rates.
                merchant_index = merchants.index(merchant)
                cancel_rate = 0.12 + merchant_index * 0.02
                failed_rate = 0.06 + merchant_index * 0.01
                outcomes = rng.choices(
                    ["delivered", "canceled", "failed"],
                    weights=[1 - cancel_rate - failed_rate, cancel_rate, failed_rate],
                    k=total,
                )
                canceled = outcomes.count("canceled")
                failed = outcomes.count("failed")
                # Merchant cancellations and voids are subsets of cancellations.
                cancel_reasons = rng.choices(
                    ["merchant", "void", "other"], weights=[0.55, 0.25, 0.20], k=canceled,
                )
                delivered = total - canceled - failed
                unit_price = rng.uniform(15, 29)
                paid = round(delivered * unit_price, 2)
                fee = round(delivered * rng.uniform(2.5, 4.5), 2)
                earnings = round(fee * 0.72, 2)
                commission = round(paid * 0.18, 2)
                discount = round(paid * 0.04, 2)
                margin = round(commission + fee - earnings - discount, 2)
                orders.append(dict(
                    date=pd.Timestamp(day) + pd.Timedelta(hours=hour), city=city,
                    merchant_city=f"{merchant} - {city}",
                    channel=rng.choice(["App", "Web"]),
                    account_manager=f"Demo Manager {city_index + 1}",
                    total_orders=total, cancelled_orders=canceled,
                    delivered_orders=delivered, not_delivered_orders=failed,
                    total_orders_amount=round(unit_price * total, 2),
                    delivered_paid_amount=paid, discount_funded=discount,
                    sum_delivery_fee=fee, sum_driver_earning=earnings,
                    service_commission=commission, firstlevel=margin,
                    l2_l1=round(margin * 0.85, 2),
                    tiptop_share_delivery_discount=round(discount * 0.4, 2),
                    coupon_on_delivery_tiptop_share=round(discount * 0.2, 2),
                    coupon_on_item_discount_tiptop_share=round(discount * 0.4, 2),
                    direct_distance=round(delivered * rng.uniform(2, 6), 2),
                    voided_orders=cancel_reasons.count("void"),
                    canceled_orders_by_branch=cancel_reasons.count("merchant"),
                ))
            for _ in range(int(24 * demand)):
                status = rng.choices(
                    ["Successful", "Canceled Before Reserved", "Canceled After Reserved",
                     "Canceled After Arrived", "Canceled After Start", "Timeout"],
                    weights=[82, 5, 4, 3, 1, 5],
                )[0]
                distance = rng.uniform(1, 18)
                price = round(3 + distance * 1.4, 2)
                trips.append(dict(
                    request_id=f"demo-trip-{len(trips) + 1}",
                    created_date=pd.Timestamp(day) + pd.Timedelta(
                        hours=rng.choices(range(24), weights=TAXI_HOUR_WEIGHTS)[0],
                        minutes=rng.randrange(60), seconds=rng.randrange(60),
                    ), city=city,
                    user_id=f"demo-user-{rng.randint(1, 1800)}",
                    driver_id=f"demo-driver-{rng.randint(1, 240)}",
                    status=status, price=price, distance=round(distance, 2),
                    duration=round(distance * rng.uniform(2, 4), 2),
                    tiptop_commission=round(price * 0.2, 2) if status == "Successful" else 0,
                    offer_discount_amount=round(price * 0.03, 2),
                    coupon_discount_amount=round(price * rng.choice([0, 0, 0.08]), 2),
                ))
    partitions = directory / "orders_by_year"
    partitions.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(orders)
    for year, group in frame.groupby(frame.date.dt.year):
        group.to_parquet(partitions / f"orders_{year}.parquet", index=False)
    # App derives fresh filter choices from these synthetic partitions.
    (partitions / "_metadata.json").unlink(missing_ok=True)
    pd.DataFrame(trips).to_parquet(directory / "trips.parquet", index=False)
    marker.write_text(f"{DATA_VERSION}:{today}")
