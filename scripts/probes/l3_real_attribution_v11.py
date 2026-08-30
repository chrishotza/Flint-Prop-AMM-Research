import sys
import asyncio
import time
import json
import csv
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone

import grpc

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"

OUT = (
    ROOT
    / "results"
    / "l3_real_attribution_v11"
)

OUT.mkdir(
    parents=True,
    exist_ok=True
)

sys.path.insert(
    0,
    str(GENERATED)
)

import api_pb2
import api_pb2_grpc


HOST = "mainnet.api.flint.trade:443"

PAIR = "USDT/USDC"

CAPTURE_SECONDS = 15


def num(x):

    try:

        if x is None:
            return None

        if hasattr(
            x,
            "value"
        ):

            return float(
                x.value
            )

        return float(x)

    except Exception:

        return None


def get_maker_id(
    maker_id
):

    # IMPORTANT:
    # Read protobuf field directly.
    # Do not use MessageToDict because default
    # scalar values may disappear from JSON.

    try:

        return int(
            maker_id.id
        )

    except Exception:

        return None


async def main():

    print("=" * 100)
    print("FLINT REAL L3 ATTRIBUTION v1.1")
    print("=" * 100)

    print()
    print(
        "HOST:",
        HOST
    )

    print(
        "PAIR:",
        PAIR
    )

    print(
        "CAPTURE:",
        CAPTURE_SECONDS,
        "seconds"
    )

    print()
    print(
        "READ ONLY — NO TRANSACTIONS."
    )

    # ============================================================
    # CONNECT
    # ============================================================

    print()
    print("[1] CONNECT")

    channel = grpc.aio.secure_channel(

        HOST,

        grpc.ssl_channel_credentials()
    )

    market = (
        api_pb2_grpc
        .MarketDataServiceStub(
            channel
        )
    )

    print(
        "[OK]",
        HOST
    )

    # ============================================================
    # PAIRS
    # ============================================================

    print()
    print("[2] LOOKUP PAIR")

    response = await market.ListPairs(
        api_pb2.ListPairsRequest(),
        timeout=20
    )

    catalog = {

        item.pair.label:
            item

        for item
        in response.pairs
    }

    if PAIR not in catalog:

        await channel.close()

        raise RuntimeError(
            f"Pair not found: {PAIR}"
        )

    item = catalog[PAIR]

    pair = api_pb2.Pair(

        base_id=int(
            item.pair.base_id
        ),

        base=item.pair.base,

        quote_id=int(
            item.pair.quote_id
        ),

        quote=item.pair.quote,

        label=PAIR
    )

    print(
        "[FOUND]",
        PAIR
    )

    print(
        "base decimals:",
        item.base.decimals
    )

    print(
        "quote decimals:",
        item.quote.decimals
    )

    # ============================================================
    # L3 LEVEL
    # ============================================================

    print()
    print("[3] L3 SUBSCRIPTION")

    level = api_pb2.FEED_LEVEL_L3

    print(
        "FEED_LEVEL_L3 =",
        level
    )

    request = api_pb2.SubscribeRequest(

        pairs=[

            api_pb2.PairSubscription(

                pair=pair,

                level=level,

                snapshot_only=True
            )
        ]
    )

    stream = market.Subscribe(
        request
    )

    # ============================================================
    # STORAGE
    # ============================================================

    snapshots = []

    maker_totals = defaultdict(
        lambda: {

            "bid_size": 0.0,

            "ask_size": 0.0,

            "bid_notional": 0.0,

            "ask_notional": 0.0,

            "bid_orders": 0,

            "ask_orders": 0
        }
    )

    deadline = (
        time.monotonic()
        +
        CAPTURE_SECONDS
    )

    event_count = 0

    print(
        "\nSTREAM STARTED"
    )

    try:

        while (
            time.monotonic()
            <
            deadline
        ):

            remaining = (

                deadline
                -
                time.monotonic()
            )

            try:

                event = await asyncio.wait_for(

                    stream.read(),

                    timeout=max(
                        0.25,
                        remaining
                    )
                )

            except asyncio.TimeoutError:

                break

            if event is grpc.aio.EOF:

                print(
                    "EOF"
                )

                break

            if event is None:

                break

            try:

                kind = event.WhichOneof(
                    "kind"
                )

            except Exception:

                kind = None

            if kind != "l3_snapshot":

                continue

            event_count += 1

            snap = event.l3_snapshot

            # ----------------------------------------------------
            # Metadata
            # ----------------------------------------------------

            slot = int(
                snap.metadata.slot
            )

            try:

                timestamp_us = int(
                    snap.metadata.ts.micros
                )

            except Exception:

                timestamp_us = None

            # ----------------------------------------------------
            # PROCESS SIDES
            # ----------------------------------------------------

            print()
            print(
                "=" * 90
            )

            print(
                f"L3 SNAPSHOT #{event_count}"
            )

            print(
                "slot:",
                slot
            )

            print(
                "timestamp_us:",
                timestamp_us
            )

            print(
                "=" * 90
            )

            snapshot_record = {

                "pair":
                    PAIR,

                "slot":
                    slot,

                "timestamp_us":
                    timestamp_us,

                "bids":
                    [],

                "asks":
                    []
            }

            for side_name, entries in [

                (
                    "BID",
                    snap.bids
                ),

                (
                    "ASK",
                    snap.asks
                )
            ]:

                print()
                print(
                    side_name
                )

                print(

                    f"{'N':>4} "
                    f"{'MAKER_ID':>12} "
                    f"{'PRICE':>18} "
                    f"{'SIZE':>18} "
                    f"{'NOTIONAL':>18}"
                )

                print(
                    "-" * 75
                )

                for i, entry in enumerate(
                    entries,
                    1
                ):

                    maker_id = get_maker_id(
                        entry.maker_id
                    )

                    price = num(
                        entry.price
                    )

                    size = num(
                        entry.size
                    )

                    notional = None

                    if (
                        price is not None
                        and
                        size is not None
                    ):

                        notional = (
                            price *
                            size
                        )

                    print(

                        f"{i:>4} "
                        f"{str(maker_id):>12} "
                        f"{str(price):>18} "
                        f"{str(size):>18} "
                        f"{str(notional):>18}"
                    )

                    snapshot_record[
                        "bids"
                        if side_name == "BID"
                        else "asks"
                    ].append({

                        "maker_id":
                            maker_id,

                        "price":
                            price,

                        "size":
                            size,

                        "notional":
                            notional
                    })

                    key = (
                        maker_id
                        if maker_id is not None
                        else "UNKNOWN"
                    )

                    if side_name == "BID":

                        maker_totals[
                            key
                        ][
                            "bid_size"
                        ] += (
                            size or 0
                        )

                        maker_totals[
                            key
                        ][
                            "bid_notional"
                        ] += (
                            notional or 0
                        )

                        maker_totals[
                            key
                        ][
                            "bid_orders"
                        ] += 1

                    else:

                        maker_totals[
                            key
                        ][
                            "ask_size"
                        ] += (
                            size or 0
                        )

                        maker_totals[
                            key
                        ][
                            "ask_notional"
                        ] += (
                            notional or 0
                        )

                        maker_totals[
                            key
                        ][
                            "ask_orders"
                        ] += 1

            snapshots.append(
                snapshot_record
            )

    except Exception as error:

        print()
        print(
            "STREAM ERROR:",
            repr(error)
        )

    finally:

        try:
            stream.cancel()
        except Exception:
            pass

    # ============================================================
    # SUMMARY
    # ============================================================

    print()
    print(
        "=" * 100
    )

    print(
        "L3 SUMMARY"
    )

    print(
        "=" * 100
    )

    print(
        "snapshots:",
        len(snapshots)
    )

    print(
        "events:",
        event_count
    )

    print()

    print(
        f"{'MAKER_ID':>12} "
        f"{'BID ORD':>9} "
        f"{'BID SIZE':>12} "
        f"{'BID NOTIONAL':>15} "
        f"{'ASK ORD':>9} "
        f"{'ASK SIZE':>12} "
        f"{'ASK NOTIONAL':>15}"
    )

    print(
        "-" * 100
    )

    for maker_id, totals in sorted(
        maker_totals.items(),
        key=lambda x: (
            x[1]["bid_notional"]
            +
            x[1]["ask_notional"]
        ),
        reverse=True
    ):

        print(

            f"{str(maker_id):>12} "

            f"{totals['bid_orders']:>9} "

            f"{totals['bid_size']:>12.6f} "

            f"{totals['bid_notional']:>15.6f} "

            f"{totals['ask_orders']:>9} "

            f"{totals['ask_size']:>12.6f} "

            f"{totals['ask_notional']:>15.6f}"
        )

    # ============================================================
    # SIDE TOTALS
    # ============================================================

    total_bid = sum(

        totals["bid_notional"]

        for totals
        in maker_totals.values()
    )

    total_ask = sum(

        totals["ask_notional"]

        for totals
        in maker_totals.values()
    )

    print()
    print(
        "TOTAL BID NOTIONAL:",
        f"${total_bid:.6f}"
    )

    print(
        "TOTAL ASK NOTIONAL:",
        f"${total_ask:.6f}"
    )

    if total_ask > 0:

        print(
            "BID/ASK NOTIONAL RATIO:",
            f"{total_bid / total_ask:.6f}"
        )

    else:

        print(
            "BID/ASK NOTIONAL RATIO: undefined"
        )

    # ============================================================
    # RAW JSON
    # ============================================================

    raw_path = (
        OUT /
        "l3_snapshots.json"
    )

    raw_path.write_text(

        json.dumps(
            {
                "pair":
                    PAIR,

                "snapshot_count":
                    len(snapshots),

                "snapshots":
                    snapshots
            },
            indent=2
        ),

        encoding="utf-8"
    )

    # ============================================================
    # MAKER CSV
    # ============================================================

    maker_csv = (
        OUT /
        "maker_attribution.csv"
    )

    with open(
        maker_csv,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        writer = csv.writer(
            f
        )

        writer.writerow([

            "maker_id",

            "bid_orders",
            "bid_size",
            "bid_notional",

            "ask_orders",
            "ask_size",
            "ask_notional"
        ])

        for maker_id, totals in sorted(
            maker_totals.items()
        ):

            writer.writerow([

                maker_id,

                totals["bid_orders"],

                totals["bid_size"],

                totals["bid_notional"],

                totals["ask_orders"],

                totals["ask_size"],

                totals["ask_notional"]
            ])

    # ============================================================
    # FINAL
    # ============================================================

    master = {

        "experiment":
            "Flint Real L3 Attribution v1.1",

        "host":
            HOST,

        "pair":
            PAIR,

        "capture_seconds":
            CAPTURE_SECONDS,

        "snapshots":
            len(snapshots),

        "maker_count":
            len(maker_totals),

        "total_bid_notional":
            total_bid,

        "total_ask_notional":
            total_ask,

        "maker_attribution":
            {
                str(k): v
                for k, v
                in maker_totals.items()
            },

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }

    master_path = (
        OUT /
        "master.json"
    )

    master_path.write_text(

        json.dumps(
            master,
            indent=2
        ),

        encoding="utf-8"
    )

    await channel.close()

    print()
    print(
        "=" * 100
    )

    print(
        "DONE"
    )

    print(
        "=" * 100
    )

    print(
        "L3 JSON:",
        raw_path
    )

    print(
        "Maker CSV:",
        maker_csv
    )

    print(
        "Master:",
        master_path
    )

    print()
    print(
        "Terminal remains open."
    )


try:

    asyncio.run(
        main()
    )

except KeyboardInterrupt:

    print(
        "\nInterrupted."
    )

finally:

    input(
        "\nPress ENTER to close..."
    )

