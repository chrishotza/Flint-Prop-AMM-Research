import sys
import asyncio
import time
import json
import csv
import traceback
from pathlib import Path
from datetime import datetime, timezone

import grpc

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"

OUT = (
    ROOT
    / "results"
    / "stream_semantics_probe_v22"
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
PAIR_LABEL = "USDT/USDC"

CAPTURE_SECONDS = 20
HEARTBEAT_SECONDS = 2
READ_TIMEOUT_SECONDS = 2


# ============================================================
# HELPERS
# ============================================================

def ts_us_from_snapshot(obj):

    try:
        return int(obj.ts.micros)
    except Exception:
        pass

    try:
        return int(obj.metadata.ts.micros)
    except Exception:
        pass

    return None


def slot_from_snapshot(obj):

    try:
        return int(obj.slot)
    except Exception:
        pass

    try:
        return int(obj.metadata.slot)
    except Exception:
        pass

    return None


def build_pair(item):

    pair = api_pb2.Pair()

    pair.base_id = int(
        item.pair.base_id
    )

    pair.base = str(
        item.pair.base
    )

    pair.quote_id = int(
        item.pair.quote_id
    )

    pair.quote = str(
        item.pair.quote
    )

    pair.label = str(
        item.pair.label
    )

    return pair


def event_kind(event):

    try:
        return event.WhichOneof(
            "kind"
        )
    except Exception:
        return None


def inspect_snapshot(
    snap
):

    result = {

        "timestamp_us":
            ts_us_from_snapshot(
                snap.metadata
            ),

        "slot":
            slot_from_snapshot(
                snap.metadata
            ),

        "bid_count":
            len(
                snap.bids
            ),

        "ask_count":
            len(
                snap.asks
            ),

        "bids":
            [],

        "asks":
            []
    }

    for level in snap.bids:

        try:

            price = float(
                level.price.value
            )

        except Exception:

            try:
                price = float(
                    level.price
                )
            except Exception:
                price = None

        try:

            size = float(
                level.size.value
            )

        except Exception:

            try:
                size = float(
                    level.size
                )
            except Exception:
                size = None

        result["bids"].append({

            "price":
                price,

            "size":
                size,

            "notional":
                (
                    price * size
                    if (
                        price is not None
                        and
                        size is not None
                    )
                    else None
                )
        })

    for level in snap.asks:

        try:

            price = float(
                level.price.value
            )

        except Exception:

            try:
                price = float(
                    level.price
                )
            except Exception:
                price = None

        try:

            size = float(
                level.size.value
            )

        except Exception:

            try:
                size = float(
                    level.size
                )
            except Exception:
                size = None

        result["asks"].append({

            "price":
                price,

            "size":
                size,

            "notional":
                (
                    price * size
                    if (
                        price is not None
                        and
                        size is not None
                    )
                    else None
                )
        })

    return result


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 100)
    print("FLINT STREAM SEMANTICS PROBE v2.2")
    print("=" * 100)

    print()
    print("HOST:", HOST)
    print("PAIR:", PAIR_LABEL)
    print("CAPTURE:", CAPTURE_SECONDS, "seconds")

    print()
    print(
        "TESTING:"
    )

    print(
        "  A = L2 snapshot_only=True"
    )

    print(
        "  B = L2 snapshot_only=False"
    )

    print(
        "  C = L3 snapshot_only=False"
    )

    print()
    print(
        "READ ONLY — NO TRANSACTIONS."
    )

    # ========================================================
    # CONNECT
    # ========================================================

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

    # ========================================================
    # PAIR
    # ========================================================

    print()
    print("[2] PAIR LOOKUP")

    response = await market.ListPairs(
        api_pb2.ListPairsRequest(),
        timeout=15
    )

    target = None

    for item in response.pairs:

        try:

            label = str(
                item.pair.label
            )

        except Exception:

            continue

        if label == PAIR_LABEL:

            target = item
            break

    if target is None:

        await channel.close()

        raise RuntimeError(
            f"Pair not found: {PAIR_LABEL}"
        )

    pair = build_pair(
        target
    )

    print(
        "registered:",
        len(response.pairs)
    )

    print(
        "[FOUND]",
        PAIR_LABEL
    )

    # ========================================================
    # STORAGE
    # ========================================================

    results = {

        "L2_SNAPSHOT_ONLY_TRUE": [],

        "L2_SNAPSHOT_ONLY_FALSE": [],

        "L3_SNAPSHOT_ONLY_FALSE": []
    }

    errors = {}

    start = time.monotonic()

    # ========================================================
    # GENERIC STREAM WORKER
    # ========================================================

    async def worker(
        name,
        level,
        snapshot_only
    ):

        stream = None

        try:

            print()
            print(
                f"[{name}] START"
            )

            request = api_pb2.SubscribeRequest(

                pairs=[

                    api_pb2.PairSubscription(

                        pair=pair,

                        level=level,

                        snapshot_only=
                            snapshot_only
                    )
                ]
            )

            stream = market.Subscribe(
                request
            )

            deadline = (
                time.monotonic()
                +
                CAPTURE_SECONDS
            )

            count = 0

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

                        timeout=min(
                            READ_TIMEOUT_SECONDS,
                            max(
                                0.25,
                                remaining
                            )
                        )
                    )

                except asyncio.TimeoutError:

                    continue

                if event is grpc.aio.EOF:

                    print(
                        f"[{name}] EOF"
                    )

                    break

                if event is None:

                    break

                kind = event_kind(
                    event
                )

                # ------------------------------------------------
                # Record every event kind.
                # ------------------------------------------------

                record = {

                    "received_local_us":
                        int(
                            time.time()
                            *
                            1_000_000
                        ),

                    "kind":
                        kind
                }

                # ------------------------------------------------
                # L2
                # ------------------------------------------------

                if kind == "l2_snapshot":

                    snap = inspect_snapshot(
                        event.l2_snapshot
                    )

                    record[
                        "snapshot"
                    ] = snap

                    count += 1

                    results[
                        name
                    ].append(
                        record
                    )

                    bid = (
                        snap["bids"][0]["price"]
                        if snap["bids"]
                        else None
                    )

                    ask = (
                        snap["asks"][0]["price"]
                        if snap["asks"]
                        else None
                    )

                    print(

                        f"[{name}] "
                        f"#{count:<3} "
                        f"slot={snap['slot']} "
                        f"bid={bid} "
                        f"ask={ask} "
                        f"bids={snap['bid_count']} "
                        f"asks={snap['ask_count']}"
                    )

                # ------------------------------------------------
                # L3
                # ------------------------------------------------

                elif kind == "l3_snapshot":

                    snap = event.l3_snapshot

                    record[
                        "snapshot"
                    ] = {

                        "timestamp_us":
                            ts_us_from_snapshot(
                                snap.metadata
                            ),

                        "slot":
                            slot_from_snapshot(
                                snap.metadata
                            ),

                        "bid_count":
                            len(
                                snap.bids
                            ),

                        "ask_count":
                            len(
                                snap.asks
                            )
                    }

                    count += 1

                    # Save maker/price/size.
                    record[
                        "bids"
                    ] = []

                    record[
                        "asks"
                    ] = []

                    for entry in snap.bids:

                        try:

                            maker_id = int(
                                entry
                                .maker_id
                                .id
                            )

                        except Exception:

                            maker_id = None

                        try:

                            price = float(
                                entry
                                .price
                                .value
                            )

                        except Exception:

                            price = None

                        try:

                            size = float(
                                entry
                                .size
                                .value
                            )

                        except Exception:

                            size = None

                        record[
                            "bids"
                        ].append({

                            "maker_id":
                                maker_id,

                            "price":
                                price,

                            "size":
                                size
                        })

                    for entry in snap.asks:

                        try:

                            maker_id = int(
                                entry
                                .maker_id
                                .id
                            )

                        except Exception:

                            maker_id = None

                        try:

                            price = float(
                                entry
                                .price
                                .value
                            )

                        except Exception:

                            price = None

                        try:

                            size = float(
                                entry
                                .size
                                .value
                            )

                        except Exception:

                            size = None

                        record[
                            "asks"
                        ].append({

                            "maker_id":
                                maker_id,

                            "price":
                                price,

                            "size":
                                size
                        })

                    results[
                        name
                    ].append(
                        record
                    )

                    print(

                        f"[{name}] "
                        f"#{count:<3} "
                        f"slot={record['snapshot']['slot']} "
                        f"bids={record['snapshot']['bid_count']} "
                        f"asks={record['snapshot']['ask_count']}"
                    )

                else:

                    # Preserve non-snapshot event kinds too.
                    results[
                        name
                    ].append(
                        record
                    )

            print(
                f"[{name}] WINDOW END"
            )

        except Exception as error:

            errors[
                name
            ] = repr(error)

            print(
                f"[{name}] ERROR:",
                repr(error)
            )

        finally:

            if stream is not None:

                try:
                    stream.cancel()
                except Exception:
                    pass

            print(
                f"[{name}] FINISHED"
            )

    # ========================================================
    # HEARTBEAT
    # ========================================================

    async def heartbeat():

        while True:

            elapsed = (
                time.monotonic()
                -
                start
            )

            if elapsed >= (
                CAPTURE_SECONDS
            ):

                break

            print()

            print(

                f"[HEARTBEAT] "
                f"{elapsed:5.1f}s "
                f"A={len(results['L2_SNAPSHOT_ONLY_TRUE']):<3} "
                f"B={len(results['L2_SNAPSHOT_ONLY_FALSE']):<3} "
                f"C={len(results['L3_SNAPSHOT_ONLY_FALSE']):<3}"
            )

            await asyncio.sleep(
                HEARTBEAT_SECONDS
            )

    # ========================================================
    # RUN
    # ========================================================

    print()
    print(
        "[3] STARTING THREE STREAMS"
    )

    await asyncio.gather(

        worker(
            "L2_SNAPSHOT_ONLY_TRUE",
            api_pb2.FEED_LEVEL_L2,
            True
        ),

        worker(
            "L2_SNAPSHOT_ONLY_FALSE",
            api_pb2.FEED_LEVEL_L2,
            False
        ),

        worker(
            "L3_SNAPSHOT_ONLY_FALSE",
            api_pb2.FEED_LEVEL_L3,
            False
        ),

        heartbeat(),

        return_exceptions=True
    )

    # ========================================================
    # CLASSIFY
    # ========================================================

    print()
    print(
        "=" * 100
    )

    print(
        "STREAM SEMANTICS RESULT"
    )

    print(
        "=" * 100
    )

    for name, events in results.items():

        snapshots = [

            x

            for x
            in events

            if x.get(
                "kind"
            )
            in (
                "l2_snapshot",
                "l3_snapshot"
            )
        ]

        print()
        print(
            name
        )

        print(
            "  total events:",
            len(events)
        )

        print(
            "  snapshot events:",
            len(snapshots)
        )

        if snapshots:

            first = snapshots[0]

            last = snapshots[-1]

            print(
                "  first:",
                first.get(
                    "snapshot",
                    {}
                ).get(
                    "slot"
                )
            )

            print(
                "  last:",
                last.get(
                    "snapshot",
                    {}
                ).get(
                    "slot"
                )
            )

            if len(snapshots) >= 2:

                slots = [

                    x.get(
                        "snapshot",
                        {}
                    ).get(
                        "slot"
                    )

                    for x
                    in snapshots

                ]

                slots = [

                    x

                    for x
                    in slots

                    if x is not None
                ]

                if len(slots) >= 2:

                    changes = sum(

                        1

                        for a, b
                        in zip(
                            slots,
                            slots[1:]
                        )

                        if a != b
                    )

                    print(
                        "  adjacent slot changes:",
                        changes
                    )

    # ========================================================
    # SAVE
    # ========================================================

    print()
    print(
        "[4] SAVE"
    )

    master = {

        "experiment":
            "Flint Stream Semantics Probe v2.2",

        "host":
            HOST,

        "pair":
            PAIR_LABEL,

        "capture_seconds":
            CAPTURE_SECONDS,

        "results":
            results,

        "errors":
            errors,

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "interpretation_rule":
            "Do not infer quote-update frequency from "
            "snapshot_only=True alone. Compare L2 true, "
            "L2 false and L3 false."
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

    # --------------------------------------------------------
    # Compact CSV
    # --------------------------------------------------------

    rows = []

    for name, events in results.items():

        for index, event in enumerate(
            events,
            1
        ):

            snap = event.get(
                "snapshot",
                {}
            )

            rows.append({

                "stream":
                    name,

                "event_index":
                    index,

                "kind":
                    event.get(
                        "kind"
                    ),

                "slot":
                    snap.get(
                        "slot"
                    ),

                "timestamp_us":
                    snap.get(
                        "timestamp_us"
                    ),

                "bid_count":
                    snap.get(
                        "bid_count"
                    ),

                "ask_count":
                    snap.get(
                        "ask_count"
                    )
            })

    csv_path = (
        OUT /
        "stream_events.csv"
    )

    with open(
        csv_path,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        fields = [

            "stream",
            "event_index",
            "kind",
            "slot",
            "timestamp_us",
            "bid_count",
            "ask_count"
        ]

        writer = csv.DictWriter(
            f,
            fieldnames=fields,
            extrasaction="ignore"
        )

        writer.writeheader()

        writer.writerows(
            rows
        )

    print()
    print(
        "=" * 100
    )

    print(
        "COMPLETE"
    )

    print(
        "=" * 100
    )

    print(
        "A L2 snapshot_only=True:",
        len(
            results[
                "L2_SNAPSHOT_ONLY_TRUE"
            ]
        )
    )

    print(
        "B L2 snapshot_only=False:",
        len(
            results[
                "L2_SNAPSHOT_ONLY_FALSE"
            ]
        )
    )

    print(
        "C L3 snapshot_only=False:",
        len(
            results[
                "L3_SNAPSHOT_ONLY_FALSE"
            ]
        )
    )

    print()
    print(
        "MASTER:",
        master_path
    )

    print(
        "CSV:",
        csv_path
    )

    print()
    print(
        "NO TRANSACTIONS."
    )

    await channel.close()


try:

    asyncio.run(
        main()
    )

except KeyboardInterrupt:

    print(
        "\nInterrupted."
    )

except Exception:

    print()
    print(
        "=" * 100
    )

    print(
        "FATAL ERROR"
    )

    print(
        "=" * 100
    )

    traceback.print_exc()

finally:

    input(
        "\nPress ENTER to close..."
    )

