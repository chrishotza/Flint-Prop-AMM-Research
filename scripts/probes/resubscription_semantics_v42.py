import sys
import asyncio
import json
import math
import traceback
import time

from pathlib import Path
from datetime import datetime, timezone

import grpc


# ============================================================
# PATHS
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"
OUT = ROOT / "results" / "resubscription_semantics_v42"

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


# ============================================================
# CONFIG
# ============================================================

HOST = "mainnet.api.flint.trade:443"
PAIR_LABEL = "USDT/USDC"

CYCLES = 4

POST_SNAPSHOT_EVENTS = 3

READ_TIMEOUT = 3.0

PAUSE_BETWEEN_CYCLES = 3.0

CYCLE_TIMEOUT = 12.0


# ============================================================
# HELPERS
# ============================================================

def now_us():
    return int(
        time.time() * 1_000_000
    )


def num(value):
    try:

        if hasattr(
            value,
            "value"
        ):
            value = value.value

        x = float(value)

        if math.isfinite(x):
            return x

    except Exception:
        pass

    return None


def metadata_slot(meta):
    try:
        return int(
            meta.slot
        )
    except Exception:
        return None


def metadata_ts(meta):
    try:
        return int(
            meta.ts.micros
        )
    except Exception:
        return None


def spread_bps(
    bid,
    ask
):

    if (
        bid is None
        or ask is None
        or bid <= 0
        or ask < bid
    ):
        return None

    mid = (
        bid + ask
    ) / 2.0

    if mid <= 0:
        return None

    return (
        (ask - bid)
        /
        mid
        *
        10000.0
    )


def depth(
    levels,
    n
):

    total = 0.0

    for level in levels[:n]:

        p = num(
            level.price
        )

        s = num(
            level.size
        )

        if (
            p is None
            or s is None
            or p <= 0
            or s <= 0
        ):
            continue

        total += (
            p * s
        )

    return total


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


def save_json(
    path,
    obj
):

    path.write_text(
        json.dumps(
            obj,
            indent=2,
            default=str
        ),
        encoding="utf-8"
    )


def append_jsonl(
    path,
    obj
):

    with open(
        path,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(
                obj,
                default=str
            )
        )

        f.write("\n")


# ============================================================
# PAIR LOOKUP
# ============================================================

async def lookup_pair(
    stub
):

    response = await stub.ListPairs(
        api_pb2.ListPairsRequest(),
        timeout=15
    )

    for item in response.pairs:

        if str(
            item.pair.label
        ) == PAIR_LABEL:

            return build_pair(
                item
            )

    raise RuntimeError(
        "Pair not found: "
        +
        PAIR_LABEL
    )


# ============================================================
# CAPTURE ONE L2 CYCLE
# ============================================================

async def capture_l2(
    stub,
    pair,
    cycle
):

    request = api_pb2.SubscribeRequest(
        pairs=[
            api_pb2.PairSubscription(
                pair=pair,
                level=api_pb2.FEED_LEVEL_L2,
                snapshot_only=False
            )
        ]
    )

    call = stub.Subscribe(
        request
    )

    events = []

    started = time.monotonic()

    try:

        while (
            len(events)
            <
            1 + POST_SNAPSHOT_EVENTS
        ):

            if (
                time.monotonic()
                -
                started
                >
                CYCLE_TIMEOUT
            ):

                break

            try:

                event = await asyncio.wait_for(
                    call.read(),
                    timeout=READ_TIMEOUT
                )

            except asyncio.TimeoutError:

                break

            if event is grpc.aio.EOF:
                break

            if event is None:
                break

            kind = None

            try:
                kind = event.WhichOneof(
                    "kind"
                )
            except Exception:
                pass

            row = {

                "cycle":
                    cycle,

                "local_us":
                    now_us(),

                "kind":
                    kind
            }

            if kind == "l2_snapshot":

                snap = event.l2_snapshot

                bids = list(
                    snap.bids
                )

                asks = list(
                    snap.asks
                )

                bid = (
                    num(
                        bids[0].price
                    )
                    if bids
                    else None
                )

                ask = (
                    num(
                        asks[0].price
                    )
                    if asks
                    else None
                )

                row.update({

                    "slot":
                        metadata_slot(
                            snap.metadata
                        ),

                    "timestamp_us":
                        metadata_ts(
                            snap.metadata
                        ),

                    "best_bid":
                        bid,

                    "best_ask":
                        ask,

                    "spread_bps":
                        spread_bps(
                            bid,
                            ask
                        ),

                    "bid_levels":
                        len(bids),

                    "ask_levels":
                        len(asks),

                    "bid_depth_1":
                        depth(
                            bids,
                            1
                        ),

                    "ask_depth_1":
                        depth(
                            asks,
                            1
                        ),

                    "bid_depth_10":
                        depth(
                            bids,
                            10
                        ),

                    "ask_depth_10":
                        depth(
                            asks,
                            10
                        )
                })

            elif kind == "l2_update":

                row["update_seen"] = True

            events.append(
                row
            )

    except asyncio.CancelledError:

        raise

    except Exception as error:

        row = {
            "cycle":
                cycle,

            "local_us":
                now_us(),

            "kind":
                "ERROR",

            "error":
                repr(error)
        }

        events.append(
            row
        )

    finally:

        try:
            call.cancel()
        except Exception:
            pass

    return events


# ============================================================
# CAPTURE ONE L3 CYCLE
# ============================================================

async def capture_l3(
    stub,
    pair,
    cycle
):

    request = api_pb2.SubscribeRequest(
        pairs=[
            api_pb2.PairSubscription(
                pair=pair,
                level=api_pb2.FEED_LEVEL_L3,
                snapshot_only=False
            )
        ]
    )

    call = stub.Subscribe(
        request
    )

    events = []

    started = time.monotonic()

    try:

        while (
            len(events)
            <
            1 + POST_SNAPSHOT_EVENTS
        ):

            if (
                time.monotonic()
                -
                started
                >
                CYCLE_TIMEOUT
            ):

                break

            try:

                event = await asyncio.wait_for(
                    call.read(),
                    timeout=READ_TIMEOUT
                )

            except asyncio.TimeoutError:

                break

            if event is grpc.aio.EOF:
                break

            if event is None:
                break

            kind = None

            try:
                kind = event.WhichOneof(
                    "kind"
                )
            except Exception:
                pass

            row = {

                "cycle":
                    cycle,

                "local_us":
                    now_us(),

                "kind":
                    kind
            }

            if kind == "l3_snapshot":

                snap = event.l3_snapshot

                bids = []
                asks = []

                for entry in snap.bids:

                    maker = None
                    price = None
                    size = None

                    try:
                        maker = int(
                            entry
                            .maker_id
                            .id
                        )
                    except Exception:
                        pass

                    try:
                        price = float(
                            entry
                            .price
                            .value
                        )
                    except Exception:
                        pass

                    try:
                        size = float(
                            entry
                            .size
                            .value
                        )
                    except Exception:
                        pass

                    bids.append({

                        "maker_id":
                            maker,

                        "price":
                            price,

                        "size":
                            size
                    })

                for entry in snap.asks:

                    maker = None
                    price = None
                    size = None

                    try:
                        maker = int(
                            entry
                            .maker_id
                            .id
                        )
                    except Exception:
                        pass

                    try:
                        price = float(
                            entry
                            .price
                            .value
                        )
                    except Exception:
                        pass

                    try:
                        size = float(
                            entry
                            .size
                            .value
                        )
                    except Exception:
                        pass

                    asks.append({

                        "maker_id":
                            maker,

                        "price":
                            price,

                        "size":
                            size
                    })

                row.update({

                    "slot":
                        metadata_slot(
                            snap.metadata
                        ),

                    "timestamp_us":
                        metadata_ts(
                            snap.metadata
                        ),

                    "bid_levels":
                        len(bids),

                    "ask_levels":
                        len(asks),

                    "bids":
                        bids,

                    "asks":
                        asks
                })

            events.append(
                row
            )

    except asyncio.CancelledError:

        raise

    except Exception as error:

        events.append({

            "cycle":
                cycle,

            "local_us":
                now_us(),

            "kind":
                "ERROR",

            "error":
                repr(error)
        })

    finally:

        try:
            call.cancel()
        except Exception:
            pass

    return events


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 90)
    print("FLINT RESUBSCRIPTION SEMANTICS v4.2")
    print("=" * 90)

    print()
    print(
        "HOST:",
        HOST
    )

    print(
        "PAIR:",
        PAIR_LABEL
    )

    print(
        "CYCLES:",
        CYCLES
    )

    print(
        "POST-SNAPSHOT EVENTS:",
        POST_SNAPSHOT_EVENTS
    )

    print()
    print(
        "READ ONLY — NO TRANSACTIONS."
    )

    # --------------------------------------------------------
    # CONNECT
    # --------------------------------------------------------

    print()
    print("[1] CONNECT")

    channel = grpc.aio.secure_channel(
        HOST,
        grpc.ssl_channel_credentials()
    )

    stub = (
        api_pb2_grpc
        .MarketDataServiceStub(
            channel
        )
    )

    print(
        "[OK] stub"
    )

    # --------------------------------------------------------
    # PAIR
    # --------------------------------------------------------

    pair = await lookup_pair(
        stub
    )

    print(
        "[OK]",
        pair.label
    )

    all_cycles = []

    # --------------------------------------------------------
    # FOUR INDEPENDENT CYCLES
    # --------------------------------------------------------

    print()
    print("[2] INDEPENDENT SUBSCRIPTIONS")

    for cycle in range(
        1,
        CYCLES + 1
    ):

        print()
        print(
            "-" * 70
        )

        print(
            f"CYCLE {cycle}/{CYCLES}"
        )

        print(
            "-" * 70
        )

        cycle_start = time.time()

        l2_events = await capture_l2(
            stub,
            pair,
            cycle
        )

        print(
            "L2 events:",
            len(l2_events)
        )

        for event in l2_events:

            print(
                "  L2",
                event.get(
                    "kind"
                ),
                "slot=",
                event.get(
                    "slot"
                )
            )

        l3_events = await capture_l3(
            stub,
            pair,
            cycle
        )

        print(
            "L3 events:",
            len(l3_events)
        )

        for event in l3_events:

            print(
                "  L3",
                event.get(
                    "kind"
                ),
                "slot=",
                event.get(
                    "slot"
                )
            )

        all_cycles.append({

            "cycle":
                cycle,

            "started_unix":
                cycle_start,

            "l2":
                l2_events,

            "l3":
                l3_events
        })

        if cycle < CYCLES:

            await asyncio.sleep(
                PAUSE_BETWEEN_CYCLES
            )

    await channel.close()

    # --------------------------------------------------------
    # ANALYSIS
    # --------------------------------------------------------

    print()
    print("=" * 90)
    print("ANALYSIS")
    print("=" * 90)

    comparisons = []

    for cycle_data in all_cycles:

        l2 = cycle_data[
            "l2"
        ]

        l3 = cycle_data[
            "l3"
        ]

        l2_snapshot = next(

            (
                x
                for x in l2
                if x.get(
                    "kind"
                )
                ==
                "l2_snapshot"
            ),

            None
        )

        l3_snapshot = next(

            (
                x
                for x in l3
                if x.get(
                    "kind"
                )
                ==
                "l3_snapshot"
            ),

            None
        )

        l2_after = [

            x
            for x
            in l2

            if x.get(
                "kind"
            )
            !=
            "l2_snapshot"
        ]

        l3_after = [

            x
            for x
            in l3

            if x.get(
                "kind"
            )
            !=
            "l3_snapshot"
        ]

        l2_update_count = sum(

            1

            for x
            in l2_after

            if x.get(
                "kind"
            )
            ==
            "l2_update"
        )

        l3_update_count = sum(

            1

            for x
            in l3_after

            if x.get(
                "kind"
            )
            ==
            "l3_update"
        )

        same_l2_l3_slot = False

        if (
            l2_snapshot is not None
            and
            l3_snapshot is not None
        ):

            same_l2_l3_slot = (

                l2_snapshot.get(
                    "slot"
                )
                ==
                l3_snapshot.get(
                    "slot"
                )
            )

        comparisons.append({

            "cycle":
                cycle_data[
                    "cycle"
                ],

            "l2_event_count":
                len(l2),

            "l3_event_count":
                len(l3),

            "l2_first_kind":
                (
                    l2[0].get(
                        "kind"
                    )
                    if l2
                    else None
                ),

            "l3_first_kind":
                (
                    l3[0].get(
                        "kind"
                    )
                    if l3
                    else None
                ),

            "l2_post_snapshot_updates":
                l2_update_count,

            "l3_post_snapshot_updates":
                l3_update_count,

            "l2_first_slot":
                (
                    l2_snapshot.get(
                        "slot"
                    )
                    if l2_snapshot
                    else None
                ),

            "l3_first_slot":
                (
                    l3_snapshot.get(
                        "slot"
                    )
                    if l3_snapshot
                    else None
                ),

            "same_initial_slot":
                same_l2_l3_slot
        })


    # --------------------------------------------------------
    # REINITIALIZATION CHECK
    # --------------------------------------------------------

    initial_slots = [

        x[
            "l2_first_slot"
        ]

        for x in comparisons

        if x[
            "l2_first_slot"
        ]
        is not None
    ]

    print()
    print(
        "Cycle initial L2 slots:"
    )

    for value in initial_slots:

        print(
            " ",
            value
        )

    distinct_initial_slots = len(
        set(
            initial_slots
        )
    )

    print()
    print(
        "Distinct initial L2 slots:",
        distinct_initial_slots
    )

    print()
    print(
        "Cycle comparison:"
    )

    for row in comparisons:

        print(
            f"  cycle={row['cycle']} "
            f"L2={row['l2_event_count']} "
            f"L3={row['l3_event_count']} "
            f"L2post={row['l2_post_snapshot_updates']} "
            f"L3post={row['l3_post_snapshot_updates']} "
            f"same_slot={row['same_initial_slot']}"
        )


    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

    result = {

        "experiment":
            "Flint Resubscription Semantics v4.2",

        "host":
            HOST,

        "pair":
            PAIR_LABEL,

        "cycles":
            CYCLES,

        "post_snapshot_events":
            POST_SNAPSHOT_EVENTS,

        "pause_between_cycles":
            PAUSE_BETWEEN_CYCLES,

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "cycle_data":
            all_cycles,

        "comparisons":
            comparisons,

        "reinitialization": {

            "initial_l2_slots":
                initial_slots,

            "distinct_initial_l2_slots":
                distinct_initial_slots
        }
    }


    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    print()
    print("[3] SAVE")

    result_path = (
        OUT
        /
        "result.json"
    )

    raw_path = (
        OUT
        /
        "cycles.jsonl"
    )

    save_json(
        result_path,
        result
    )

    for cycle_data in all_cycles:

        append_jsonl(
            raw_path,
            cycle_data
        )


    # --------------------------------------------------------
    # REPORT
    # --------------------------------------------------------

    report = []

    report.append(
        "=" * 90
    )

    report.append(
        "FLINT RESUBSCRIPTION SEMANTICS REPORT v4.2"
    )

    report.append(
        "=" * 90
    )

    report.append("")

    report.append(
        f"Pair: {PAIR_LABEL}"
    )

    report.append(
        f"Independent cycles: {CYCLES}"
    )

    report.append(
        f"Post-snapshot events requested: "
        f"{POST_SNAPSHOT_EVENTS}"
    )

    report.append("")

    report.append(
        "CYCLE RESULTS"
    )

    report.append(
        "-" * 90
    )

    for row in comparisons:

        report.append(

            f"Cycle {row['cycle']}: "
            f"L2 events={row['l2_event_count']}, "
            f"L3 events={row['l3_event_count']}, "
            f"L2 post-snapshot updates="
            f"{row['l2_post_snapshot_updates']}, "
            f"L3 post-snapshot updates="
            f"{row['l3_post_snapshot_updates']}, "
            f"L2 initial slot="
            f"{row['l2_first_slot']}, "
            f"L3 initial slot="
            f"{row['l3_first_slot']}, "
            f"same initial slot="
            f"{row['same_initial_slot']}"
        )


    report.append("")

    report.append(
        "REINITIALIZATION"
    )

    report.append(
        "-" * 90
    )

    report.append(
        "Initial L2 slots: "
        +
        str(
            initial_slots
        )
    )

    report.append(
        "Distinct initial L2 slots: "
        +
        str(
            distinct_initial_slots
        )
    )

    report.append("")

    report.append(
        "INTERPRETATION"
    )

    report.append(
        "-" * 90
    )

    report.append(
        "Each cycle creates a new independent subscription."
    )

    report.append(
        "The first observed L2/L3 snapshot is treated as "
        "the initialization event for that subscription."
    )

    report.append(
        "Post-snapshot events are recorded separately."
    )

    report.append(
        "Repeatedly receiving a new snapshot after a new "
        "subscription is evidence of subscription "
        "reinitialization at the feed level."
    )

    report.append(
        "It is not, by itself, evidence about the internal "
        "state of Flint's matching or quoting engine."
    )

    report.append("")

    report.append(
        "NO TRANSACTIONS WERE SUBMITTED."
    )


    report_path = (
        OUT
        /
        "REPORT.txt"
    )

    report_path.write_text(
        "\n".join(
            report
        ),
        encoding="utf-8"
    )


    print()
    print(
        "RESULT:",
        result_path
    )

    print(
        "RAW:",
        raw_path
    )

    print(
        "REPORT:",
        report_path
    )

    print()
    print(
        "NO TRANSACTIONS."
    )


# ============================================================
# ENTRY
# ============================================================

try:

    asyncio.run(
        main()
    )

except KeyboardInterrupt:

    print()
    print(
        "Interrupted by user."
    )

except Exception:

    print()
    print(
        "=" * 90
    )

    print(
        "FATAL ERROR"
    )

    print(
        "=" * 90
    )

    traceback.print_exc()

finally:

    input(
        "\nPress ENTER to close..."
    )

