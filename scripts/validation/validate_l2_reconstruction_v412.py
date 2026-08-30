import sys
import asyncio
import json
import math
import time
import hashlib
import traceback

from pathlib import Path
from datetime import datetime, timezone

import grpc


ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"
OUT = ROOT / "results" / "validate_l2_reconstruction_v412"

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

TOTAL_SECONDS = 180
MAX_CASES = 3

SNAPSHOT_TIMEOUT = 10.0
CASE_SETTLE_SECONDS = 2.0


# ============================================================
# HELPERS
# ============================================================

def scalar(value):

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


def make_pair(item):

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

            return make_pair(
                item
            )

    raise RuntimeError(
        "Pair not found: "
        + PAIR_LABEL
    )


def meta(message):

    result = {

        "update_id":
            None,

        "slot":
            None,

        "timestamp_us":
            None
    }

    try:

        result[
            "update_id"
        ] = int(
            message.metadata.update_id
        )

    except Exception:
        pass

    try:

        result[
            "slot"
        ] = int(
            message.metadata.slot
        )

    except Exception:
        pass

    try:

        result[
            "timestamp_us"
        ] = int(
            message.metadata.ts.micros
        )

    except Exception:
        pass

    return result


def levels(message):

    bids = []
    asks = []

    for item in message.bids:

        price = scalar(
            item.price
        )

        size = scalar(
            item.size
        )

        if (
            price is not None
            and
            size is not None
        ):

            bids.append({
                "price":
                    price,

                "size":
                    size
            })

    for item in message.asks:

        price = scalar(
            item.price
        )

        size = scalar(
            item.size
        )

        if (
            price is not None
            and
            size is not None
        ):

            asks.append({
                "price":
                    price,

                "size":
                    size
            })

    return bids, asks


def book_from_message(
    message
):

    bids, asks = levels(
        message
    )

    return {

        "bids": {
            round(
                x["price"],
                12
            ):
                x["size"]

            for x in bids
        },

        "asks": {
            round(
                x["price"],
                12
            ):
                x["size"]

            for x in asks
        }
    }


def normalize(
    book
):

    bids = [

        [
            float(price),
            float(size)
        ]

        for price, size
        in sorted(
            book[
                "bids"
            ].items(),
            reverse=True
        )
    ]

    asks = [

        [
            float(price),
            float(size)
        ]

        for price, size
        in sorted(
            book[
                "asks"
            ].items()
        )
    ]

    return {

        "bids":
            bids,

        "asks":
            asks
    }


def digest(
    book
):

    raw = json.dumps(

        normalize(
            book
        ),

        sort_keys=True,
        separators=(",", ":")
    )

    return hashlib.sha256(
        raw.encode(
            "utf-8"
        )
    ).hexdigest()


def apply_update(
    book,
    message
):

    bids, asks = levels(
        message
    )

    changes = {

        "bid_removed":
            0,

        "bid_added_or_modified":
            0,

        "ask_removed":
            0,

        "ask_added_or_modified":
            0
    }


    for item in bids:

        price = round(
            item["price"],
            12
        )

        size = item[
            "size"
        ]

        if size == 0.0:

            if price in book[
                "bids"
            ]:

                del book[
                    "bids"
                ][price]

                changes[
                    "bid_removed"
                ] += 1

        else:

            book[
                "bids"
            ][price] = size

            changes[
                "bid_added_or_modified"
            ] += 1


    for item in asks:

        price = round(
            item["price"],
            12
        )

        size = item[
            "size"
        ]

        if size == 0.0:

            if price in book[
                "asks"
            ]:

                del book[
                    "asks"
                ][price]

                changes[
                    "ask_removed"
                ] += 1

        else:

            book[
                "asks"
            ][price] = size

            changes[
                "ask_added_or_modified"
            ] += 1


    return changes


def compare_books(
    a,
    b
):

    na = normalize(
        a
    )

    nb = normalize(
        b
    )

    if na == nb:

        return {

            "exact_match":
                True,

            "bid_differences":
                [],

            "ask_differences":
                []
        }


    def diff_side(
        av,
        bv
    ):

        aa = dict(
            av
        )

        bb = dict(
            bv
        )

        prices = sorted(
            set(
                aa.keys()
            )
            |
            set(
                bb.keys()
            ),
            reverse=True
        )

        result = []

        for price in prices:

            x = aa.get(
                price
            )

            y = bb.get(
                price
            )

            if x != y:

                result.append({

                    "price":
                        price,

                    "reconstructed":
                        x,

                    "reference":
                        y
                })

        return result


    return {

        "exact_match":
            False,

        "bid_differences":
            diff_side(
                na["bids"],
                nb["bids"]
            ),

        "ask_differences":
            diff_side(
                na["asks"],
                nb["asks"]
            )
    }


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
# READ FIRST EVENT
# ============================================================

async def read_first_event(
    call
):

    try:

        event = await asyncio.wait_for(
            call.read(),
            timeout=SNAPSHOT_TIMEOUT
        )

        if event is grpc.aio.EOF:
            return None

        if event is None:
            return None

        return event

    finally:

        try:
            call.cancel()
        except Exception:
            pass


# ============================================================
# MAIN
# ============================================================

async def main():

    print("=" * 100)
    print(
        "FLINT CONDITIONAL L2 RECONSTRUCTION VALIDATION v4.12"
    )
    print("=" * 100)

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
        "MAX CASES:",
        MAX_CASES
    )

    print(
        "MAX DURATION:",
        TOTAL_SECONDS,
        "seconds"
    )

    print()
    print(
        "CONDITION:"
    )

    print(
        "A validation case only counts if a real "
        "L2 update is observed."
    )

    print(
        "NO TRANSACTIONS."
    )

    events_path = (
        OUT / "persistent_events.jsonl"
    )

    probes_path = (
        OUT / "reference_snapshots.jsonl"
    )

    result_path = (
        OUT / "result.json"
    )

    report_path = (
        OUT / "REPORT.txt"
    )

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

    print()
    print("[1] CONNECT")
    print("[OK] stub")

    pair = await lookup_pair(
        stub
    )

    print(
        "[FOUND]",
        pair.label
    )

    started = time.monotonic()

    initial = None
    updates = []

    # --------------------------------------------------------
    # PERSISTENT STREAM
    # --------------------------------------------------------

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

    print()
    print(
        "[2] PERSISTENT STREAM OPEN"
    )

    async def persistent_reader():

        nonlocal initial

        try:

            while (
                time.monotonic()
                -
                started
                <
                TOTAL_SECONDS
            ):

                event = await call.read()

                if event is grpc.aio.EOF:

                    print(
                        "[PERSISTENT] EOF"
                    )

                    return

                if event is None:

                    return

                kind = event.WhichOneof(
                    "kind"
                )

                elapsed = (
                    time.monotonic()
                    -
                    started
                )

                if kind == "l2_snapshot":

                    snap = (
                        event.l2_snapshot
                    )

                    m = meta(
                        snap
                    )

                    book = book_from_message(
                        snap
                    )

                    if initial is None:

                        initial = {

                            "update_id":
                                m[
                                    "update_id"
                                ],

                            "slot":
                                m[
                                    "slot"
                                ],

                            "timestamp_us":
                                m[
                                    "timestamp_us"
                                ],

                            "book":
                                book,

                            "digest":
                                digest(
                                    book
                                ),

                            "elapsed":
                                elapsed
                        }

                        print(
                            "[INITIAL]",
                            f"id={initial['update_id']}",
                            f"slot={initial['slot']}"
                        )

                    append_jsonl(
                        events_path,
                        {

                            "type":
                                "SNAPSHOT",

                            "elapsed":
                                elapsed,

                            "update_id":
                                m[
                                    "update_id"
                                ],

                            "slot":
                                m[
                                    "slot"
                                ],

                            "timestamp_us":
                                m[
                                    "timestamp_us"
                                ],

                            "digest":
                                digest(
                                    book
                                )
                        }
                    )

                elif kind == "l2_update":

                    update = (
                        event.l2_update
                    )

                    m = meta(
                        update
                    )

                    updates.append({

                        "elapsed":
                            elapsed,

                        "update_id":
                            m[
                                "update_id"
                            ],

                        "slot":
                            m[
                                "slot"
                            ],

                        "timestamp_us":
                            m[
                                "timestamp_us"
                            ],

                        "update":
                            update
                    })

                    append_jsonl(
                        events_path,
                        {

                            "type":
                                "UPDATE",

                            "elapsed":
                                elapsed,

                            "update_id":
                                m[
                                    "update_id"
                                ],

                            "slot":
                                m[
                                    "slot"
                                ],

                            "timestamp_us":
                                m[
                                    "timestamp_us"
                                ]
                        }
                    )

                    print(
                        "[UPDATE]",
                        f"#{len(updates)}",
                        f"id={m['update_id']}",
                        f"slot={m['slot']}",
                        f"elapsed={elapsed:.3f}s"
                    )

        except asyncio.CancelledError:

            raise

        except grpc.RpcError as error:

            print(
                "[PERSISTENT RPC ERROR]",
                error.code(),
                error.details()
            )

    reader_task = asyncio.create_task(
        persistent_reader()
    )

    # --------------------------------------------------------
    # WAIT FOR INITIAL SNAPSHOT
    # --------------------------------------------------------

    while initial is None:

        if (
            time.monotonic()
            -
            started
            >
            15
        ):

            reader_task.cancel()

            await asyncio.gather(
                reader_task,
                return_exceptions=True
            )

            await channel.close()

            raise RuntimeError(
                "Initial snapshot not received."
            )

        if reader_task.done():

            break

        await asyncio.sleep(
            0.25
        )

    if initial is None:

        reader_task.cancel()

        await asyncio.gather(
            reader_task,
            return_exceptions=True
        )

        await channel.close()

        raise RuntimeError(
            "Persistent stream ended before initial snapshot."
        )


    # --------------------------------------------------------
    # CONDITIONAL CASES
    # --------------------------------------------------------

    cases = []

    next_case_after = (
        time.monotonic()
    )

    print()
    print(
        "[3] WAITING FOR REAL UPDATES"
    )

    while (
        len(cases)
        <
        MAX_CASES
    ):

        if (
            time.monotonic()
            -
            started
            >=
            TOTAL_SECONDS
        ):

            break

        if not updates:

            await asyncio.sleep(
                0.1
            )

            continue

        # Take the newest update that has not yet been tested.
        tested_ids = {
            x.get(
                "source_update_id"
            )
            for x in cases
        }

        candidate = None

        for item in updates:

            if (
                item[
                    "update_id"
                ]
                not in tested_ids
            ):

                candidate = item
                break

        if candidate is None:

            await asyncio.sleep(
                0.1
            )

            continue

        if (
            time.monotonic()
            <
            next_case_after
        ):

            await asyncio.sleep(
                0.1
            )

            continue

        source_id = candidate[
            "update_id"
        ]

        source_elapsed = candidate[
            "elapsed"
        ]

        print()
        print(
            "-" * 80
        )

        print(
            "VALIDATION CASE",
            len(cases) + 1
        )

        print(
            "source update_id:",
            source_id
        )

        print(
            "source elapsed:",
            source_elapsed
        )

        print(
            "-" * 80
        )

        # ----------------------------------------------------
        # Independent snapshot
        # ----------------------------------------------------

        probe_request = api_pb2.SubscribeRequest(
            pairs=[
                api_pb2.PairSubscription(
                    pair=pair,
                    level=api_pb2.FEED_LEVEL_L2,
                    snapshot_only=True
                )
            ]
        )

        probe_call = stub.Subscribe(
            probe_request
        )

        try:

            reference_event = (
                await read_first_event(
                    probe_call
                )
            )

            if reference_event is None:

                case = {

                    "case":
                        len(cases) + 1,

                    "source_update_id":
                        source_id,

                    "status":
                        "NO_REFERENCE"
                }

                cases.append(
                    case
                )

                print(
                    "[CASE] no reference"
                )

                next_case_after = (
                    time.monotonic()
                    +
                    CASE_SETTLE_SECONDS
                )

                continue

            reference_kind = (
                reference_event
                .WhichOneof(
                    "kind"
                )
            )

            if reference_kind != "l2_snapshot":

                case = {

                    "case":
                        len(cases) + 1,

                    "source_update_id":
                        source_id,

                    "status":
                        "UNEXPECTED_REFERENCE_KIND",

                    "reference_kind":
                        reference_kind
                }

                cases.append(
                    case
                )

                print(
                    "[CASE] unexpected:",
                    reference_kind
                )

                next_case_after = (
                    time.monotonic()
                    +
                    CASE_SETTLE_SECONDS
                )

                continue

            reference = (
                reference_event
                .l2_snapshot
            )

            ref_meta = meta(
                reference
            )

            reference_book = (
                book_from_message(
                    reference
                )
            )

            ref_digest = digest(
                reference_book
            )

            reference_id = (
                ref_meta[
                    "update_id"
                ]
            )

            reference_slot = (
                ref_meta[
                    "slot"
                ]
            )

            print(
                "reference update_id:",
                reference_id
            )

            print(
                "reference slot:",
                reference_slot
            )

            # ------------------------------------------------
            # Reconstruct only if reference is at/after
            # the source update.
            # ------------------------------------------------

            if (
                reference_id is None
                or
                source_id is None
                or
                reference_id < source_id
            ):

                case = {

                    "case":
                        len(cases) + 1,

                    "source_update_id":
                        source_id,

                    "reference_update_id":
                        reference_id,

                    "reference_slot":
                        reference_slot,

                    "status":
                        "REFERENCE_BEFORE_SOURCE"
                }

                cases.append(
                    case
                )

                print(
                    "[CASE] reference is older."
                )

                next_case_after = (
                    time.monotonic()
                    +
                    CASE_SETTLE_SECONDS
                )

                continue

            # ------------------------------------------------
            # Reconstruct
            # ------------------------------------------------

            reconstructed = {

                "bids":
                    dict(
                        initial[
                            "book"
                        ][
                            "bids"
                        ]
                    ),

                "asks":
                    dict(
                        initial[
                            "book"
                        ][
                            "asks"
                        ]
                    )
            }

            used_ids = []

            reconstruction_ok = True
            sequence_error = None

            expected = initial[
                "update_id"
            ]

            if (
                expected is None
                or
                source_id < expected
            ):

                reconstruction_ok = False

                sequence_error = (
                    "source update_id precedes "
                    "initial snapshot"
                )

            else:

                for update_item in updates:

                    update_id = update_item[
                        "update_id"
                    ]

                    if update_id is None:
                        continue

                    if update_id > reference_id:
                        break

                    if (
                        expected is not None
                        and
                        update_id != expected + 1
                    ):

                        reconstruction_ok = False

                        sequence_error = (
                            f"expected {expected + 1}, "
                            f"got {update_id}"
                        )

                        break

                    apply_update(
                        reconstructed,
                        update_item[
                            "update"
                        ]
                    )

                    used_ids.append(
                        update_id
                    )

                    expected = update_id

                    if (
                        update_id
                        ==
                        reference_id
                    ):

                        break

            if (
                reference_id
                ==
                initial[
                    "update_id"
                ]
            ):

                reconstruction_ok = True
                used_ids = []

            reached_reference = (

                reference_id
                ==
                initial[
                    "update_id"
                ]

            ) or (

                used_ids
                and
                used_ids[-1]
                ==
                reference_id
            )

            if not reached_reference:

                reconstruction_ok = False

                if sequence_error is None:

                    sequence_error = (
                        "persistent stream did not "
                        "reach reference update_id"
                    )

            if not reconstruction_ok:

                case = {

                    "case":
                        len(cases) + 1,

                    "source_update_id":
                        source_id,

                    "reference_update_id":
                        reference_id,

                    "reference_slot":
                        reference_slot,

                    "status":
                        "RECONSTRUCTION_NOT_VALIDATED",

                    "sequence_error":
                        sequence_error,

                    "used_update_ids":
                        used_ids
                }

                cases.append(
                    case
                )

                print(
                    "[CASE] not validated:",
                    sequence_error
                )

            else:

                comparison = compare_books(
                    reconstructed,
                    reference_book
                )

                case = {

                    "case":
                        len(cases) + 1,

                    "source_update_id":
                        source_id,

                    "source_slot":
                        candidate[
                            "slot"
                        ],

                    "source_elapsed":
                        source_elapsed,

                    "reference_update_id":
                        reference_id,

                    "reference_slot":
                        reference_slot,

                    "status":
                        "VALIDATED",

                    "used_update_ids":
                        used_ids,

                    "reconstructed_digest":
                        digest(
                            reconstructed
                        ),

                    "reference_digest":
                        ref_digest,

                    "comparison":
                        comparison
                }

                cases.append(
                    case
                )

                print(
                    "[CASE] exact match:",
                    comparison[
                        "exact_match"
                    ]
                )

                print(
                    "reconstructed digest:",
                    case[
                        "reconstructed_digest"
                    ]
                )

                print(
                    "reference digest:",
                    case[
                        "reference_digest"
                    ]

                )

        finally:

            next_case_after = (
                time.monotonic()
                +
                CASE_SETTLE_SECONDS
            )

    # --------------------------------------------------------
    # FINISH
    # --------------------------------------------------------

    if not reader_task.done():

        reader_task.cancel()

        await asyncio.gather(
            reader_task,
            return_exceptions=True
        )

    await channel.close()

    elapsed = (
        time.monotonic()
        -
        started
    )

    validated = [

        x
        for x
        in cases

        if x.get(
            "status"
        )
        ==
        "VALIDATED"
    ]

    exact = [

        x
        for x
        in validated

        if x[
            "comparison"
        ][
            "exact_match"
        ]
    ]

    output = {

        "experiment":
            "Flint Conditional L2 Reconstruction Validation v4.12",

        "host":
            HOST,

        "pair":
            PAIR_LABEL,

        "requested_seconds":
            TOTAL_SECONDS,

        "actual_seconds":
            elapsed,

        "initial_snapshot":
            initial,

        "persistent_updates":
            [

                {

                    "update_id":
                        x[
                            "update_id"
                        ],

                    "slot":
                        x[
                            "slot"
                        ],

                    "elapsed":
                        x[
                            "elapsed"
                        ]
                }

                for x
                in updates
            ],

        "cases":
            cases,

        "summary":
            {

                "cases":
                    len(cases),

                "validated":
                    len(validated),

                "exact_matches":
                    len(exact)
            },

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }

    result_path.write_text(
        json.dumps(
            output,
            indent=2,
            ensure_ascii=False,
            default=str
        ),
        encoding="utf-8"
    )

    report = []

    report.append(
        "=" * 100
    )

    report.append(
        "FLINT CONDITIONAL L2 RECONSTRUCTION REPORT v4.12"
    )

    report.append(
        "=" * 100
    )

    report.append("")

    report.append(
        f"Pair: {PAIR_LABEL}"
    )

    report.append(
        f"Requested seconds: {TOTAL_SECONDS}"
    )

    report.append(
        f"Actual seconds: {elapsed:.3f}"
    )

    report.append("")

    report.append(
        "PERSISTENT STREAM"
    )

    report.append(
        "-" * 100
    )

    report.append(
        f"Initial snapshot update_id: "
        f"{initial['update_id']}"
    )

    report.append(
        f"Initial snapshot slot: "
        f"{initial['slot']}"
    )

    report.append(
        f"Updates observed: "
        f"{len(updates)}"
    )

    for item in updates:

        report.append(
            f"  update_id={item['update_id']} "
            f"slot={item['slot']} "
            f"elapsed={item['elapsed']:.3f}s"
        )

    report.append("")

    report.append(
        "VALIDATION CASES"
    )

    report.append(
        "-" * 100
    )

    for case in cases:

        report.append(
            f"Case {case['case']}: "
            f"status={case['status']} "
            f"source={case.get('source_update_id')} "
            f"reference={case.get('reference_update_id')}"
        )

        if case.get(
            "comparison"
        ):

            comparison = case[
                "comparison"
            ]

            report.append(
                f"  exact_match="
                f"{comparison['exact_match']}"
            )

            report.append(
                f"  bid_differences="
                f"{len(comparison['bid_differences'])}"
            )

            report.append(
                f"  ask_differences="
                f"{len(comparison['ask_differences'])}"
            )

    report.append("")

    report.append(
        "SUMMARY"
    )

    report.append(
        "-" * 100
    )

    report.append(
        f"Cases: {len(cases)}"
    )

    report.append(
        f"Validated: {len(validated)}"
    )

    report.append(
        f"Exact matches: {len(exact)}"
    )

    report.append("")

    report.append(
        "INTERPRETATION"
    )

    report.append(
        "-" * 100
    )

    report.append(
        "A validation case requires a real persistent "
        "L2 update before a fresh reference snapshot."
    )

    report.append(
        "The reconstruction starts from the initial "
        "snapshot and applies sequential L2 updates."
    )

    report.append(
        "The reference snapshot is compared only after "
        "its update_id reaches or exceeds the source update."
    )

    report.append(
        "Exact equality is based on all observed price/size "
        "levels on both sides of the book."
    )

    report.append(
        "No claim is made about undocumented internal "
        "implementation beyond the public feed behavior."
    )

    report.append(
        "No transaction was submitted."
    )

    report_path.write_text(
        "\n".join(
            report
        ),
        encoding="utf-8"
    )

    print()
    print("=" * 100)
    print("COMPLETE")
    print("=" * 100)

    print(
        "Persistent updates:",
        len(updates)
    )

    print(
        "Validation cases:",
        len(cases)
    )

    print(
        "Validated:",
        len(validated)
    )

    print(
        "Exact matches:",
        len(exact)
    )

    print()
    print(
        "RESULT:",
        result_path
    )

    print(
        "PROBES:",
        probes_path
    )

    print(
        "EVENTS:",
        events_path
    )

    print(
        "REPORT:",
        report_path
    )


try:

    asyncio.run(
        main()
    )

except KeyboardInterrupt:

    print(
        "\nInterrupted by user."
    )

except Exception:

    print()
    print("=" * 100)
    print("FATAL ERROR")
    print("=" * 100)

    traceback.print_exc()

finally:

    input(
        "\nPress ENTER to close..."
    )

