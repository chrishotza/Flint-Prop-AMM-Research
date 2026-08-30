import sys
import asyncio
import json
import time
import traceback
from pathlib import Path
from datetime import datetime, timezone

import grpc

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"
OUT = ROOT / "results" / "single_stream_raw_continuity_v45"

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

RUN_SECONDS = 90
HEARTBEAT_SECONDS = 5


def now_us():
    return int(
        time.time() * 1_000_000
    )


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


async def lookup_pair(stub):

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
        + PAIR_LABEL
    )


def append_event(path, obj):

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
# MAIN
# ============================================================

async def main():

    print("=" * 100)
    print("FLINT SINGLE STREAM RAW CONTINUITY v4.5")
    print("=" * 100)

    print()
    print("HOST:", HOST)
    print("PAIR:", PAIR_LABEL)
    print("DURATION:", RUN_SECONDS, "seconds")
    print()
    print("ONE SINGLE L2 SUBSCRIPTION.")
    print("NO RECONNECTION.")
    print("NO READ TIMEOUT.")
    print("NO wait_for() AROUND stream.read().")
    print("NO TRANSACTIONS.")

    events_path = (
        OUT / "events.jsonl"
    )

    result_path = (
        OUT / "result.json"
    )

    report_path = (
        OUT / "REPORT.txt"
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

    print()
    print("[2] PAIR")

    pair = await lookup_pair(
        stub
    )

    print(
        "[FOUND]",
        pair.label
    )

    # --------------------------------------------------------
    # REQUEST
    # --------------------------------------------------------

    print()
    print("[3] OPEN SINGLE STREAM")

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

    print(
        "[OK] stream opened"
    )

    # --------------------------------------------------------
    # STATE
    # --------------------------------------------------------

    start_monotonic = (
        time.monotonic()
    )

    stop_event = asyncio.Event()

    reader_done = asyncio.Event()

    stats = {

        "total_events": 0,

        "snapshots": 0,

        "updates": 0,

        "other": 0,

        "eof": 0,

        "rpc_errors": 0,

        "other_errors": 0,

        "last_event_monotonic":
            start_monotonic,

        "first_event_monotonic":
            None
    }

    kinds = {}

    # --------------------------------------------------------
    # READER
    # --------------------------------------------------------

    async def reader():

        try:

            while True:

                # IMPORTANT:
                # Direct await.
                # No asyncio.wait_for().
                event = await call.read()

                if event is grpc.aio.EOF:

                    stats[
                        "eof"
                    ] += 1

                    append_event(

                        events_path,

                        {

                            "type":
                                "EOF",

                            "local_us":
                                now_us(),

                            "elapsed":
                                time.monotonic()
                                -
                                start_monotonic
                        }
                    )

                    print(
                        "[EOF]"
                    )

                    break

                if event is None:

                    append_event(

                        events_path,

                        {

                            "type":
                                "NONE",

                            "local_us":
                                now_us(),

                            "elapsed":
                                time.monotonic()
                                -
                                start_monotonic
                        }
                    )

                    print(
                        "[NONE]"
                    )

                    break

                elapsed = (
                    time.monotonic()
                    -
                    start_monotonic
                )

                if (
                    stats[
                        "first_event_monotonic"
                    ]
                    is None
                ):

                    stats[
                        "first_event_monotonic"
                    ] = (
                        time.monotonic()
                    )

                stats[
                    "last_event_monotonic"
                ] = (
                    time.monotonic()
                )

                stats[
                    "total_events"
                ] += 1

                try:

                    kind = event.WhichOneof(
                        "kind"
                    )

                except Exception:

                    kind = None

                kinds[kind] = (
                    kinds.get(
                        kind,
                        0
                    )
                    + 1
                )

                if kind == "l2_snapshot":

                    stats[
                        "snapshots"
                    ] += 1

                elif kind == "l2_update":

                    stats[
                        "updates"
                    ] += 1

                else:

                    stats[
                        "other"
                    ] += 1

                row = {

                    "type":
                        "EVENT",

                    "local_us":
                        now_us(),

                    "elapsed":
                        elapsed,

                    "kind":
                        kind
                }

                if kind == "l2_snapshot":

                    snap = (
                        event
                        .l2_snapshot
                    )

                    row[
                        "slot"
                    ] = int(
                        snap.metadata.slot
                    )

                    try:

                        row[
                            "timestamp_us"
                        ] = int(
                            snap.metadata
                            .ts
                            .micros
                        )

                    except Exception:

                        row[
                            "timestamp_us"
                        ] = None

                    row[
                        "bid_levels"
                    ] = len(
                        snap.bids
                    )

                    row[
                        "ask_levels"
                    ] = len(
                        snap.asks
                    )

                    if snap.bids:

                        try:

                            row[
                                "best_bid"
                            ] = float(
                                snap.bids[
                                    0
                                ]
                                .price
                                .value
                            )

                        except Exception:

                            row[
                                "best_bid"
                            ] = None

                    else:

                        row[
                            "best_bid"
                        ] = None

                    if snap.asks:

                        try:

                            row[
                                "best_ask"
                            ] = float(
                                snap.asks[
                                    0
                                ]
                                .price
                                .value
                            )

                        except Exception:

                            row[
                                "best_ask"
                            ] = None

                    else:

                        row[
                            "best_ask"
                        ] = None


                append_event(
                    events_path,
                    row
                )

                if (
                    stats[
                        "total_events"
                    ] <= 10
                ):

                    print(
                        f"[EVENT] "
                        f"#{stats['total_events']} "
                        f"kind={kind} "
                        f"elapsed={elapsed:.3f}s"
                    )

                elif (
                    stats[
                        "total_events"
                    ] % 25
                    == 0
                ):

                    print(
                        f"[EVENT] "
                        f"#{stats['total_events']} "
                        f"kind={kind} "
                        f"elapsed={elapsed:.3f}s"
                    )

        except asyncio.CancelledError:

            append_event(

                events_path,

                {

                    "type":
                        "READER_CANCELLED",

                    "local_us":
                        now_us(),

                    "elapsed":
                        time.monotonic()
                        -
                        start_monotonic
                }
            )

            print(
                "[READER] CancelledError"
            )

            raise

        except grpc.RpcError as error:

            stats[
                "rpc_errors"
            ] += 1

            append_event(

                events_path,

                {

                    "type":
                        "RPC_ERROR",

                    "local_us":
                        now_us(),

                    "elapsed":
                        time.monotonic()
                        -
                        start_monotonic,

                    "code":
                        str(
                            error.code()
                        ),

                    "details":
                        error.details()
                }
            )

            print(
                "[READER] RPC ERROR",
                error.code(),
                error.details()
            )

        except Exception as error:

            stats[
                "other_errors"
            ] += 1

            append_event(

                events_path,

                {

                    "type":
                        "ERROR",

                    "local_us":
                        now_us(),

                    "elapsed":
                        time.monotonic()
                        -
                        start_monotonic,

                    "error":
                        repr(error)
                }
            )

            print(
                "[READER] ERROR",
                repr(error)
            )

        finally:

            reader_done.set()


    # --------------------------------------------------------
    # HEARTBEAT
    # --------------------------------------------------------

    async def heartbeat():

        while not stop_event.is_set():

            await asyncio.sleep(
                HEARTBEAT_SECONDS
            )

            elapsed = (
                time.monotonic()
                -
                start_monotonic
            )

            last_event_age = (
                time.monotonic()
                -
                stats[
                    "last_event_monotonic"
                ]
            )

            print(
                "[HEARTBEAT]",
                f"elapsed={elapsed:.1f}s",
                f"events={stats['total_events']}",
                f"snapshots={stats['snapshots']}",
                f"updates={stats['updates']}",
                f"last_event_age={last_event_age:.1f}s"
            )

            if reader_done.is_set():

                print(
                    "[HEARTBEAT] "
                    "reader already finished"
                )

                return


    # --------------------------------------------------------
    # START TASKS
    # --------------------------------------------------------

    reader_task = asyncio.create_task(
        reader()
    )

    heartbeat_task = asyncio.create_task(
        heartbeat()
    )

    # --------------------------------------------------------
    # TIMED OBSERVATION
    # --------------------------------------------------------

    print()
    print("[4] OBSERVATION START")

    try:

        await asyncio.sleep(
            RUN_SECONDS
        )

    finally:

        elapsed = (
            time.monotonic()
            -
            start_monotonic
        )

        print()
        print(
            "[5] OBSERVATION WINDOW ENDED"
        )

        print(
            "elapsed:",
            f"{elapsed:.3f}s"
        )

        # ----------------------------------------------------
        # Only now do we deliberately terminate the reader.
        # This cancellation belongs to the end of the test,
        # not to an artificial read timeout.
        # ----------------------------------------------------

        if not reader_task.done():

            reader_task.cancel()

            try:

                await reader_task

            except asyncio.CancelledError:

                pass

        stop_event.set()

        heartbeat_task.cancel()

        try:

            await heartbeat_task

        except asyncio.CancelledError:

            pass

        try:

            call.cancel()

        except Exception:

            pass

        await channel.close()


    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

    actual_elapsed = (
        time.monotonic()
        -
        start_monotonic
    )

    final_gap = (
        time.monotonic()
        -
        stats[
            "last_event_monotonic"
        ]
    )

    result = {

        "experiment":
            "Flint Single Stream Raw Continuity v4.5",

        "host":
            HOST,

        "pair":
            PAIR_LABEL,

        "requested_seconds":
            RUN_SECONDS,

        "actual_seconds":
            actual_elapsed,

        "stats":
            stats,

        "kinds":
            kinds,

        "final_silence_seconds":
            final_gap,

        "reader_finished_before_end":
            reader_done.is_set(),

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat()
    }


    result_path.write_text(

        json.dumps(
            result,
            indent=2,
            default=str
        ),

        encoding="utf-8"
    )


    # --------------------------------------------------------
    # REPORT
    # --------------------------------------------------------

    report = []

    report.append(
        "=" * 100
    )

    report.append(
        "FLINT SINGLE STREAM RAW CONTINUITY REPORT v4.5"
    )

    report.append(
        "=" * 100
    )

    report.append("")

    report.append(
        f"Pair: {PAIR_LABEL}"
    )

    report.append(
        f"Requested seconds: {RUN_SECONDS}"
    )

    report.append(
        f"Actual seconds: {actual_elapsed:.3f}"
    )

    report.append("")

    report.append(
        "EVENT COUNTS"
    )

    report.append(
        "-" * 100
    )

    for kind, count in sorted(
        kinds.items(),
        key=lambda x: str(x[0])
    ):

        report.append(
            f"{kind}: {count}"
        )

    report.append("")

    report.append(
        "STREAM TERMINATION"
    )

    report.append(
        "-" * 100
    )

    report.append(
        f"EOF observed: {stats['eof']}"
    )

    report.append(
        f"RPC errors: {stats['rpc_errors']}"
    )

    report.append(
        f"Other errors: {stats['other_errors']}"
    )

    report.append(
        "Reader finished before test end: "
        +
        str(
            reader_done.is_set()
        )
    )

    report.append("")

    report.append(
        "CONTINUITY"
    )

    report.append(
        "-" * 100
    )

    report.append(
        f"Total events: "
        f"{stats['total_events']}"
    )

    report.append(
        f"Snapshots: "
        f"{stats['snapshots']}"
    )

    report.append(
        f"Updates: "
        f"{stats['updates']}"
    )

    report.append(
        f"Final silence before forced shutdown: "
        f"{final_gap:.3f}s"
    )

    report.append("")

    report.append(
        "IMPORTANT INTERPRETATION"
    )

    report.append(
        "-" * 100
    )

    report.append(
        "No read timeout was used."
    )

    report.append(
        "A timeout therefore cannot be confused "
        "with a stream cancellation."
    )

    report.append(
        "The only deliberate cancellation occurs "
        "after the requested observation window."
    )

    report.append(
        "If no EOF/RPC error occurs and the reader "
        "remains pending until the end, the public "
        "stream remained open during the observed window "
        "even if no additional events arrived."
    )

    report.append(
        "Silence of the public feed does not prove "
        "absence of internal market activity."
    )

    report.append(
        "NO TRANSACTIONS WERE SUBMITTED."
    )

    report_path.write_text(
        "\n".join(report),
        encoding="utf-8"
    )

    print()
    print("=" * 100)
    print("COMPLETE")
    print("=" * 100)

    print(
        "Events:",
        stats["total_events"]
    )

    print(
        "Snapshots:",
        stats["snapshots"]
    )

    print(
        "Updates:",
        stats["updates"]
    )

    print(
        "Kinds:",
        kinds
    )

    print(
        "EOF:",
        stats["eof"]
    )

    print(
        "RPC errors:",
        stats["rpc_errors"]
    )

    print(
        "Reader finished before end:",
        reader_done.is_set()
    )

    print()
    print(
        "RESULT:",
        result_path
    )

    print(
        "REPORT:",
        report_path
    )

    print(
        "RAW:",
        events_path
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

