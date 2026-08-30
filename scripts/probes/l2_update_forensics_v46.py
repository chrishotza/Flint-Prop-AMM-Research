import sys
import json
import base64
import hashlib
import traceback
from pathlib import Path
from datetime import datetime, timezone

import google.protobuf
from google.protobuf.json_format import MessageToDict

ROOT = Path(__file__).resolve().parents[2]
GENERATED = ROOT / "generated"
SOURCE = ROOT / "results" / "l2_update_forensics_v46" / "events.jsonl"
OUT = ROOT / "results" / "l2_update_forensics_v47"

OUT.mkdir(
    parents=True,
    exist_ok=True
)

sys.path.insert(
    0,
    str(GENERATED)
)

import api_pb2


def load_events():

    rows = []

    with open(
        SOURCE,
        "r",
        encoding="utf-8"
    ) as f:

        for line in f:

            line = line.strip()

            if not line:
                continue

            rows.append(
                json.loads(line)
            )

    return rows


def message_descriptor_info():

    # Find the protobuf type used by the SubscribeResponse
    # and inspect all oneof members.

    response_desc = (
        api_pb2.SubscribeResponse.DESCRIPTOR
    )

    oneofs = response_desc.oneofs

    info = []

    for oneof in oneofs:

        members = []

        for field in oneof.fields:

            members.append({
                "name":
                    field.name,

                "number":
                    field.number,

                "type":
                    field.type,

                "cpp_type":
                    field.cpp_type,

                "message_type":
                    (
                        field.message_type.full_name
                        if field.message_type
                        else None
                    )
            })

        info.append({

            "name":
                oneof.name,

            "members":
                members
        })

    return info


def decode_wire_events(
    events
):

    decoded = []

    for row in events:

        if row.get("type") != "EVENT":
            continue

        wire = row.get(
            "wire"
        )

        if not wire:
            continue

        encoded = wire.get(
            "protobuf_base64"
        )

        if not encoded:
            continue

        raw = base64.b64decode(
            encoded
        )

        event = (
            api_pb2.SubscribeResponse()
        )

        event.ParseFromString(
            raw
        )

        kind = event.WhichOneof(
            "kind"
        )

        record = {

            "elapsed":
                row.get(
                    "elapsed"
                ),

            "captured_kind":
                row.get(
                    "kind"
                ),

            "decoded_kind":
                kind,

            "protobuf_bytes":
                len(raw),

            "protobuf_sha256":
                hashlib.sha256(
                    raw
                ).hexdigest(),

            "decoded":
                MessageToDict(
                    event,
                    preserving_proto_field_name=True
                )
        }

        decoded.append(
            record
        )

    return decoded


def find_update_descriptor():

    response_desc = (
        api_pb2.SubscribeResponse.DESCRIPTOR
    )

    for field in response_desc.fields:

        if field.name == "l2_update":

            if field.message_type:

                return field.message_type

    return None


def descriptor_fields(desc):

    if desc is None:
        return []

    result = []

    for field in desc.fields:

        result.append({

            "name":
                field.name,

            "number":
                field.number,

            "type":
                field.type,

            "label":
                field.label,

            "message_type":
                (
                    field.message_type.full_name
                    if field.message_type
                    else None
                ),

            "enum_type":
                (
                    field.enum_type.full_name
                    if field.enum_type
                    else None
                )
        })

    return result


def extract_updates(decoded):

    updates = []

    for item in decoded:

        if (
            item.get(
                "decoded_kind"
            )
            != "l2_update"
        ):
            continue

        decoded = item.get(
            "decoded",
            {}
        )

        updates.append({

            "elapsed":
                item.get(
                    "elapsed"
                ),

            "protobuf_bytes":
                item.get(
                    "protobuf_bytes"
                ),

            "protobuf_sha256":
                item.get(
                    "protobuf_sha256"
                ),

            "message":
                decoded.get(
                    "l2_update"
                )
        })

    return updates


def main():

    print("=" * 100)
    print("FLINT L2 UPDATE OFFLINE FORENSICS v4.7")
    print("=" * 100)

    print()
    print(
        "SOURCE:"
    )

    print(
        SOURCE
    )

    if not SOURCE.exists():

        raise FileNotFoundError(
            str(SOURCE)
        )

    events = load_events()

    print()
    print(
        "Events loaded:",
        len(events)
    )

    decoded = decode_wire_events(
        events
    )

    print(
        "Wire events decoded:",
        len(decoded)
    )

    # --------------------------------------------------------
    # COUNTS
    # --------------------------------------------------------

    kinds = {}

    for item in decoded:

        kind = item.get(
            "decoded_kind"
        )

        kinds[kind] = (
            kinds.get(
                kind,
                0
            )
            + 1
        )

    print()
    print(
        "EVENT KINDS"
    )

    for kind, count in sorted(
        kinds.items(),
        key=lambda x: str(x[0])
    ):

        print(
            " ",
            kind,
            count
        )

    # --------------------------------------------------------
    # RESPONSE DESCRIPTOR
    # --------------------------------------------------------

    print()
    print(
        "SUBSCRIBE RESPONSE ONEOF"
    )

    oneof_info = message_descriptor_info()

    for oneof in oneof_info:

        print()
        print(
            "ONEOF:",
            oneof["name"]
        )

        for field in oneof["members"]:

            print(
                " ",
                field["name"],
                "number=",
                field["number"],
                "message=",
                field["message_type"]
            )

    # --------------------------------------------------------
    # L2 UPDATE DESCRIPTOR
    # --------------------------------------------------------

    update_desc = find_update_descriptor()

    print()
    print(
        "L2 UPDATE DESCRIPTOR"
    )

    if update_desc is None:

        print(
            "L2 update descriptor not found."
        )

    else:

        print(
            "full_name:",
            update_desc.full_name
        )

        for field in descriptor_fields(
            update_desc
        ):

            print(
                " ",
                field
            )

    # --------------------------------------------------------
    # UPDATES
    # --------------------------------------------------------

    updates = extract_updates(
        decoded
    )

    print()
    print(
        "L2 UPDATE PAYLOADS"
    )

    print(
        "count:",
        len(updates)
    )

    for index, update in enumerate(
        updates,
        1
    ):

        print()
        print(
            "UPDATE #",
            index
        )

        print(
            "elapsed:",
            update["elapsed"]
        )

        print(
            "bytes:",
            update["protobuf_bytes"]
        )

        print(
            "sha256:",
            update["protobuf_sha256"]
        )

        print(
            json.dumps(
                update["message"],
                indent=2,
                ensure_ascii=False
            )
        )

    # --------------------------------------------------------
    # SAVE
    # --------------------------------------------------------

    result = {

        "experiment":
            "Flint L2 Update Offline Forensics v4.7",

        "source":
            str(SOURCE),

        "generated_at":
            datetime.now(
                timezone.utc
            ).isoformat(),

        "event_count":
            len(events),

        "decoded_event_count":
            len(decoded),

        "kind_counts":
            kinds,

        "response_oneofs":
            oneof_info,

        "l2_update_descriptor":
            descriptor_fields(
                update_desc
            ),

        "updates":
            updates
    }

    result_path = (
        OUT /
        "result.json"
    )

    result_path.write_text(

        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
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
        "FLINT L2 UPDATE OFFLINE FORENSICS REPORT v4.7"
    )

    report.append(
        "=" * 100
    )

    report.append("")

    report.append(
        f"Source: {SOURCE}"
    )

    report.append(
        f"Events loaded: {len(events)}"
    )

    report.append(
        f"Wire events decoded: {len(decoded)}"
    )

    report.append("")

    report.append(
        "EVENT KINDS"
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
        "L2 UPDATE DESCRIPTOR"
    )

    report.append(
        "-" * 100
    )

    if update_desc is None:

        report.append(
            "Descriptor not found."
        )

    else:

        report.append(
            f"full_name: {update_desc.full_name}"
        )

        for field in descriptor_fields(
            update_desc
        ):

            report.append(
                json.dumps(
                    field,
                    ensure_ascii=False,
                    default=str
                )
            )

    report.append("")

    report.append(
        "RAW L2 UPDATE PAYLOADS"
    )

    report.append(
        "-" * 100
    )

    for index, update in enumerate(
        updates,
        1
    ):

        report.append(
            f"UPDATE #{index}"
        )

        report.append(
            f"elapsed={update['elapsed']}"
        )

        report.append(
            f"bytes={update['protobuf_bytes']}"
        )

        report.append(
            f"sha256={update['protobuf_sha256']}"
        )

        report.append(
            json.dumps(
                update["message"],
                indent=2,
                ensure_ascii=False,
                default=str
            )
        )

        report.append("")

    report.append(
        "INTERPRETATION"
    )

    report.append(
        "-" * 100
    )

    report.append(
        "This analysis decodes the exact protobuf payloads "
        "already captured during v4.6."
    )

    report.append(
        "It does not generate new market observations."
    )

    report.append(
        "The wire payload hash identifies the exact "
        "serialized message."
    )

    report.append(
        "No transaction was submitted."
    )

    report_path = (
        OUT /
        "REPORT.txt"
    )

    report_path.write_text(
        "\n".join(report),
        encoding="utf-8"
    )

    print()
    print("=" * 100)
    print("ANALYSIS COMPLETE")
    print("=" * 100)

    print()
    print(
        "RESULT:",
        result_path
    )

    print(
        "REPORT:",
        report_path
    )


try:

    main()

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

