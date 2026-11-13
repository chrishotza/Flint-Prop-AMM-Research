#!/usr/bin/env python3
"""Check internal consistency of archived v4.12 evidence, entirely offline.

This does NOT replay wire events or independently verify the observed market.
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ARCHIVE = ROOT / "results" / "raw" / "reconstruction_validation_result.json"


def check_archived_result(payload: dict) -> dict:
    """Reject overstated case counts and self-contradictory archived records."""
    if not isinstance(payload, dict):
        raise ValueError("Archive must contain an object")
    cases = payload.get("cases")
    updates = payload.get("persistent_updates")
    summary = payload.get("summary")
    if not isinstance(cases, list) or not isinstance(updates, list) or not isinstance(summary, dict):
        raise ValueError("Missing cases, persistent_updates, or summary")
    if not cases or not updates:
        raise ValueError("No observed update-driven validation cases")
    ids = [u.get("update_id") for u in updates]
    if any(not isinstance(x, int) or isinstance(x, bool) for x in ids):
        raise ValueError("Non-integer update identifier")
    if len(set(ids)) != len(ids) or ids != sorted(ids):
        raise ValueError("Update identifiers must be unique and increasing")
    seen_cases: set[int] = set()
    validated = exact = 0
    for c in cases:
        number = c.get("case")
        if not isinstance(number, int) or number in seen_cases:
            raise ValueError("Duplicate or invalid case number")
        seen_cases.add(number)
        source_id = c.get("source_update_id")
        used = c.get("used_update_ids")
        if source_id not in ids or not isinstance(used, list) or not used:
            raise ValueError(f"Case {number}: missing observed source update")
        if any(x not in ids for x in used) or used != sorted(set(used)) or used[-1] != source_id:
            raise ValueError(f"Case {number}: invalid or out-of-order provenance")
        if c.get("status") == "VALIDATED":
            validated += 1
            if c.get("reference_update_id") != source_id:
                raise ValueError(f"Case {number}: reference is from a different update")
            match = c.get("comparison")
            if not isinstance(match, dict):
                raise ValueError(f"Case {number}: no comparison")
            digest_equal = c.get("reconstructed_digest") == c.get("reference_digest")
            if match.get("exact_match") is True:
                if not digest_equal or not c.get("reconstructed_digest"):
                    raise ValueError(f"Case {number}: exact match contradicted by digests")
                if match.get("bid_differences") or match.get("ask_differences"):
                    raise ValueError(f"Case {number}: exact match has level differences")
                exact += 1
            elif match.get("exact_match") is not False:
                raise ValueError(f"Case {number}: invalid match classification")
    observed = {"cases": len(cases), "validated": validated, "exact_matches": exact}
    if summary != observed:
        raise ValueError(f"Archived summary conflicts with case records: {observed}")
    return observed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", nargs="?", type=Path, default=ARCHIVE)
    args = parser.parse_args()
    payload = json.loads(args.archive.read_text(encoding="utf-8-sig"))
    print("Offline archived-evidence consistency:", check_archived_result(payload))
    print("Scope: internal consistency only; not live or independently replicated market validation.")


if __name__ == "__main__":
    main()
