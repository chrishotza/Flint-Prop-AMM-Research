#!/usr/bin/env python3
"""Check internal consistency of archived v4.12 evidence, entirely offline.

This does NOT replay wire events or independently verify the observed market.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import re

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
    if any(not isinstance(item, dict) for item in cases + updates):
        raise ValueError("Case and update entries must be objects")
    if any(type(summary.get(key)) is not int or summary[key] < 0
           for key in ("cases", "validated", "exact_matches")):
        raise ValueError("Summary counts must be nonnegative integers")
    ids = [u.get("update_id") for u in updates]
    if any(not isinstance(x, int) or isinstance(x, bool) for x in ids):
        raise ValueError("Non-integer update identifier")
    if len(set(ids)) != len(ids) or ids != sorted(ids):
        raise ValueError("Update identifiers must be unique and increasing")
    update_by_id = {}
    previous_slot = None
    previous_elapsed = None
    for update in updates:
        identifier = update["update_id"]
        slot = update.get("slot")
        elapsed = update.get("elapsed")
        if type(slot) is not int or slot < 0:
            raise ValueError("Update slot must be a nonnegative integer")
        if type(elapsed) not in (int, float):
            raise ValueError("Update elapsed must be a finite nonnegative number")
        try:
            valid_elapsed = math.isfinite(elapsed)
        except OverflowError:
            valid_elapsed = False
        if not valid_elapsed or elapsed < 0:
            raise ValueError("Update elapsed must be a finite nonnegative number")
        if (previous_slot is not None and slot < previous_slot
                or previous_elapsed is not None and elapsed < previous_elapsed):
            raise ValueError("Update slot and elapsed must be nondecreasing")
        update_by_id[identifier] = update
        previous_slot, previous_elapsed = slot, elapsed
    seen_cases: set[int] = set()
    validated = exact = 0
    for c in cases:
        number = c.get("case")
        if type(number) is not int or number < 1 or number in seen_cases:
            raise ValueError("Duplicate or invalid case number")
        seen_cases.add(number)
        source_id = c.get("source_update_id")
        used = c.get("used_update_ids")
        if type(source_id) is not int or source_id not in ids or not isinstance(used, list) or not used:
            raise ValueError(f"Case {number}: missing observed source update")
        if (any(type(x) is not int or x not in ids for x in used)
                or used != sorted(set(used)) or used[-1] != source_id):
            raise ValueError(f"Case {number}: invalid or out-of-order provenance")
        # Reconstructing from the initial snapshot requires every archived
        # update up through the case's claimed source, not a selected subset.
        if used != ids[:ids.index(source_id) + 1]:
            raise ValueError(f"Case {number}: omitted source updates")
        source = update_by_id[source_id]
        if c.get("source_slot") != source["slot"] or type(c.get("source_slot")) is not int:
            raise ValueError(f"Case {number}: source slot conflicts with update")
        elapsed = c.get("source_elapsed")
        if type(elapsed) not in (int, float):
            raise ValueError(f"Case {number}: source elapsed conflicts with update")
        try:
            elapsed_matches = math.isfinite(elapsed) and math.isclose(
                elapsed, source["elapsed"], rel_tol=0, abs_tol=1e-6
            )
        except OverflowError:
            elapsed_matches = False
        if not elapsed_matches:
            raise ValueError(f"Case {number}: source elapsed conflicts with update")
        if c.get("status") != "VALIDATED":
            raise ValueError(f"Case {number}: unsupported archive case status")
        if c.get("status") == "VALIDATED":
            validated += 1
            if c.get("reference_update_id") != source_id:
                raise ValueError(f"Case {number}: reference is from a different update")
            if type(c.get("reference_slot")) is not int or c["reference_slot"] != source["slot"]:
                raise ValueError(f"Case {number}: reference slot differs from observed update")
            match = c.get("comparison")
            if not isinstance(match, dict):
                raise ValueError(f"Case {number}: no comparison")
            reconstructed = c.get("reconstructed_digest")
            reference = c.get("reference_digest")
            digest_equal = reconstructed == reference
            if match.get("exact_match") is True:
                if not (isinstance(reconstructed, str)
                        and isinstance(reference, str)
                        and re.fullmatch(r"[a-fA-F0-9]{64}", reconstructed)
                        and re.fullmatch(r"[a-fA-F0-9]{64}", reference)):
                    raise ValueError(f"Case {number}: invalid SHA256 digest")
                if not digest_equal:
                    raise ValueError(f"Case {number}: exact match contradicted by digests")
                bids = match.get("bid_differences")
                asks = match.get("ask_differences")
                if not isinstance(bids, list) or not isinstance(asks, list):
                    raise ValueError(f"Case {number}: invalid level differences structure")
                if bids or asks:
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
