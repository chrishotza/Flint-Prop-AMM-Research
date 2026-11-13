import copy
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts" / "validation"))
from audit_archived_reconstruction import check_archived_result


def archive():
    return json.loads((ROOT / "results" / "raw" /
                       "reconstruction_validation_result.json").read_text(encoding="utf-8-sig"))


class ArchivedEvidenceTests(unittest.TestCase):
    def test_historical_v412_case_count_and_provenance(self):
        # This checks claims against the archived document, not a live replay.
        self.assertEqual(check_archived_result(archive()),
                         {"cases": 3, "validated": 3, "exact_matches": 3})

    def test_rejects_nonobject_case_and_update_records(self):
        for section in ("cases", "persistent_updates"):
            with self.subTest(section=section):
                record = archive()
                record[section][0] = None
                with self.assertRaisesRegex(ValueError, "must be objects"):
                    check_archived_result(record)

    def test_rejects_boolean_ids_disguised_as_integers(self):
        bad_number = archive()
        bad_number["cases"][0]["case"] = True
        with self.assertRaisesRegex(ValueError, "invalid case number"):
            check_archived_result(bad_number)
        bad_source = archive()
        bad_source["cases"][0]["source_update_id"] = True
        with self.assertRaisesRegex(ValueError, "source update"):
            check_archived_result(bad_source)
        bad_used = archive()
        bad_used["cases"][0]["used_update_ids"] = [True]
        with self.assertRaisesRegex(ValueError, "provenance"):
            check_archived_result(bad_used)

    def test_rejects_fabricated_or_malformed_exact_sha256(self):
        record = archive()
        record["cases"][0]["reconstructed_digest"] = "fabricated"
        record["cases"][0]["reference_digest"] = "fabricated"
        with self.assertRaisesRegex(ValueError, "SHA256 digest"):
            check_archived_result(record)

    def test_rejects_null_diff_lists_and_boolean_summary(self):
        record = archive()
        record["cases"][0]["comparison"]["bid_differences"] = None
        with self.assertRaisesRegex(ValueError, "level differences structure"):
            check_archived_result(record)
        bad_summary = archive()
        bad_summary["summary"]["validated"] = True
        with self.assertRaisesRegex(ValueError, "Summary counts"):
            check_archived_result(bad_summary)

    def test_detects_summary_overstatement(self):
        fixture = archive()
        fixture["summary"]["exact_matches"] += 1
        with self.assertRaisesRegex(ValueError, "summary conflicts"):
            check_archived_result(fixture)

    def test_detects_digest_mismatch(self):
        fixture = archive()
        fixture["cases"][0]["reference_digest"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "digests"):
            check_archived_result(fixture)

    def test_detects_unobserved_provenance(self):
        fixture = archive()
        fixture["cases"][0]["used_update_ids"] = [123]
        with self.assertRaisesRegex(ValueError, "provenance"):
            check_archived_result(fixture)

    def test_detects_mismatched_reference_time(self):
        fixture = archive()
        fixture["cases"][0]["reference_update_id"] += 1
        with self.assertRaisesRegex(ValueError, "different update"):
            check_archived_result(fixture)


    def test_detects_case_slot_and_elapsed_metadata_tampering(self):
        for field, mutated, message in (
            ("source_slot", 123456, "source slot"),
            ("reference_slot", 123456, "reference slot"),
            ("source_elapsed", 0, "source elapsed"),
            ("source_elapsed", float("inf"), "source elapsed"),
            ("source_elapsed", True, "source elapsed"),
        ):
            with self.subTest(field=field, mutated=mutated):
                fixture = archive()
                fixture["cases"][1][field] = mutated
                with self.assertRaisesRegex(ValueError, message):
                    check_archived_result(fixture)

    def test_detects_corrupt_update_timing_and_slot(self):
        for field, changed, message in (
            ("slot", True, "Update slot"),
            ("slot", -1, "Update slot"),
            ("slot", 1, "nondecreasing"),
            ("elapsed", float("nan"), "Update elapsed"),
            ("elapsed", -1, "Update elapsed"),
            ("elapsed", 1, "nondecreasing"),
        ):
            with self.subTest(field=field, changed=changed):
                fixture = archive()
                fixture["persistent_updates"][1][field] = changed
                with self.assertRaisesRegex(ValueError, message):
                    check_archived_result(fixture)

    def test_detects_missing_intermediate_replay_updates(self):
        fixture = archive()
        updates = fixture["persistent_updates"]
        fixture["cases"][2]["used_update_ids"] = [
            updates[0]["update_id"], updates[2]["update_id"]
        ]
        with self.assertRaisesRegex(ValueError, "omitted source updates"):
            check_archived_result(fixture)

    def test_rejects_unsupported_status_even_if_summary_rewritten(self):
        fixture = archive()
        fixture["cases"][0]["status"] = "PENDING"
        fixture["summary"]["validated"] -= 1
        fixture["summary"]["exact_matches"] -= 1
        with self.assertRaisesRegex(ValueError, "unsupported archive case status"):
            check_archived_result(fixture)


if __name__ == "__main__":
    unittest.main()
