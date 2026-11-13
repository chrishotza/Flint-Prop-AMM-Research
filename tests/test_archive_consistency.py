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


if __name__ == "__main__":
    unittest.main()
