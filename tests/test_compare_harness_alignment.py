from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[3] / 'tools' / 'compare_baseline_execfw_monthly.py'
spec = importlib.util.spec_from_file_location('compare_baseline_execfw_monthly', SCRIPT)
if spec is None or spec.loader is None:
    raise RuntimeError(f'failed to load {SCRIPT}')
compare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(compare)


class CompareHarnessAlignmentCase(unittest.TestCase):
    def test_advance_visible_idx_uses_closed_bar_boundary(self) -> None:
        bars_15m = [
            compare.OfflineKline('ETHUSDT', '15m', [0, 1, 1, 1, 1, 1, 899999]),
            compare.OfflineKline('ETHUSDT', '15m', [900000, 2, 2, 2, 2, 1, 1799999]),
        ]
        idx = 0
        idx = compare._advance_visible_idx(bars_15m, idx, 899999)
        self.assertEqual(idx, 1)
        idx = compare._advance_visible_idx(bars_15m, idx, 900000)
        self.assertEqual(idx, 1)
        idx = compare._advance_visible_idx(bars_15m, idx, 1799999)
        self.assertEqual(idx, 2)

    def test_bar_label_iso_uses_close_time_label(self) -> None:
        bar = compare.OfflineKline('ETHUSDT', '1h', [0, 1, 1, 1, 1, 1, 3599999])
        self.assertEqual(compare._bar_label_iso(bar), '1970-01-01T00:59:59.999000+00:00')


if __name__ == '__main__':
    unittest.main()
