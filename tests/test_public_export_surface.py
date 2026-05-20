from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PublicExportSurfaceTest(unittest.TestCase):
    def test_private_or_runtime_artifacts_are_not_tracked(self) -> None:
        forbidden_paths = [
            'exec_framework/runtime_worker.py',
            'exec_framework/market_data.py',
            'exec_framework/feature_builder.py',
            'exec_framework/strategy_adapter_selector.py',
            'exec_framework/v6c_adapter.py',
            'exec_framework/v6c_adapter_baseline.py',
            'exec_framework/v6c_la_free_v1_adapter.py',
            'docs/REAL_SUBMIT_ANOMALY_MATRIX_2026-04-16.md',
            'docs/REAL_SUBMIT_ANOMALY_SUBMIT_CANDIDATES_2026-04-16.md',
        ]
        for relpath in forbidden_paths:
            self.assertFalse((ROOT / relpath).exists(), relpath)

    def test_manual_probe_modules_are_not_exported(self) -> None:
        module_paths = sorted((ROOT / 'exec_framework').glob('manual_*'))
        test_paths = sorted((ROOT / 'tests').glob('test_manual_*'))
        self.assertEqual(module_paths, [])
        self.assertEqual(test_paths, [])

    def test_runtime_output_is_ignored_not_tracked_surface(self) -> None:
        self.assertTrue((ROOT / '.gitignore').read_text(encoding='utf-8').count('runtime/') >= 1)


if __name__ == '__main__':
    unittest.main()
