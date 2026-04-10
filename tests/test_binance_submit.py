from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from exec_framework.binance_submit import BinanceSignedSubmitClient


class BinanceSubmitParseResponseCase(unittest.TestCase):
    def test_parse_submit_response_prefers_transact_time(self):
        submitter = BinanceSignedSubmitClient.__new__(BinanceSignedSubmitClient)
        response = submitter._parse_submit_response(
            200,
            {
                'orderId': 12345,
                'clientOrderId': 'cid-1',
                'status': 'NEW',
                'transactTime': 1711380000123,
                'updateTime': 1711380000999,
                'executedQty': '0',
                'avgPrice': '0.0',
            },
        )
        self.assertEqual(response.transact_time_ms, 1711380000123)


if __name__ == '__main__':
    unittest.main()
