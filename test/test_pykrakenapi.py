import unittest
from pykrakenapi.pykrakenapi import *

import krakenex as k
import time


class TestPykrakenapi(unittest.TestCase):
    def test_public_callratelimiter(self):
        raw_api = k.API()
        pyApi = KrakenAPI(raw_api, crl_sleep=0)

        # check api calls with frequency lower than 1/1s
        time.sleep(1.1)
        ldt, lut = pyApi.get_server_time()
        for i in range(5):
            time.sleep(1.1)
            dt, ut = pyApi.get_server_time()
            self.assertGreaterEqual(ut - lut, 1.0)
            ldt = dt
            lut = ut

        # check api calls with frequency higher than 1/1s
        with self.assertRaises(CallRateLimitError):
            dt, ut = pyApi.get_server_time()


if __name__ == '__main__':
    unittest.main()
