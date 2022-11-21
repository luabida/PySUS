import unittest

import pandas as pd

from pysus.online_data.vaccine import download_covid


class VaccineTestCase(unittest.TestCase):
    def test_Download(self):
        """Careful! this download can take a long time"""
        df = download_covid("BA", only_header=True)
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
