import unittest

import pandas as pd

from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler

class TemporalTest(unittest.TestCase):

    def setUp(self):

        self.celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
        self.api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"



    def test_temporal_profile_end_time(self):
        """
        tests temporal profile when dataset has end-timestamp
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token, url=self.celonis_url, key_type="USER_KEY")
        connector.set_parameters(
            model_id="99e62af6-ce51-445d-a1e6-634aaeafff11", end_timestamp='end_time')
        temporal_profiler = TemporalProfiler(connector=connector)
        # compute temporal profile
        profile = temporal_profiler.temporal_profile()

        # expected output
        waiting_expected = pd.DataFrame({'source': ['a','b','a','a'], 'target': ['b','c','a','c'], 'avg waiting time': [3600.0,3600.0,3600.0,3600.0], 'std waiting time': [0.0,0.0,0.0,0.0]})
        sojourn_expected = pd.DataFrame({'ACTIVITY': ['a','b','c'], 'avg sojourn time': [105.0,60.0,60.0], 'std sojourn': [90.0,0.0,0.0]})

        # assert that output matches exptected result
        self.assertTrue(profile['waiting times'].equals(waiting_expected))  # add assertion here

        self.assertTrue(profile['sojourn times'].equals(sojourn_expected))

    def test_temporal_profile_no_end_time(self):
        """
        tests temporal profile when dataset has no end-timestamp
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token, url=self.celonis_url, key_type="USER_KEY")
        connector.set_parameters(
            model_id="99e62af6-ce51-445d-a1e6-634aaeafff11")
        temporal_profiler = TemporalProfiler(connector=connector)
        # compute temporal profile
        profile = temporal_profiler.temporal_profile()

        # expected output
        waiting_expected = pd.DataFrame({'source': ['a', 'b', 'a', 'a'], 'target': ['b', 'c', 'a', 'c'],
                                         'avg waiting time': [3660.0, 3660.0, 3660.0, 3660.0],
                                         'std waiting time': [0.0, 0.0, 0.0, 0.0]})
        sojourn_expected = pd.DataFrame()

        # assert that output matches exptected result
        self.assertTrue(profile['waiting times'].equals(waiting_expected))  # add assertion here

        self.assertTrue(profile['sojourn times'].equals(sojourn_expected))

    def test_deviating_cases(self):
        """
        tests identification of deviating cases
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token, url=self.celonis_url, key_type="USER_KEY")
        connector.set_parameters(
            model_id="32b0abb8-bbcf-4700-8123-d11443e57bdd", end_timestamp="end_time")
        temporal_profiler = TemporalProfiler(connector=connector)

        # compute deviating cases
        deviating_cases = temporal_profiler.deviating_cases(sigma=1)

        # expected output
        expected_deviations = pd.DataFrame({connector.case_col():[1, 2, 2], 'source': ['a', 'a', 'a'], 'target': ['b', 'a', 'c']})

        self.assertTrue(deviating_cases[[connector.case_col(), 'source', 'target']].equals(expected_deviations))



if __name__ == '__main__':
    unittest.main()
