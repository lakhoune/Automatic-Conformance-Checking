import unittest
from dotenv import load_dotenv
import pandas as pd
import os
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler


class TemporalTest(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        self.celonis_url = os.getenv("URL_CFI")
        self.api_token = os.getenv("TOKEN_CFI")
        self.key_type = os.getenv("KEY_TYPE_CFI")

    def test_temporal_profile_end_time(self):
        """
        tests temporal profile when dataset has end-timestamp
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type=self.key_type)
        model_id = os.getenv("ID_TEMPORAL_TEST_LOG")
        connector.set_parameters(
            model_id=model_id, end_timestamp='end_time')
        temporal_profiler = TemporalProfiler(connector=connector)
        # compute temporal profile
        profile = temporal_profiler.temporal_profile()

        # expected output
        waiting_expected = pd.DataFrame({'source': ['a', 'b', 'a', 'a'], 'target': ['b', 'c', 'a', 'c'], 'avg waiting time': [
                                        3600.0, 3600.0, 3600.0, 3600.0], 'std waiting time': [0.0, 0.0, 0.0, 0.0]})
        sojourn_expected = pd.DataFrame({'ACTIVITY': ['a', 'b', 'c'], 'avg sojourn time': [
                                        105.0, 60.0, 60.0], 'std sojourn': [90.0, 0.0, 0.0]})

        # assert that output matches exptected result
        self.assertTrue(profile['waiting times'].equals(
            waiting_expected))  # add assertion here

        self.assertTrue(profile['sojourn times'].equals(sojourn_expected))

    def test_temporal_profile_no_end_time(self):
        """
        tests temporal profile when dataset has no end-timestamp
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type="USER_KEY")
        model_id = os.getenv("ID_TEMPORAL_TEST_LOG")
        connector.set_parameters(
            model_id=model_id)
        temporal_profiler = TemporalProfiler(connector=connector)
        # compute temporal profile
        profile = temporal_profiler.temporal_profile()

        # expected output
        waiting_expected = pd.DataFrame({'source': ['a', 'b', 'a', 'a'], 'target': ['b', 'c', 'a', 'c'],
                                         'avg waiting time': [3660.0, 3660.0, 3660.0, 3660.0],
                                         'std waiting time': [0.0, 0.0, 0.0, 0.0]})
        sojourn_expected = pd.DataFrame()

        # assert that output matches exptected result
        self.assertTrue(profile['waiting times'].equals(
            waiting_expected))  # add assertion here

        self.assertTrue(profile['sojourn times'].equals(sojourn_expected))

    def test_deviating_cases(self):
        """
        tests identification of deviating cases
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type=self.key_type)
        model_id = os.getenv("ID_DEVIATION_LOG")
        connector.set_parameters(
            model_id=model_id, end_timestamp="end_time")
        temporal_profiler = TemporalProfiler(connector=connector)

        # compute deviating cases
        deviating_cases = temporal_profiler.deviating_cases(sigma=1)
        # expected output
        expected_deviations = pd.DataFrame({connector.case_col(): [1,1,1, 2, 2, 2], 'source': [
                                           'a', 'b', 'c', 'a', 'a', 'c'], 'target': ['b', 'c', 'END', 'a', 'c', 'END']})

        self.assertTrue(deviating_cases[[connector.case_col(
        ), 'source', 'target']].equals(expected_deviations))


if __name__ == '__main__':
    unittest.main()
