import unittest
from dotenv import load_dotenv
import pandas as pd
import os
from pyinsights import Connector
from pyinsights.anomaly_detection import anomaly_detection


class AnomalyDetectionTest(unittest.TestCase):

    def setUp(self):
        load_dotenv()
        self.celonis_url = os.getenv("URL_CFI")
        self.api_token = os.getenv("TOKEN_CFI")
        self.key_type = os.getenv("KEY_TYPE_CFI")

    def test_anomaly_detection(self):
        """
        tests anomaly detection on simple log
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type=self.key_type)

        running_ex_log = os.getenv("ID_RUNNING_CFI")
        connector.set_parameters(
            model_id=running_ex_log, resource_column="org:resource")

        # get anomalies
        anomalies = anomaly_detection(
            connector=connector, parameter_optimization=False)

        # assert that deviating case is detected
        case_col = connector.case_col()
        self.assertTrue(any(anomalies[case_col].isin([5])))


if __name__ == '__main__':
    unittest.main()
