import unittest

import pandas as pd

from pyinsights import Connector
from pyinsights.anomaly_detection import anomaly_detection


class AnomalyDetectionTest(unittest.TestCase):

    def setUp(self):

        self.celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
        self.api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"

    def test_anomaly_detection(self):
        """
        tests anomaly detection on simple log
        :return:
        """
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type="USER_KEY")

        connector.set_parameters(
            model_id="bf7dfa1e-5e86-470e-9f7c-5672e8b1637f", resource_column="org:resource")

        # get anomalies
        anomalies = anomaly_detection(
            connector=connector, parameter_optimization=False)

        # assert that deviating case is detected
        case_col = connector.case_col()
        self.assertTrue(any(anomalies[case_col].isin([5])))


if __name__ == '__main__':
    unittest.main()
