import unittest
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler

class TemporalTest(unittest.TestCase):

    def setUp(self):

        self.celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
        self.api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"


    def test_waiting_time(self):
        # define connector and connect to celonis
        self.connector = Connector(api_token=self.api_token, url=self.celonis_url)
        self.connector.set_paramters(model_id="")
        temporal_profiler = TemporalProfiler(connector=self.connector)

        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()
