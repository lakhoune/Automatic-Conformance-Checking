import unittest
import pandas as pd
from pyinsights import Connector
from pyinsights.log_skeleton import LogSkeleton


class ResourceTester(unittest.TestCase):
    def setUp(self):
        self.celonis_url = "https://academic-rastoder-erdzan-rwth-aachen-de.eu-2.celonis.cloud/"
        self.api_token = "MDVkYWJkOGMtMDQ1OC00Mjc2LTk4ZjEtYzFkYTM5ZTliN2Q2OjA5WnZvUGtqUkNEK1JjUE9lVzMrckNUUm8vbnJ0WXBodmNnK0dCNTJDeDVi"
        self.key_type = "APP_KEY"
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type=self.key_type)
        connector.set_parameters(
            model_id="db5c61d7-fa76-494f-b081-a902a972bc4f")
        self.log_skeleton = LogSkeleton(
            connector=connector)

    def test_directly_follows(self):
        """
        tests directly follows relation
        :return:
        """
        # compute directly follows relation
        directly_follows = self.log_skeleton._get_directly_follows(None, 0)
        expected = set()
        self.assertTrue(directly_follows == expected)

    def test_directly_follows_for_case(self):
        """
            tests directly follows relation for a specific case id
            :return:
            """
        # check directly follows relation for specific case
        directly_follows_for_case = self.log_skeleton._get_directly_follows(
            None, 0, case_id_filter="078cf197-ee81-43dd-b233-36f15cca06d4")

        self.assertTrue(directly_follows_for_case == set([('check ticket', 'examine thoroughly'), (
            'decide', 'reject request'), ('examine thoroughly', 'decide'), ('register request', 'check ticket')]))


if __name__ == '__main__':
    unittest.main()
