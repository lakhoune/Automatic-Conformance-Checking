import unittest
import pandas as pd
from pyinsights import Connector
from pyinsights.log_skeleton import LogSkeleton
import os
from dotenv import load_dotenv

class SkeletonTester(unittest.TestCase):
    def setUp(self):
        load_dotenv()
        self.celonis_url = os.getenv("URL_ERA")
        self.api_token = os.getenv("TOKEN_ERA")
        self.key_type = os.getenv("KEY_TYPE_ERA")
        # define connector and connect to celonis
        connector = Connector(api_token=self.api_token,
                              url=self.celonis_url, key_type=self.key_type)
        running_model = os.getenv("ID_RUNNING_ERA")
        connector.set_parameters(
            model_id=running_model)
        self.log_skeleton = LogSkeleton(
            connector=connector)

    def test_directly_follows(self):
        """
        tests directly follows relation
        :return:
        """
        # compute directly follows relation
        directly_follows = self.log_skeleton._get_directly_follows(0)
        expected = set()
        self.assertTrue(directly_follows == expected)

    def test_directly_follows_for_case(self):
        """
            tests directly follows relation for a specific case id
            :return:
            """
        # check directly follows relation for specific case
        directly_follows_for_case = self.log_skeleton._get_directly_follows(
            0, case_id="078cf197-ee81-43dd-b233-36f15cca06d4")

        self.assertTrue(directly_follows_for_case == set([('check ticket', 'examine thoroughly'), (
            'decide', 'reject request'), ('examine thoroughly', 'decide'), ('register request', 'check ticket')]))

    def test_log_skeleton(self):
        celonis_url = os.getenv("URL_CFI")
        api_token = os.getenv("TOKEN_CFI")
        key_type = os.getenv("KEY_TYPE_CFI")
        model_id = os.getenv("ID_DEVIATION_LOG")
        connector = Connector(api_token=api_token,
                              url=celonis_url, key_type=key_type)
        connector.set_parameters(model_id=model_id)

        skeleton = LogSkeleton(connector)

        log_skeleton = skeleton.get_log_skeleton()

        expected_skeleton = {'equivalence': set(), 'always_after': {('b', 'c')}, 'always_before': {('b', 'a'), ('c', 'a')},
                             'never_together': set(), 'directly_follows': set(),
                             'activ_freq': {'a': {1, 2}, 'b': {0, 1}, 'c': {0, 1}}}

        self.assertTrue(log_skeleton == expected_skeleton)


if __name__ == '__main__':
    unittest.main()
