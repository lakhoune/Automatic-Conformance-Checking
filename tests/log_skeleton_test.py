import unittest
import pandas as pd
from pyinsights import Connector
from pyinsights.log_skeleton import LogSkeleton


class SkeletonTester(unittest.TestCase):
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
        directly_follows = self.log_skeleton._get_directly_follows(0)
        expected = set()
        self.assertTrue(directly_follows == expected)

    def test_directly_follows_for_case(self):
        """
            tests directly follows relation for a specific case id
            :return:
            """
        # check directly follows relation for specific case
        directly_follows_for_case = self.log_skeleton._get_directly_follows(0, case_id="078cf197-ee81-43dd-b233-36f15cca06d4")

        self.assertTrue(directly_follows_for_case == set([('check ticket', 'examine thoroughly'), (
            'decide', 'reject request'), ('examine thoroughly', 'decide'), ('register request', 'check ticket')]))


    def test_log_skeleton(self):
        celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
        api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"
        model_id = "bf7dfa1e-5e86-470e-9f7c-5672e8b1637f"

        connector = Connector(api_token=api_token, url=celonis_url, key_type="USER_KEY")
        connector.set_parameters(model_id=model_id)

        skeleton = LogSkeleton(connector)

        log_skeleton = skeleton.get_log_skeleton()

        expected_skeleton = {'equivalence': {('check ticket', 'decide'), ('decide', 'check ticket')}, 'always_after': {('register request', 'check ticket'), ('examine thoroughly', 'decide'), ('register request', 'decide')}, 'always_before': {('pay compensation', 'examine casually'), ('pay compensation', 'register request'), ('reject request', 'check ticket'), ('pay compensation', 'check ticket'), ('examine thoroughly', 'register request'), ('reject request', 'decide'), ('reject request', 'register request'), ('pay compensation', 'decide')}, 'never_together': {('reject request', 'pay compensation'), ('pay compensation', 'reject request')}, 'directly_follows': set(), 'activ_freq': {'check ticket': {1, 2, 3}, 'decide': {1, 2, 3}, 'examine casually': {0, 1, 3}, 'examine thoroughly': {0, 1}, 'pay compensation': {0, 1}, 'register request': {1}, 'reinitiate request': {0, 1, 2}, 'reject request': {0, 1}}}
        {('pay compensation', 'examine casually'), ('pay compensation', 'register request'), ('reject request', 'register request'), ('examine thoroughly', 'register request')}
        
        
        self.assertTrue(log_skeleton == expected_skeleton)

if __name__ == '__main__':
    unittest.main()
