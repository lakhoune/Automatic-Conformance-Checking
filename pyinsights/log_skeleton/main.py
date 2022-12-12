import os
import pm4py
# from dotenv import load_dotenv
from pyinsights.log_skeleton import LogSkeleton
from pyinsights import Connector

dirname = os.path.dirname(__file__)
# load_dotenv(os.path.join(dirname, "../../.env"))

if __name__ == "__main__":
    celonis_url = "https://academic-rastoder-erdzan-rwth-aachen-de.eu-2.celonis.cloud/"
    api_token = "MDVkYWJkOGMtMDQ1OC00Mjc2LTk4ZjEtYzFkYTM5ZTliN2Q2OjA5WnZvUGtqUkNEK1JjUE9lVzMrckNUUm8vbnJ0WXBodmNnK0dCNTJDeDVi"
    key_type = "APP_KEY"
    connector = Connector(api_token=api_token,
                          url=celonis_url, key_type=key_type)
    # choose data model
    # print("Available datamodels:")
    # print(connector.celonis.datamodels)

    # print("Input id of datamodel:")
    # id = input()
    # connector.set_parameters(model_id=id)

