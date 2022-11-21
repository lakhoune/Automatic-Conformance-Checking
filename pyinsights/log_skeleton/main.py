import os
from dotenv import load_dotenv
from pyinsights.log_skeleton import LogSkeleton
from pyinsights import Connector

dirname = os.path.dirname(__file__)
load_dotenv(os.path.join(dirname, "../../.env"))

if __name__ == "__main__":
    print("Available datamodels:")
    celonis_url = "https://academic-rastoder-erdzan-rwth-aachen-de.eu-2.celonis.cloud/"
    api_token = "MDVkYWJkOGMtMDQ1OC00Mjc2LTk4ZjEtYzFkYTM5ZTliN2Q2OjA5WnZvUGtqUkNEK1JjUE9lVzMrckNUUm8vbnJ0WXBodmNnK0dCNTJDeDVi"

    connector = Connector(api_token=api_token, url=celonis_url)
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    datamodel = input()

    connector.set_datamodel(datamodel)

    log_skeleton = LogSkeleton(connector=connector)
