import os
from dotenv import load_dotenv
from pyinsights.log_skeleton import LogSkeleton
from pyinsights import Connector

dirname = os.path.dirname(__file__)
load_dotenv(os.path.join(dirname, "../../.env"))

if __name__ == "__main__":
    print("Available datamodels:")
    celonis_url = os.getenv("CELONIS_URL")
    api_token = os.getenv("CELONIS_API_TOKEN")

    connector = Connector(api_key=api_token, url=celonis_url)
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    datamodel = input()

    connector.set_datamodel(datamodel)

    log_skeleton = LogSkeleton(connector=connector)
