import os
from dotenv import load_dotenv
from pyinsights import Connector
from pyinsights.log_skeleton import LogSkeleton

load_dotenv("../../.env")

if __name__ == "__main__":
    celonis_url = os.getenv("CELONIS_URL")
    api_token = os.getenv("API_TOKEN")

    connector = Connector(api_key=api_token, url=celonis_url)
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    datamodel = input()

    connector.set_datamodel(datamodel)

    log_skeleton = LogSkeleton(connector=connector)
