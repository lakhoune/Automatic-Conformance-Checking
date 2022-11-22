import os
from dotenv import load_dotenv
from pyinsights.log_skeleton import LogSkeleton
from pyinsights import Connector

dirname = os.path.dirname(__file__)
load_dotenv(os.path.join(dirname, "../../.env"))

if __name__ == "__main__":
    celonis_url = os.getenv(
        "CELONIS_URL") or "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = os.getenv(
        "API_TOKEN") or "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"
    print(celonis_url)
    key_type = os.getenv(
        "KEY_TYPE") or "USER_KEY"
    connector = Connector(api_token=api_token,
                          url=celonis_url, key_type=key_type)

    log_skeleton = LogSkeleton(connector=connector)
