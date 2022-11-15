
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
if __name__ == "__main__":
    celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"

    connector = Connector(api_key=api_token, url=celonis_url)
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    datamodel = input()

    connector.set_datamodel(datamodel)

    temporal_profiler = TemporalProfiler(connector=connector)

    df = temporal_profiler.deviations(sigma=6)
    df2 = temporal_profiler.temporal_profile()
    deviating_cases_df = temporal_profiler.deviating_cases()

    print("Deviating cases:")
    print(deviating_cases_df.to_string())















