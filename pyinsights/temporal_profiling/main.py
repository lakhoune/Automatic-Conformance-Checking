
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights import Connector
from pyinsights.organisational_profiling import ResourceProfiler
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.conformance import alignment_scores
from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery
from pm4py.algo.conformance.temporal_profile import algorithm as temporal_profile_conformance


if __name__ == "__main__":
    celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"

    # define connector and connect to celonis
    connector = Connector(api_token=api_token, url=celonis_url, key_type="USER_KEY")

    # choose data model
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    #id = input()
    connector.set_paramters(model_id="01184974-3604-49fc-b410-ad88143f9802")#, end_timestamp="END_DATE")

    res_profiler = ResourceProfiler(connector=connector, resource_column="CE_UO")
    df = res_profiler.cases_with_batches()
    res_profile = res_profiler.resource_profile()
    print(df.to_string())

