
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

    from pyinsights import Connector
    from pyinsights.organisational_profiling import ResourceProfiler

    # define connector and connect to celonis
    connector = Connector(api_token=api_token, url=celonis_url, key_type="USER_KEY")

    # choose data model
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    #id = input()

    connector.set_parameters(model_id="376145f1-790d-4deb-8e20-083a4dfd7ca7", end_timestamp="END_DATE")

    # init resource profiler
    res_profiler = ResourceProfiler(connector=connector, resource_column="CE_UO")

    # compute resource profile (not needed for next step)
    #res_profile = res_profiler.resource_profile(time_unit="HOURS", reference_unit="DAY")
    # get cases with batches
    df = res_profiler.cases_with_batches(time_unit="HOURS", reference_unit="DAY", min_batch_size=2, batch_percentage=0.1
                       , grouped_by_batches=True, batch_types=True)
    print(df.head(n=500).to_string())

