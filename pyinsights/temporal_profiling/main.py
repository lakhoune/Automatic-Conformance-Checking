
from pyinsights.ml.statistics import batches_per_case
from pyinsights.log_skeleton import LogSkeleton
from pm4py.algo.conformance.temporal_profile import algorithm as temporal_profile_conformance
from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery
from pyinsights.conformance import alignment_scores
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler
from pyinsights import Connector
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pycelonis import get_celonis
import pandas as pd
import numpy as np
import sys
sys.path.append(
    "C:/Users/infer/PycharmProjects/Automatic-Conformance-Checking/")

if __name__ == "__main__":
    celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"

    from pyinsights import Connector
    from pyinsights.organisational_profiling import ResourceProfiler

    # define connector and connect to celonis
    connector = Connector(api_token=api_token,
                          url=celonis_url, key_type="USER_KEY")

    # choose data model
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    id = "376145f1-790d-4deb-8e20-083a4dfd7ca7"

    connector.set_parameters(
        model_id=id, end_timestamp="END_DATE", resource_column="CE_UO")

    from pyinsights.ml import anomaly_detection
    print(anomaly_detection(connector=connector).head(n=100).to_string())
    
