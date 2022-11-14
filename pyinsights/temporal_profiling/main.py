
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from pyinsights.temporal_profiling import Connector
from pyinsights.temporal_profiling import TemporalProfiler
if __name__ == "__main__":
    celonis_url = "academic-m-s-qafari-pads-rwth-aachen-de.eu-2.celonis.cloud"
    api_token = "ZTUxZGNjNmItYzEwNy00MTI4LWJjZDctZmU1Zjg0Y2ZiYmQ0OkxRaHNRaEd6eHFMYXBwSlhyUkg1Z0NlUjBOUDlMbzdpcFZNNGx4cGdhdlJx"

    connector = Connector(api_key=api_token, url=celonis_url)
    connector.set_datamodel("d910eca4-b173-4a3a-aea6-cbc0fa86fcdd")

    temporal_profiler = TemporalProfiler(connector=connector)

    df = temporal_profiler.deviating_cases(sigma=6)
    df2 = temporal_profiler.temporal_profile()

    print(df2.to_string())















