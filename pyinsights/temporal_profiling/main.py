import itertools

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px
from pycelonis import get_celonis
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights import Connector
from pyinsights.organisational_profiling import ResourceProfiler
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.conformance import alignment_scores
from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery
from pm4py.algo.conformance.temporal_profile import algorithm as temporal_profile_conformance
import itertools

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
    connector.set_parameters(model_id="376145f1-790d-4deb-8e20-083a4dfd7ca7")#, end_timestamp="END_DATE")

    datamodel = connector.datamodel
    activity_table = connector.activity_table()
    case_col = connector.case_col()
    act_col = connector.activity_col()
    timestamp = connector.timestamp()
    transition_mode = "ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]"

    query = PQL()
    query.add(PQLColumn(name=case_col,
                        query=f"""DISTINCT "{activity_table}"."{case_col}"  """))
    query.add(PQLColumn(name=act_col,
                        query=f""" "{activity_table}"."{act_col}"  """))
    query.add(PQLColumn(
        name="max nr", query=f"""
        PU_MAX( DOMAIN_TABLE("{activity_table}"."{case_col}", "{activity_table}"."{act_col}"),
        ACTIVATION_COUNT ( "{activity_table}"."{act_col}" ) ) """))
    query += PQLFilter(f"""
    PU_MAX( DOMAIN_TABLE("{activity_table}"."{case_col}", "{activity_table}"."{act_col}"),
        ACTIVATION_COUNT ( "{activity_table}"."{act_col}" ) ) > 1
    """)
    df = datamodel.get_data_frame(query)

    print(df.to_string())
    df_toy = pd.DataFrame({case_col: [1,1,2], act_col: ['a','b','a'], "max nr":[1,1,1]})
    grouped = df_toy.groupby(by=[case_col, "max nr"], axis=0)
    act_list = df_toy[act_col].unique()
    pairs = itertools.combinations(act_list,2)
    print(list(pairs))
    print(grouped.groups)



