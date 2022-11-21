
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.conformance import alignment_scores
from pm4py.algo.discovery.temporal_profile import algorithm as temporal_profile_discovery
from pm4py.algo.conformance.temporal_profile import algorithm as temporal_profile_conformance


if __name__ == "__main__":
    celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"

    #celonis_url = "academic-m-s-qafari-pads-rwth-aachen-de.eu-2.celonis.cloud"
    #api_token = "ZTUxZGNjNmItYzEwNy00MTI4LWJjZDctZmU1Zjg0Y2ZiYmQ0OkxRaHNRaEd6eHFMYXBwSlhyUkg1Z0NlUjBOUDlMbzdpcFZNNGx4cGdhdlJx"
    connector = Connector(api_token=api_token, url=celonis_url)
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    id = input()
    connector.set_paramters(model_id=id, end_timestamp="END_DATE")

    temporal_profiler = TemporalProfiler(connector=connector)

    df2 = temporal_profiler.temporal_profile()
    #deviating_cases_df = temporal_profiler.deviating_cases(extended_view=False)

    query = PQL()
    query.add(PQLColumn(name=connector.case_col(), query=f"SOURCE(\"{connector.activity_table()}\".\"{connector.case_col()}\", FIRST_OCCURRENCE[] TO ANY_OCCURRENCE_WITH_SELF[])"))
    query.add(PQLColumn(name=connector.activity_col(), query=f"SOURCE(\"{connector.activity_table()}\".\"{connector.activity_col()}\")"))
    query.add(PQLColumn(name="target", query=f"TARGET(\"{connector.activity_table()}\".\"{connector.activity_col()}\")"))
    query.add(PQLColumn(name=connector.timestamp(), query=f"SOURCE(\"{connector.activity_table()}\".\"END_DATE\")"))
    query.add(PQLColumn(name="end time", query=f"tARGET(\"{connector.activity_table()}\".\"START_DATE\")"))
    query.add(PQLColumn(name="diff time", query=f"DATEDIFF(ms,SOURCE(\"{connector.activity_table()}\".\"END_DATE\"),TARGET(\"{connector.activity_table()}\".\"START_DATE\"))"))
    #query.add(PQLFilter(f"DATEDIFF(ms,\"{connector.activity_table()}\".\"START_DATE\",\"{connector.activity_table()}\".\"END_DATE\") > 0"))
    query.add(PQLColumn(name="avg duration", query=f"""
                            PU_AVG ( DOMAIN_TABLE (SOURCE("{connector.activity_table()}"."{connector.activity_col()}",
                            FIRST_OCCURRENCE[] TO ANY_OCCURRENCE_WITH_SELF[]), 
                                                TARGET("{connector.activity_table()}"."{connector.activity_col()}")),
                                                SECONDS_BETWEEN(SOURCE("{connector.activity_table()}"."END_DATE", FIRST_OCCURRENCE[] TO ANY_OCCURRENCE_WITH_SELF[]),
                        TARGET("{connector.activity_table()}"."START_DATE")) )
                        """))
    events = connector.datamodel.get_data_frame(query)
    print(events[:100].to_string())

    print("Temporal Profile")
    print(df2.to_string())


    print("Deviating cases:")
    #print(deviating_cases_df.to_string())

    # print("pm4py comparison")
    #
    # import pm4py
    # log = connector.events()
    # df_formatted = pm4py.format_dataframe(log, case_id=connector.case_col(), activity_key=connector.activity_col(),
    #                                       timestamp_key=connector.timestamp())
    # temporal_profile = temporal_profile_discovery.apply(df_formatted)
    #
    # results = temporal_profile_conformance.apply(df_formatted, temporal_profile)
    # length = 0
    # print(temporal_profile)
    # print(df2.to_string())
    # case_ids = []
    # for i in range(len(results)):
    #     length += len(results[i])
    #     if len(results[i]):
    #         case_ids.append(df_formatted[connector.case_col()].unique()[i])
    # print(length)
    # print(len(deviating_cases_df))
    # print(deviating_cases_df.isin(case_ids))
















