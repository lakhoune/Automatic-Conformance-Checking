from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights.temporal_profiling import TemporalProfiler


def get_features(connector):
    
    datamodel = connector.datamodel
    activity_table = connector.activity_table()
    case_col = connector.case_col()
    act_col = connector.activity_col()
    timestamp = connector.timestamp()
    end_timestamp = connector.end_timestamp()
    has_endtime = connector.has_end_timestamp()
    
    case_query = f""" "{activity_table}"."{case_col}" """
    throughput = f"""  CALC_THROUGHPUT ( CASE_START TO CASE_END , REMAP_TIMESTAMPS ( "{activity_table}"."{timestamp}" , SECONDS ) )   """
    num_activities = f"""CALC_REWORK()  """
    biggest_loop = f"""  MAX( INDEX_ACTIVITY_LOOP ( "{activity_table}"."{act_col}" ) ) """
    temporal_features = _temporal_features(connector)
    print(temporal_features.head(n=100).to_string())
    
    query = PQL()
    query += PQLColumn(name=case_col, query=case_query)
    query += PQLColumn(name="throughput", query=throughput)
    query += PQLColumn(name="num activities", query=num_activities)
    query += PQLColumn(name="biggest loop", query=biggest_loop)
   
    df = datamodel.get_data_frame(query)
    df = df.join(temporal_features, on=case_col, how="left")
    if has_endtime:
        df.loc[:, "wasted time"] = df.loc[:, "throughput"] - df.loc[:, "sojourn"]
        
    print(df.head(n=100).to_string())
        
def _temporal_features(connector):
    has_endtime = connector.has_end_timestamp()
    case_col = connector.case_col()
    
    temporal_profiler = TemporalProfiler(connector)
    temp_profile = temporal_profiler.deviating_cases(sigma=0, deviation_cost=False, extended_view=True)
    
    if has_endtime:
        columns =  [case_col, "waiting time", "z-score (waiting time)", "sojourn", "z-score (sojourn)"]
        temp_profile = temp_profile.loc[:, columns]
        temporal_features = temp_profile.groupby(case_col).agg(
                waiting_time = ("waiting time", 'sum'),
                max_waiting = ("waiting time", 'max'),
                z_score_waiting = ("z-score (waiting time)", 'mean'),
                z_score_waiting_max = ("z-score (waiting time)", 'max'),
                sojourn = ("sojourn", 'sum'),
                max_sojourn = ("sojourn", 'max'),
                z_score_sojourn = ("z-score (sojourn)", 'mean'),
                z_score_sojourn_max = ("z-score (sojourn)", 'max')
        )
    
    else:
        columns =  [case_col, "waiting time", "z-score (waiting time)"]
        temp_profile = temp_profile.loc[:, columns]
        temporal_features = temp_profile.groupby(case_col).agg(
                waiting_time = ("waiting time", 'sum'),
                max_waiting = ("waiting time", 'max'),
                z_score_waiting = ("z-score (waiting time)", 'mean'),
                z_score_waiting_max = ("z-score (waiting time)", 'max')
        ) 
    
    return temporal_features
