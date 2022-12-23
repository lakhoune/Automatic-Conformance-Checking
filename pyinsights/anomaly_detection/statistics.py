from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler
import datetime
import pandas as pd


def get_features(connector, concurrent_cases=False):
    """
    Computes numerical features for cases
    Args:
        connector (pyinsights.Connector): connector

    Returns:
        pandas.Dataframe: numerical features on case level
    """

    # vars
    datamodel = connector.datamodel
    activity_table = connector.activity_table()
    case_col = connector.case_col()
    act_col = connector.activity_col()
    timestamp = connector.timestamp()
    end_timestamp = connector.end_timestamp()
    has_endtime = connector.has_end_timestamp()

    # queries for features
    case_query = f""" DISTINCT "{activity_table}"."{case_col}" """
    timestamp_query = f""" "{activity_table}"."{timestamp}" """
    throughput = f"""  CALC_THROUGHPUT ( CASE_START TO CASE_END , REMAP_TIMESTAMPS ( "{activity_table}"."{timestamp}" , SECONDS ) )   """
    num_activities = f"""CALC_REWORK()  """
    biggest_loop = f"""  MAX( INDEX_ACTIVITY_LOOP ( "{activity_table}"."{act_col}" ) ) """

    temporal_features = _temporal_features(connector)

    # get df with other features
    query = PQL()
    query += PQLColumn(name=case_col, query=case_query)
    query += PQLColumn(name="throughput", query=throughput)
    query += PQLColumn(name="num activities", query=num_activities)
    query += PQLColumn(name="biggest loop", query=biggest_loop)

    # join with temporal features
    df = datamodel.get_data_frame(query)
    df = df.join(temporal_features, on=case_col, how="left")

    # if there are two timestamps, compute wasted time per case
    if has_endtime:
        df.loc[:, "wasted time"] = df.loc[:,
                                          "throughput"] - df.loc[:, "sojourn"]

    # takes too long to compute
    if concurrent_cases:
        df.loc[:, "concurrent cases"] = df.apply(func=lambda row: count_values_in_range(
            df, row[timestamp], row[timestamp]+datetime.timedelta(seconds=row["throughput"]), timestamp), axis=1)

    # get workload per case
    workload_stats = workload(connector)
    df = df.join(workload_stats, on=case_col, how="left")

    df.drop_duplicates(inplace=True)

    return df


def _temporal_features(connector):
    """
    returns temporal features

    Args:
        connector (pyinsights.Connector): connector

    Returns:
        pandas.DataFrame: temporal features per case
    """

    # usual vars
    has_endtime = connector.has_end_timestamp()
    case_col = connector.case_col()

    # get temporal profile per case
    temporal_profiler = TemporalProfiler(connector)
    temp_profile = temporal_profiler.deviating_cases(
        sigma=0, deviation_cost=False, extended_view=True)

    # compute features
    # #if there are two timestamps per event, also use sojourn time
    if has_endtime:
        columns = [case_col, "waiting time",
                   "z-score (waiting time)", "sojourn", "z-score (sojourn)"]
        temp_profile = temp_profile.loc[:, columns]
        temporal_features = temp_profile.groupby(case_col).agg(
            waiting_time=("waiting time", 'sum'),
            max_waiting=("waiting time", 'max'),
            z_score_waiting=("z-score (waiting time)", 'mean'),
            z_score_waiting_max=("z-score (waiting time)", 'max'),
            sojourn=("sojourn", 'sum'),
            max_sojourn=("sojourn", 'max'),
            z_score_sojourn=("z-score (sojourn)", 'mean'),
            z_score_sojourn_max=("z-score (sojourn)", 'max')
        )

    else:
        columns = [case_col, "waiting time", "z-score (waiting time)"]
        temp_profile = temp_profile.loc[:, columns]
        temporal_features = temp_profile.groupby(case_col).agg(
            waiting_time=("waiting time", 'sum'),
            max_waiting=("waiting time", 'max'),
            z_score_waiting=("z-score (waiting time)", 'mean'),
            z_score_waiting_max=("z-score (waiting time)", 'max')
        )

    return temporal_features


def count_values_in_range(df, range_min, range_max, timestamp):

    # "between" returns a boolean Series equivalent to left <= series <= right.
    # NA values will be treated as False.
    return df[timestamp].between(left=range_min, right=range_max).sum()


def workload(connector):
    """computes the maximum and average workload of resources working on the case

    Args:
        connector (pyinsights.Connector): connector

    Returns:
        pandas.Series: maximum and average workload
    """
    case_col = connector.case_col()
    res_col = connector.resource_column()
    profiler = ResourceProfiler(connector)
    res_profile = profiler.resource_profile()
    workload = res_profile[[case_col, res_col, "# this HOURS"]].groupby([case_col]).agg(
        max_workload=('# this HOURS', 'max'),
        avg_workload=('# this HOURS', 'mean')
    )

    return workload
