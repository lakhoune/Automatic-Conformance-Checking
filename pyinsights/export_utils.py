import pandas as pd
import pm4py

def df_to_parquet(df, path):
    """
    exports resulting df to parquet
    :param df: event log
    :param path: file path
    :return:
    """
    df.to_parquet(path)

def df_to_pickle(df, path):
    """
    exports resulting df to pickle
    :param df: event log
    :param path: file path
    :return:
    """
    df.to_pickle(path)

def df_to_csv(df, path):
    """
    exports resulting df to csv
    :param df: event log
    :param path: file path
    :return:
    """
    df.to_csv(path)

def df_to_xes(df, path, connector):
    """
    exports resulting df to xes
    :param df: event log
    :param path: file path
    :param connector: pycelonis.Connector
    :return:
    """
    # format df as event_log
    df_formatted = pm4py.format_dataframe(df, case_id=connector.case_col(),
                                             activity_key=connector.activity_col(),
                                             timestamp_key=connector.timestamp())
    # convert it to a pm4py event log object
    event_log = pm4py.convert_to_event_log(df_formatted)
    # export event log as xes
    pm4py.write_xes(event_log, path)


