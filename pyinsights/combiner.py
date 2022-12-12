import numpy as np
import pandas as pd


class Combiner:
    """
    :param connector: Connector to connect to celonis
    :type connector: pyinsights.Connector

    """
    datamodel = None
    activity_table = None
    case_col = None
    act_col = None
    timestamp = None
    transition_mode = None
    end_timestamp = None

    def __init__(self, connector):
        """
        constructor
        :param connector: pyinsights.Connector
        """
        # init class
        global datamodel
        global activity_table
        global case_col
        global act_col
        global timestamp
        global transition_mode
        global end_timestamp
        global has_endtime

        self.connector = connector

        # init process variables from connector
        datamodel = self.connector.datamodel
        activity_table = self.connector.activity_table()
        case_col = self.connector.case_col()
        act_col = self.connector.activity_col()
        timestamp = self.connector.timestamp()
        end_timestamp = self.connector.end_timestamp()
        has_endtime = self.connector.has_end_timestamp()

    def combine_deviations(self, deviations, how="union"):
        """

        :param deviations:
        :param how:
        :return:
        """
        deviations_df = deviations.copy()

        result = pd.DataFrame(columns=[case_col, act_col, timestamp])
        # prepare dataframes
        for df in deviations_df:

            if how == "union":
                if "source" in list(df.columns.values):
                    df.loc[:, act_col] = df.apply(axis=1, func=lambda x: f""" {x["source"]} -> {x["target"]}""")

                columns_to_drop = list(df.columns.values)
                columns_to_drop.remove(case_col)
                columns_to_drop.remove(act_col)
                columns_to_drop.remove(timestamp)
            else:
                if "source" not in list(df.columns.values):
                    df.loc[:, "source"] = df.loc[:, act_col]
                    df.loc[:, "target"] = "(atomic activity)"
                columns_to_drop = list(df.columns.values)
                columns_to_drop.remove(case_col)
                columns_to_drop.remove("source")
                columns_to_drop.remove("target")
                columns_to_drop.remove(timestamp)

            df.drop(columns_to_drop, axis=1, inplace=True)

        if how == "union":
            for df in deviations_df:
                result = pd.concat([result, df], join="outer", ignore_index=True)

        elif how == "intersection":

            result = deviations_df[0]

            for i in range(1,len(deviations_df)):
                df = deviations_df[i]
                result[[case_col, timestamp]] = result[result[[case_col, timestamp]].isin(df[[case_col, timestamp]])].dropna(how='all')[[case_col, timestamp]]
                result.dropna(inplace=True)

        result.sort_values(by=[case_col, timestamp], inplace=True)

        return result




