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

        initial_cols = [case_col, act_col, timestamp]

        result = pd.DataFrame(columns=initial_cols)
        # prepare dataframes
        for method, df in deviations_df.items():
            if df.empty:
                continue
            if how == "union":
                # these columns are used to check which method detected the deviation
                deviation_cols = ["detected by " +
                                    method for method in deviations_df.keys()]

                if "source" in list(df.columns.values):
                    df.loc[:, act_col] = df.apply(
                        axis=1, func=lambda x: f""" {x["source"]} -> {x["target"]}""")  # transform source and target to one column

                # set deviation column true for method
                df[deviation_cols] = False
                df["detected by " + method] = True

                # final columns of dataframe that is returned
                final_columns = initial_cols + deviation_cols
                columns_to_drop = [col for col in list(
                    df.columns.values) if col not in final_columns]  # drop all columns that are not needed

            else:
                if "source" not in list(df.columns.values) and act_col in list(df.columns.values):
                    df.loc[:, "source"] = df.loc[:, act_col]
                    df.loc[:, "target"] = "(atomic activity)"
                columns_to_drop = [col for col in df.columns.values if col not in [
                    case_col, timestamp, act_col]]

            df.drop(columns_to_drop, axis=1, inplace=True)

        # combine dataframes
        if how == "union":
            for method, df in deviations_df.items():
                result = pd.concat(
                    [result, df], join="outer", ignore_index=True)  # outer join

            result[deviation_cols] = result[deviation_cols].fillna(
                value=False)  # fills empty cells with False (not detected)
        elif how == "intersection":
            result = list(deviations_df.values())[0]
            for i in range(1, len(deviations_df.values())):
                df = list(deviations_df.values())[i]
                temp_cols = [case_col, timestamp] if timestamp in df.columns else [
                    case_col]
                result[temp_cols] = result[result[temp_cols].isin(
                    df[temp_cols])].dropna(how='all')[temp_cols]  # intersection
                result.dropna(inplace=True)

        result.sort_values(by=[case_col, timestamp], inplace=True)

        return result
