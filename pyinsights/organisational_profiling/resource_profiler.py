import pandas as pd
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights.conformance import alignment_scores


class ResourceProfiler:
    """
    Instantiate a ResourceProfiler
    Computes a resource profile of the cases,
    Returns deviating cases based on it

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

    def __init__(self, connector, resource_column):
        # init class
        global datamodel
        global activity_table
        global case_col
        global act_col
        global timestamp
        global transition_mode
        global end_timestamp
        global res_col

        self.connector = connector

        # init process variables from connector
        datamodel = self.connector.datamodel
        activity_table = self.connector.activity_table()
        case_col = self.connector.case_col()
        act_col = self.connector.activity_col()
        timestamp = self.connector.timestamp()
        end_timestamp = self.connector.end_timestamp()
        transition_mode = "ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]"
        res_col = resource_column

    def resource_profile(self, time_unit = "MONTH", reference="YEAR"):
        """
        Computes resource profile

        :returns df: resource profile as df
        :type df: pandas.core.Dataframe
        """

        # pql queries
        case_query = f"\"{self.connector.activity_table()}\".\"{self.connector.case_col()}\""
        act_query = f"\"{self.connector.activity_table()}\".\"{self.connector.activity_col()}\""
        res_query = f"\"{self.connector.activity_table()}\".\"{res_col}\""
        timestamp_query = f"\"{self.connector.activity_table()}\".\"{self.connector.timestamp()}\""

        # calculate times resource executes activity per month
        times_per_month = f"""
            PU_COUNT (
            DOMAIN_TABLE({res_query},
            {act_query},
            {time_unit}({timestamp_query}),
            YEAR({timestamp_query})),
            {act_query} )
            """

        # calculate times resource executes activity per month

        times_per_year = f"""
                    PU_COUNT (
                    DOMAIN_TABLE({res_query},
                    {act_query},
                    {reference}({timestamp_query})),
                    {act_query} )
                    """

        query = PQL()
        query.add(PQLColumn(name=self.connector.case_col(), query=case_query))
        query.add(PQLColumn(name=self.connector.activity_col(),
                            query=act_query))
        query.add(PQLColumn(name="resource", query=res_query))
        query.add(PQLColumn(name=self.connector.timestamp(),
                            query=timestamp_query))
        query += PQLColumn(name="# this month", query=times_per_month)
        query += PQLColumn(name="# this year", query=times_per_year)

        df = datamodel.get_data_frame(query)

        return df

    def cases_with_batches(self, time_unit="MONTH", reference="YEAR", min_batch_size=2, batch_percentage=0.1):
        """
        Detects cases with batches

        :returns df: cases with batches as df
        :type df: pandas.core.Dataframe
        """

        # pql queries
        case_query = f"\"{self.connector.activity_table()}\".\"{self.connector.case_col()}\""
        act_query = f"\"{self.connector.activity_table()}\".\"{self.connector.activity_col()}\""
        res_query = f"\"{self.connector.activity_table()}\".\"{res_col}\""
        timestamp_query = f"\"{self.connector.activity_table()}\".\"{self.connector.timestamp()}\""

        # calculate times resource executes activity per month
        times_per_month = f"""
            PU_COUNT (
            DOMAIN_TABLE({res_query},
            {act_query},
            {time_unit}({timestamp_query}),
            YEAR({timestamp_query})),
            {act_query} )
            """

        # calculate times resource executes activity per month

        times_per_year = f"""
                    PU_COUNT (
                    DOMAIN_TABLE({res_query},
                    {act_query},
                    {reference}({timestamp_query})),
                    {act_query} )
                    """

        query = PQL()
        query.add(PQLColumn(name=self.connector.case_col(), query=case_query))
        query.add(PQLColumn(name=self.connector.activity_col(),
                            query=act_query))
        query.add(PQLColumn(name="resource", query=res_query))
        query.add(PQLColumn(name=self.connector.timestamp(),
                            query=timestamp_query))
        query += PQLColumn(name="# this month", query=times_per_month)
        query += PQLColumn(name="# this year", query=times_per_year)

        # filter for batches
        filter = f"""
            FILTER
            {times_per_month} >= {batch_percentage} * {times_per_year}
            AND
            {times_per_month} >= {min_batch_size}
            """

        query += PQLFilter(filter)

        df = datamodel.get_data_frame(query)

        return df



