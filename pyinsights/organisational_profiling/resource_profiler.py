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
        """
        init class
        :param connector: pycelonis.Connector
        :param resource_column: name of resource column in log
        """

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

    def resource_profile(self, time_unit = "HOURS", reference_unit=None):
        """
        Computes resource profile
        defined as number of times resource executes activity per time_unit
        :returns df: resource profile as df
        :type df: pandas.core.Dataframe
        """

        # pql queries
        case_query = f"\"{self.connector.activity_table()}\".\"{self.connector.case_col()}\""
        act_query = f"\"{self.connector.activity_table()}\".\"{self.connector.activity_col()}\""
        res_query = f"\"{self.connector.activity_table()}\".\"{res_col}\""
        timestamp_query = f"\"{self.connector.activity_table()}\".\"{self.connector.timestamp()}\""

        # build query to group by time unit
        time_unit_query = ""
        if time_unit == "SECONDS":
            time_unit_query += f"""SECONDS({timestamp_query}), """
            time_unit_query += f"""MINUTES({timestamp_query}), """
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "MINUTES":
            time_unit_query += f"""MINUTES({timestamp_query}), """
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "HOURS":
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "DAY":
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "MONTH":
            time_unit_query += f"""MONTH({timestamp_query}), """

        time_unit_query += f"""YEAR({timestamp_query}) """

        # calculate times resource executes activity per time unit
        times_per_unit = f"""
                    PU_COUNT (
                    DOMAIN_TABLE({res_query},
                    {act_query},
                    {time_unit_query}),
                    {act_query} )
                    """

        # build query to group by reference unit

        # calculate times resource executes activity per reference unit
        # build query to group by time unit
        if reference_unit is not None:
            reference_unit_query = ""
            if reference_unit == "MINUTES":
                reference_unit_query = f"""MINUTES({timestamp_query}), """
                reference_unit_query = f"""HOURS({timestamp_query}), """
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "HOURS":
                reference_unit_query = f"""HOURS({timestamp_query}), """
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "DAY":
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "MONTH":
                reference_unit_query = f"""MONTH({timestamp_query}), """

            reference_unit_query += f"""YEAR({timestamp_query}) """

            # query to count executions per time unit
            times_per_reference = f"""
                            PU_COUNT (
                            DOMAIN_TABLE({res_query},
                            {act_query},
                            {reference_unit_query}),
                            {act_query} )
                            """

        query = PQL()
        query.add(PQLColumn(name=self.connector.case_col(), query=case_query))
        query.add(PQLColumn(name=self.connector.activity_col(),
                            query=act_query))
        query.add(PQLColumn(name="resource", query=res_query))
        query.add(PQLColumn(name=self.connector.timestamp(),
                            query=timestamp_query))
        query += PQLColumn(name=f"# this {time_unit}", query=times_per_unit)

        # include occurrences per reference unit
        if reference_unit is not None:
            query += PQLColumn(name=f"# this {reference_unit}", query=times_per_reference)

        df = datamodel.get_data_frame(query)

        return df

    def cases_with_batches(self, time_unit="HOURS", reference_unit=None, min_batch_size=2, batch_percentage=0.1):
        """
        Detects cases with batches
        batches are defined as more than batch_percentage * reference_unit occurrences in a time_unit
        :returns df: cases with batches as df
        :type df: pandas.core.Dataframe
        """

        # pql queries
        case_query = f"\"{self.connector.activity_table()}\".\"{self.connector.case_col()}\""
        act_query = f"\"{self.connector.activity_table()}\".\"{self.connector.activity_col()}\""
        res_query = f"\"{self.connector.activity_table()}\".\"{res_col}\""
        timestamp_query = f"\"{self.connector.activity_table()}\".\"{self.connector.timestamp()}\""


       # build query to group by time unit
        time_unit_query = ""
        if time_unit == "SECONDS":
            time_unit_query += f"""SECONDS({timestamp_query}), """
            time_unit_query += f"""MINUTES({timestamp_query}), """
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "MINUTES":
            time_unit_query += f"""MINUTES({timestamp_query}), """
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "HOURS":
            time_unit_query += f"""HOURS({timestamp_query}), """
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "DAY":
            time_unit_query += f"""DAY({timestamp_query}), """
            time_unit_query += f"""MONTH({timestamp_query}), """
        elif time_unit == "MONTH":
            time_unit_query += f"""MONTH({timestamp_query}), """

        time_unit_query += f"""YEAR({timestamp_query}) """

        # calculate times resource executes activity per time unit
        times_per_unit = f"""
            PU_COUNT (
            DOMAIN_TABLE({res_query},
            {act_query},
            {time_unit_query}),
            {act_query} )
            """

        # build query to group by reference unit


        # calculate times resource executes activity per reference unit
        # build query to group by time unit
        if reference_unit is not None:
            reference_unit_query = ""
            if reference_unit == "MINUTES":
                reference_unit_query = f"""MINUTES({timestamp_query}), """
                reference_unit_query = f"""HOURS({timestamp_query}), """
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "HOURS":
                reference_unit_query = f"""HOURS({timestamp_query}), """
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "DAY":
                reference_unit_query = f"""DAY({timestamp_query}), """
                reference_unit_query = f"""MONTH({timestamp_query}), """
            elif reference_unit == "MONTH":
                reference_unit_query = f"""MONTH({timestamp_query}), """

            reference_unit_query += f"""YEAR({timestamp_query}) """

            # query to count executions per time unit
            times_per_reference = f"""
                    PU_COUNT (
                    DOMAIN_TABLE({res_query},
                    {act_query},
                    {reference_unit_query}),
                    {act_query} )
                    """

        query = PQL()
        query.add(PQLColumn(name=self.connector.case_col(), query=case_query))
        query.add(PQLColumn(name=self.connector.activity_col(),
                            query=act_query))
        query.add(PQLColumn(name="resource", query=res_query))
        query.add(PQLColumn(name=self.connector.timestamp(),
                            query=timestamp_query))
        query += PQLColumn(name=f"# this {time_unit}", query=times_per_unit)

        # include occurrences per reference unit
        if reference_unit is not None:
            query += PQLColumn(name=f"# this {reference_unit}", query=times_per_reference)
            percent_filter = f"""{times_per_unit} >= {batch_percentage} * {times_per_reference}"""
            query += PQLFilter(percent_filter)

        # filter for batches
        batch_filter = f"""
            {times_per_unit} >= {min_batch_size}
            """

        query += PQLFilter(batch_filter)

        df = datamodel.get_data_frame(query)

        return df



