import pandas as pd
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights.conformance import alignment_scores
from tqdm import tqdm

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
        global has_endtime

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
        has_endtime = self.connector.has_end_timestamp()

    def resource_profile(self, time_unit="HOURS", reference_unit=None):
        """
        computes the number of times a resource executes an activity per time_unit
        :param time_unit: in ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"]
        :param reference_unit: in ["MINUTES", "HOURS", "DAY", "MONTH"]
        :return: pandas.core.Dataframe
        """

        # get query for resource profiler
        query = self._resource_profile_query(time_unit=time_unit, reference_unit=reference_unit)
        df = datamodel.get_data_frame(query)

        return df

    def _resource_profile_query(self, time_unit, reference_unit, filtered=False, min_batch_size=None, batch_percentage=None):
        """
        builds query for resource profiling
        :param time_unit: in ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"]
        :param reference_unit: in ["MINUTES", "HOURS", "DAY", "MONTH"]
        :return:
        """
        # pql queries
        case_query = f"\"{activity_table}\".\"{case_col}\""
        act_query = f"\"{activity_table}\".\"{act_col}\""
        res_query = f"\"{activity_table}\".\"{res_col}\""
        timestamp_query = f"\"{activity_table}\".\"{timestamp}\""
        endtime_query = f"\"{activity_table}\".\"{end_timestamp}\""

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
                reference_unit_query += f"""MINUTES({timestamp_query}), """
                reference_unit_query += f"""HOURS({timestamp_query}), """
                reference_unit_query += f"""DAY({timestamp_query}), """
                reference_unit_query += f"""MONTH({timestamp_query}), """
            elif reference_unit == "HOURS":
                reference_unit_query += f"""HOURS({timestamp_query}), """
                reference_unit_query += f"""DAY({timestamp_query}), """
                reference_unit_query += f"""MONTH({timestamp_query}), """
            elif reference_unit == "DAY":
                reference_unit_query += f"""DAY({timestamp_query}), """
                reference_unit_query += f"""MONTH({timestamp_query}), """
            elif reference_unit == "MONTH":
                reference_unit_query += f"""MONTH({timestamp_query}), """

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
        query.add(PQLColumn(name=case_col, query=case_query))
        query.add(PQLColumn(name=act_col,
                            query=act_query))
        query.add(PQLColumn(name="resource", query=res_query))
        query.add(PQLColumn(name=timestamp,
                            query=timestamp_query))
        if has_endtime:
            query += PQLColumn(name=end_timestamp, query=endtime_query)

        query += PQLColumn(name=f"# this {time_unit}", query=times_per_unit)

        # include occurrences per reference unit
        if reference_unit is not None:
            query += PQLColumn(name=f"# this {reference_unit}", query=times_per_reference)

        if filtered:
            # include occurrences per reference unit
            if reference_unit is not None:
                percent_filter = f"""
                {times_per_unit} >= {batch_percentage}*{times_per_reference}
                """
                query += PQLFilter(percent_filter)

            # filter for batches
            batch_filter = f"""
                                {times_per_unit} >= {min_batch_size}
                                """

            query += PQLFilter(batch_filter)

        return query


    def _detect_batches(self, time_unit="HOURS", reference_unit=None, min_batch_size=2, batch_percentage=0.1):
        """
        identifies batches, returns df with
        :param time_unit: in ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"]
        :param reference_unit: in ["MINUTES", "HOURS", "DAY", "MONTH"]
        :param min_batch_size: minimum size of a batch
        :param batch_percentage: percentage of occurrences per reference_unit to count as batch
        :return: pandas.core.Dataframe
        """

        # get query for filtered resource profile (with batches)
        query = self._resource_profile_query(time_unit=time_unit, reference_unit=reference_unit, filtered=True,
                                             min_batch_size=min_batch_size, batch_percentage=batch_percentage)

        df = datamodel.get_data_frame(query)

        return df

    def cases_with_batches(self, time_unit="HOURS", reference_unit=None, min_batch_size=8, batch_percentage=0.1
                           , grouped_by_batches=True, batch_types=True):

        """
        returns cases with batches according to occurrences per time_unit, can also identify batch type
        type one of [ "batching at start", batching at end", "sequential", "concurrent" ]
        :param time_unit: in ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"]
        :param reference_unit: in ["MINUTES", "HOURS", "DAY", "MONTH"]
        :param min_batch_size: minimum size of a batch
        :param batch_percentage: percentage of occurrences per reference_unit to count as batch
        :param grouped_by_batches: if true groups df by batches
        :param batch_types: if true adds batch type to df
        :return: pondas.core.Dataframe
        """

        # get batches
        df = self._detect_batches(time_unit=time_unit, reference_unit=reference_unit, min_batch_size=min_batch_size,
                                  batch_percentage=batch_percentage)

        # identify batches
        if batch_types:
            # get groups
            df_grouped, groups = self._group_by_batches(df, time_unit)
            # if grouping is wanted, identify batches on grouped_df
            if grouped_by_batches:
                df_with_types = self._identify_batch_type(df_grouped, groups)
                grouped_by_batches = False
            else:
                df_with_types = self._identify_batch_type(df, groups)

            # drop truncated column as it is only needed for computations
            df_with_types.drop("truncated", axis=1, inplace=True)
            # set end df
            df = df_with_types

        # group df by batches
        if grouped_by_batches:
            df, _ = self._group_by_batches(df, time_unit)

        return df

    def _group_by_batches(self, df, time_unit):
        """
        groups df by batches
        :param df:
        :param time_unit: in ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"]
        :return: (pandas.core.Dataframe , groups as list of arrays)
        """
        # determine timeseries-offset by time_unit
        freq_offset = ""
        if time_unit == "SECONDS":
            freq_offset = "S"
        elif time_unit == "MINUTES":
            freq_offset = "min"
        elif time_unit == "HOURS":
            freq_offset = "H"
        elif time_unit == "DAY":
            freq_offset = "D"
        elif time_unit == "MONTH":
            freq_offset = "M"

        # truncated timestamp to time_unit
        # group by groups
        df["truncated"] = df[timestamp].dt.to_period(freq_offset).dt.to_timestamp()
        grouped = df.groupby(['truncated', act_col, 'resource'])
        # get groups as arrays in list
        groups = [group.values for group in grouped.groups.values()]
        # flatten groups to get events
        flattened = [event for group in groups for event in group]
        # reindex df
        df = df.reindex(flattened)

        return df, groups

    def _identify_batch_type(self, df, groups):
        """
        computes the batch type within the groups
        all events have to satisfy type for group to get type
        type one of [ "batching at start", batching at end", "sequential", "concurrent" ]
        :param df: df
        :param groups: list of arrays
        :return: pandas.core.Dataframe
        """
        df["batch type"] = ""


        # iterate over groups

        bar = tqdm(groups)
        bar.set_description("Batch Types:")
        for group in bar:

            # remember timestamps of last iteration
            last_start = 0
            last_end = 0
            batching_at_start = True
            batching_at_end = has_endtime
            sequential = has_endtime
            group_type = "concurrent"

            # sort batch in df by timestamp
            group.sort()
            # iterate over events in group
            for event in group:
                current_start = 0
                current_end = 0
                # if first iteration, set timestamps
                if last_start == 0:
                    last_start = df.loc[event][timestamp]
                    current_start = df.loc[event][timestamp]
                else:
                    current_start = df.loc[event][timestamp]
                    # if timestamp is different from last iteration --> no batching at start
                    if last_start != current_start:
                        batching_at_start = False

                # if log has end_timestamp
                if has_endtime:
                    # if first iteration, set timestamp
                    if last_end == 0:
                        last_end = df.loc[event][end_timestamp]
                        current_end = df.loc[event][end_timestamp]
                    else:
                        current_end = df.loc[event][end_timestamp]
                        # if timestamp is different from last iteration --> no batching at end
                        if last_end != current_end:
                            batching_at_end = False
                        # if current activity started before last one ended --> not sequential
                        if current_start < last_end:
                            sequential = False
            # check batch type
            if batching_at_start:
                # if both at start & at end --> simultaneous
                if batching_at_end:
                    group_type = "simultaneous"
                else:
                    # else batching at start
                    group_type = "batching at start"
            # check for batching at end
            elif batching_at_end:
                group_type = "batching at end"
            # check for sequential batching
            elif sequential:
                group_type = "sequential"
            # set batch type
            df.loc[group, "batch type"] = group_type
        return df

