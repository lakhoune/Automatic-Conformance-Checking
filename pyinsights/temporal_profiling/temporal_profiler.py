import pandas as pd
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights.conformance import alignment_scores


class TemporalProfiler:
    """
    Instantiate a TemporalProfiler
    Computes a temporal profile of the cases,
    Returns deviating cases based on sigma

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
        transition_mode = "ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]"

    def temporal_profile(self):
        """
        Computes temporal profile

        :returns df: waiting time and sojourn times as dataframes
        :type df: pandas.core.Dataframe
        """

        # init vars
        waiting_times = pd.DataFrame()
        sojourn_times = pd.DataFrame()


        # queries for waiting time
        source_act = f"""SOURCE("{activity_table}"."{act_col}",{transition_mode})"""

        target_act = f"""TARGET("{activity_table}"."{act_col}")"""

        waiting = f"""SECONDS_BETWEEN(SOURCE("{activity_table}"."{end_timestamp}",
                            {transition_mode}),
                            TARGET("{activity_table}"."{timestamp}"))"""

        std_waiting = f"""
                            PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                            {transition_mode}), TARGET("{activity_table}"."{act_col}")),
                                                    {waiting} )
                            """

        avg_waiting = f"""  PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                                {transition_mode}),
                                                    TARGET("{activity_table}"."{act_col}")),
                                                    {waiting} )
                            """

        query = PQL()


        query.add(PQLColumn(name="source", query=source_act))
        query.add(PQLColumn(name="target", query=target_act))

        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg waiting time", query=avg_waiting))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std waiting time", query=std_waiting))

        # get dataframe
        waiting_times = datamodel.get_data_frame(query)
        # pql returns profile for every occurrence of activity
        waiting_times.drop_duplicates(subset=["source", "target"], inplace=True)

        if has_endtime:
            # queries for sojourn
            sojourn = f"""SECONDS_BETWEEN("{activity_table}"."{timestamp}", "{activity_table}"."{end_timestamp}")"""

            std_sojourn = f""" PU_STDEV ( DOMAIN_TABLE ("{activity_table}"."{act_col}"),{sojourn} ) """

            avg_sojourn = f"""   PU_AVG ( DOMAIN_TABLE ("{activity_table}"."{act_col}"),{sojourn}) """

            sojourn_query = PQL()
            sojourn_query.add(PQLColumn(name=act_col, query=f""""{activity_table}"."{act_col}" """))

            # compute average sojourn time
            # needs pull-up and domain table because of celonis joins
            # compute average sojourn time
            # needs pull-up and domain table because of celonis joins
            sojourn_query.add(PQLColumn(name="avg sojourn time", query=avg_sojourn))

            # compute standard sojourn of waiting time
            # needs pull-up and domain table because of celonis joins
            sojourn_query.add(PQLColumn(name="std sojourn", query=std_sojourn))

            sojourn_times = datamodel.get_data_frame(sojourn_query)
            # pql returns profile for every occurrence of activity
            sojourn_times.drop_duplicates(subset=[act_col], inplace=True)

        # resulting temporal profile
        temporal_profile = {'waiting times': waiting_times, 'sojourn times': sojourn_times}

        return temporal_profile

    def deviations(self, sigma=6):
        """
        Computes deviating transitions

        :param sigma: statistical sigma
        :type sigma: int

        :returns df: deviating transitions as dataframe
        :type df: pandas dataframe
        """

        # declaring pql variables
        source_act = f"""SOURCE("{activity_table}"."{act_col}",{transition_mode} WITH START())"""

        target_act = f"""TARGET("{activity_table}"."{act_col}", WITH END())"""

        waiting = f"""SECONDS_BETWEEN(SOURCE("{activity_table}"."{end_timestamp}",
                    {transition_mode} WITH START()),
                    TARGET("{activity_table}"."{timestamp}", WITH END()))"""

        std_waiting = f"""
                    PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                    {transition_mode} WITH START()),
                                            TARGET("{activity_table}"."{act_col}", WITH END())),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{end_timestamp}",
                    {transition_mode} WITH START()),
                    TARGET("{activity_table}"."{timestamp}", WITH END())) )
                    """

        avg_waiting = f"""
                        PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                    {transition_mode} WITH START()),
                                            TARGET("{activity_table}"."{act_col}", WITH END())),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{end_timestamp}",
                    {transition_mode} WITH START()),
                    TARGET("{activity_table}"."{timestamp}" , WITH END())) )
                    """


        query = PQL()

        # only end transitions are needed for sojourn time
        filter_start = f""" FILTER SOURCE("{activity_table}"."{act_col}",
                    {transition_mode} WITH START()) != 'START'  """

        query.add(PQLFilter(filter_start))

        query.add(PQLColumn(name=case_col, query=f""" SOURCE("{activity_table}"."{case_col}", {transition_mode} WITH START())  """))
        query.add(PQLColumn(name="source", query=source_act))
        query.add(PQLColumn(name="target", query=target_act))
        query.add(PQLColumn(name="waiting time", query=waiting))

        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg waiting time", query=avg_waiting))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std waiting time", query=std_waiting))

        # compute z-score of duration
        query.add(PQLColumn(name="z-score (waiting time)", query=f"""
                case when {std_waiting} = 0 then 0

                 else

                 ({waiting} - {avg_waiting}) / ({std_waiting})

                end
                 """))

        # filter all cases with waiting time deviating more than average +- sigma
        # nice example of Pull-up with domain table
        filter_larger_than_average = f"""( ( {waiting}) > ({avg_waiting} + {sigma} * {std_waiting}) )
                    OR
                    ( ( {waiting} ) < ({avg_waiting} - {sigma} * {std_waiting}) )
                    """

        # checks if log has end_timestamps and computes statistics on sojourn time
        if has_endtime:
            # queries for sojourn time
            sojourn = f"""SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                                           {transition_mode} WITH START()),
                                           Source("{activity_table}"."{end_timestamp}"))"""

            std_sojourn = f""" PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}")),
                                                                        {sojourn} )
                                                """
            avg_sojourn = f"""   PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}")),{sojourn})
                                    """


            query.add(PQLColumn(name="sojourn", query=sojourn))

            # compute average sojourn time
            # needs pull-up and domain table because of celonis joins
            query.add(PQLColumn(name="avg sojourn time", query=avg_sojourn))

            # compute standard sojourn of waiting time
            # needs pull-up and domain table because of celonis joins
            query.add(PQLColumn(name="std sojourn", query=std_sojourn))



            # compute z-score of sojourn
            query.add(PQLColumn(name="z-score (sojourn)", query=f"""
                            case when {std_sojourn} = 0 then 0

                             else

                             ({sojourn} - {avg_sojourn}) / ({std_sojourn})
                            end
                             """))

            # add deviating sojourn times to filter
            filter_larger_than_average += "OR"
            filter_larger_than_average += f"""( ( {sojourn}) > ({avg_sojourn} + {sigma} * {std_sojourn}) )
                    OR
                    ( ( {sojourn} ) < ({avg_sojourn} - {sigma} * {std_sojourn}) ) """

        # filter deviations
        query.add(PQLFilter(filter_larger_than_average))

        # get dataframe
        df = datamodel.get_data_frame(query)

        # start/end transitions get na for temporal times
        df.fillna(value=0, inplace=True)

        return df

    def deviating_cases(self, sigma=6, deviation_cost=True, extended_view=True):
        """
        Returns deviating cases as dataframe
        :param sigma: Stastitical sigma to determine deviations
        :param deviation_cost: If true calculates deviation cost
        :param extended_view: if true shows mean, stdev and duration of activity transition
        :return: pandas.core.Dataframe
        """

        # get deviating case ids
        deviations = self.deviations(sigma)
        case_ids = deviations[case_col].drop_duplicates()
        cols = list(deviations.columns)

        # load event log and filter to deviating cases
        event_log = self.connector.events()
        events_to_align = event_log[event_log[case_col].isin(case_ids)]

        # compute deviation cost
        if deviation_cost:
            # compute alignment cost
            alignment_cost = alignment_scores(events_to_align=events_to_align, event_log=event_log, connector=self.connector)
            deviating_cases = pd.DataFrame({f'{case_col}': case_ids, "alignment cost": alignment_cost})
            # append alignment cost to deviations df
            deviations = deviations.merge(deviating_cases.set_index(case_col), on=case_col, how="left")

            # factor sojourn time into deviation cost if applicable
            if has_endtime:
                deviations["deviation cost"] = deviations["z-score (waiting time)"] + deviations["z-score (sojourn)"]\
                                               + deviations["alignment cost"]
            else:
                deviations["deviation cost"] = deviations["z-score (waiting time)"] + deviations["alignment cost"]

        # final view ( extended or basic)
        if not extended_view:
            cols = [case_col, "source", "target"]
        if deviation_cost:
            cols.append("deviation cost")

        return deviations[cols]
