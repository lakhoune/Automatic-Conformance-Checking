import pandas as pd
from pycelonis.pql import PQL, PQLColumn, PQLFilter
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

    def __init__(self, connector):
        global datamodel
        global activity_table
        global case_col
        global act_col
        global timestamp
        global transition_mode

        self.connector = connector

        datamodel = self.connector.datamodel
        activity_table = self.connector.activity_table()
        case_col = self.connector.case_col()
        act_col = self.connector.activity_col()
        timestamp = self.connector.timestamp()
        transition_mode = "FIRST_OCCURRENCE[] TO ANY_OCCURRENCE[]"
    def temporal_profile(self):
        """
        Computes temporal profile

        :returns df: temporal profile as dataframe
        :type df: pandas.core.Dataframe
        """
        query = PQL()


        query.add(PQLColumn(name="source", query=f"""SOURCE("{activity_table}"."{act_col}",
            {transition_mode})"""))
        query.add(PQLColumn(name="target", query=f"""TARGET("{activity_table}"."{act_col}")"""))


        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg duration", query=f"""
                PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",{transition_mode}), 
                                    TARGET("{activity_table}"."{act_col}")),
                                    SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
            {transition_mode}),
            TARGET("{activity_table}"."{timestamp}")) )
            """))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std duration", query=f"""
            PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"
                                    , {transition_mode}), 
                                    TARGET("{activity_table}"."{act_col}")),
                                    SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
            {transition_mode}),
            TARGET("{activity_table}"."{timestamp}")) )
            """))

        # get dataframe
        df = datamodel.get_data_frame(query)
        df.drop_duplicates(subset=["source","target"], inplace=True)
        return df

    def deviations(self, sigma=6):
        """
        Computes deviating transitions

        :param sigma: statistical sigma
        :type sigma: int

        :returns df: deviating transitions as dataframe
        :type df: pandas dataframe
        """

        query = PQL()

        query.add(PQLColumn(name=case_col, query=f""" SOURCE("{activity_table}"."{case_col}"
                                , {transition_mode})  """))
        query.add(PQLColumn(name="source", query=f"""SOURCE("{activity_table}"."{act_col}",
                    {transition_mode})"""))
        query.add(PQLColumn(name="target", query=f"""TARGET("{activity_table}"."{act_col}")"""))
        query.add(PQLColumn(name="duration", query=f"""SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}"))"""))

        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg duration", query=f"""
                        PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                        {transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) )
                    """))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std duration", query=f"""
                    PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                    {transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) )
                    """))

        # compute z-score of duration
        query.add(PQLColumn(name="z.score", query=f"""
                case when PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                {transition_mode}), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                {transition_mode}),
                TARGET("{activity_table}"."{timestamp}")) ) = 0 then 0
                 
                 else

                 (SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                 {transition_mode}),
                 TARGET("{activity_table}"."{timestamp}"))
                 - PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                 {transition_mode}), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                {transition_mode}),
                TARGET("{activity_table}"."{timestamp}")) ) )
                /
                PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                {transition_mode}), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                {transition_mode}),
                TARGET("{activity_table}"."{timestamp}")) ) 
                end
                 """))

        # filter all cases with waiting time deviating more than average +- sigma
        # nice example of Pull-up with domain table
        filter_larger_than_average = f""" ( SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}"))
                     >= 
                    PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                            {transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) )
                    +
                    {sigma} * PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                    {transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) ) 
                    )
                    OR
                    ( SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}"))
                     <=
                    PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",{transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) )
                    -
                    {sigma} * PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}",
                            {transition_mode}), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE(END_TIMESTAMP_COLUMN(),
                    {transition_mode}),
                    TARGET("{activity_table}"."{timestamp}")) ) 
                    )


                    """
        query.add(PQLFilter(filter_larger_than_average))

        # get dataframe
        df = datamodel.get_data_frame(query)

        return df

    def deviating_cases(self, sigma=6, deviation_cost=True, extended_view=True):
        """
        Returns deviating cases as dataframe
        :param sigma: Stastitical sigma to determine deviations
        :param deviation_cost: If true calculates deviation cost
        :param extended_view: if true shows mean, stdev and duration of activity transition
        :return: pandas.core.Dataframe
        """
        deviations = self.deviations(sigma)
        case_ids = deviations[case_col].drop_duplicates()
        cols = deviations.columns.values

        event_log = self.connector.events()
        events_to_align = event_log[event_log[case_col].isin(case_ids)]

        if deviation_cost:
            alignment_cost = alignment_scores(events_to_align=events_to_align, event_log=event_log, connector=self.connector)
            deviating_cases = pd.DataFrame({f'{case_col}': case_ids, "alignment cost": alignment_cost})
            deviations = deviations.merge(deviating_cases.set_index(case_col), on=case_col, how="left")
            deviations["deviation cost"] = deviations["z.score"] + deviations["alignment cost"]

        if not extended_view:
            cols = [case_col, "source", "target"]
        if deviation_cost:
            cols.append("deviation cost")

        return deviations[cols]
