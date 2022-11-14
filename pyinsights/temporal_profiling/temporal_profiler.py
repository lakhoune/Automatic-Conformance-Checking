from pycelonis.pql import PQL, PQLColumn, PQLFilter


class TemporalProfiler:
    """
    Instantiate a TemporalProfiler
    Computes a temporal profile of the cases,
    Returns deviating cases based on sigma

    :param sigma: The sigma threshold
    :type sigma: int

    :param datamodel: The celonis datamodel
    :type datamodel: PyCelonis.Datamodel

    """
    datamodel = None
    activity_table = None
    case_col = None
    act_col = None
    timestamp = None

    def __init__(self, connector):
        global datamodel
        global activity_table
        global case_col
        global act_col
        global timestamp

        self.connector = connector

        datamodel = self.connector.datamodel
        activity_table = self.connector.activity_table()
        case_col = self.connector.case_col()
        act_col = self.connector.activity_col()
        timestamp = self.connector.timestamp()

    def temporal_profile(self):
        """
        Computes temporal profile

        :returns df: temporal profile as dataframe
        :type df: pandas dataframe
        """
        query = PQL()

        query.add(PQLColumn(name="case ID", query=f"""TARGET("{activity_table}"."{case_col}")"""))
        query.add(PQLColumn(name="source", query=f"""SOURCE("{activity_table}"."{act_col}",
            ANY_OCCURRENCE[] TO ANY_OCCURRENCE[])"""))
        query.add(PQLColumn(name="target", query=f"""TARGET("{activity_table}"."{act_col}")"""))
        query.add(PQLColumn(name="duration", query=f"""SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
            ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
            TARGET("{activity_table}"."{timestamp}"))"""))

        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg duration", query=f"""
                PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                    TARGET("{activity_table}"."{act_col}")),
                                    SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
            ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
            TARGET("{activity_table}"."{timestamp}")) )
            """))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std duration", query=f"""
            PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                    TARGET("{activity_table}"."{act_col}")),
                                    SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
            ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
            TARGET("{activity_table}"."{timestamp}")) )
            """))

        # compute z-score of duration
        query.add(PQLColumn(name="z.score", query="""
        case when PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                TARGET("receipt_xes"."concept:name")),
                                MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("receipt_xes"."time:timestamp")) ) = 0 then 0 else

         (MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp"),
         TARGET("receipt_xes"."time:timestamp"))
         - PU_AVG ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                TARGET("receipt_xes"."concept:name")),
                                MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("receipt_xes"."time:timestamp")) ) )
        /
        PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                TARGET("receipt_xes"."concept:name")),
                                MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("receipt_xes"."time:timestamp")) ) 
        end
         """))

        # get dataframe
        df = datamodel.get_data_frame(query)

        return df

    def deviating_cases(self, sigma=6):
        """
        Computes deviating cases

        :param sigma: statistical sigma
        :type sigma: int

        :returns df: deviating cases as dataframe
        :type df: pandas dataframe
        """

        query = PQL()

        query.add(PQLColumn(name="case ID", query=f"""TARGET("{activity_table}"."{case_col}")"""))
        query.add(PQLColumn(name="source", query=f"""SOURCE("{activity_table}"."{act_col}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[])"""))
        query.add(PQLColumn(name="target", query=f"""TARGET("{activity_table}"."{act_col}")"""))
        query.add(PQLColumn(name="duration", query=f"""SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}"))"""))

        # compute average waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="avg duration", query=f"""
                        PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) )
                    """))

        # compute standard deviation of waiting time
        # needs pull-up and domain table because of celonis joins
        query.add(PQLColumn(name="std duration", query=f"""
                    PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) )
                    """))

        # compute z-score of duration
        query.add(PQLColumn(name="z.score", query="""
                case when PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                        TARGET("receipt_xes"."concept:name")),
                                        MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("receipt_xes"."time:timestamp")) ) = 0 then 0 else

                 (MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp"),
                 TARGET("receipt_xes"."time:timestamp"))
                 - PU_AVG ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                        TARGET("receipt_xes"."concept:name")),
                                        MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("receipt_xes"."time:timestamp")) ) )
                /
                PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                                        TARGET("receipt_xes"."concept:name")),
                                        MINUTES_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("receipt_xes"."time:timestamp")) ) 
                end
                 """))

        # filter all cases with waiting time deviating more than average +- 3 sigma
        # nice example of Pull-up with domain table
        filter_larger_than_average = f""" ( SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}"))
                     >= 
                    PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) )
                    +
                    {sigma / 2} * PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) ) 
                    )
                    OR
                    ( SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}"))
                     <=
                    PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) )
                    -
                    {sigma / 2} * PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                            TARGET("{activity_table}"."{act_col}")),
                                            SECONDS_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                    TARGET("{activity_table}"."{timestamp}")) ) 
                    )


                    """
        query.add(PQLFilter(filter_larger_than_average))

        # get dataframe
        df = datamodel.get_data_frame(query)

        return df
