import pandas as pd
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from pyinsights.conformance import alignment_scores_by_id

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

        query.add(PQLColumn(name=case_col, query=f"""TARGET("{activity_table}"."{case_col}")"""))
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
        query.add(PQLColumn(name="z.score", query=f"""
        case when PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                TARGET("{activity_table}"."{act_col}")),
                                MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("{activity_table}"."{timestamp}")) ) = 0 then 0 else

         (MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}"),
         TARGET("{activity_table}"."{timestamp}"))
         - PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                TARGET("{activity_table}"."{act_col}")),
                                MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("{activity_table}"."{timestamp}")) ) )
        /
        PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                TARGET("{activity_table}"."{act_col}")),
                                MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
        ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
        TARGET("{activity_table}"."{timestamp}")) ) 
        end
         """))

        # get dataframe
        df = datamodel.get_data_frame(query)

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

        query.add(PQLColumn(name=case_col, query=f"""TARGET("{activity_table}"."{case_col}")"""))
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
        query.add(PQLColumn(name="z.score", query=f"""
                case when PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("{activity_table}"."{timestamp}")) ) = 0 then 0 else

                 (MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}"),
                 TARGET("{activity_table}"."{timestamp}"))
                 - PU_AVG ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("{activity_table}"."{timestamp}")) ) )
                /
                PU_STDEV ( DOMAIN_TABLE (SOURCE("{activity_table}"."{act_col}"), 
                                        TARGET("{activity_table}"."{act_col}")),
                                        MINUTES_BETWEEN(SOURCE("{activity_table}"."{timestamp}",
                ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
                TARGET("{activity_table}"."{timestamp}")) ) 
                end
                 """))

        # filter all cases with waiting time deviating more than average +- sigma
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

    def deviating_cases(self, sigma=6, deviation_cost = True, extended_view=True):
        deviations = self.deviations(sigma)
        case_ids = deviations[case_col].drop_duplicates()
        cols = deviations.columns.values

        if deviation_cost:
            alignment_cost = alignment_scores_by_id(case_ids, self.connector)
            deviating_cases = pd.DataFrame({f'{case_col}':case_ids, "alignment cost": alignment_cost})
            print(deviating_cases)
            deviations = deviations.merge(deviating_cases.set_index(case_col),on=case_col, how="left")
            print(deviations)
            deviations["deviation cost"] = deviations["z.score"] + deviations["alignment cost"]



        if not extended_view:
            cols = [case_col, "source", "target"]
        if deviation_cost:
            cols.append("deviation cost")

        return deviations[cols]
