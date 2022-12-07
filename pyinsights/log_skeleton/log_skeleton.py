from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
import numpy as np
import itertools


class LogSkeleton:
    """
    Class for log skeleton analysis.
    """
    datamodel = None
    activity_table = None
    case_col = None
    act_col = None
    timestamp = None
    transition_mode = None

    def __init__(self, connector):
        """
        :param connector: Connector object
        """
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
        transition_mode = "ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]"

    def get_log_skeleton(self, noise_threshold):
        """
        Returns the log skeleton of the data model.
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        log_skeleton = None
        # Get the extended log
        extended_log = self._extend_log()

        equivalence, always_after, always_before, never_together, directly_follows = self._get_relations(
            extended_log, noise_threshold)

        directly_follows_counter = None
        sum_counter = None
        min_counter = None
        max_counter = None
        log_skeleton = (equivalence, always_after, always_before, never_together,
                        directly_follows, directly_follows_counter, sum_counter, min_counter, max_counter)

        return log_skeleton

    def _extend_log(self):
        """
        Extends the log by adding an artificial start and end event to each trace.
        :return: pandas.DataFrame
        """
        query = PQL()
        query.add(PQLColumn(name=act_col,
                            query=f"""  "{activity_table}"."{act_col}" """))
        query.add(PQLColumn(name=case_col,
                            query=f"""  "{activity_table}"."{case_col}" """))
        query.add(PQLColumn(name=timestamp,
                            query=f"""    "{activity_table}"."{timestamp}"  """))

        df = datamodel.get_data_frame(query)

        # should be sorted by timestamp per default by celonis but we sort it just to be sure
        sorted_by_timestamp = df.sort_values(timestamp)
        bag_of_traces = sorted_by_timestamp.groupby(
            by=case_col).agg({act_col: lambda x: ["START"] + list(x) + ["END"]})  # construct the bag of traces while adding an artificial start and end
        return bag_of_traces.values

    def _get_relations(self, extended_log, noise_threshold):
        """
        Returns the relations of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        equivalence = self._get_equivalence(extended_log, noise_threshold)
        always_after = self._get_always_after(extended_log, noise_threshold)
        always_before = self._get_always_before(extended_log, noise_threshold)
        never_together = self._get_never_together(
            extended_log, noise_threshold)
        directly_follows = self._get_directly_follows(
            extended_log, noise_threshold)
        # Get the relations

        return equivalence, always_after, always_before, never_together, directly_follows

    def _get_equivalence(self, extended_log, noise_threshold):
        """
        Returns the equivalence relation of the log skeleton. two activities are related if and only if they occur equally often in every trace
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        equivalence = set()
        # Get the number of occurrences of each activity per case

        query = PQL()
        query.add(PQLColumn(name=case_col,
                            query=f"""DISTINCT "{activity_table}"."{case_col}"  """))
        query.add(PQLColumn(name=act_col,
                            query=f""" "{activity_table}"."{act_col}"  """))
        query.add(PQLColumn(
            name="max nr", query=f"""
                PU_MAX( DOMAIN_TABLE("{activity_table}"."{case_col}", "{activity_table}"."{act_col}"),
                ACTIVATION_COUNT ( "{activity_table}"."{act_col}" ) ) """))

        df = datamodel.get_data_frame(query)

        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]] for k, v in groups.items()}

        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)

        # iterate over pairs
        for pair in combs:
            # check if occurrence profile of act1 is subset of act2's
            if len(groups_expanded[pair[0]].merge(groups_expanded[pair[1]])) == len(groups_expanded[pair[0]]):
                # only add tuple if same in reverse order is not already in set
                # if tuple((pair[1], pair[0])) not in equivalence:
                equivalence.add(pair)

        return equivalence

    def _get_always_after(self, extended_log, noise_threshold):
        """
        Returns the always after relation of the log skeleton.  two activities are related if and only if after any occurrence of the first activity the second activity always occurs
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        always_after = set()
        # Get the always after relation
        # get for every activity its position in the trace
        query = PQL()
        query.add(PQLColumn(name=case_col,
                            query=f""" "{activity_table}"."{case_col}"  """))
        query.add(PQLColumn(name=act_col,
                            query=f""" "{activity_table}"."{act_col}"  """))
        query.add(PQLColumn(
            name="order", query=f""" INDEX_ACTIVITY_ORDER( "{activity_table}"."{act_col}")
                         """))
        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "order"]] for k, v in groups.items()}

        # get cartesian product of activities (because relation is not symmetrical)
        combs = itertools.product(groups_expanded.keys(), repeat=2)
        # iterate over pairs
        for pair in combs:

            # get positions of both acts in one df
            merged = groups_expanded[pair[0]].merge(groups_expanded[pair[1]], on=case_col, how='left')
            # replace nas so they don't mess up the maximum
            merged.fillna(0, inplace=True)
            # get the result per column and get compute for every case
            # the greatest position for both activities
            grouped = merged.groupby(case_col)
            test = grouped.agg({'order_x': 'max',
                                'order_y': 'max'})

            # handling if both activities are the same
            if pair[0] == pair[1]:
                # if they actually occur more than 1 time in every case --> they are in the relation
                if all(merged[case_col].value_counts() > 1):
                    always_after.add(pair)

            # else test if the last occurrence of act2 is after the last occurrence of act1
            # and add to relation
            elif all(test["order_x"] < test["order_y"]):
                always_after.add(pair)

        return always_after

    def _get_always_before(self, extended_log, noise_threshold):
        """
        Returns the always before relation of the log skeleton.  two activities are related if and only if before any occurrence of the first activity the second activity always occurs.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        always_before = set()
        # Get the always after relation
        # get for every activity its position in the trace
        query = PQL()
        query.add(PQLColumn(name=case_col,
                            query=f""" "{activity_table}"."{case_col}"  """))
        query.add(PQLColumn(name=act_col,
                            query=f""" "{activity_table}"."{act_col}"  """))
        query.add(PQLColumn(
            name="order", query=f""" INDEX_ACTIVITY_ORDER( "{activity_table}"."{act_col}")
                                 """))
        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "order"]] for k, v in groups.items()}

        # get cartesian product of activities (because relation is not symmetrical)
        combs = itertools.product(groups_expanded.keys(), repeat=2)
        # iterate over pairs
        for pair in combs:

            # get positions of both acts in one df
            merged = groups_expanded[pair[0]].merge(groups_expanded[pair[1]], on=case_col, how='left')
            # replace nas so they don't mess up the minimum
            merged.fillna(1000000000, inplace=True)
            # get the result per column and compute for every case
            # the lowest position for both activities
            grouped = merged.groupby(case_col)
            test = grouped.agg({'order_x': 'min',
                                'order_y': 'min'})

            # handling if both activities are the same
            if pair[0] == pair[1]:
                # if they actually occur more than 1 time in every case --> they are in the relation
                if all(merged[case_col].value_counts() > 1):
                    always_before.add(pair)

            # else test if the first occurrence of act1 is after the first occurrence of act2
            # and add to relation
            elif all(test["order_x"] > test["order_y"]):
                always_before.add(pair)

        return always_before


    def _get_never_together(self, extended_log, noise_threshold):
        """
        Returns the never together relation of the log skeleton. two activities are related if and only if they do not occur together in any trace.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        never_together = set()

        # Get the never together relation
        query = PQL()
        query.add(PQLColumn(name=case_col,
                            query=f"""DISTINCT "{activity_table}"."{case_col}"  """))
        query.add(PQLColumn(name=act_col,
                            query=f""" "{activity_table}"."{act_col}"  """))
        query.add(PQLColumn(
            name="max nr", query=f"""
                        PU_MAX( DOMAIN_TABLE("{activity_table}"."{case_col}", "{activity_table}"."{act_col}"),
                        ACTIVATION_COUNT ( "{activity_table}"."{act_col}" ) ) """))

        df = datamodel.get_data_frame(query)

        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]] for k, v in groups.items()}

        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)

        # iterate over pairs
        for pair in combs:
            # check if act1 and act2 do not occur in same case
            if not any(groups_expanded[pair[0]][case_col].isin(groups_expanded[pair[1]][case_col])):
                # only add tuple if same in reverse order is not already in set
                # if tuple((pair[1], pair[0])) not in equivalence:
                never_together.add(pair)
        return never_together

    def _get_directly_follows(self, extended_log, noise_threshold):
        """
        Returns the directly follows relation of the log skeleton. two activities are related if and only if an occurrence the first activity can directly be followed by an occurrence of the second.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        directly_follows = None
        # Get the directly follows relation
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" ) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}") """))
        directly_follows = datamodel.get_data_frame(query)

        return directly_follows[["SOURCE", "TARGET"]]


def log_subsumes_log(log1, log2):
    """
    Checks if log1 subsumes log2.
    :return: bool
    """
    # Check if log1 subsumes log2
    return False


def log_subsumes_trace(log, trace):
    """
    Checks if log subsumes trace.
    :return: bool
    """
    # Check if log subsumes trace
    return False
