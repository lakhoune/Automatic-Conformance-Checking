from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
import numpy as np


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
        Returns the equivalence relation of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        equivalence = None
        # Get the equivalence relation
        query = PQL()

        query.add(PQLColumn(name=case_col,
                  query=f""" "{activity_table}"."{case_col}"  """))
        query.add(PQLColumn(name=act_col,
                  query=f""" "{activity_table}"."{act_col}"  """))
        query.add(PQLColumn(
            name="nr", query=f""" ACTIVATION_COUNT ( "{activity_table}"."{act_col}" )  """))
        df = datamodel.get_data_frame(query)

        # reverse the order of the rows
        df = df.iloc[::-1]

        # dictionary to store the case id and the activity name with the highest activation count
        max_act = {}

        # loop over the rows
        for index, row in df.iterrows():
            # check if the case id is already in the dictionary
            if row[case_col] in max_act:
                # check if the activation column is already in the dictionary
                # if row[act_col] in max_act[row[case_col]]:
                if any(row[act_col] in act for act in max_act[row[case_col]]):
                    # do nothing
                    pass
                else:
                    # append the activation count to the dictionary
                    max_act[row[case_col]].append({row[act_col]: row["nr"]})
            else:
                # add the case id and the activity name with the highest activation count to the dictionary
                max_act[row[case_col]] = [{row[act_col]: row["nr"]}]

        activities_of_cases_with_same_max_act = {}
        # get the activities in each case where a pair of activities has the same activation count
        for case in max_act:
            for act in max_act[case]:
                for act2 in max_act[case]:
                    if act != act2:
                        if list(act.values())[0] == list(act2.values())[0]:
                            if case in activities_of_cases_with_same_max_act:
                                activities_of_cases_with_same_max_act[case].append(
                                    {list(act.keys())[0]: list(act2.keys())[0]})
                            else:
                                activities_of_cases_with_same_max_act[case] = [
                                    {list(act.keys())[0]: list(act2.keys())[0]}]
                            # print(act)
                            # print(act2)
                            # print("")

        print(activities_of_cases_with_same_max_act)

        return equivalence

    def _get_always_after(self, extended_log, noise_threshold):
        """
        Returns the always after relation of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        always_after = None
        # Get the always after relation
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" ) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}", ANY_OCCURRENCE[] TO LAST_OCCURRENCE[]) """))

        always_after = datamodel.get_data_frame(query)
        # could be that for two different cases in one a always after b and in the other b always after a so we need. Not sure on that might need to look into that

        return always_after[["SOURCE", "TARGET"]]

    def _get_always_before(self, extended_log, noise_threshold):
        """
        Returns the always before relation of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        always_before = None
        # Get the always before relation
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" , FIRST_OCCURRENCE[] TO ANY_OCCURRENCE[]) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}") """))

        always_before = datamodel.get_data_frame(query)

        # could be that for two different cases in one a always after b and in the other b always after a so we need. Not sure on that might need to look into that

        return always_before[["SOURCE", "TARGET"]]

    def _get_never_together(self, extended_log, noise_threshold):
        """
        Returns the never together relation of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        never_together = None
        # Get the never together relation

        return never_together

    def _get_directly_follows(self, extended_log, noise_threshold):
        """
        Returns the directly follows relation of the log skeleton.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        directly_follows = None
        # Get the directly follows relation

        return directly_follows


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
