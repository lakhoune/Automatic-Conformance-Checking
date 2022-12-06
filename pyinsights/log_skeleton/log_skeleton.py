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
        Returns the equivalence relation of the log skeleton. two activities are related if and only if they occur equally often in every trace
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
        for activities in max_act.values():
            activities_of_cases_with_same_max_act = get_candidate_pairs(
                activities=activities, activities_of_cases_with_same_max_act=activities_of_cases_with_same_max_act)

        equivalence = [pair for pair, count in activities_of_cases_with_same_max_act.items(
        ) if count is not None]  # remove pairs with None values
        equivalence.sort()
        # expected = [('examine casually', 'pay compensation'), ('examine thoroughly', 'register request'), ('pay compensation', 'register request'),('register request', 'reject request')]

        return equivalence

    def _get_always_after(self, extended_log, noise_threshold, case_id_filter=None):
        """
        Returns the always after relation of the log skeleton.  two activities are related if and only if after any occurrence of the first activity the second activity always occurs.
        If the case ID filter is set, the always after relation is only computed for the trace with the given case ID.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :param case_id_filter: str
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

        if case_id_filter is not None:
            query.add(self._get_case_id_filter(case_id_filter))

        always_after = datamodel.get_data_frame(query)
        # could be that for two different cases in one a always after b and in the other b always after a so we need. Not sure on that might need to look into that

        return always_after[["SOURCE", "TARGET"]]

    def _get_always_before(self, extended_log, noise_threshold, case_id_filter=None):
        """
        Returns the always before relation of the log skeleton.  two activities are related if and only if before any occurrence of the first activity the second activity always occurs.
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

        if case_id_filter is not None:
            query.add(self._get_case_id_filter(case_id_filter))

        always_before = datamodel.get_data_frame(query)

        # could be that for two different cases in one a always after b and in the other b always after a so we need. Not sure on that might need to look into that

        return always_before[["SOURCE", "TARGET"]]

    def _get_never_together(self, extended_log, noise_threshold):
        """
        Returns the never together relation of the log skeleton. two activities are related if and only if they do not occur together in any trace.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        never_together = None
        # Get the never together relation

        return never_together

    def _get_directly_follows(self, extended_log, noise_threshold, case_id_filter=None):
        """
        Returns the directly follows relation of the log skeleton. two activities are related if and only if an occurrence the first activity can directly be followed by an occurrence of the second.
        :param extended_log: pandas.DataFrame
        :param noise_threshold: int
        :return: pandas.DataFrame
        """
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" ) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}") """))
        
        if case_id_filter is not None:
            query.add(self._get_case_id_filter(case_id_filter))
        
        edge_table = datamodel.get_data_frame(query)

        directly_follows = set()
        for _, row in edge_table.iterrows():
            directly_follows.add((row["SOURCE"], row["TARGET"]))

        return directly_follows

    def get_conformance(self):
        """
        Checks for each trace in the log, whether it is fitting or not.
        :return: pandas.DataFrame with the conformance of each trace ( columns: [caseID, conformance])
        """
        # get the case ids of the log
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" "{activity_table}"."{case_col}" """))
        case_ids = datamodel.get_data_frame(query).drop_duplicates()
        for index, row in case_ids.iterrows():
            case_ids.at[index, "conformance"] = self._get_conformance_for_case(
                row["ID"])
        return case_ids

    def _get_conformance_for_case(self, case_id):
        """
        Checks whether the given trace is fitting or not.
        :param case_id: str
        :return: bool
        """
        # get the equivalence relation for the given case
        equivalence_for_case = self._get_equivalence_relation(case_id)
        # get the always after relation for the given case
        always_after_for_case = self._get_always_after(
            None, None, case_id=case_id)
        # get the always before relation for the given case
        always_before_for_case = self._get_always_before(
            None, None, case_id=case_id)
        # get the directly follows relation for the given case
        directly_follows_for_case = self._get_directly_follows(
            None, None, case_id=case_id)

        if (self._get_equivalence().issubset(equivalence_for_case)) is False:
            return False
        if (self._get_always_after().issubset(always_after_for_case)) is False:
            return False
        if (self._get_always_before().issubset(always_before_for_case)) is False:
            return False

        # Note that the directly follows relation subset relation is inverted (see paper)
        if (directly_follows_for_case.issubset(self._get_directly_follows())) is False:
            return False
        return True

    def _get_case_id_filter(self, case_id):
        """
        Returns a filter for the given case ID.
        :param case_id: str
        :return: str
        """
        return PQLFilter(query=f""" "{activity_table}"."{case_col}" = '{case_id}' """)


def get_candidate_pairs(activities, activities_of_cases_with_same_max_act):
    for act1 in activities:
        activity_name_1 = list(act1.keys())[0]  # name of activity
        # how often it occurs in the trace
        activity_count_1 = list(act1.values())[0]
        for act2 in activities:
            activity_name_2 = list(act2.keys())[0]  # name of activity
            # how often it occurs in the trace
            activity_count_2 = list(act2.values())[0]
            if activity_name_1 < activity_name_2:  # relation is symmetrical. This ensures that we dont recheck existing pairs. Furthermore it ensures that the keys will be sorted
                if activity_count_1 == activity_count_2:
                    # as they both occur equally often in the current trace they are potential candidates
                    candidate_pair = (activity_name_1, activity_name_2)
                    if candidate_pair in activities_of_cases_with_same_max_act:
                        existing_pair_count = activities_of_cases_with_same_max_act[
                            candidate_pair]  # how often it has occured before.
                        if existing_pair_count is not None and existing_pair_count != activity_count_1:
                            # setting to None means we are not reconsidering it in the future
                            activities_of_cases_with_same_max_act[candidate_pair] = None
                    else:
                        activities_of_cases_with_same_max_act[candidate_pair] = activity_count_1
    return activities_of_cases_with_same_max_act
