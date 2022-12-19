from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
import numpy as np
import itertools
from tqdm import tqdm


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

    def get_log_skeleton(self, noise_threshold=0):
        """
        Returns the log skeleton of the data model.
        :param noise_threshold: [0,1]
        :return: relations and active frequencies as dict
        """
        log_skeleton = None
        # Get the extended log

        equivalence, always_after, always_before, never_together, directly_follows \
            = self._get_relations(noise_threshold)

        active_frequs = self._active_freq()

        log_skeleton = {"equivalence": equivalence, "always_after": always_after, "always_before": always_before, "never_together": never_together,
                        "directly_follows": directly_follows, "activ_freq": active_frequs}

        return log_skeleton

    def get_log_skeleton_per_case(self, case_id):
        """
        Returns the log skeleton of .
        :param noise_threshold: [0,1]
        :return: relations and active frequencies as dict
        """
        log_skeleton = None
        # Get the extended log

        equivalence, always_after, always_before, never_together, directly_follows \
            = self._get_relations_per_case(case_id=case_id)

        # active_frequs = self._active_freq()

        log_skeleton = {"equivalence": equivalence, "always_after": always_after, "always_before": always_before, "never_together": never_together,
                        "directly_follows": directly_follows}

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

    def _get_relations(self, noise_threshold=0, case_id=None):
        """
        Returns the relations of the log skeleton.
        :param noise_threshold: [0,1]
        :return: (equivalence, always_after, always_before, never_together, directly_follows)
        """
        equivalence = self._get_equivalence(noise_threshold, case_id=case_id)
        always_after = self._get_always_after(noise_threshold, case_id)
        always_before = self._get_always_before(
            noise_threshold, case_id=case_id)
        never_together = self._get_never_together(
            noise_threshold, case_id=case_id)
        directly_follows = self._get_directly_follows(
            noise_threshold, case_id=case_id)
        # Get the relations

        return equivalence, always_after, always_before, never_together, directly_follows

    def _get_relations_per_case(self, case_id=None):
        """
        Returns the relations per case
        :param noise_threshold: [0,1]
        :return: (equivalence, always_after, always_before, never_together, directly_follows)
        """
        equivalence = self._get_equivalence_per_case(case_id=case_id)
        always_after = self._get_always_after_per_case(case_id)
        always_before = self._get_always_before_per_case(case_id=case_id)
        never_together = self._get_never_together_per_case(case_id=case_id)
        directly_follows = self._get_directly_follows_per_case(case_id=case_id)
        # Get the relations

        return equivalence, always_after, always_before, never_together, directly_follows

    def _get_equivalence(self, noise_threshold, case_id=None):
        """
        Returns the equivalence relation of the log skeleton. two activities are related if and only if they occur equally often in every trace
        :param noise_threshold: [0,1]
        :return: set
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

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        df = datamodel.get_data_frame(query)

        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]]
                           for k, v in groups.items()}
        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)
        bar = tqdm(list(combs))
        bar.set_description("Calculating equivalence")
        # iterate over pairs
        for pair in bar:
            occ_act1 = groups_expanded[pair[0]]
            occ_act2 = groups_expanded[pair[1]]
            # determine activity that occurs more times
            if len(occ_act1) >= len(occ_act2):
                max = pair[0]
                min = pair[1]
            else:
                max = pair[1]
                min = pair[0]

            # get profiles for larger and smaller acts
            occ_max = groups_expanded[max]
            occ_min = groups_expanded[min]

            occurrences_max = len(groups_expanded[max])

            # get difference betwwen two profiles
            differences = occ_max[~occ_max.apply(
                tuple, 1).isin(occ_min.apply(tuple, 1))]

            # if the two profiles deviate no more than noise * size of larger profile
            # they are equivalent
            if len(differences) <= occurrences_max*noise_threshold:
                equivalence.add(pair)

        return equivalence

    def _get_always_after(self, noise_threshold, case_id=None):
        """
        Returns the always after relation of the log skeleton.  two activities are related if and only if after any occurrence of the first activity the second activity always occurs.
        If the case ID filter is set, the always after relation is only computed for the trace with the given case ID.
        :param noise_threshold: [0,1]
        :param case_id: str
        :return: set
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

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "order"]]
                           for k, v in groups.items()}

        # get cartesian product of activities (because relation is not symmetrical)
        combs = itertools.product(groups_expanded.keys(), repeat=2)

        bar = tqdm(list(combs))
        bar.set_description("Calculating always-after")
        # iterate over pairs
        for pair in bar:
            # get cases where act1 occurs merged with occurrences of act2 in these cases
            merged = groups_expanded[pair[0]].merge(
                groups_expanded[pair[1]], how="left", on=case_col)

            # get the result per case and get compute for every case
            # the greatest position for both activities
            grouped = merged.groupby(case_col)
            pos_per_case = grouped.agg({'order_x': 'max',
                                        'order_y': 'max'})

            # fill nas so they don't screw up max
            pos_per_case.fillna(0, inplace=True)
            # length of act1's profile
            num = groups_expanded[pair[0]][case_col].nunique()

            # handling if both activities are the same
            if pair[0] == pair[1]:
                # if they actually occur more than 1 time in every case --> they are in the relation
                if np.sum(merged[case_col].value_counts() > 1) >= num*(1-noise_threshold):
                    always_after.add(pair)
            # else test if the last occurrence of act2 is after the last occurrence of act1
            # in at least (1-noise) percent of times act1 occurs
            elif np.sum(pos_per_case["order_x"] <= pos_per_case["order_y"]) >= num*(1-noise_threshold):
                always_after.add(pair)

        return always_after

    def _get_always_before(self, noise_threshold, case_id=None):
        """
        Returns the always before relation of the log skeleton.  two activities are related if and only if before any occurrence of the first activity the second activity always occurs.
        :param noise_threshold: [0,1]
        :return: set
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

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "order"]]
                           for k, v in groups.items()}

        # get cartesian product of activities (because relation is not symmetrical)
        combs = itertools.product(groups_expanded.keys(), repeat=2)
        # iterate over pairs

        bar = tqdm(list(combs))
        bar.set_description("Calculating always-before")
        for pair in bar:

            # get positions of both acts in one df
            merged = groups_expanded[pair[0]].merge(
                groups_expanded[pair[1]], on=case_col, how='left')
            # replace nas so they don't mess up the minimum
            merged.fillna(10000000000, inplace=True)
            # get the result per column and compute for every case
            # the lowest position for both activities
            grouped = merged.groupby(case_col)
            test = grouped.agg({'order_x': 'min',
                                'order_y': 'min'})

            # length of act1's profile
            num = groups_expanded[pair[0]][case_col].nunique()
            # handling if both activities are the same
            if pair[0] == pair[1]:
                # if they actually occur more than 1 time in every case --> they are in the relation
                if np.sum(merged[case_col].value_counts() > 1) >= num*(1-noise_threshold):
                    always_before.add(pair)

            # else test if the first occurrence of act1 is after the first occurrence of act2
            # in more than (1-noise) times act1 occurs
            elif np.sum(test["order_x"] >= test["order_y"]) >= num*(1-noise_threshold):
                always_before.add(pair)

        return always_before

    def _get_never_together(self, noise_threshold, case_id=None):
        """
        Returns the never together relation of the log skeleton. two activities are related if and only if they do not occur together in any trace.
        :return: set
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
        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))
        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]]
                           for k, v in groups.items()}

        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)

        bar = tqdm(list(combs))
        bar.set_description("Calculating never-together")
        # iterate over pairs
        for pair in bar:
            occ_act1 = groups_expanded[pair[0]][case_col]
            occ_act2 = groups_expanded[pair[1]][case_col]
            # determine activity that occurs more times
            if len(occ_act1) >= len(occ_act2):
                max = pair[0]
                min = pair[1]
            else:
                max = pair[1]
                min = pair[0]

            # larger and smaller profile w.r.t. cases
            occ_max = groups_expanded[max][case_col]
            occ_min = groups_expanded[min][case_col]
            num = len(occ_max)
            # check if smaller profile and larger profile do not occur together
            # more than larger profile * noise
            if np.sum(occ_max.isin(occ_min)) <= num*(noise_threshold):
                never_together.add(pair)
        return never_together

    def _get_directly_follows(self, noise_threshold, case_id=None):
        """
        Returns the directly follows relation of the log skeleton. two activities are related if and only if an occurrence the first activity can directly be followed by an occurrence of the second.
        :param noise_threshold: [0,1]
        :return: set
        """
        query = PQL()
        query.add(
            PQLColumn(name="ID", query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" ) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}") """))

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        edge_table = datamodel.get_data_frame(query)

        case_count = edge_table['ID'].nunique()  # number of cases in the log
        # threshold above which we consider the relation as a direct follow
        threshold = (1-noise_threshold) * case_count

        edge_table = edge_table.groupby(
            ['ID', 'SOURCE', 'TARGET']).size().reset_index(name='count')

        # set all counts to 1 since we only consider whether the relation exists or not (not how often) (also need this for the filter below)
        edge_table['count'] = 1
        # now group by source and target and sum up the count
        edge_table = edge_table.groupby(
            ['SOURCE', 'TARGET']).sum(numeric_only=True).reset_index()

        # now filter out the ones that are below the noise threshold
        edge_table = edge_table[edge_table['count'] >= threshold]

        directly_follows = set()

        iterations = len(edge_table)
        i = 1
        bar = tqdm(total=iterations)
        bar.set_description("Calculating directly-follows")
        for _, row in edge_table.iterrows():
            directly_follows.add((row["SOURCE"], row["TARGET"]))

            bar.update(1)
            i += 1

        return directly_follows

    def get_conformance(self, noise_threshold=0, cases_to_compare=None):
        """
        Checks for each trace in the log, whether it is fitting or not.
        :return: pandas.DataFrame with the conformance of each trace ( columns: [caseID, conformance])
        """
        # get the case ids of the log
        query = PQL()
        query.add(
            PQLColumn(name=case_col, query=f""" "{activity_table}"."{case_col}" """))
        case_ids = datamodel.get_data_frame(query).drop_duplicates()

        lsk = self.get_log_skeleton(noise_threshold)

        lsk_compare_traces = self.get_log_skeleton_per_case(
            case_id=cases_to_compare)

        # check for each case if relation is subset of lsk

        non_conforming = {case for relation in lsk_compare_traces.keys(
        ) for case in lsk_compare_traces[relation].keys() if not self._conforms(lsk_compare_traces, relation, case, lsk)}

        return non_conforming

    def _conforms(self, lsk_traces, relation, case, lsk):
        """checks if relation of trace conforms to lsk

        Args:
            lsk_traces (_type_): _description_
            relation (_type_): _description_
            case (_type_): _description_
            lsk (_type_): _description_

        Returns:
            bool: conformity
        """
        if relation != "directly_follows":
            if not lsk_traces[relation][case].issubset(lsk[relation]):
                print(relation)
                return False
        else:
            if not lsk[relation].issubset(lsk_traces[relation][case]):
                print(relation)
                return False
        return True

    def _get_conformance_for_case(self, case_id, relations, noise_threshold):
        """
        Checks whether the given trace is fitting or not.
        :param case_id: str
        :return: bool
        """

        equivalence, always_after, always_before, never_together, directly_follows = relations

        # get the equivalence relation for the given case
        equivalence_for_case = self._get_equivalence(
            case_id=case_id, noise_threshold=noise_threshold)
        # get the always after relation for the given case
        always_after_for_case = self._get_always_after(
            noise_threshold, case_id=case_id)
        # get the always before relation for the given case
        always_before_for_case = self._get_always_before(
            noise_threshold, case_id=case_id)
        # get the directly follows relation for the given case
        directly_follows_for_case = self._get_directly_follows(
            noise_threshold, case_id=case_id)

        if always_after.issubset(always_after_for_case) is False:
            return False
        if always_before.issubset(always_before_for_case) is False:
            return False
        if equivalence.issubset(equivalence_for_case) is False:
            return False

        # Note that the directly follows relation subset relation is inverted (see paper)
        if directly_follows_for_case.issubset(directly_follows) is False:
            return False

        return True

    def _get_case_id_filter(self, case_id):
        """
        Returns a filter for the given case ID.
        :param case_id: str
        :return: str
        """
        # otherwise event logs with integer case ids wont work
        if type(case_id) == str:
            case_id = f""" '{case_id}' """
        return PQLFilter(query=f""" "{activity_table}"."{case_col}" = {case_id} """)

    def _active_freq(self):
        """
        returns for each activity, the number of possible occurrences per trace
        :return:
        """
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

        case_ids = list(df[case_col].unique())
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]]
                           for k, v in groups.items()}

        bar = tqdm(groups_expanded.items())
        bar.set_description("Computing active frequencies")
        for act, occ in bar:
            groups_expanded[act] = set(groups_expanded[act]["max nr"])
            if not (case_ids == list(occ[case_col].unique())):
                groups_expanded[act].add(0)

        return groups_expanded

    
    def _get_always_before_per_case(self, case_id=None):
        """
        Returns the always before relation per case.  two activities are related if and only if before any occurrence of the first activity the second activity always occurs.
        :param noise_threshold: [0,1]
        :return: set
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

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        df = datamodel.get_data_frame(query)

        case_ids = df[case_col].unique()
        ab_all_cases = {case: set() for case in case_ids}

        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "order"]]
                           for k, v in groups.items()}

        # get cartesian product of activities (because relation is not symmetrical)
        combs = itertools.product(groups_expanded.keys(), repeat=2)

        # iterate over pairs

        bar = tqdm(list(combs))
        bar.set_description("Calculating always-before for cases")
        for pair in bar:

            # get positions of both acts in one df
            merged = groups_expanded[pair[0]].merge(
                groups_expanded[pair[1]], on=case_col, how='left')
            # replace nas so they don't mess up the minimum
            merged.fillna(10000000000, inplace=True)
            # get the result per column and compute for every case
            # the lowest position for both activities
            grouped = merged.groupby(case_col)
            test = grouped.agg({'order_x': 'min',
                                'order_y': 'min'})
            
            # handling if both activities are the same
            if pair[0] == pair[1]:
                # if they actually occur more than 1 time in case --> they are in the relation
                indices = np.where(merged[case_col].value_counts() > 1)
                case_ids_ab = list(merged[case_col].value_counts().iloc[indices[0]].index)
                
            else:
                # get cases where first occurence of act1 is after act2 
                indices = np.where(test['order_x'] >= test['order_y'])
                if len(indices[0]) > 0:
                    case_ids_ab = test.iloc[indices[0], :].index
                else:
                    case_ids_ab = []
            
            # add pair to dict for cases from before
            for index in case_ids_ab:
                ab_all_cases[index].add(pair)
        return ab_all_cases    
    

    def _get_always_after_per_case(self, case_id=None):
            """
            Returns the always before relation per case.  two activities are related if and only if before any occurrence of the first activity the second activity always occurs.
            :param noise_threshold: [0,1]
            :return: set
            """
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

            if case_id is not None:
                query.add(self._get_case_id_filter(case_id))

            df = datamodel.get_data_frame(query)

            case_ids = df[case_col].unique()
            aa_all_cases = {case: set() for case in case_ids}

            # group by activity
            grouped = df.groupby(by=[act_col], axis=0)
            # get groups as dict
            groups = grouped.groups
            # currently groups contain only row index, expand with case id and count
            groups_expanded = {k: df.loc[v, [case_col, "order"]]
                            for k, v in groups.items()}

            # get cartesian product of activities (because relation is not symmetrical)
            combs = itertools.product(groups_expanded.keys(), repeat=2)

            # iterate over pairs

            bar = tqdm(list(combs))
            bar.set_description("Calculating always-after for cases")
            for pair in bar:

                # get positions of both acts in one df
                merged = groups_expanded[pair[0]].merge(
                    groups_expanded[pair[1]], on=case_col, how='left')
                # replace nas so they don't mess up the minimum
                merged.fillna(0, inplace=True)
                # get the result per column and compute for every case
                # the lowest position for both activities
                grouped = merged.groupby(case_col)
                test = grouped.agg({'order_x': 'max',
                                    'order_y': 'max'})

                 # handling if both activities are the same
                if pair[0] == pair[1]:
                    # if they actually occur more than 1 time in case --> they are in the relation
                    indices = np.where(merged[case_col].value_counts() > 1)
                    case_ids_aa = list(merged[case_col].value_counts().iloc[indices[0]].index)
                    
                else:
                    # get cases where first occurence of act1 is after act2 
                    indices = np.where(test['order_x'] >= test['order_y'])
                    if len(indices[0]) > 0:
                        case_ids_aa = test.iloc[indices[0], :].index
                    else:
                        case_ids_aa = []
                
                # add pair to dict for cases from before
                for index in case_ids_aa:
                    aa_all_cases[index].add(pair)

            return aa_all_cases
    
    
    def _get_never_together_per_case(self, case_id=None):
        """
        Returns the never together relation per case. two activities are related if and only if they do not occur together in any trace.
        :return: set
        """
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
        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))
        df = datamodel.get_data_frame(query)
        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]]
                           for k, v in groups.items()}
        print(groups_expanded)
        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)

        case_ids = df[case_col].unique()
        nt_all_cases = {case: set() for case in case_ids}
        bar = tqdm(list(combs))
        bar.set_description("Calculating never-together for cases")
        # iterate over pairs
        for pair in bar:
            occ_act1 = groups_expanded[pair[0]][case_col]
            occ_act2 = groups_expanded[pair[1]][case_col]
            # determine activity that occurs more times
            if len(occ_act1) >= len(occ_act2):
                max = pair[0]
                min = pair[1]
            else:
                max = pair[1]
                min = pair[0]

            # larger and smaller profile w.r.t. cases
            occ_max = set(groups_expanded[max][case_col])
            occ_min = set(groups_expanded[min][case_col])
            
            # check if smaller profile and larger profile do not occur together

            nt_case_ids = occ_max.symmetric_difference(occ_min)
            for case in nt_case_ids:
                nt_all_cases[case].add(pair)
        return nt_all_cases
    

    def _get_equivalence_per_case(self, case_id=None):
        """
        Returns the equivalence relation of the log skeleton. two activities are related if and only if they occur equally often in every trace
        :param noise_threshold: [0,1]
        :return: set
        """
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

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        df = datamodel.get_data_frame(query)

        # group by activity
        grouped = df.groupby(by=[act_col], axis=0)
        # get groups as dict
        groups = grouped.groups
        # currently groups contain only row index, expand with case id and count
        groups_expanded = {k: df.loc[v, [case_col, "max nr"]]
                           for k, v in groups.items()}

        case_ids = df[case_col].unique()
        eq_all_cases = {case: set() for case in case_ids}
        # get all pairs of activities
        combs = itertools.permutations(groups_expanded.keys(), 2)
        bar = tqdm(list(combs))
        bar.set_description("Calculating equivalence for cases")
        # iterate over pairs
        for pair in bar:
            occ_act1 = groups_expanded[pair[0]]
            occ_act2 = groups_expanded[pair[1]]
            # determine activity that occurs more times
            if len(occ_act1) >= len(occ_act2):
                max = pair[0]
                min = pair[1]
            else:
                max = pair[1]
                min = pair[0]

            # get profiles for larger and smaller acts
            occ_max = groups_expanded[max]
            occ_min = groups_expanded[min]

            occurrences_max = len(groups_expanded[max])

            # get difference betwwen two profiles
            differences = occ_max[~occ_max.apply(
                tuple, 1).isin(occ_min.apply(tuple, 1))]

            # get all cases where max and min occur equivally often
            equ_relation = [x for x in occ_max[case_col]
                            if x not in differences[case_col]]
            # y.loc[np.in1d(y['name'], names)
            # add them to relation
            for case in equ_relation:
                eq_all_cases[case].add(pair)
        return eq_all_cases

    def _get_directly_follows_per_case(self, case_id=None):
        """
        Returns the directly follows relation per case. two activities are related if and only if an occurrence the first activity can directly be followed by an occurrence of the second.
        :param noise_threshold: [0,1]
        :return: set
        """
        query = PQL()
        query.add(
            PQLColumn(name=case_col, query=f""" SOURCE("{activity_table}"."{case_col}") """))
        query.add(PQLColumn(name="SOURCE",
                  query=f""" SOURCE ( "{activity_table}"."{act_col}" ) """))
        query.add(PQLColumn(name="TARGET",
                  query=f"""  TARGET ( "{activity_table}"."{act_col}") """))

        if case_id is not None:
            query.add(self._get_case_id_filter(case_id))

        edge_table = datamodel.get_data_frame(query)

        # number of cases in the log
        case_count = edge_table[case_col].nunique()

        case_ids = edge_table[case_col].unique()

        df_all_cases = {case: set() for case in case_ids}

        iterations = len(edge_table)

        bar = tqdm(total=iterations)
        bar.set_description("Calculating directly- for cases")
        for _, row in edge_table.iterrows():
            df_all_cases[row[case_col]].add((row["SOURCE"], row["TARGET"]))

            bar.update(1)

        return df_all_cases
