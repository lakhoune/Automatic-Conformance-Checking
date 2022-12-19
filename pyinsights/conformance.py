import typing
from pycelonis import pql
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pm4py.discovery import discover_petri_net_inductive
import pandas as pd
import pm4py
from pm4py.algo.evaluation.simplicity import algorithm as simplicity_evaluator
from pm4py.algo.evaluation.generalization import algorithm as generalization_evaluator


def _get_top_variants(connector):
    """
    returns the top variants of an event log as df
    :param connector:
    :return:
    """
    datamodel = connector.datamodel
    activity_table = connector.activity_table()
    case_col = connector.case_col()
    act_col = connector.activity_col()
    timestamp = connector.timestamp()
    # get number of cases
    query = PQL()
    query += PQLColumn(name="case count",
                       query=f""" FLOOR(COUNT (DISTINCT "{activity_table}"."{case_col}")*0.01) """)
    df = datamodel.get_data_frame(query)
    num_cases = df["case count"].values[0]
    # cluster variants, retain the ones covering at least 1% of traces
    query = PQL()
    query.add(PQLColumn(name=case_col,
              query=f""" "{activity_table}"."{case_col}" """))
    query.add(PQLColumn(name=act_col,
              query=f""" "{activity_table}"."{act_col}" """))
    query.add(PQLColumn(name=timestamp,
              query=f""" "{activity_table}"."{timestamp}" """))
    query.add(PQLColumn(name="cluster",
                        query=f"""
                        CLUSTER_VARIANTS( VARIANT ("{activity_table}"."{act_col}") , {num_cases}, 2)
                    """))
    query += PQLFilter(f"""
                        CLUSTER_VARIANTS( VARIANT ("{activity_table}"."{act_col}") , {num_cases}, 2) >= 0
                    """)
    df = datamodel.get_data_frame(query)

    # pm4py formatting
    df_formatted = pm4py.format_dataframe(df, case_id=connector.case_col(), activity_key=connector.activity_col(),
                                          timestamp_key=connector.timestamp())

    return df_formatted


def _discover_petri_net_from_log(connector, events, evaluate=False):
    """
    returns discovered petri net
    :param events: events as dataframe
    :return: petri net, initial & final markings
    """

    df_formatted = pm4py.format_dataframe(events, case_id=connector.case_col(), activity_key=connector.activity_col(),
                                          timestamp_key=connector.timestamp())
    # filter log to top variants

    filtered_dataframe = _get_top_variants(connector)

    # discover model
    net, initial_marking, final_marking = discover_petri_net_inductive(
        filtered_dataframe)

    if evaluate:
        # evaluate discovered model (takes a while on big logs)
        fitness = pm4py.fitness_token_based_replay(
            df_formatted, net, initial_marking, final_marking)
        precision = pm4py.precision_token_based_replay(
            df_formatted, net, initial_marking, final_marking)
        gen = generalization_evaluator.apply(
            df_formatted, net, initial_marking, final_marking)
        simp = simplicity_evaluator.apply(net)

        print(f"fitness: {fitness}")
        print(f"precision: {precision}")
        print(f"generalization: {gen}")
        print(f"simplicity: {simp}")

    return net, initial_marking, final_marking


# alignment base conformance checking

def _petri_net_pql(net, inital_marking, final_marking):
    """
    turn pm4py petri net representation into pql-conforming string (not implemented)
    :param net: pm4py petri net
    :param inital_marking: initial marking
    :param final_marking: final marking
    :return: petri net as string
    """

    net_str = "["
    for p in net.places:
        net_str += f""" "{p}" """
    net_str += "]"
    print(net_str)

    trans_str = "["
    for t in net.transitions:
        trans_str += f""" "{t}" """
    trans_str += "]"
    print(trans_str)

    arcs_str = "["
    for arc in net.arcs:
        arc = str(arc)
        splitted = arc.split("->")
        helper_str = f""" [{splitted[0]} {splitted[1]}]  """
        arcs_str += helper_str
    arcs_str += "]"


def alignment_scores(events_to_align, event_log, connector):
    """
    Computes alignment scores for cases and returns them
    as dataframe
    :param event_log: event log as df
    :param events_to_align: events to align as df
    :param connector: pyinsights.Connector
    :return: alignment scores as dataframe
    """
    # pm4py formatting
    events_to_align = pm4py.format_dataframe(events_to_align, case_id=connector.case_col(),
                                             activity_key=connector.activity_col(),
                                             timestamp_key=connector.timestamp())
    # discover model
    net, im, fm = _discover_petri_net_from_log(
        events=event_log, connector=connector, evaluate=False)
    # align traces with pm4py (because not feasible in pql)
    aligned_traces = pm4py.conformance_diagnostics_alignments(
        events_to_align, net, im, fm)
    # extract alignment cost per trace
    costs = [aligned_traces[x]["cost"] for x in range(len(aligned_traces))]
    return costs


def tbr_scores(events_to_replay, event_log, connector):
    """
    Computes tbr scores for cases and returns them
    as dataframe
    :param event_log: event log as df
    :param events_to_replay: events to replay as df
    :param connector: pyinsights.Connector
    :return: replay scores as dataframe
    """
    # pm4py formatting
    events_to_replay = pm4py.format_dataframe(events_to_replay, case_id=connector.case_col(),
                                              activity_key=connector.activity_col(),
                                              timestamp_key=connector.timestamp())
    # discover model
    net, im, fm = _discover_petri_net_from_log(
        events=event_log, connector=connector, evaluate=False)
    # replay traces with pm4py (because not feasible in pql)
    tbr_diagnostics = pm4py.conformance_diagnostics_token_based_replay(
        events_to_replay, net, im, fm)
    # extract cost per trace, defined as #missing tokens + #remaining tokens
    costs = [trace['missing_tokens'] + trace['remaining_tokens']
             for trace in tbr_diagnostics]
    return costs
