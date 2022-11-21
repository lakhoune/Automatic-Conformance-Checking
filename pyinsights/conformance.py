import typing
from pycelonis import pql
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pm4py.discovery import discover_petri_net_inductive
import pandas as pd
import pm4py


def discover_petri_net_from_log(connector, events):
    """
    returns discovered petri net
    :param events: events as dataframe
    :return: petri net, initial & final markings
    """

    df_formatted = pm4py.format_dataframe(events, case_id=connector.case_col(), activity_key=connector.activity_col(),
                                          timestamp_key=connector.timestamp())
    # filtered_log = pm4py.filter_variants_top_k(df_formatted, 10)

    net, initial_marking, final_marking = discover_petri_net_inductive(df_formatted)

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


def alignment_scores(events_to_align,event_log, connector):
    """
    Computes alignment scores for cases and returns them
    as dataframe
    :param connection: Connector
    :return: alignment scores as dataframe
    """

    events_to_align = pm4py.format_dataframe(events_to_align, case_id=connector.case_col(), activity_key=connector.activity_col(),
                                    timestamp_key=connector.timestamp())
    net, im, fm = discover_petri_net_from_log(events=event_log, connector=connector)
    aligned_traces = pm4py.conformance_diagnostics_alignments(events_to_align, net, im, fm)
    costs = [aligned_traces[x]["cost"] for x in range(len(aligned_traces))]
    return costs



