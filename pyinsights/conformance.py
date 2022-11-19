import typing
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter
from pm4py.discovery import discover_petri_net_inductive
import pandas as pd

def discover_petri_net_from_log(events):
    """
    returns discovered petri net
    :param events: events as dataframe
    :return: petri net, initial & final markings
    """

    net, initial_marking, final_marking = discover_petri_net_inductive(events)

    return net, initial_marking, final_marking

# alignment base conformance checking

def _petri_net_pql(net, inital_marking, final_marking):
    """
    turn pm4py petri net representation into pql-conforming string
    :param net: pm4py petri net
    :param inital_marking: initial marking
    :param final_marking: final marking
    :return: petri net as string
    """

    net_pql = net



def alignment_score(connection):

    """
    Computes alignment scores for cases and returns them
    as dataframe
    :param connection: Connector
    :return: alignment scores as dataframe
    """
    # model = """[ "source" "sink" "p3"
    # "p4" "p2" "p1" ],
    # [ "T0" "T1" "T2" "T3" "T4" ],
    # [ ["source" "T4"] ["p1" "T1"] ["T1" "p2"] ["p2" "T0"] ["T4" "p1"] ["T0" "p3"] ["p3" "T2"]
    # ["T2" "p4"] ["p4" "T3"] ["T3" "sink"] ],
    # [ ['T04 Determine confirmation of receipt' "T0"] ['T02 Check confirmation of receipt' "T1"]
    # ['T05 Print and send confirmation of receipt' "T2"] ['T06 Determine necessity of stop
    # advice' "T3"] ['Confirmation of receipt' "T4"] ],
    # [ "source" ],
    # [ "sink" ]
    # """

    events = connection.events()
    net, inital_marking, final_marking = discover_petri_net_from_log(events)
    model = _petri_net_pql(net, inital_marking, final_marking)

    q3 = f"""PU_COUNT( DOMAIN_TABLE("{connection.activity_table()}"."{connection.case_col()}"),
        REMAP_VALUES(ALIGN_MOVE("{connection.activity_table()}"."{connection.act_col()}",{model}),
        ['[S]',NULL])
        )"""

    query2 = PQL()
    query2.add(PQLColumn(name="case Id", query=f"\"{connection.activity_table()}\".\"{connection.case_col()}\""))
    query2.add(PQLColumn(name="alignment_score", query=q3))

    df2 = connection.datamodel.get_data_frame(query2)
    df2.drop_duplicates(inplace=True)

    return df2
