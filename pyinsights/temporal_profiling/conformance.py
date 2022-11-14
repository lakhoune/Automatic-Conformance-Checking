import typing
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter


# alignment base conformance checking
def alignment_score(model, datamodel):

    model = """[ "source" "sink" "p3"
    "p4" "p2" "p1" ],
    [ "T0" "T1" "T2" "T3" "T4" ],
    [ ["source" "T4"] ["p1" "T1"] ["T1" "p2"] ["p2" "T0"] ["T4" "p1"] ["T0" "p3"] ["p3" "T2"]
    ["T2" "p4"] ["p4" "T3"] ["T3" "sink"] ],
    [ ['T04 Determine confirmation of receipt' "T0"] ['T02 Check confirmation of receipt' "T1"]
    ['T05 Print and send confirmation of receipt' "T2"] ['T06 Determine necessity of stop
    advice' "T3"] ['Confirmation of receipt' "T4"] ],
    [ "source" ],
    [ "sink" ]
    """

    q1 = "ALIGN_ACTIVITY(\"receipt_xes\".\"concept:name\"," + model + ")"

    q2 = "ALIGN_MOVE(\"receipt_xes\".\"concept:name\"," + model + ")"

    q3 = f"""PU_COUNT( DOMAIN_TABLE("receipt_xes"."case id"),
        REMAP_VALUES(ALIGN_MOVE("receipt_xes"."concept:name",{model}),
        ['[S]',NULL])
        )"""

    query2 = PQL()
    query2.add(PQLColumn(name="caseId", query="\"receipt_xes\".\"case id\""))
    query2.add(PQLColumn(name="alignment_score", query=q3))

    df2 = datamodel.get_data_frame(query2)

    return df2
