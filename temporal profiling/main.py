
import numpy as np
import pandas as pd
from pycelonis import get_celonis
from pycelonis import pql
from pycelonis.pql import PQL, PQLColumn, PQLFilter

if __name__ == "__main__":
    celonis_url = "academic-m-s-qafari-pads-rwth-aachen-de.eu-2.celonis.cloud"
    api_token = "ZTUxZGNjNmItYzEwNy00MTI4LWJjZDctZmU1Zjg0Y2ZiYmQ0OkxRaHNRaEd6eHFMYXBwSlhyUkg1Z0NlUjBOUDlMbzdpcFZNNGx4cGdhdlJx"

    try:
        celonis = get_celonis(celonis_url=celonis_url, api_token=api_token)
    except:
        print("error")

    print(celonis.pools)
    print("-------------------------------")
    print(celonis.datamodels)

    datamodel = celonis.datamodels.find("d910eca4-b173-4a3a-aea6-cbc0fa86fcdd")

    # Any activity to any activity
    # Start timestamp and completion timestamp are the same: time:timestamp


    # cases with waiting time larger than average
    query6 = PQL()

    query6.add(PQLColumn(name = "case ID", query="""TARGET("receipt_xes"."CASE ID")"""))
    query6.add(PQLColumn(name = "source", query="""SOURCE("receipt_xes"."concept:name",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[])"""))
    query6.add(PQLColumn(name = "target", query="""TARGET("receipt_xes"."concept:name")"""))
    query6.add(PQLColumn(name = "duration", query="""SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp"))"""))

    #compute average waiting time
    # needs pull-up and domain table because of celonis joins
    query6.add(PQLColumn(name = "avg duration", query="""
        PU_AVG ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) )
    """))

    #compute standard deviation of waiting time
    # needs pull-up and domain table because of celonis joins
    query6.add(PQLColumn(name="std duration", query="""
    PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) )
    """))

    # filter all cases with waiting time deviating more than average +- 3 sigma
    # nice example of Pull-up with domain table
    filter_larger_than_average = """ ( SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp"))
     >= 
    PU_AVG ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) )
    +
    3*PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) ) 
    )
    OR
    ( SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp"))
     <=
    PU_AVG ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) )
    -
    3*PU_STDEV ( DOMAIN_TABLE (SOURCE("receipt_xes"."concept:name"), 
                            TARGET("receipt_xes"."concept:name")),
                            SECONDS_BETWEEN(SOURCE("receipt_xes"."time:timestamp",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[]),
    TARGET("receipt_xes"."time:timestamp")) ) 
    )
    
    
    """
    query6.add(PQLFilter(filter_larger_than_average))


    df6 = datamodel.get_data_frame(query6)
    print(df6)



    ## corresponding counts

    query7 = PQL()
    query7.add(PQLColumn(name = "source", query="""SOURCE("receipt_xes"."concept:name",
    ANY_OCCURRENCE[] TO ANY_OCCURRENCE[])"""))
    query7.add(PQLColumn(name = "target", query="""TARGET("receipt_xes"."concept:name")"""))
    query7.add(PQLColumn(name="occurrences", query="""COUNT(SOURCE("receipt_xes"."concept:name"))"""))












