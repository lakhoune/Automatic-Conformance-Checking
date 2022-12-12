from pyinsights import Connector
from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter


def segregation_of_duties(connector, resource_column):
    """
    computes violations of the four-eyes principle
    :param connector: pycelonis.Connector
    :param resource_column: name of resource column in event log
    :return: pandas.core.Dataframe
    """
    act_source = f"""SOURCE("{connector.activity_table()}"."{connector.activity_col()}") """
    act_target = f"""TARGET("{connector.activity_table()}"."{connector.activity_col()}") """
    case_id = f"""SOURCE("{connector.activity_table()}"."{connector.case_col()}") """
    resource = f"""SOURCE("{connector.activity_table()}"."{resource_column}") """
    timestamp = f"""SOURCE("{connector.activity_table()}"."{connector.timestamp()}") """
    filter = f""" FILTER SOURCE("{connector.activity_table()}"."{resource_column}") = TARGET("{connector.activity_table()}"."{resource_column}")   """

    query = PQL()
    query += PQLFilter(filter)
    query += PQLColumn(name=connector.case_col(), query=case_id)
    query += PQLColumn(name="source", query=act_source)
    query += PQLColumn(name="target", query=act_target)
    query += PQLColumn(name=connector.timestamp(), query=timestamp)
    query += PQLColumn(name=resource_column, query=resource)

    df = connector.datamodel.get_data_frame(query)

    return df

