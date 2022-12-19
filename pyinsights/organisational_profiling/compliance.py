from pycelonis.celonis_api.pql.pql import PQL, PQLColumn, PQLFilter
from pyinsights import Connector


def _build_filter(connector, resource_column, activities):
    # build pql-conform strings from activity lists
    act_list = ''.join(
        f"""['{act}','{act}'],""" for act in activities)[:-1]

    act_query = f""" "{connector.activity_table()}"."{connector.activity_col()}" """
    res_query = f""" "{connector.activity_table()}"."{resource_column}"  """
    # filter on cases when same resource executes associated activities
    resource_filter = f"""
    SOURCE({res_query}, REMAP_VALUES({act_query}, {act_list}, NULL)) 
        = TARGET({res_query})   """

    return PQLFilter(resource_filter)


def segregation_of_duties(connector, resource_column, activities):
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
    same_activity_filter = f""" FILTER SOURCE("{connector.activity_table()}"."{resource_column}") = TARGET("{connector.activity_table()}"."{resource_column}")   """

    query = PQL()
    query += PQLColumn(name=connector.case_col(), query=case_id)
    query += PQLColumn(name="source", query=act_source)
    query += PQLColumn(name="target", query=act_target)
    query += PQLColumn(name=connector.timestamp(), query=timestamp)
    query += PQLColumn(name=resource_column, query=resource)

    # filter
    resource_filter = _build_filter(
        connector, resource_column, activities)
    query += resource_filter

    df = connector.datamodel.get_data_frame(query)

    return df

