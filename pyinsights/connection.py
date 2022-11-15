from pycelonis import get_celonis
from pycelonis.pql import PQL, PQLColumn

class Connector:
    """
Encapsulates connection to celonis
provides datamodel, activity_table, case_col, activity_col, timestamp

:param api_key: Celonis api token
:type api_key: string

:param url: Celonis url
:type url: string

"""

    def __init__(self, api_key, url):
        self.datamodel = None
        self.api_key = api_key
        self.url = url
        try:
            self.celonis = get_celonis(api_token=self.api_key, celonis_url=self.url)
        except:
            self.celonis = None
            print("error")

    def connect(self):
        """
        Connects to Celonis
        """

        try:
            self.celonis = get_celonis(api_token=self.api_key, celonis_url=self.url)
        except:
            print("error")

    def activity_table(self):
        """
           returns name of activity table
           """
        process = self.datamodel.process_configurations[0]
        return process.activity_table.source_name

    def case_col(self):
        """
        returns name of case column
        """

        process_config = self.datamodel.process_configurations[0]
        case_col = process_config.case_column
        act_col = process_config.activity_column
        timestamp = process_config.timestamp_column

        return case_col

    def activity_col(self):
        """
        returns name of activity column
        """

        process_config = self.datamodel.process_configurations[0]
        act_col = process_config.activity_column

        return act_col

    def timestamp(self):
        """
           returns name of timestamp column
           """
        process_config = self.datamodel.process_configurations[0]
        timestamp = process_config.timestamp_column

        return timestamp

    def set_datamodel(self, id):
        """
            sets datamodel
            :param id: id of datamodel
            :type id: string
            """
        self.datamodel = self.celonis.datamodels.find(id)

    def events(self):
        """
             returns all events as dataframe
        """
        query = PQL()
        query.add(PQLColumn(name="case:concept:name", query=f"\"{self.activity_table()}\".\"{self.case_col()}\""))
        query.add(PQLColumn(name="concept:name", query=f"\"{self.activity_table()}\".\"{self.activity_col()}\""))
        query.add(PQLColumn(name="timestamp", query=f""" "{self.activity_table()}"."{self.timestamp()}"  """))
        events = self.datamodel.get_data_frame(query)