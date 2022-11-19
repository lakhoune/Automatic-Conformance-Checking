from pycelonis.pql import PQL, PQLColumn, PQLFilter


class LogSkeleton:
    """
    Class for log skeleton analysis.
    """
    datamodel = None
    activity_table = None
    case_col = None
    act_col = None
    timestamp = None

    def __init__(self, connector):
        """
        :param connector: Connector object
        """
        global datamodel
        global activity_table
        global case_col
        global act_col
        global timestamp

        self.connector = connector
        datamodel = self.connector.datamodel
        activity_table = self.connector.activity_table()
        case_col = self.connector.case_col()
        act_col = self.connector.activity_col()
        timestamp = self.connector.timestamp()

    def log_skeleton(self, noise_treshold):
        """
        Returns the log skeleton of the data model.
        :param noise_treshold: int
        :return: pandas.DataFrame
        """
