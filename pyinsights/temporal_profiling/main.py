
from pyinsights import Connector
from pyinsights.organisational_profiling import ResourceProfiler
from pyinsights.temporal_profiling import TemporalProfiler
if __name__ == "__main__":


    celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
    api_token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"


    # define connector and connect to celonis
    connector = Connector(api_token=api_token,
                          url=celonis_url, key_type="USER_KEY")

    # choose data model
    print("Available datamodels:")
    print(connector.celonis.datamodels)
    print("Input id of datamodel:")
    #id = input()
    connector.set_parameters(model_id="376145f1-790d-4deb-8e20-083a4dfd7ca7")#, end_timestamp="END_DATE")


    connector.set_parameters(
        model_id=id, end_timestamp="END_DATE", resource_column="CE_UO")

    from pyinsights.ml import anomaly_detection
    print(anomaly_detection(connector=connector).head(n=100).to_string())

