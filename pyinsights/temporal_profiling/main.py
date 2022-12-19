
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

    # init temporal profiler
    temporal_profiler = TemporalProfiler(connector=connector)

    #compute temporal profile (not necessary for next steps)
    temporal_profile = temporal_profiler.temporal_profile()
    # compute deviating cases with deviation cost    # compute deviating events
    deviations = temporal_profiler.deviating_cases(extended_view=False, sigma=1)
    profiler = ResourceProfiler(connector, resource_column="CE_UO")
    df3 = profiler.cases_with_batches(min_batch_size=2)
    combiner = Combiner(connector)
    print(deviations.iloc[:10].to_string())
    print(deviations.iloc[8:20].to_string())
    df = combiner.combine_deviations([deviations.iloc[:10], deviations.iloc[11:20], deviations.iloc[5:10]])
    df2 = combiner.combine_deviations([deviations.iloc[:10], deviations.iloc[7:20], deviations.iloc[5:10]], how="intersection")
    df = combiner.combine_deviations([deviations, df3], how="union")
    df2 = combiner.combine_deviations([deviations, df3], how="intersection")
    print(df.to_string())
    print(df[df[connector.activity_col()].apply(lambda x: "->" in x)].to_string())
    print(df2.to_string())
    print(eq)
    print(connector.events())
