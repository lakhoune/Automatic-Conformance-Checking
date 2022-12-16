from pyinsights.ml import get_features
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pandas as pd
from prince import PCA
import numpy as np


def anomaly_detection(connector, contamination='auto'):
    """
    Detects anomalous cases based on isolation forests
    Args:
        connector (pyinsights.Connector): connector
        contamination ('auto' or float): contamination in dataset        
    Returns:
        pandas.DataFrame: case ids of anomalous cases
    """
    # set up usual variables
    datamodel = connector.datamodel
    activity_table = connector.activity_table()
    case_col = connector.case_col()
    act_col = connector.activity_col()
    timestamp = connector.timestamp()
    end_timestamp = connector.end_timestamp()
    has_endtime = connector.has_end_timestamp()

    # get features and scale them with standardscaler
    feature_df = get_features(connector=connector)
    feature_df.dropna(inplace=True)
    X = feature_df.drop(case_col, axis=1)
    scaler = StandardScaler()
    X = pd.DataFrame(scaler.fit_transform(X), columns=X.columns)

    # transform data with pca
    X_pca = _pca(X)

    # train isolationforest on log
    clf = IsolationForest(
        random_state=42, contamination=contamination).fit(X_pca)

    # filter for anomalies
    feature_df.loc[:, "anomaly score"] = clf.score_samples(X_pca)
    feature_df.loc[:, "flag"] = clf.predict(X_pca)
    feature_df = feature_df[feature_df["flag"] == -1]

    # sort result
    result = feature_df.loc[:, [case_col, "anomaly score"]].sort_values(
        by="anomaly score")

    print(f"""percent anomalies: {len(result) / len(X)} """)
    # return
    return result


def _pca(X):
    # reduce dimensionalities with pca
    pca = PCA(n_components=3, n_iter=5, random_state=42)
    pca.fit(X)
    X_pca = pca.transform(X)
    # show stats for pca
    print(f"Explained inertia: {pca.explained_inertia_}")
    print("Column Correlations")
    print(pca.column_correlations(X))

    return X_pca
