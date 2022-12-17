from pyinsights.ml import get_features
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import make_scorer, calinski_harabasz_score
from sklearn import model_selection
import pandas as pd
from prince import PCA
import numpy as np
import math


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

    num_cases = len(X)

    # transform data with pca
    X_pca = pca(X)

    # train isolationforest on log
    clf = IsolationForest(
        random_state=42, contamination=contamination)

    tuned_clf = parameter_tuning(43, num_cases=num_cases, clf=clf)
    tuned_clf.fit(X_pca)
    # filter for anomalies
    feature_df.loc[:, "anomaly score"] = tuned_clf.score_samples(X_pca)
    feature_df.loc[:, "flag"] = tuned_clf.predict(X_pca)
    feature_df = feature_df[feature_df["flag"] == -1]

    # sort result
    result = feature_df.loc[:, [case_col, "anomaly score"]].sort_values(
        by="anomaly score")

    print(f"""percent anomalies: {len(result) / len(X)} """)
    # return
    return result


def pca(X):
    # reduce dimensionalities with pca
    pca = PCA(n_components=3, n_iter=5, random_state=42)
    pca.fit(X)
    X_pca = pca.transform(X)
    # show stats for pca
    print(f"Explained inertia: {pca.explained_inertia_}")
    print("Column Correlations")
    print(pca.column_correlations(X))

    return X_pca


def parameter_tuning(random_state, num_cases, clf):
    min_samples = math.floor(num_cases*0.1)
    max_samples = math.floor(num_cases*0.8)
    step_size = math.floor(num_cases*0.1)
    param_grid = {'n_estimators': list(range(100, 800, 50)),
                  'max_samples': list(range(min_samples, max_samples, step_size)),
                  'contamination': [0.1, 0.2, 0.3, 0.4, 0.5],
                  'max_features': [5, 10, 15],
                  'bootstrap': [True, False],
                  'n_jobs': [5, 10, 20, 30]}

    ch_sc = make_scorer(calinski_harabasz_score)

    grid_dt_estimator = model_selection.GridSearchCV(clf,
                                                     param_grid,
                                                     scoring=ch_sc,
                                                     refit=True,
                                                     cv=10,
                                                     return_train_score=False)
    return grid_dt_estimator
