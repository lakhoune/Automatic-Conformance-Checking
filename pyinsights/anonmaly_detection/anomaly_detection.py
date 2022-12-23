from pyinsights.anonmaly_detection import get_features
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import make_scorer, calinski_harabasz_score
from sklearn import model_selection
import pandas as pd
from prince import PCA
import numpy as np
import math


def anomaly_detection(connector, parameter_optimization=True, contamination='auto'):
    """
    Detects anomalous cases based on isolation forests
    Args:
        connector (pyinsights.Connector): connector
        parameter_optimization (bool): Wether to use hyperparameter optimization
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
    X_pca = pca(X)

    # train isolationforest on log
    clf = IsolationForest(
        random_state=42, contamination=contamination, verbose=2)

    # hyperparameter optimization
    if parameter_optimization:
        tuned_clf = parameter_tuning(42, clf=clf)
    else:
        tuned_clf = clf

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
    pca = PCA(n_components=5, n_iter=5, random_state=42)
    pca.fit(X)
    X_pca = pca.transform(X)
    # show stats for pca
    print(f"Explained inertia: {pca.explained_inertia_}")
    print("Column Correlations")
    print(pca.column_correlations(X))

    return X_pca


def parameter_tuning(random_state, clf):
    # hyperparameter optimization with random search
    param_grid = {'n_estimators': list(range(100, 800, 100)),
                  'max_samples': ['auto'],
                  'contamination': [0.1, 0.2, 0.3, 0.4, 0.5],
                  'max_features': [3, 4, 5],
                  'bootstrap': [True, False],
                  'n_jobs': [-1, 20, 30]}

    grid_dt_estimator = model_selection.RandomizedSearchCV(clf,
                                                           param_grid,
                                                           scoring=scorer_ch,
                                                           refit=True,
                                                           cv=10,
                                                           return_train_score=False,
                                                           verbose=2)
    return grid_dt_estimator


def scorer_ch(estimator, X):
    """score estimated clusters with calinski_harabasz score

    Args:
        estimator (_type_): _description_
        X (_type_): _description_

    Returns:
        _type_: _description_
    """
    labels = estimator.fit_predict(X)
    return calinski_harabasz_score(X, labels)
