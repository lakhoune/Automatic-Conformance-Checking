import streamlit as st
import numpy as np
import pandas as pd
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler
from pyinsights.log_skeleton import LogSkeleton
from pyinsights.ml import anomaly_detection
from pyinsights import Combiner
celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"


def logout():
    del st.session_state.connector


@st.experimental_memo
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv().encode('utf-8')


@st.experimental_memo
def columns(_model, model_url):
    model_columns = _model.default_activity_table.columns
    cols = {"endtime": [{'name': ""}] + [x for x in model_columns if x["type"] == "DATE"],
            "resource": [{'name': ""}] + [x for x in model_columns if x["type"] != "DATE"]}

    return cols


def name_of_col(model):
    if model["name"] == "":
        return "None"
    else:
        return f"""{model["name"]}, {model["type"]}"""


def name_of_model(model):
    return model._data["name"]


def color_cost(val, quantile):
    color = 'gray' if val < quantile else 'red'
    return f'background-color: {color}'


@st.experimental_memo
def set_datamodel(_model, endtime, resource_co, url):
    st.session_state.connector.datamodel = _model
    if endtime['name'] == "":
        st.session_state.connector.end_time = None
    else:
        st.session_state.connector.end_time = endtime['name']
    st.session_state.resource_col = resource_col["name"]


@st.experimental_memo(show_spinner=True)
def temporal_deviations(endtime, resource_col, simga, deviation_cost, extended_view, url):
    profiler = TemporalProfiler(connector=st.session_state.connector)
    df = profiler.deviating_cases(
        sigma=sigma, extended_view=extended_view, deviation_cost=deviation_cost)

    return df


@st.experimental_memo(show_spinner=True)
def lsk_deviations(noise_threshold, url):
    lsk = LogSkeleton(connector=st.session_state.connector)
    df = lsk.get_non_conforming_cases(noise_threshold=noise_threshold)

    return df


@st.experimental_memo(show_spinner=True)
def anomaly_deviations(contamination, param_optimization, url, endtime, resource_col):

    df = anomaly_detection(st.session_state.connector)

    return df


@st.experimental_memo(show_spinner=True)
def anomaly_deviations(contamination, param_optimization, url, endtime, resource_col):

    df = anomaly_detection(st.session_state.connector)

    return df


@st.experimental_memo(show_spinner=True)
def _combine_deviations(_combiner, deviations, how, url):

    df = combiner.combine_deviations(deviations=deviations, how=how)
    return df


@st.experimental_memo(show_spinner=True)
def resource_deviations(endtime, resource_col, time_unit, reference_unit, min_batch_size, batch_percentage, grouped_by_batches, batch_types, url):
    st.session_state.connector.resource_col = resource_col

    if st.session_state.connector.resource_col != None:
        profiler = ResourceProfiler(
            connector=st.session_state.connector)
    else:
        st.error("Not all parameters set")
        return

    df = profiler.cases_with_batches(time_unit=time_unit, reference_unit=reference_unit, min_batch_size=min_batch_size,
                                     batch_percentage=batch_percentage, grouped_by_batches=grouped_by_batches, batch_types=batch_types)

    return df


st.set_page_config(page_title="Automatic Conformance Checking", layout="wide")

st.markdown("""<style>
            div.css-18e3th9 {
    flex: 1 1 0%;
    width: 100%;
    padding: 1rem 2rem 1.5rem;
    min-width: auto;
    max-width: initial;
    }
       div.css-1vq4p4l {
    padding: 0.0rem 2rem 1.5rem;
    }

            </style>""", unsafe_allow_html=True)

st.header("Automatic Conformance Checking Insights")


if "success" not in st.session_state:
    st.session_state.success = False

if "deviations" not in st.session_state:
    st.session_state.deviations = []

if "connector" not in st.session_state:
    with st.form("login"):
        url = st.text_input("Celonis URL", celonis_url)
        api_token = st.text_input("Api token", token)
        key_type = st.selectbox(
            options=["USER_KEY", "APP_KEY"], label="Key type")
        login = st.form_submit_button("Login")

    if login:
        try:
            st.session_state.connector = Connector(
                api_token=api_token, url=url, key_type=key_type)
        except:
            st.error("Couldn't login. Try again")
        st.experimental_rerun()


elif "connector" in st.session_state:
    st.info("""Select a datamodel and the necessary columns on the left sidebar. Then, select your prefered methods and set the necessary parameters.
After that, you can just click on 'Get deviations'!""",  icon="ℹ️")

    col1, col2, _, col4 = st.columns(4)

    with col1:
        run = st.button("Get deviations", type="primary")
    with col4:
        st.button("Logout", on_click=logout)

    with st.sidebar:
        st.subheader("Config")
        models = st.session_state.connector.celonis.datamodels
        model_option = st.selectbox(
            "Choose datamodel", models, format_func=name_of_model)

        if model_option is None:
            st.error("Not enough permissions to get models. Change key type!")

        else:

            columns = columns(model_option, model_option.url)
            end_timestamp = st.selectbox(
                "Input end-timestamp column", columns["endtime"], format_func=name_of_col)
            resource_col = st.selectbox(
                "Input resource column", columns["resource"], format_func=name_of_col)

            method_option = st.multiselect(
                "Choose methods", ("Temporal Profiling", "Resource Profiling", "Log Skeleton", "Anomaly Detection"))

            if len(method_option) > 1:
                combine_method = st.selectbox(label="Combination method", options=[
                                              "union", "intersection"])
            st.subheader("Parameters")
            tab1, tab2, tab3, tab4 = st.tabs(
                ["Temporal Profile", "Resource Profile", "Log Skeleton", "Anomaly Detection"])
            with tab1:
                col1, col2 = st.columns(2)
                with col1:
                    sigma = st.number_input(label="Sigma", value=6)
                with col2:
                    deviation_cost = st.checkbox("Deviation cost", value=True)
                    extended_view = st.checkbox("Extended view", value=True)

            with tab2:
                col1, col2 = st.columns(2)

                with col1:
                    time_unit = st.selectbox(
                        "Time unit", ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"], index=2)
                    reference_unit = st.selectbox(
                        "Reference unit", ["MINUTES", "HOURS", "DAY", "MONTH", None], index=4)
                    min_batch_size = st.number_input(
                        label="Min batch size", value=8)

                with col2:
                    batch_percentage = st.number_input(
                        label="Batch percentage", value=0.1, step=0.1)
                    grouped_by_batches = st.selectbox("Grouped", [True, False])
                    batch_types = st.selectbox("Batch types", [True, False])
            with tab3:
                noise_treshold = st.number_input(
                    label="Noise-threshold", value=0.2, step=0.1)
            with tab4:
                param_opti = st.checkbox(
                    label="Hyperparameter Optimization", value=True)
                contamination = "auto"
                if not param_opti:
                    contamination = st.number_input(
                        label="Contamination", value=0.2, step=0.1)
    if run:
        if st.session_state.connector.end_time == "":
            st.session_state.connector.end_time = None
        else:
            st.session_state.end_time = end_timestamp
        st.session_state.connector.resource_col = resource_col["name"]

        st.session_state.connector.datamodel = model_option
        if len(method_option) == 0:
            st.error("Please select a method!")
        else:
            st.session_state.success = False
            st.session_state.deviations = []
            st.subheader("Deviations:")
            with st.spinner("Calculating deviations"):
                if "Temporal Profiling" in method_option:
                    # sanity check due to bug

                    df = temporal_deviations(end_timestamp["name"], resource_col["name"],
                                             sigma, deviation_cost, extended_view, model_option.url)
                    st.session_state.deviations.append(df)

                if "Resource Profiling" in method_option:
                    if resource_col["name"] != "":
                        df = resource_deviations(end_timestamp["name"], resource_col["name"],
                                                 time_unit=time_unit, reference_unit=reference_unit,
                                                 min_batch_size=min_batch_size, batch_percentage=batch_percentage, grouped_by_batches=grouped_by_batches, batch_types=batch_types, url=model_option.url
                                                 )
                        st.session_state.deviations.append(df)

                    else:
                        st.error("Please select a valid resource column!")
                if "Log Skeleton" in method_option:
                    df = lsk_deviations(noise_treshold, url=model_option.url)
                    st.session_state.deviations.append(df)
                if "Anomaly Detection" in method_option:

                    df = anomaly_deviations(contamination=contamination, param_optimization=param_opti, url=model_option.url,
                                            endtime=end_timestamp["name"], resource_col=resource_col["name"])
                    st.session_state.deviations.append(df)
                if len(st.session_state.deviations) == len(method_option):
                    st.session_state.success = True
                else:
                    st.error("Error")

    if st.session_state.success:
        if len(method_option) > 1:
            combiner = Combiner(connector=st.session_state.connector)
            df = _combine_deviations(
                combiner, st.session_state.deviations, how=combine_method, url=model_option.url)
        else:
            df = st.session_state.deviations[0]

        st.write(f"{len(df)} deviations found")
        if "deviation cost" in list(df.columns):
            quantile = np.quantile(df["deviation cost"], q=0.75)
            st.dataframe(df.style.applymap(color_cost,
                                           quantile=quantile, subset=['deviation cost']))
            st.bar_chart(pd.cut(df["deviation cost"], 3, labels=[
                "small", "medium", "big"]).value_counts())
        else:
            st.dataframe(df)
        csv = convert_df(df)
        st.download_button(
            label="Download data as CSV",
            data=csv,
            file_name='deviations.csv',
            mime='text/csv',
        )
