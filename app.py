import streamlit as st
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler


st.header("Automatic Conformance Checking Insights")
#celonis_url = "https://christian-fiedler1-rwth-aachen-de.training.celonis.cloud/"
#token = "MzdhNWNlNDItOTJhNC00ZTE1LThlMGMtOTc4MGVmOWNjYjIyOjVTcW8wSlVmbFVkMG84bFZTRUw4bTJDZVNIazVZWlJsZWQ2bTUzbWtLSDJM"


@st.experimental_memo
def columns(_model, model_url):
    return [None] + _model.default_activity_table.columns


def name_of_model(model):
    return model


@st.experimental_memo
def set_datamodel(_model, endtime, resource_col):
    st.session_state.connector.datamodel = _model
    if endtime is not None:
        st.session_state.connector.set_parameters(end_timestamp=endtime["name"])
    else:
        st.session_state.connector.set_parameters(end_timestamp="")
    if resource_col is not None:
        st.session_state.resource_col = resource_col["name"]
    else:
        st.session_state.resource_col = None


@st.experimental_memo
def temporal_deviations(endtime, resource_col):
    profiler = TemporalProfiler(connector=st.session_state.connector)
    df = profiler.deviating_cases(sigma=sigma)

    return df


@st.experimental_memo
def resource_deviations(endtime, resource_col):
    if "resource_col" in st.session_state:
        profiler = ResourceProfiler(connector=st.session_state.connector,resource_column=st.session_state.resource_col)
    else:
        st.error("Not all parameters set")
        return

    df = profiler.cases_with_batches(time_unit=time_unit, reference_unit=reference_unit, min_batch_size=min_batch_size
                                     , batch_percentage=batch_percentage
                                     , grouped_by_batches=grouped_by_batches, batch_types=batch_types)

    return df


if "connector" not in st.session_state:
    with st.form("login"):
        url = st.text_input("Celonis URL")
        api_token = st.text_input("Api token")
        login = st.form_submit_button("Login")

    if login:
        st.session_state.connector = Connector(api_token=api_token, url=url, key_type="USER_KEY")
        st.experimental_rerun()


if "connector" in st.session_state:
    st.success("Successfully logged in to celonis")
    with st.sidebar:
        st.subheader("Config")
        models = st.session_state.connector.celonis.datamodels
        model_option = st.selectbox("Choose datamodel", models, format_func=name_of_model)
        with st.form("model"):
            columns = columns(model_option, model_option.url)
            end_timestamp = st.selectbox("Input end-timestamp column", columns)
            resource_col = st.selectbox("Input resource column", columns)
            model_submitted = st.form_submit_button("Select Model")

        if model_submitted:
            set_datamodel(model_option, end_timestamp, resource_col)

        method_option = st.selectbox("Choose method", ("Temporal", "Resource"))

        if method_option == "Temporal":
            sigma = st.number_input(label="Sigma", value=6)

        if method_option == "Resource":
            col1, col2 = st.columns(2)
            with col1:
                time_unit = st.selectbox("Time unit", ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"], index=2)
                reference_unit = st.selectbox("Reference unit", ["MINUTES", "HOURS", "DAY", "MONTH", None], index=4)
                min_batch_size = st.number_input(label="Min batch size", value=2)
            with col2:
                batch_percentage = st.number_input(label="Batch percentage", value=0.1, step=0.1)
                grouped_by_batches = st.selectbox("Grouped", [True, False])
                batch_types = st.selectbox("Batch types", [True, False])

    run = st.button("Get deviations")
    if run:
        if method_option == "Temporal":
            st.subheader("Deviations:")
            with st.spinner("Calculating deviations"):
                st.session_state.connector.set_parameters(end_timestamp=end_timestamp["name"])
                print(st.session_state.connector.end_timestamp())
                print(resource_col)
                df = temporal_deviations(end_timestamp["name"], resource_col["name"])
                st.write(f"{df.shape[0]} deviations found")
                st.dataframe(df)

        elif method_option == "Resource":
            st.subheader("Deviations:")
            with st.spinner("Calculating deviations"):
                print(st.session_state.resource_col)
                print(st.session_state.connector.end_timestamp())
                print(st.session_state.connector.timestamp())
                df = resource_deviations(end_timestamp, resource_col)
                st.write(f"{df.shape[0]} deviations found")
                st.dataframe(df)











