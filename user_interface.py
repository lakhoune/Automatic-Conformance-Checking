import streamlit as st

from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler


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


@st.experimental_memo
def set_datamodel(_model, endtime, resource_co, url):
    st.session_state.connector.datamodel = _model
    if endtime['name'] == "":
        st.session_state.connector.end_time = None
    else:
        st.session_state.connector.end_time = endtime['name']
    st.session_state.resource_col = resource_col["name"]


@st.experimental_memo(show_spinner=False)
def temporal_deviations(endtime, resource_col, simga, deviation_cost, extended_view, url):
    profiler = TemporalProfiler(connector=st.session_state.connector)
    df = profiler.deviating_cases(
        sigma=sigma, extended_view=extended_view, deviation_cost=deviation_cost)

    return df


@st.experimental_memo(show_spinner=False)
def resource_deviations(endtime, resource_col, time_unit, reference_unit, min_batch_size, batch_percentage, grouped_by_batches, batch_types, url):
    if "resource_col" in st.session_state:
        profiler = ResourceProfiler(
            connector=st.session_state.connector, resource_column=st.session_state.resource_col)
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
    padding: 1rem 1rem 1.5rem;
    min-width: auto;
    max-width: initial;
    }
       div.css-1vq4p4l {
    padding: 0.0rem 1rem 1.5rem;
    }

            </style>""", unsafe_allow_html=True)

st.header("Automatic Conformance Checking Insights")


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
    st.caption("""Select a datamodel and the necessary columns on the right.
Select your choices with the button below them. Then, select your prefered methods and set the necessary parameters.
After that, you can just click on 'Get deviations'!""")

    col1, col2, _, col4 = st.columns(4)

    with col1:
        run = st.button("Get deviations", type="primary")
    with col4:
        st.button("Logout", on_click=logout)

    with st.sidebar:
        st.subheader("Config")
        models = st.session_state.connector.celonis.datamodels
        model_option = st.selectbox(
            "Choose datamodel", models, label_visibility="collapsed")

        if model_option is None:
            st.error("Not enough permissions to get models. Change key type!")

        else:
            with st.form("model"):
                columns = columns(model_option, model_option.url)
                end_timestamp = st.selectbox(
                    "Input end-timestamp column", columns["endtime"], format_func=name_of_col)
                resource_col = st.selectbox(
                    "Input resource column", columns["resource"], format_func=name_of_col)
                model_submitted = st.form_submit_button("Select Model")

            # if model_submitted:
                # set_datamodel(model_option, end_timestamp,
                #               resource_col, model_option.url)

            method_option = st.multiselect(
                "Choose methods", ("Temporal Profiling", "Resource Profiling", "Log Skeleton", "Anomaly Detection"))

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
                        label="Min batch size", value=2)
                with col2:
                    batch_percentage = st.number_input(
                        label="Batch percentage", value=0.1, step=0.1)
                    grouped_by_batches = st.selectbox("Grouped", [True, False])
                    batch_types = st.selectbox("Batch types", [True, False])
            with tab3:
                noise_treshold = st.number_input(
                    label="Noise-threshold", value=0.0, step=0.1)
            with tab4:
                contamination = st.number_input(
                    label="Contamination", value=0.2, step=0.1)
    if run:
        if st.session_state.connector.end_time == "":
            st.session_state.connector.end_time = None
        else:
            st.session_state.end_time = end_timestamp
        st.session_state.resource_col = resource_col
        st.session_state.connector.datamodel = model_option
        if len(method_option) == 0:
            st.error("Please select a method!")
        else:
            success = False

            st.subheader("Deviations:")
            with st.spinner("Calculating deviations"):
                if "Temporal Profiling" in method_option:
                    # sanity check due to bug

                    df = temporal_deviations(end_timestamp["name"], resource_col["name"],
                                             sigma, deviation_cost, extended_view, model_option.url)
                    success = True
                elif "Resource Profiling" in method_option:
                    if resource_col["name"] != "":
                        df = resource_deviations(end_timestamp["name"], resource_col["name"],
                                                 time_unit=time_unit, reference_unit=reference_unit,
                                                 min_batch_size=min_batch_size, batch_percentage=batch_percentage, grouped_by_batches=grouped_by_batches, batch_types=batch_types, url=model_option.url
                                                 )
                        success = True
                    else:
                        st.error("Please select a valid resource column!")

            if success:
                st.write(f"{df.shape[0]} deviations found")
                st.dataframe(df)

                csv = convert_df(df)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='deviations.csv',
                    mime='text/csv',
                )
