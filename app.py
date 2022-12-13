import streamlit as st
#from st_aggrid import AgGrid
# import pillow as pil
# from docutils.nodes import title
# import AgGrid as ag
#from streamlit_pagination import pagination_component
# from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode, JsCode
# from st_aggrid import AgGrid
from pyinsights import Connector
from pyinsights.temporal_profiling import TemporalProfiler
from pyinsights.organisational_profiling import ResourceProfiler
import pandas as pd
# import seaborn as sns
# import plotly.express as px

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
    return [{'name': ""}] + _model.default_activity_table.columns


def name_of_col(model):
    if model["name"] == "":
        return "None"
    else:
        return model["name"]


@st.experimental_memo
def set_datamodel(_model, endtime, resource_col):
    st.session_state.connector.datamodel = _model
    st.session_state.connector.set_parameters(end_timestamp=endtime["name"])
    st.session_state.resource_col = resource_col["name"]


@st.experimental_memo(show_spinner=False)
def temporal_deviations(endtime, resource_col, simga, deviation_cost, extended_view):
    profiler = TemporalProfiler(connector=st.session_state.connector)
    df = profiler.deviating_cases(sigma=sigma, extended_view=extended_view, deviation_cost=deviation_cost)

    return df


@st.experimental_memo(show_spinner=False)
def resource_deviations(endtime, resource_col, time_unit, reference_unit, min_batch_size
                                     , batch_percentage, grouped_by_batches, batch_types):
    if "resource_col" in st.session_state:
        profiler = ResourceProfiler(connector=st.session_state.connector,resource_column=st.session_state.resource_col)
    else:
        st.error("Not all parameters set")
        return

    df = profiler.cases_with_batches(time_unit=time_unit, reference_unit=reference_unit, min_batch_size=min_batch_size
                                     , batch_percentage=batch_percentage
                                     , grouped_by_batches=grouped_by_batches, batch_types=batch_types)

    return df


def highlight_survived(s):
    return ['background-color: green']*len(s) if s.Survived else ['background-color: red']*len(s)


def color_survived(val):
    color = 'gray' if val > 30 else 'red'
    return f'background-color: {color}'


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
    padding: 3rem 1rem 1.5rem;
    }
    div.css-12ttj6m{
    position: center;
    padding: 5rem;
    margin: 5rem;
    box-shadow: 5px 5px 5px brown;
    background-color: gray;

    }
    div.css-1629p8f {
    color: #4CAF50;
    padding-block: inherit;
    padding-top: 2rem;
    color: #4CAF50;
    margin: 3rem;

        }
    div.css-zt5igj{
    color: #4CAF5;

    }
    div.css-1wrcr25{
    background: #a7a7a7;
    }
    header.css-18ni7ap{
    background: #a7a7a7;

    }
    section.css-163ttbj{
    background: #b3cccc;

    }
    div.css-184tjsw p{
    font-size: 20px;
    font-weight: bold;
    }
    button.css-1x8cf1d{
    font-size: 20px;
    font-weight: bold;

    text-align: center;


    }


            </style>""", unsafe_allow_html=True)

st.header("Automatic Conformance Checking Insights")


if "connector" not in st.session_state:
    with st.form("login"):
        url = st.text_input("Celonis URL", celonis_url)
        api_token = st.text_input("Api token", token)
        login = st.form_submit_button("Login")

    if login:
        st.session_state.connector = Connector(api_token=api_token, url=url, key_type="USER_KEY")
        st.experimental_rerun()


if "connector" in st.session_state:
    col1, col2, _, col4 = st.columns(4)
    # with col1:
    #     st.success("Successfully logged in to celonis", icon="✅")
    with col1:
        run = st.button("Get deviations", type="primary")
    with col4:
        st.button("Logout", on_click=logout)

    with st.sidebar:
        st.subheader("Config")
        models = st.session_state.connector.celonis.datamodels
        model_option = st.selectbox("Choose Data Model", models)
        with st.form("model"):
            columns = columns(model_option, model_option.url)
            end_timestamp = st.selectbox("Input End-Timestamp Column", columns, format_func=name_of_col)
            resource_col = st.selectbox("Input resource column", columns, format_func=name_of_col)
            model_submitted = st.form_submit_button("Select Model")

        if model_submitted:
            set_datamodel(model_option, end_timestamp, resource_col)

        method_option = st.selectbox("Choose Method", ("Temporal Profiling", "Resource Profiling"))

        if method_option == "Temporal Profiling":
            sigma = st.number_input(label="Sigma", value=6)
            deviation_cost = st.selectbox("Deviation cost", [True, False])
            extended_view = st.selectbox("Extended view", [True, False])

        if method_option == "Resource Profiling":
            col1, col2 = st.columns(2)
            with col1:
                time_unit = st.selectbox("Time unit", ["SECONDS", "MINUTES", "HOURS", "DAY", "MONTH"], index=2)
                reference_unit = st.selectbox("Reference unit", ["MINUTES", "HOURS", "DAY", "MONTH", None], index=4)
                min_batch_size = st.number_input(label="Min batch size", value=2)

            with col2:
                batch_percentage = st.number_input(label="Batch percentage", value=0.1, step=0.1)
                grouped_by_batches = st.selectbox("Grouped", [True, False])
                batch_types = st.selectbox("Batch types", [True, False])
    if run:
        success = False
        st.subheader("Deviations:")
        with st.spinner("Calculating deviations"):
            if method_option == "Temporal Profiling":
                st.session_state.connector.set_parameters(end_timestamp=end_timestamp["name"])
                df = temporal_deviations(end_timestamp["name"], resource_col["name"],
                                         sigma, deviation_cost, extended_view)
                success = True
            elif method_option == "Resource Profiling":
                if resource_col["name"] != "":
                    df = resource_deviations(end_timestamp["name"], resource_col["name"],
                                             time_unit=time_unit, reference_unit=reference_unit,
                                             min_batch_size=min_batch_size
                                             , batch_percentage=batch_percentage
                                             , grouped_by_batches=grouped_by_batches, batch_types=batch_types
                                             )
                    success = True
                else:
                    st.error("Please select a valid resource column!")

            if success:
                st.write(f"{df.shape[0]} deviations found")
                st.dataframe(df.style.applymap(color_survived, subset=['deviation cost']))
#                AgGrid(df)
#                st.line_chart(df)
                csv = convert_df(df)

#                opt = st.radio('Plot type:', ['Bar', 'Pie'])
#            if opt == 'Bar':
#                fig = px.bar(df, y="waiting time", title='Bar Chart')
#                st.plotly_chart(fig)
#            else:
#               fig = px.pie(df, y="waiting time", title='Pie Chart')
#               st.plotly_chart(fig)

                st.bar_chart(data=df,  y="waiting time", use_container_width=True)
                st.pie_chart(data=df, y="waiting time", use_container_width=True)
                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='deviations.csv',
                    mime='text/csv',
                )
