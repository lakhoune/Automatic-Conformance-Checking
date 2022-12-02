import streamlit as st
import pandas as pd
import seaborn as sns
import numpy as np
import plotly.express as px
from pyinsights.main import Temp

st.header("Automatic-Conformance-Checking")
with open('style.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

col1, col2, col3, col4 = st.columns(4)
with col1:
    title = st.text_input('Api Key', 'Enter Api Key')
    st.text("Log Skeleton")
    st.text("Temporal")
    
with col2:
    
    st.button('Confirm')
    load=st.checkbox(" ")
    load1=st.checkbox("  ")
    data1={
    "Name":["Mango","Apple","Banana"],
    "Quantity":[45,38,90]
    }
    df1=pd.DataFrame(data1)
if 'load_state' not in st.session_state:
    st.session_state.load_state = False

if load1 or st.session_state.load_state:
    st.session_state.load_state= True
    st.write(df1)



with col3:
    data={
    "Name":["Mango","Apple","Banana"],
    "Quantity":[45,38,90]
    }
  
    df=pd.DataFrame(data)
#    load=st.button('Load Data')
    
if 'load_state' not in st.session_state:
    st.session_state.load_state = False

if load or st.session_state.load_state:
    st.session_state.load_state= True
    st.write(df)
    
    opt=st.radio('Plot type:',['Bar','Pie'])
with col1:
 if opt=='Bar':
        fig=px.bar(df, x='Name', y='Quantity',title='Bar Chart')
        st.plotly_chart(fig)
 elif opt=='Pie':
        fig=px.pie(df, names='Name', values='Quantity',title='Pie Chart')
        st.plotly_chart(fig)

 elif opt=='Bar':
        fig=px.bar(df1, x='Name', y='Quantity',title='Bar Chart')
        st.plotly_chart(fig)
 else:
        fig=px.pie(df1, names='Name', values='Quantity',title='Pie Chart')
        st.plotly_chart(fig)

  
  
  
