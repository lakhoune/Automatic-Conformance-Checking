import streamlit as st
import pandas as pd
import seaborn as sns
import numpy as np
import plotly.express as px
st.header("Automatic-Conformance-Checking")
_data={
	"Name":["Mango","Apple","Banana"],
	"Quantity":[45,38,90]
}
df=pd.DataFrame(_data)
load=st.button('Load Data')
if 'load_state' not in st.session_state:
	st.session_state.load_state = False

if load or st.session_state.load_state:
	st.session_state.load_state= True
	st.write(df)
	opt=st.radio('Plot type:',['Bar','Pie'])
	if opt=='Bar':
		fig=px.bar(df, x='Name', y='Quantity',title='Bar Chart')
		st.plotly_chart(fig)
	else:
		fig=px.pie(df, names='Name', values='Quantity',title='Pie Chart')
		st.plotly_chart(fig)
