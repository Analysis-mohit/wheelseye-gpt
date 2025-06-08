#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime as dt
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import time as tm
from pymongo import MongoClient
import calendar
from tqdm import tqdm
import sqlalchemy
import warnings
import time
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials
import requests
import json
import google.auth
# from google.cloud import bigquery
from google.oauth2 import service_account
from functools import reduce
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, select, inspect, and_, or_
from oauth2client.service_account import ServiceAccountCredentials
usr='kumarmohit'
pasw='W1BbX99CjQYy'
galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
# credentials = ServiceAccountCredentials.from_json_keyfile_name("mohit_bq.json", scope)
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/mohit/Downloads/mohit_kumar_bq.json', scope)

gc = gspread.authorize(credentials)


# In[2]:


# In[2]:


import redshift_connector
conn = redshift_connector.connect(
    host='redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com',
    port=5439,
    database='datalake',
    user='kumarmohit',
    password='W1BbX99CjQYy'
 )
read_sql = conn.cursor()


# In[3]:


# In[2]:


import streamlit as st
import pandas as pd
import openai
import redshift_connector


openai.api_key = st.secrets["openai"]["api_key"]
# 🚀 Wheellytics by MK



import altair as alt
import psycopg2
import os

# ✅ Always FIRST in Streamlit
st.set_page_config(page_title="🚀 Wheellytics by MK", layout="wide")

# 🔐 Redshift Config (you can move to environment vars for security)
# 🔐 Load secrets

REDSHIFT_CONFIG = {
    "host": st.secrets["redshift"]["host"],
    "port": st.secrets["redshift"]["port"],
    "database": st.secrets["redshift"]["database"],
    "user": st.secrets["redshift"]["user"],
    "password": st.secrets["redshift"]["password"]
}


# 🧠 Dummy fallback data
def get_dummy_df():
    return pd.DataFrame({
        "month": pd.date_range("2023-01-01", periods=6, freq="M"),
        "trips_ended": [100, 120, 140, 160, 180, 200],
        "ontime_delivery": [90, 110, 130, 150, 160, 190],
        "percentage_ontime_delivery": [90, 91.7, 92.9, 93.75, 88.9, 95.0]
    })

# 🛠️ Run Redshift query
def run_query(sql):
    try:
        conn = psycopg2.connect(**REDSHIFT_CONFIG)
        return pd.read_sql_query(sql, conn)
    except Exception as e:
        st.warning(f"Query failed: {e}, using dummy data instead")
        return get_dummy_df()

# 🧠 Replace with actual GPT logic or hardcoded SQL
def ask_gpt(question):
    return """
        WITH monthly_trips AS (
            SELECT DATE_TRUNC('month', unloading_done_time) AS month,
                   COUNT(DISTINCT demand_id) AS trips_ended
            FROM ss_1_consignerservice_stats_table_pms
            GROUP BY month
        ),
        monthly_ontime_deliveries AS (
            SELECT DATE_TRUNC('month', unloading_done_time) AS month,
                   COUNT(DISTINCT CASE WHEN sys_trnst_dly='in_tat' THEN demand_id END) AS ontime_delivery
            FROM ss_1_consignerservice_stats_table_pms
            GROUP BY month
        )
        SELECT
            t.month,
            t.trips_ended,
            COALESCE(o.ontime_delivery, 0) AS ontime_delivery,
            COALESCE(o.ontime_delivery, 0)*100.0 / t.trips_ended AS percentage_ontime_delivery
        FROM monthly_trips t
        LEFT JOIN monthly_ontime_deliveries o ON t.month = o.month
        ORDER BY t.month DESC
        LIMIT 6;
    """

# 💬 Streamlit UI
st.title("🚛 Wheellytics GPT")
question = st.text_input("Ask your logistics question 👇", placeholder="e.g. Show me monthly on-time delivery trend")

if question:
    with st.spinner("🧠 MK is thinking and creating your stuff..."):
        try:
            sql = ask_gpt(question)
            st.code(sql, language="sql")
            df = run_query(sql)

            if df.empty:
                st.warning("No data found for this query.")
            else:
                st.success("✨ Boom! Here's your answer:")
                st.dataframe(df)

                # ⏱️ Try visualizing time-series
                date_cols = [col for col in df.columns if 'date' in col.lower() or 'month' in col.lower()]
                if date_cols:
                    time_col = date_cols[0]
                    metric_cols = [col for col in df.columns if col != time_col]

                    for metric in metric_cols:
                        chart = alt.Chart(df).mark_line(point=True).encode(
                            x=f'{time_col}:T',
                            y=f'{metric}:Q',
                            tooltip=[time_col, metric]
                        ).properties(
                            title=f"{metric} Over Time",
                            width=700,
                            height=400
                        )
                        st.altair_chart(chart, use_container_width=True)

        except Exception as e:
            st.error(f"❌ Something went wrong: {e}")
