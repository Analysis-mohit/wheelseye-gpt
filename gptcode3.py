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
usr=st.secrets["redshift"]["user"]
pasw='st.secrets["redshift"]["password"]

galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
# credentials = ServiceAccountCredentials.from_json_keyfile_name("mohit_bq.json", scope)
credentials = ServiceAccountCredentials.from_json_keyfile_name('/Users/mohit/Downloads/mohit_kumar_bq.json', scope)

gc = gspread.authorize(credentials)


# In[2]:


# In[2]:


import redshift_connector
conn = redshift_connector.connect(
    "host": st.secrets["redshift"]["host"],
    "port": st.secrets["redshift"]["port"],
    "database": st.secrets["redshift"]["database"],
    "user": st.secrets["redshift"]["user"],
    "password": st.secrets["redshift"]["password"]
 )
read_sql = conn.cursor()


# In[3]:

# gptcode3.py

import streamlit as st
import pandas as pd
import altair as alt
import openai
import psycopg2
import os


# 🔐 Load secrets securely
REDSHIFT_CONFIG = {
    "host": st.secrets["redshift"]["host"],
    "port": st.secrets["redshift"]["port"],
    "database": st.secrets["redshift"]["database"],
    "user": st.secrets["redshift"]["user"],
    "password": st.secrets["redshift"]["password"]
}

openai.api_key = st.secrets["openai"]["api_key"]

st.set_page_config(page_title="🚀 Wheellytics by MK", layout="wide")

# Streamlit Page Config
st.title("🚀 Wheellytics by MK")
st.markdown("Ask a question about trips, ratings, delays or escalations:")

# OpenAI API Key
# GPT System Prompt
schema_knowledge = """
You are a SQL expert analyst for Wheelseye. Always query the table: ss_1_consignerservice_stats_table_pms.

Base columns:
- demand_id: Unique trip ID
- unloading_done_time: When the trip ended
- ticket_code: 'T' in it means escalation raised
- gtl_delay: 'ontime' means vehicle reached loading point on time
- sys_trnst_dly: 'ontime' means delivery was on time
- rating: 1-5 (0 = not rated)
- tat_resolve_cat: 1 = resolved in time, 0 = delayed
- type: if not in ['nan', 'NO_DAMAGE', 'None'], then damage occurred

Joined table `mp_demand_details` adds:
- region: Location name (NCR, PUNE, etc.)
- ncr_flag: Filter flag
- consigner_type: 'New consigner' or 'Repeat Consigner'

Metrics:
- Total Trips: COUNT(DISTINCT demand_id)
- Tickets Raised: COUNT(DISTINCT ticket_code)
- Tickets/Trip: tickets / total trips
- On-Time Pickup: gtl_delay = 'ontime'
- On-Time Delivery: sys_trnst_dly = 'ontime'
- Damage Trips: type NOT IN ('nan', 'NO_DAMAGE', 'None')
- Escalation Raised Trips: ticket_code IS NOT NULL
- Escalation Solved On-Time: tat_resolve_cat = 1
- Good Rated: rating = 5
- Bad Rated: rating IN (1,2,3)
- Rated Trips: rating > 0
- Promoters: rating = 5
- Passives: rating = 4
- Detractors: rating IN (1,2,3)
- NPS: Promoters - Detractors

Filters supported:
- Timeline: DAY, WEEK, MONTH (used in `date_trunc`)
- Region: ct.region
- NCR_FLAG: mp.ncr_flag
- Consigner Type: mp.consigner_type
"""

# --- GPT SQL GENERATOR ---
def ask_gpt(question):
    try:
        response = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": schema_knowledge},
                {"role": "user", "content": f"Write SQL query to answer: {question}. Label each block with -- metric_name: <metric_name> if multiple."}
            ]
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        raise RuntimeError(f"GPT error: {e}")

# --- REDSHIFT CONNECTION ---
def get_redshift_connection():
    return redshift_connector.connect(**REDSHIFT_CONFIG)

def run_query(sql):
    conn = get_redshift_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(result, columns=columns)
    finally:
        conn.close()

# --- MULTI-BLOCK SQL SPLITTER ---
def split_sql_blocks(sql_blob):
    import re
    blocks = {}
    matches = re.split(r'--\s*metric_name:\s*(\w+)', sql_blob)
    for i in range(1, len(matches), 2):
        metric = matches[i].strip()
        query = matches[i+1].strip()
        blocks[metric] = query
    return blocks


question = st.text_input("Your question", placeholder="e.g. Show month on month good ratings, delays and escalations")

if question:
    with st.spinner("🧠 MK is thinking and creating your stuff..."):
        try:
            sql_blob = ask_gpt(question)
            st.code(sql_blob, language="sql")

            sql_blocks = split_sql_blocks(sql_blob)

            for metric_name, sub_sql in sql_blocks.items():
                st.subheader(f"📊 {metric_name.replace('_', ' ').title()}")
                try:
                    df = run_query(sub_sql)
                except Exception as e:
                    st.warning(f"Query failed: {e}, using dummy data instead")
                    df = pd.DataFrame({
                        "month": pd.date_range(start="2024-01-01", periods=6, freq="MS"),
                        metric_name: np.random.randint(50, 200, size=6)
                    })

                st.dataframe(df)

                date_cols = [col for col in df.columns if 'date' in col.lower() or 'month' in col.lower() or 'week' in col.lower()]
                if date_cols:
                    time_col = date_cols[0]
                    metric_cols = [col for col in df.columns if col != time_col]
                    for metric in metric_cols:
                        chart = alt.Chart(df).mark_line(point=True).encode(
                            x=f"{time_col}:T",
                            y=f"{metric}:Q",
                            tooltip=[time_col, metric]
                        ).properties(
                            title=f"{metric.replace('_', ' ').title()} Over Time",
                            width=700,
                            height=400
                        )
                        st.altair_chart(chart, use_container_width=True)

        except Exception as e:
            st.error(f"Something went wrong: {e}")
