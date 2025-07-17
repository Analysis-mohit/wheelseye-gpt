#!/usr/bin/env python
# coding: utf-8

# In[5]:


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




import streamlit as st
import openai
import pandas as pd
import numpy as np
import altair as alt
import re
import redshift_connector
import io   
import hashlib





# --- SETUP ---
st.set_page_config(page_title="🚀 Wheellytics AI Agent", layout="wide")
st.title("🚀 Wheellytics AI Agent")
st.markdown("Ask a question about placements,take rate, trips, ratings, delays or escalations:")


# --- CONFIGURE YOUR REDSHIFT CONNECTION HERE ---
REDSHIFT_CONFIG = {
    "host": "redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com",
    "database": "datalake",
    "user": "kumarmohit",
    "password": "W1BbX99CjQYy",
    "port": 5439,
}

# --- OPENAI API KEY ---
openai.api_key="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"

# --- SCHEMA AND GPT PROMPT ---

schema_knowledge = """

You are a top-tier SQL expert working with Redshift on the table analytics.hackathon_demand_view if anyone ask you with filteration of two parameters
then use the and condition in where statement. 
For example:
if anyone ask for ncr consigners then : ncr_flag = 'NCR'
if anyone ask for ncr hp consigners then : ncr_flag = 'NCR' and consigner_type = 'HP'
if anyone ask for retention consigners then : sales_subs_team = 'RETENTION'
if anyone ask for ncr retention consigners then : ncr_flag = 'NCR' and sales_subs_team = 'RETENTION'
if anyone ask for state level bifurcation then use column state to give the result as per required metric
if anyone ask for vehicle_type level bifurcation then use column vehicle_type to give the result as per required metric

# Schema knowledge as a dictionary in Python

schema_knowledge = {
    'a.demand_date': {
        'datatype': 'timestamp',
        'description': 'Timestamp when demand was created'
    },
    'a.consigner_user_code': {
        'datatype': 'object',
        'description': 'Identifier for the consigner user who created the demand'
    },
    'a.consigner_type': {
        'datatype': 'object',
        'description': 'Likely category of consigner (e.g., HP = Large Player with > 3 potential/month)'
    },
    'a.demand_id': {
        'datatype': 'int64',
        'description': 'Unique ID for the demand'
    },
    'a.odvt': {
        'datatype': 'object',
        'description': 'ODVT is origin-destination-vehicle-type combination as per the consigner requirement for truck booking'
    },
    'b.baserate_confidence': {
        'datatype': 'object',
        'description': 'The confidence of price shown to consigner for truck booking on app so that he/she can confirm the demand'
    },
    'a.vehicle_type': {
        'datatype': 'object',
        'description': 'Type of vehicle'
    },
    'a.ptl_demands': {
        'datatype': 'int64',
        'description': 'Part Truck Load demand indicator'
    },
    'a.special_req': {
        'datatype': 'int64',
        'description': 'Flag for special requirements'
    },
    'a.ncr_flag': {
        'datatype': 'object',
        'description': 'ncr if ncr_flag = 'NCR' else 'Non NCR' used for identification of region'
    },
    'a.sales_team': {
        'datatype': 'object',
        'description': 'Sales team category'
    },
    'a.sales_sub_team': {
        'datatype': 'object',
        'description': 'Sub-team under sales'
    },
    'a.state': {
        'datatype': 'object',
        'description': 'State of consigner/demand'
    },
    'a.right_demand': {
        'datatype': 'int64',
        'description': 'Flag indicating valid demand'
    },
    'a.dr_flag': {
        'datatype': 'int64',
        'description': 'Stage where the demand is confirmed by the consignor; 
                        major asked question is demand to DR percentage'
    },
    'a.demand_placed': {
    'datatype': 'object',
    description: 'Required filter: demand_placed = 'FULFILLED''
                 'If asked NCR trip count then ncr_flag = 'NCR' AND demand_placed = 'FULFILLED''
                'If asked trip count then demand_placed = 'FULFILLED''
                'If asked trip count for HP then demand_placed = 'FULFILLED' and consigner_type = 'HP''
                'If asked trip count for NCR, HP then demand_placed = 'FULFILLED' and consigner_type = 'HP' AND ncr_flag = 'NCR''
    },
    'a.placement_type': {
        'datatype': 'object',
        'description': 'generaly asked to calculate auto placement that is placement_type = 'Automation' and demand_placed = 'FULFILLED''
                        'generaly asked to calculate auto placement percntage that is placement_type = 'Automation' and demand_placed = 'FULFILLED' divided by total demands placed where demand_placed = 'FULFILLED' Example total demand placed via automation is 5 and total demand placed is 8 then automation placement percentage is 5/8'
                        'major asked question is DR to placement percentage which is basically placement/dr ratio'
    },
    'a.consigner_fare': {
        'datatype': 'int64',
        'description': 'Fare quoted by the consigner, used for calculating GMV (sum(a.consigner_fare)/1,000,000) only for placed demand = 'FULFILLED''
    },
    'a.supplyfare': {
        'datatype': 'float64',
        'description': 'Fare at which fleet operator agreed to give the truck against the demand'
    },
    'a.trip_pnl': {
        'datatype': 'float64',
        'description': 'Profit or loss in the trip, used for calculating revenue (sum(a.trip_pnl)/1,000,000) only for placed demand = 'FULFILLED''
                        'If asked take rate then sum(trip_pnl) where demand_placed = 'FULFILLED'/sum(consigner_fare) where demand_placed = 'FULFILLED''
    },
    'a.operator_code': {
        'datatype': 'object',
        'description': 'Code for fleet operator (if available)'
    },
    'a.operator_type': {
        'datatype': 'object',
        'description': 'Type/category of fleet operator'
    },
    'a.cancelled_consignment': {
        'datatype': 'int64',
        'description': 'Flag if the demand was cancelled or not'
    },
    'a.fo_quoted': {
        'datatype': 'int64',
        'description': 'Count of operators quoted in the demand'
    },
    'a.trip_rating': {
        'datatype': 'object',
        'description': 'used when asked about rating whether the feedback given by consigner was god or bad'
    },
    'a.gtl_delay': {
        'datatype': 'object',
        'description': 'gtl_delay = ontime and demand_placed = 'FULFILLED' then the gtl was on time'
    },
    'a.sys_trnst_dly': {
        'datatype': 'object',
        'description': 'sys_trnst_dly = in_tat and demand_placed = 'FULFILLED' then the transit was on time or within the tat'
    },
    'a.total_tickets': {
        'datatype': 'float64',
        'description': 'Count of support tickets raised against the trip'
    }
}

# Example of accessing the schema knowledge
column = 'a.demand_date'
print(f"Column: {column}")
print(f"Data Type: {schema_knowledge[column]['datatype']}")
print(f"Description: {schema_knowledge[column]['description']}")


select * from analytics.hackathon_demand_view

-- show the demand funnel for month on month 

SELECT 
    DATE_TRUNC('month', a.demand_date) AS month,
    COUNT(a.demand_id) AS demand_count,
    SUM(a.right_demand) AS right_demand_count,
    SUM(a.dr_flag) AS dr_count,
    COUNT(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.demand_id END) AS trip_count
FROM 
    analytics.hackathon_demand_view a
GROUP BY 
    month
ORDER BY 
    month; 

if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- GMV, REVENUE, TAKE RATE 

SELECT 
    DATE_TRUNC('month', a.demand_date) AS month,
    COUNT(a.demand_id) AS demand_count,
    SUM(a.right_demand) AS right_demand_count,
    SUM(a.dr_flag) AS dr_count,
    COUNT(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.demand_id END) AS trip_count,
    SUM(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.consigner_fare END)/1000000||' m' as GMV,
    SUM(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.consigner_fare END)/1000000||' m' as revenue,
    round(SUM(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.consigner_fare END)*1.0/(SUM(CASE WHEN a.demand_placed = 'FULFILLED' THEN a.consigner_fare END)),1)||' %' as TAKE_RATE
FROM 
    analytics.hackathon_demand_view a
GROUP BY 
    month
ORDER BY 
    month; 

major asked question :
what is our pnl ?
what is take rate ?
what is our gmv ?
what is our revenue ?
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- when asked average trip cost , average freight per trip, median freight, median of trip cost

SELECT 
avg(consigner_fare) as avg_tripfare
FROM 
    analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'


-- when asked what is the % of bad rated trips, what is the % of good rated trips, what is the % of rated trips,what is average customer rating
,what is average customer feedback,in how many trips customer gives trip feedback,in how many trips customer gives rating

select 
count(case when demand_placed = 'FULFILLED' and trip_rating is not null then demand_id else null end) as trips_rated,
count(case when demand_placed = 'FULFILLED' and trip_rating = good then demand_id else null end) as trips_good_rated,
count(case when demand_placed = 'FULFILLED' and trip_rating = bad then demand_id else null end) as trips_bad_rated
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- when asked what is the % of bad rated trips, what is the % of good rated trips, what is the % of rated trips,what is average customer rating
,what is average customer feedback,in how many trips customer gives trip feedback,in how many trips customer gives rating

select 
count(case when demand_placed = 'FULFILLED' and gtl_delay = 'ontime' then demand_id else null end) as ontime_gtl,
count(case when demand_placed = 'FULFILLED' and gtl_delay = 'delay_2hr' then demand_id else null end) as gtl_delay_2hr,
count(case when demand_placed = 'FULFILLED' and gtl_delay = 'delay_2hr_to_4hr' then demand_id else null end) as gtl_delay_2hr_to_4hr,
count(case when demand_placed = 'FULFILLED' and gtl_delay = 'delay_4hours+' then demand_id else null end) as gtl_delay_4hours+
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'


-- case when asked about consigner level data, unique customers, demands per consignor

select 
count(distinct consigner_user_code) as unique_consigner_with_demand,
count(demand_id)*1.0/unique_consigner_with_demand as avg_demand_per_consigner,
count(distinct case when demand_placed = 'FULFILLED' then consigner_user_code else null end) as unique_consigner_with_placement,
count(distinct case when demand_placed = 'FULFILLED' then consigner_user_code else null end)*1.0/unique_consigner_with_demand as unique_consigner_with_placement_percent,
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- case when asked about operator level data, unique fo, demands per fo

select 
count(distinct case when demand_placed = 'FULFILLED' then operator_code else null end) as unique_fo_with_placement,
count(distinct case when demand_placed = 'FULFILLED' then operator_code else null end)*1.0/count(case when demand_placed = 'FULFILLED' then demand_id else null end) as placement_per_fo,
count(distinct case when demand_placed = 'FULFILLED' and operator_type = 'SVO' then operator_code else null end) as svo_fo_with_placement,
count(distinct case when demand_placed = 'FULFILLED' and operator_type = 'SFO' then operator_code else null end) as sfo_fo_with_placement,
count(distinct case when demand_placed = 'FULFILLED' and operator_type = 'MFO' then operator_code else null end) as mfo_fo_with_placement,
count(distinct case when demand_placed = 'FULFILLED' and operator_type = 'LFO' then operator_code else null end) as lfo_fo_with_placement
from analytics.hackathon_demand_view a;


also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'
if asked fo bifurcation for placement then give all the result svo_fo_with_placement, sfo_fo_with_placement, mfo_fo_with_placement,lfo_fo_with_placement

-- when asked what is ontime transit, what is ontime delivery % 

select 
count(case when demand_placed = 'FULFILLED' and sys_trnst_dly = 'in_tat' then demand_id else null end) as in_tat_delivery
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- when asked how many tickets were created, what is the ratio of tickets per trip

select 
sum(case when demand_placed = 'FULFILLED' then total_tickets else null end) as total_tickets,
total_tickets/count(case when demand_placed = 'FULFILLED' then demand_id else null end) as tickets_per_trip
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- when asked how many trips are cancelled, what is trend of backouts ,what is trend of operator cancellations

select 
count(case when cancelled_consignment > 0 then demand_id else null end) as trip_cancelled,
count(case when cancelled_consignment > 0 and demand_placed = 'EXPIRED' then demand_id else null end) as backout
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'

-- when asked high confidence demand, low confidence demand

select 
count(case when baserate_confidence = 'High' then demand_id else null end) as high_confidence_demand,
count(case when baserate_confidence = 'Low' then demand_id else null end) as low_confidence_demand
from analytics.hackathon_demand_view a;

also we can use the above queryies to mix this input if asked additionally 
if asked for hp consigners add filter where consigner_type = 'HP'
if asked for lp consigners add filter where consigner_type = 'LP'
if asked for vehicle_type cut then use vehicle_type column
if asked for any state cut then state like 'asked state'


When asked a question, write a SQL query using these templates or combine their logic accordingly.
"""


def extract_sql(text):
    """
    Extract SQL query starting from the first SELECT to last semicolon.
    Fallback to full text if no match.
    """
    match = re.search(r"(SELECT.+;)", text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.strip()

def ask_gpt(question, model_choice="gpt-4o-mini"):
    try:
        response = openai.chat.completions.create(
            model=model_choice,
            messages=[
                {"role": "system", "content": schema_knowledge},
                {"role": "user", "content": f"Write a single SQL query to answer: {question}"}
            ]
        )
        raw_sql = response.choices[0].message.content.strip()
        sql = extract_sql(raw_sql)
        return sql
    except Exception as e:
        st.warning(f"GPT {model_choice} failed: {e}")
        return None

def get_redshift_connection():
    return redshift_connector.connect(**REDSHIFT_CONFIG)

def run_query(sql):
    conn = get_redshift_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(result, columns=columns)
    finally:
        conn.close()

# Simple in-memory cache dictionary
query_cache = {}

def cache_key(question, model):
    # Hash question+model for cache key
    key = f"{model}:{question}"
    return hashlib.sha256(key.encode()).hexdigest()

def get_cached_query(question, model):
    key = cache_key(question, model)
    return query_cache.get(key)

def set_cached_query(question, model, sql):
    key = cache_key(question, model)
    query_cache[key] = sql

# === STREAMLIT UI ===


question = st.text_input("Your question", placeholder="e.g. Show month on month good ratings, delays and escalations")

if question:
    with st.spinner("🧠 Wheelsye AI is thinking and creating your SQL..."):
        # Try GPT-4 first, then fallback to GPT-3.5
        sql = get_cached_query(question, "gpt-4o-mini")
        if not sql:
            sql = ask_gpt(question, model_choice="gpt-4o-mini")
            if sql:
                set_cached_query(question, "gpt-4o-mini", sql)
        
        if not sql:
            sql = get_cached_query(question, "gpt-3.5-turbo")
            if not sql:
                sql = ask_gpt(question, model_choice="gpt-3.5-turbo")
                if sql:
                    set_cached_query(question, "gpt-3.5-turbo", sql)

        if sql:
            st.code(sql, language="sql")
            try:
                df = run_query(sql)
                if df.empty:
                    st.warning("Query executed successfully but returned no data.")
                else:
                    st.dataframe(df)

                    # CSV Download button
                    csv_data = df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_data,
                        file_name="query_results.csv",
                        mime="text/csv"
                    )

                    # Plot line chart if date/month/week column found
                    date_cols = [c for c in df.columns if 'date' in c.lower() or 'month' in c.lower() or 'week' in c.lower()]
                    if date_cols:
                        time_col = date_cols[0]
                        metric_cols = [c for c in df.columns if c != time_col]
                        for metric in metric_cols:
                            chart = alt.Chart(df).mark_line(point=True).encode(
                                x=alt.X(f"{time_col}:T"),
                                y=alt.Y(f"{metric}:Q"),
                                tooltip=[time_col, metric]
                            ).properties(
                                width=700,
                                height=400,
                                title=f"{metric.replace('_', ' ').title()} over time"
                            )
                            st.altair_chart(chart, use_container_width=True)
            except Exception as e:
                st.error(f"Failed to execute query: {e}")
        else:
            st.error("Sorry, GPT failed to generate a valid SQL query for your question.")


# In[ ]:





# In[ ]:




