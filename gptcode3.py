#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Wheellytics by MK – Streamlit GPT-to-SQL dashboard
-------------------------------------------------
Run locally with:
$ streamlit run wheellytics_app.py
"""

import re
import os
import datetime as dt
import numpy as np
import pandas as pd
import altair as alt
import redshift_connector
import streamlit as st
import openai

# ------------------------------
# 1️⃣  Secrets & globals
# ------------------------------
REDSHIFT_CONFIG = {
    "host":     st.secrets["redshift"]["host"],
    "port":     int(st.secrets["redshift"]["port"]),
    "database": st.secrets["redshift"]["database"],
    "user":     st.secrets["redshift"]["user"],
    "password": st.secrets["redshift"]["password"],
}
OPENAI_KEY = st.secrets["openai"]["api_key"]

# ------------------------------
# 2️⃣  System prompt for GPT
# ------------------------------
SCHEMA_KNOWLEDGE = """
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

# ------------------------------
# 3️⃣  Helper functions
# ------------------------------
def ask_gpt(question: str) -> str:
    openai.api_key = OPENAI_KEY
    resp = openai.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": SCHEMA_KNOWLEDGE},
            {"role": "user", "content": f"Write SQL query to answer: {question}. "
                                        f"Label each block with -- metric_name: <metric_name> if multiple."}
        ]
    )
    return resp.choices[0].message.content.strip()

def run_query(sql: str) -> pd.DataFrame:
    conn = redshift_connector.connect(**REDSHIFT_CONFIG)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()

def split_sql_blocks(blob: str):
    parts = re.split(r"--\s*metric_name:\s*(\w+)", blob)
    blocks = {}
    for i in range(1, len(parts), 2):
        blocks[parts[i].strip()] = parts[i+1].strip()
    return blocks

# ------------------------------
# 4️⃣  Streamlit UI
# ------------------------------
st.set_page_config(page_title="🚀 Wheellytics by MK", layout="wide")
st.title("🚀 Wheellytics by MK")
st.markdown("Ask a question about trips, ratings, delays or escalations:")

question = st.text_input(
    "Your question",
    placeholder="e.g. Show month on month good ratings, delays and escalations"
)

if question:
    with st.spinner("🧠 MK is thinking and creating your stuff…"):
        sql_blob = ask_gpt(question)
        st.code(sql_blob, language="sql")

        blocks = split_sql_blocks(sql_blob)
        for metric, sql in blocks.items():
            st.subheader(f"📊 {metric.replace('_', ' ').title()}")
            try:
                df = run_query(sql)
            except Exception as e:
                st.warning(f"Query failed: {e}. Displaying dummy data.")
                df = pd.DataFrame({
                    "month": pd.date_range("2024-01-01", periods=6, freq="MS"),
                    metric:  np.random.randint(50, 200, 6)
                })

            st.dataframe(df)

            # Auto-chart any date column
            date_cols = [c for c in df.columns if any(k in c.lower() for k in ("date", "month", "week"))]
            if date_cols:
                date_col = date_cols[0]
                value_cols = [c for c in df.columns if c != date_col]
                for val in value_cols:
                    chart = (
                        alt.Chart(df)
                        .mark_line(point=True)
                        .encode(
                            x=f"{date_col}:T",
                            y=f"{val}:Q",
                            tooltip=[date_col, val]
                        )
                        .properties(
                            title=f"{val.replace('_', ' ').title()} Over Time",
                            width=700,
                            height=400
                        )
                    )
                    st.altair_chart(chart, use_container_width=True)
