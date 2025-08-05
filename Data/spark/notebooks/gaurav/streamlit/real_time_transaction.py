# from streamlit_autorefresh import st_autorefresh

import streamlit as st
import pandas as pd
import mysql.connector
# from streamlit_autorefresh import st_autorefresh


# # auto‑refresh every 5 seconds
# _ = st_autorefresh(interval=5_000, key="real_time")

# st.header("Real time streaming")

# database = "rp"
# table = "rp_agg_15min_transactions"
# # import streamlit as st
# # st.write("Streamlit version:", st.__version__)

# @st.cache_data(ttl=5)  # seconds
# def load_data():
#     conn = mysql.connector.connect(
#         host="mysql", user="root", password="root", database="rp")
#     df = pd.read_sql(
#         f"""SELECT * FROM {database}.{table}
#         ORDER BY window_start DESC LIMIT 10""",
#         conn,
#         parse_dates=["window_start"]
#     )
#     conn.close()
#     return df
# data = load_data()
# data
# st.line_chart(data, x="window_start", y="total_amount")
# # st.experimental_rerun() 

import time
import streamlit as st
import pandas as pd
import mysql.connector

st.header("Real Time Streaming")

st.markdown("#### 15 minutes Aggregation of Transaction Amount")

placeholder = st.empty()
palceholder_df = st.empty()

while True:
    # 1) Load and sort your data
    conn = mysql.connector.connect(
        host="mysql", user="root", password="root", database="rp"
    )
    df = pd.read_sql(
        "SELECT * "
        "FROM rp.rp_agg_15min_transactions "
        "ORDER BY window_start DESC LIMIT 10",
        conn,
        parse_dates=["window_start"]
    )
    conn.close()
    palceholder_df.dataframe(df)
    placeholder.line_chart(df.iloc[:5,:], x="window_start", y="total_amount",x_label="Time", y_label="Transaction Amount")

    time.sleep(5)