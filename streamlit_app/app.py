import os
import time
from typing import List

import pandas as pd
import streamlit as st
import psycopg2
import altair as alt

DB_HOST = os.getenv("DB_HOST", "timescaledb")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "database")
DB_USER = os.getenv("DB_USER", "username")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")

@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def fetch_symbols() -> List[str]:
    with get_conn().cursor() as cur:
        cur.execute("SELECT DISTINCT id_index FROM tick_ema_window_data ORDER BY id_index;")
        rows = cur.fetchall()
    return [r[0] for r in rows]


def fetch_ticks(symbol: str, limit: int = 5000) -> pd.DataFrame:
    with get_conn().cursor() as cur:
        cur.execute(
            """
            SELECT window_start, last, ema38, ema100
            FROM tick_ema_window_data
            WHERE id_index = %s
            ORDER BY window_start
            LIMIT %s
            """,
            (symbol, limit),
        )
        data = cur.fetchall()
    if not data:
        return pd.DataFrame(columns=["window_start", "last", "ema38", "ema100"])  
    df = pd.DataFrame(data, columns=["window_start", "last", "ema38", "ema100"]) 
    
    # Ensure timestamps are properly handled as UTC
    if not df.empty and 'window_start' in df.columns:
        # Convert to pandas datetime and ensure UTC timezone
        df['window_start'] = pd.to_datetime(df['window_start'])
        # If timestamps don't have timezone info, assume they're UTC
        if df['window_start'].dt.tz is None:
            df['window_start'] = df['window_start'].dt.tz_localize('UTC')
        # Keep as datetime objects with UTC timezone
    
    return df


st.set_page_config(page_title="Tick Dashboard", layout="wide")

st.title("Tick Dashboard")

symbols = fetch_symbols()
if not symbols:
    st.info("No symbols yet. Waiting for ingestor to populate data...")
    st.stop()

# Search bar to jump/select by id_index

def _apply_symbol_search():
    q = st.session_state.get("symbol_search", "").strip()
    if not q:
        return
    exact = [s for s in symbols if s.lower() == q.lower()]
    candidates = exact or [s for s in symbols if q.lower() in s.lower()]
    if candidates:
        st.session_state["symbol"] = candidates[0]

st.text_input("Search id_index", key="symbol_search", placeholder="e.g. A0HN5C.ETR", on_change=_apply_symbol_search)

letter = st.sidebar.select_slider("Jump 0–Z", options=list("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
opts = [s for s in symbols if s.upper().startswith(letter)] or symbols

# Ensure a current symbol exists in session state and is within current options
if "symbol" not in st.session_state:
    st.session_state["symbol"] = opts[0]
elif st.session_state["symbol"] not in opts:
    st.session_state["symbol"] = opts[0]

selected_index = opts.index(st.session_state["symbol"]) if st.session_state["symbol"] in opts else 0
st.selectbox("Select symbol", options=opts, index=selected_index, key="symbol")

placeholder = st.empty()

refresh_ms = st.sidebar.slider("Refresh interval (ms)", min_value=250, max_value=5000, value=1000, step=250)
limit_rows = st.sidebar.slider("Max rows", min_value=200, max_value=10000, value=3000, step=200)

while True:
    current_symbol = st.session_state.get("symbol", symbols[0])
    df = fetch_ticks(current_symbol, limit=limit_rows)
    if df.empty:
        placeholder.info("No data available for the selected symbol yet.")
    else:
        df = df.sort_values("window_start").reset_index(drop=True)

        # Build a layered Altair line chart
        color_scale = alt.Scale(
            domain=["last", "ema38", "ema100"],
            range=["#e74c3c", "#efab0c", "#0e41da"],
        )

        base = (
            alt.Chart(df)
            .transform_fold(["last", "ema38", "ema100"], as_=["series", "value"]) 
            .encode(
                x=alt.X("window_start:T", axis=alt.Axis(format="%H:%M", title="time (UTC)"), timeUnit="utcyearmonthdatehoursminutes"),
                y=alt.Y("value:Q", title="value"),
                color=alt.Color("series:N", scale=color_scale, legend=alt.Legend(title="series")),
                tooltip=[
                    alt.Tooltip("window_start:T", title="Time (UTC)", format="%H:%M:%S"),
                    alt.Tooltip("value:Q", title="Value", format=".4f"),
                    alt.Tooltip("series:N", title="Series")
                ]
            )
        )

        last_line = base.transform_filter(alt.datum.series == "last").mark_line(interpolate="linear", strokeWidth=1)
        ema38_line = base.transform_filter(alt.datum.series == "ema38").mark_line(interpolate="monotone", strokeWidth=1)
        ema100_line = base.transform_filter(alt.datum.series == "ema100").mark_line(interpolate="monotone", strokeWidth=1)

        chart = alt.layer(last_line, ema38_line, ema100_line).properties(height=380)

        with placeholder.container():
            st.subheader(f"{current_symbol}")
            
            # Debug: Show time range info
            if not df.empty:
                st.caption(f"Time range: {df['window_start'].min()} to {df['window_start'].max()}")
                st.caption(f"First few timestamps: {df['window_start'].head(3).tolist()}")
            
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(df.tail(20), use_container_width=True)
    time.sleep(refresh_ms / 1000.0)
