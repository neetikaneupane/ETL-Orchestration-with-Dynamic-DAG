"""
self-healing-etl Dashboard
==========================
Streamlit dashboard for monitoring pipeline health, SLA compliance,
and run metadata in real-time.
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

st.set_page_config(
    page_title="Self-Healing ETL Dashboard",
    page_icon="",
    layout="wide",
)

st.title(" Self-Healing ETL Pipeline Dashboard")


def get_db_connection():
    from airflow.hooks.base import BaseHook
    import psycopg2

    conn_details = BaseHook.get_connection("pipeline_config_db")
    return psycopg2.connect(
        host=conn_details.host,
        port=conn_details.port or 5432,
        dbname=conn_details.schema,
        user=conn_details.login,
        password=conn_details.password,
    )


@st.cache_data(ttl=60)
def load_health_summary():
    conn = get_db_connection()
    query = """
        SELECT pipeline_id, summary_date, total_runs, successful_runs,
               failed_runs, avg_duration_seconds, total_rows_processed
        FROM pipeline_config.pipeline_health_summary
        ORDER BY summary_date DESC, pipeline_id
        LIMIT 100
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_recent_runs():
    conn = get_db_connection()
    query = """
        SELECT pipeline_id, dag_run_id, task_id, execution_date,
               rows_processed, duration_seconds, status, error_type, retry_count
        FROM pipeline_config.task_run_metadata
        WHERE execution_date >= NOW() - INTERVAL '7 days'
        ORDER BY execution_date DESC
        LIMIT 500
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_sla_breaches():
    conn = get_db_connection()
    query = """
        SELECT pipeline_id, execution_date, breach_minutes, alert_sent, created_at
        FROM pipeline_config.sla_breach_log
        ORDER BY created_at DESC
        LIMIT 50
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


@st.cache_data(ttl=60)
def load_pipeline_definitions():
    conn = get_db_connection()
    query = """
        SELECT pipeline_id, pipeline_name, schedule_interval, source_type,
               dest_type, sla_minutes, is_active, owner
        FROM pipeline_config.pipeline_definitions
        ORDER BY pipeline_id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ── Sidebar ──
st.sidebar.header("Filters")
time_range = st.sidebar.selectbox(
    "Time Range", ["Last 24 hours", "Last 7 days", "Last 30 days"], index=1
)


# ── Row 1: Key Metrics ──
try:
    health_df = load_health_summary()
    runs_df = load_recent_runs()

    if not runs_df.empty:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            total_runs = len(runs_df)
            st.metric("Total Runs (7d)", total_runs)
        with col2:
            success_count = len(runs_df[runs_df["status"] == "success"])
            success_rate = round(success_count / total_runs * 100, 1) if total_runs else 0
            st.metric("Success Rate", f"{success_rate}%")
        with col3:
            failed = len(runs_df[runs_df["status"] == "failed"])
            st.metric("Failed Runs", failed)
        with col4:
            avg_duration = runs_df["duration_seconds"].mean()
            st.metric("Avg Duration", f"{avg_duration:.1f}s" if pd.notna(avg_duration) else "N/A")
    else:
        st.info("No run data available yet. Start the Airflow stack to populate data.")

except Exception as e:
    st.warning(f"Could not load metrics: {e}. Is the Airflow stack running?")


# ── Row 2: Pipeline Definitions & Health ──
col1, col2 = st.columns(2)

with col1:
    st.subheader(" Pipeline Definitions")
    try:
        defs_df = load_pipeline_definitions()
        if not defs_df.empty:
            st.dataframe(defs_df, use_container_width=True, hide_index=True)
        else:
            st.info("No pipeline definitions found.")
    except Exception as e:
        st.warning(f"Could not load definitions: {e}")

with col2:
    st.subheader(" Pipeline Health Summary")
    try:
        if not health_df.empty:
            fig = px.bar(
                health_df,
                x="summary_date",
                y=["successful_runs", "failed_runs"],
                color_discrete_map={"successful_runs": "#2ecc71", "failed_runs": "#e74c3c"},
                title="Daily Run Status",
                labels={"value": "Runs", "variable": "Status"},
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No health summary data yet.")
    except Exception as e:
        st.warning(f"Could not load health data: {e}")


# ── Row 3: Recent Runs & SLA Breaches ──
col1, col2 = st.columns(2)

with col1:
    st.subheader(" Recent Task Runs")
    try:
        if not runs_df.empty:
            display_cols = [
                "pipeline_id", "task_id", "status", "rows_processed",
                "duration_seconds", "error_type", "retry_count", "execution_date",
            ]
            st.dataframe(
                runs_df[display_cols].head(20),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No recent task runs found.")
    except Exception as e:
        st.warning(f"Could not load runs: {e}")

with col2:
    st.subheader(" SLA Breaches")
    try:
        sla_df = load_sla_breaches()
        if not sla_df.empty:
            st.dataframe(sla_df, use_container_width=True, hide_index=True)
        else:
            st.info("No SLA breaches recorded.")
    except Exception as e:
        st.warning(f"Could not load SLA data: {e}")


# ── Row 4: Duration Trends ──
st.subheader(" Pipeline Duration Trends")
try:
    if not runs_df.empty and "duration_seconds" in runs_df.columns:
        runs_df["execution_date"] = pd.to_datetime(runs_df["execution_date"])
        dur_fig = px.line(
            runs_df[runs_df["status"] == "success"],
            x="execution_date",
            y="duration_seconds",
            color="pipeline_id",
            title="Task Duration Over Time",
            labels={"duration_seconds": "Duration (s)", "execution_date": "Execution Date"},
        )
        st.plotly_chart(dur_fig, use_container_width=True)
    else:
        st.info("Insufficient data for duration trends.")
except Exception as e:
    st.warning(f"Could not render duration trends: {e}")


st.caption("ℹ️ Data refreshes every 60 seconds. Ensure the Airflow stack is running and pipelines have executed.")
