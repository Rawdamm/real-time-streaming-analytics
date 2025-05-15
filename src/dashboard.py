import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import altair as alt
import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st
from pymongo import MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

MONGO_URI   = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
PG_HOST     = os.getenv("PG_HOST", "postgresql")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "transactions")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")

MERCHANT_CATEGORIES = [
    "grocery", "electronics", "restaurant", "gas_station",
    "entertainment", "healthcare", "utilities", "travel", "retail", "other",
]

TIME_RANGE_MINUTES = {"5 min": 5, "15 min": 15, "30 min": 30, "1 hour": 60}


@st.cache_resource
def get_mongo_collection():
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")
    logger.info("[DASHBOARD] MongoDB connected at %s", MONGO_URI)
    return client["transactions"]["raw_events"]


@st.cache_resource
def get_pg_conn():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD, connect_timeout=5,
    )
    logger.info("[DASHBOARD] PostgreSQL connected at %s:%d", PG_HOST, PG_PORT)
    return conn


def _pg_conn():
    """Return a live PostgreSQL connection, transparently reconnecting if the
    cached one has been dropped (e.g. the database container restarted)."""
    conn = get_pg_conn()
    if conn.closed:
        get_pg_conn.clear()
        conn = get_pg_conn()
    return conn


@st.cache_data(ttl=5)
def query_total_transactions(categories: tuple) -> int:
    return get_mongo_collection().count_documents(_category_filter(categories))


@st.cache_data(ttl=5)
def query_total_anomalies(categories: tuple) -> int:
    filt = {**_category_filter(categories), "anomaly_flag": True}
    return get_mongo_collection().count_documents(filt)


@st.cache_data(ttl=5)
def query_latest_metrics() -> dict:
    conn = _pg_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM metrics ORDER BY aggregation_time DESC LIMIT 1")
            row = cur.fetchone()
    except psycopg2.Error as exc:
        logger.warning("[DASHBOARD] latest-metrics query failed: %s", exc)
        get_pg_conn.clear()
        row = None
    return dict(row) if row else {}


@st.cache_data(ttl=5)
def query_volume_chart(minutes: int) -> pd.DataFrame:
    conn = _pg_conn()
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    sql = """
        SELECT aggregation_time, total_transactions, total_anomalies, anomaly_rate
        FROM metrics
        WHERE aggregation_time >= %s
        ORDER BY aggregation_time
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, (cutoff,))
            rows = cur.fetchall()
    except psycopg2.Error as exc:
        logger.warning("[DASHBOARD] volume-chart query failed: %s", exc)
        get_pg_conn.clear()
        rows = []
    return pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["aggregation_time", "total_transactions", "total_anomalies", "anomaly_rate"]
    )


@st.cache_data(ttl=5)
def query_category_breakdown(categories: tuple) -> pd.DataFrame:
    """Aggregate transaction counts and anomaly counts per merchant category."""
    col = get_mongo_collection()
    filt = _category_filter(categories)
    pipeline = [
        {"$match": filt},
        {"$group": {
            "_id": "$merchant_category",
            "total": {"$sum": 1},
            "anomalies": {"$sum": {"$cond": ["$anomaly_flag", 1, 0]}},
        }},
        {"$sort": {"total": -1}},
    ]
    try:
        results = list(col.aggregate(pipeline))
    except PyMongoError:
        results = []
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).rename(columns={"_id": "category"})


@st.cache_data(ttl=5)
def query_recent_anomalies(categories: tuple, limit: int = 10) -> pd.DataFrame:
    col = get_mongo_collection()
    filt = {**_category_filter(categories), "anomaly_flag": True}
    cursor = col.find(filt, {"_id": 0}).sort("timestamp", -1).limit(limit)
    docs = list(cursor)
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["transaction_id"] = df["transaction_id"].str[:8]
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%H:%M:%S")
    df["amount"] = df["amount"].apply(lambda x: f"${x:,.2f}")
    df["z_score"] = df["z_score"].apply(lambda x: f"{x:.2f}" if x is not None else "—")
    return df[["transaction_id", "timestamp", "amount", "card_id", "z_score", "merchant_category"]]


@st.cache_data(ttl=5)
def query_anomaly_stats(categories: tuple) -> dict:
    col = get_mongo_collection()
    filt = {**_category_filter(categories), "anomaly_flag": True}

    pipeline_z = [
        {"$match": {**filt, "z_score": {"$ne": None}}},
        {"$group": {"_id": None, "avg_z": {"$avg": "$z_score"}, "max_z": {"$max": "$z_score"}}},
    ]
    pipeline_cat = [
        {"$match": filt},
        {"$group": {"_id": "$merchant_category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    pipeline_card = [
        {"$match": filt},
        {"$group": {"_id": "$card_id", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1},
    ]

    try:
        z_stats   = list(col.aggregate(pipeline_z))
        cat_stats = list(col.aggregate(pipeline_cat))
        card_stats = list(col.aggregate(pipeline_card))
    except PyMongoError:
        z_stats, cat_stats, card_stats = [], [], []

    result: dict = {}
    if z_stats:
        result["avg_z"] = round(z_stats[0]["avg_z"], 2)
        result["max_z"] = round(z_stats[0]["max_z"], 2)
    if cat_stats:
        result["top_category"]    = f"{cat_stats[0]['_id']} ({cat_stats[0]['count']})"
        result["bottom_category"] = f"{cat_stats[-1]['_id']} ({cat_stats[-1]['count']})"
    if card_stats:
        result["top_card"]       = card_stats[0]["_id"]
        result["top_card_count"] = card_stats[0]["count"]
    return result


def _category_filter(categories: tuple) -> dict:
    if categories and set(categories) != set(MERCHANT_CATEGORIES):
        return {"merchant_category": {"$in": list(categories)}}
    return {}


def _highlight_high_z(row: pd.Series) -> list[str]:
    try:
        z = float(row["z_score"])
        color = "background-color: #ff4b4b; color: white" if z > 5 else ""
    except (ValueError, TypeError):
        color = ""
    return [color] * len(row)


def render_metric_cards(categories: tuple, latest: dict) -> None:
    total        = query_total_transactions(categories)
    anomalies    = query_total_anomalies(categories)
    anomaly_rate = latest.get("anomaly_rate", 0.0)
    avg_amount   = latest.get("average_amount", 0.0)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Transactions", f"{total:,}")
    col2.metric("Anomalies Detected", f"{anomalies:,}")
    col3.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")
    col4.metric("Avg Transaction Amount", f"${avg_amount:.2f}")


def render_volume_chart(minutes: int) -> None:
    st.subheader(f"Transaction Volume — Last {minutes} Minutes")
    df = query_volume_chart(minutes)

    if df.empty:
        st.info("No metrics data yet — waiting for the first 10-second window to complete.")
        return

    df["aggregation_time"] = pd.to_datetime(df["aggregation_time"], utc=True)
    mean_vol = df["total_transactions"].mean()

    base = alt.Chart(df).encode(x=alt.X("aggregation_time:T", title="Time"))

    bars = base.mark_bar(color="#4c9be8", opacity=0.7).encode(
        y=alt.Y("total_transactions:Q", title="Transactions / Window"),
        tooltip=["aggregation_time:T", "total_transactions:Q", "total_anomalies:Q"],
    )
    anomaly_points = (
        base.transform_filter(alt.datum.total_anomalies > 0)
        .mark_point(color="#ff4b4b", size=80, filled=True)
        .encode(
            y=alt.Y("total_transactions:Q"),
            tooltip=["aggregation_time:T", "total_anomalies:Q", "anomaly_rate:Q"],
        )
    )
    mean_rule = alt.Chart(pd.DataFrame({"mean": [mean_vol]})).mark_rule(
        color="orange", strokeDash=[6, 3]
    ).encode(y="mean:Q")

    chart = (bars + anomaly_points + mean_rule).properties(height=280).interactive()
    st.altair_chart(chart, use_container_width=True)
    st.caption("Red dots = windows with at least one anomaly | Orange dashed = mean volume")


def render_category_chart(categories: tuple) -> None:
    st.subheader("Volume by Merchant Category")
    df = query_category_breakdown(categories)

    if df.empty:
        st.info("No data yet.")
        return

    bars = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X("total:Q", title="Transactions"),
            y=alt.Y("category:N", sort="-x", title=""),
            color=alt.Color(
                "anomalies:Q",
                scale=alt.Scale(scheme="orangered"),
                title="Anomalies",
            ),
            tooltip=[
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("total:Q", title="Total"),
                alt.Tooltip("anomalies:Q", title="Anomalies"),
            ],
        )
        .properties(height=300)
    )
    st.altair_chart(bars, use_container_width=True)
    st.caption("Color intensity = anomaly count in that category")


def render_anomalies_table(categories: tuple) -> None:
    st.subheader("Recent Anomalies — Top 10")
    df = query_recent_anomalies(categories)

    if df.empty:
        st.info("No anomalies detected yet. Every 50th transaction is injected as an anomaly.")
        return

    styled = df.style.apply(_highlight_high_z, axis=1)
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption("Red rows: z-score > 5 (extreme outliers)")


def render_stats_panel(categories: tuple) -> None:
    st.subheader("Anomaly Statistics")
    stats = query_anomaly_stats(categories)

    if not stats:
        st.info("No anomaly statistics available yet.")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Avg Z-Score (anomalies)", stats.get("avg_z", "—"))
        st.metric("Highest Z-Score Seen", stats.get("max_z", "—"))
    with col2:
        st.metric("Top Anomaly Category", stats.get("top_category", "—"))
        st.metric("Fewest Anomaly Category", stats.get("bottom_category", "—"))
    with col3:
        st.metric("Card with Most Anomalies", stats.get("top_card", "—"))
        st.metric("Anomaly Count (that card)", stats.get("top_card_count", "—"))


def render_sidebar() -> tuple[int, int, tuple, bool]:
    with st.sidebar:
        st.header("Controls")

        refresh_rate = st.selectbox(
            "Refresh Rate", options=[1, 2, 5, 10], index=1,
            format_func=lambda x: f"{x}s",
        )
        time_range_label = st.selectbox(
            "Time Range", options=list(TIME_RANGE_MINUTES.keys()), index=2
        )
        time_range = TIME_RANGE_MINUTES[time_range_label]

        categories = tuple(sorted(st.multiselect(
            "Filter by Merchant Category",
            options=MERCHANT_CATEGORIES,
            default=MERCHANT_CATEGORIES,
        )))
        if set(categories) != set(MERCHANT_CATEGORIES):
            logger.info("[DASHBOARD] Category filter: %s", list(categories))

        st.divider()
        clear_clicked = st.button("Clear Anomalies", type="secondary")

    return refresh_rate, time_range, categories, clear_clicked


def handle_clear(categories: tuple) -> None:
    """Render the delete-anomalies confirmation dialog.

    Driven entirely by ``st.session_state.confirm_clear`` so it survives the
    dashboard's auto-refresh reruns. The caller pauses auto-refresh while this
    flag is set, keeping the Yes/Cancel buttons clickable.
    """
    st.warning("This will delete all anomaly records from MongoDB. Are you sure?")
    c1, c2, _ = st.columns([1, 1, 6])
    if c1.button("Yes, clear"):
        try:
            col = get_mongo_collection()
            filt = {**_category_filter(categories), "anomaly_flag": True}
            result = col.delete_many(filt)
            st.success(f"Deleted {result.deleted_count} anomaly records.")
            logger.info("[DASHBOARD] Cleared %d anomaly records", result.deleted_count)
        except PyMongoError as exc:
            st.error(f"Clear failed: {exc}")
        st.session_state.confirm_clear = False
        query_total_anomalies.clear()
        query_recent_anomalies.clear()
    if c2.button("Cancel"):
        st.session_state.confirm_clear = False
        st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="Real-Time Anomaly Detection Dashboard",
        page_icon="🚨",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    logger.info("[DASHBOARD] Page loaded at %s", datetime.now(timezone.utc).isoformat())

    refresh_rate, time_range, categories, clear_clicked = render_sidebar()

    st.title("Real-Time Anomaly Detection Dashboard")
    st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    if clear_clicked:
        st.session_state.confirm_clear = True

    confirming = st.session_state.get("confirm_clear", False)
    if confirming:
        handle_clear(categories)

    mongo_ok = pg_ok = True
    try:
        get_mongo_collection()
    except Exception as exc:
        st.error(f"MongoDB connection failed: {exc}")
        logger.error("[DASHBOARD] MongoDB unavailable: %s", exc)
        mongo_ok = False

    try:
        get_pg_conn()
    except Exception as exc:
        st.error(f"PostgreSQL connection failed: {exc}")
        logger.error("[DASHBOARD] PostgreSQL unavailable: %s", exc)
        pg_ok = False

    if not mongo_ok and not pg_ok:
        st.info("No transactions yet. Producer may still be starting.")
        time.sleep(refresh_rate)
        st.rerun()
        return

    latest = query_latest_metrics() if pg_ok else {}

    render_metric_cards(categories, latest)
    st.divider()

    if pg_ok:
        render_volume_chart(time_range)
        st.divider()

    if mongo_ok:
        col_left, col_right = st.columns([3, 2])
        with col_left:
            render_anomalies_table(categories)
        with col_right:
            render_category_chart(categories)
        st.divider()
        render_stats_panel(categories)

    # Pause auto-refresh while a clear-confirmation dialog is open, otherwise the
    # rerun would discard the user's Yes/Cancel click before it registers.
    if not st.session_state.get("confirm_clear", False):
        time.sleep(refresh_rate)
        st.rerun()


if __name__ == "__main__":
    main()
