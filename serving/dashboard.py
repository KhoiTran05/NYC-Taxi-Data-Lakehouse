import streamlit as st
from streamlit_autorefresh import st_autorefresh
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
from datetime import datetime, timedelta
import time
import json
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Lakehouse Analytics Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configuration
API_BASE_URL = "http://analytics-api:8000/api/v1"
API_KEY = "demo-api-key-2024"

# Helper functions
def get_api_headers():
    return {"API-Key": API_KEY}

def fetch_data(endpoint, params=None):
    """Fetch data from API with error handling"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/{endpoint}",
            headers=get_api_headers(),
            params=params,
            timeout=10
        )
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

# Dashboard components
def render_header():
    """Render dashboard header"""
    st.title("NYC Taxi Lakehouse Analytics Dashboard")
    st.markdown("Real-time and historical taxi analytics powered by Kafka CDC, Iceberg, and Spark")
    
    # Status indicators
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("System Status", "Online")
    with col2:
        st.metric("Last Update", datetime.now().strftime("%H:%M:%S"))
    with col3:
        st.metric("Data Freshness", "< 5 min")
    with col4:
        st.metric("Active Pipelines", "4/4")

def render_kpi_cards(stats_data):
    if "prev_stats" not in st.session_state:
        st.session_state.prev_stats = stats_data

    prev = st.session_state.prev_stats

    def pct_delta(curr, prev):
        if prev == 0:
            return 0
        return (curr - prev) / prev * 100

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        delta = pct_delta(stats_data["total_trips_today"], prev["total_trips_today"])
        st.metric(
            "Total Trips Today",
            f"{stats_data['total_trips_today']:,}",
            delta=f"{delta:.2f}%"
        )

    with col2:
        delta = pct_delta(stats_data["total_revenue_today"], prev["total_revenue_today"])
        st.metric(
            "Total Revenue Today",
            f"${stats_data['total_revenue_today']:,.2f}",
            delta=f"{delta:.2f}%"
        )

    with col3:
        delta = pct_delta(stats_data["avg_fare_today"], prev["avg_fare_today"])
        st.metric(
            "Average Fare",
            f"${stats_data['avg_fare_today']:.2f}",
            delta=f"{delta:.2f}%"
        )

    with col4:
        delta = pct_delta(stats_data["active_zones"], prev["active_zones"])
        st.metric(
            "Active Zones",
            stats_data["active_zones"],
            delta=f"{delta:.2f}%"
        )

    st.session_state.prev_stats = stats_data

def render_time_series_chart(time_series_data):
    """Render time series chart"""
    df = pd.DataFrame(time_series_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    fig = px.line(
        df, 
        x='timestamp', 
        y='value',
        title='Trip Volume Over Time (Last Days)',
        labels={'value': 'Number of Trips', 'timestamp': 'Time'}
    )
    
    fig.update_layout(
        height=400,
        xaxis_title="Time",
        yaxis_title="Number of Trips",
        hovermode='x unified'
    )
    
    return fig

def render_zone_performance(top_zones):
    """Render zone performance chart"""
    df = pd.DataFrame(top_zones)
    
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=('Trip Count by Zone', 'Revenue by Zone'),
        specs=[[{"secondary_y": False}, {"secondary_y": False}]]
    )
    
    # Trip count bar chart
    fig.add_trace(
        go.Bar(
            x=df['zone_name'],
            y=df['trips'],
            name='Trips',
            marker_color='lightblue'
        ),
        row=1, col=1
    )
    
    # Revenue bar chart
    fig.add_trace(
        go.Bar(
            x=df['zone_name'],
            y=df['revenue'],
            name='Revenue',
            marker_color='lightgreen'
        ),
        row=1, col=2
    )
    
    fig.update_layout(
        height=400,
        showlegend=False,
        title_text="Top 5 Zones Performance"
    )
    
    return fig

def render_real_time_map(real_time_activity):
    """Render real-time activity map (mock)"""
    df = pd.DataFrame(real_time_activity)
    try:
        fig = px.scatter_mapbox(
            df,
            lat="latitude",
            lon="longitude",
            size="pickup_count",
            color="activity_score",
            color_continuous_scale="Viridis",
            title="Real-time Taxi Activity by Zone",
            zoom=10,
            height=500
        )
        
        fig.update_layout(
            mapbox_style="open-street-map",
            margin={"r":0,"t":40,"l":0,"b":0}
        )
        
        return fig
    except Exception as e:
        st.error(f"Error redering real time map: {e}")
        fig = px.scatter(
            df,
            x="longitude",
            y="latitude",
            size="pickup_count",
            color="activity_score",
            title="Real-time Taxi Activity (Fallback)",
            height=500
        )

        return fig

def render_analytics_section(zone_metrics, stats_data):
    """Render analytics section"""
    st.header("Advanced Analytics")
    
    tab1, tab2 = st.tabs(["Trip Analysis", "Borough Analysis"])
    
    with tab1:
        st.subheader("Trip Analysis")
        
        if "prev_stats" not in st.session_state:
            st.session_state.prev_stats = stats_data

        prev = st.session_state.prev_stats

        def pct_delta(curr, prev):
            if prev == 0:
                return 0
            return (curr - prev) / prev * 100

        
        col1, col2, col3 = st.columns(3)
        with col1:
            delta = pct_delta(stats_data["peak_trips"], prev["peak_trips"])
            st.metric(
                "Peak Hours Trips",
                f"{stats_data['peak_trips']:.2f}",
                delta=f"{delta:.2f}%"
            )
        with col2:
            delta = pct_delta(stats_data["avg_trip_duration_minutes"], prev["avg_trip_duration_minutes"])
            st.metric(
                "Trip duration minutes",
                f"{stats_data['avg_trip_duration_minutes']:.2f}",
                delta=f"{delta:.2f}%"
            )
        with col3:
            delta = pct_delta(stats_data["avg_trip_distance"], prev["avg_trip_distance"])
            st.metric(
                "Trip distance",
                f"{stats_data['avg_trip_distance']:.2f}",
                delta=f"{delta:.2f}%"
            )
        
        map_data = pd.DataFrame({
            'lat': np.random.normal(40.7589, 0.1, 50),
            'lon': np.random.normal(-73.9851, 0.1, 50)
        })
        st.map(map_data)
        
    with tab2:
        st.subheader("Fare vs Distance by Borough")

        df = pd.DataFrame(zone_metrics)

        # Filters
        col1, col2 = st.columns(2)
        with col1:
            boroughs = ["All"] + sorted(df["borough"].dropna().unique().tolist())
            selected_borough = st.selectbox("Filter by Borough", boroughs)
        with col2:
            min_pickups = st.slider("Min Pickup Count", 0, int(df["total_pickups"].max()), 0)

        # Apply filters
        filtered = df.copy()
        if selected_borough != "All":
            filtered = filtered[filtered["borough"] == selected_borough]
        filtered = filtered[filtered["total_pickups"] >= min_pickups]

        if filtered.empty:
            st.warning("No data for selected filters.")
        else:
            fig = px.scatter(
                filtered,
                x="avg_distance",
                y="avg_fare",
                color="borough",
                size="total_pickups",
                hover_name="zone_name",
                hover_data={
                    "avg_fare": ":.2f",
                    "avg_distance": ":.2f",
                    "total_pickups": True,
                    "borough": False
                },
                labels={
                    "avg_distance": "Avg Trip Distance (miles)",
                    "avg_fare": "Avg Fare ($)",
                },
                title="Avg Fare vs Avg Distance per Zone",
                trendline="ols",
                trendline_scope="overall",
                height=520
            )

            fig.update_layout(
                legend_title="Borough",
                plot_bgcolor="white",
                xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
                yaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
            )

            st.plotly_chart(fig, use_container_width=True)

            st.markdown("**Summary**")
            summary = (
                filtered.groupby("borough")
                .agg(avg_fare=("avg_fare", "mean"), avg_distance=("avg_distance", "mean"))
                .round(2)
                .reset_index()
            )
            st.dataframe(summary, use_container_width=True, hide_index=True)

def render_sidebar():
    """Render sidebar controls"""
    st.sidebar.header("Controls")
    
    # Date range selector
    st.sidebar.subheader("Date Range")
    start_date = st.sidebar.date_input("Start Date", datetime.now() - timedelta(days=7))
    end_date = st.sidebar.date_input("End Date", datetime.now())
    
    # Filters
    st.sidebar.subheader("Filters")
    boroughs = st.sidebar.multiselect(
        "Borough",
        ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"],
        default=["Manhattan", "Brooklyn"]
    )
    
    fare_range = st.sidebar.slider(
        "Fare Range ($)",
        min_value=0,
        max_value=100,
        value=(10, 50)
    )
    
    # Refresh controls
    st.sidebar.subheader("Refresh")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)")
    
    if st.sidebar.button("Refresh Now"):
        st.experimental_rerun()
    
    # Export options
    st.sidebar.subheader("Export")
    if st.sidebar.button("Export Dashboard Data"):
        st.sidebar.success("Export initiated!")
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'boroughs': boroughs,
        'fare_range': fare_range,
        'auto_refresh': auto_refresh
    }

# Main dashboard
def main():
    st_autorefresh(interval=60_000, key="auto_refresh")
    # Render header
    render_header()
    
    # Render sidebar
    filters = render_sidebar()
    
    # Fetch dashboard data
    stats_response = fetch_data("dashboard/stats")
    
    stats_data = None
    if stats_response:
        stats_data = stats_response
    
        st.header("Key Performance Indicators")
        render_kpi_cards(stats_data)
    else:
        st.error("Failed to load dashboard statistics")
    
    st.divider()
    
    # Main charts section
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Time series chart
        time_series_response = fetch_data("analytics/time-series", {"days_back": 7})
        if time_series_response and time_series_response.get('success'):
            time_series_data = time_series_response['data']['series']
        
            fig_timeseries = render_time_series_chart(time_series_data)
            st.plotly_chart(fig_timeseries, use_container_width=True)
        else:
            st.warning("Time series data unavailable")
    with col2:
        if stats_data:
            fig_zones = render_zone_performance(stats_data['top_zones'])
            st.plotly_chart(fig_zones, use_container_width=True)
        
    st.divider()
    
    real_time_activity_response = fetch_data("realtime/activity", {"minutes_back": 60})
    if real_time_activity_response and real_time_activity_response.get('success'):
        activity_data = real_time_activity_response['data']
        
        if not activity_data:
            st.warning("No real-time activity data available.")
        else:
            st.header("Real-time Activity Map")
            fig_map = render_real_time_map(activity_data)
            st.plotly_chart(fig_map, use_container_width=True)
    
    st.divider()
    
    zone_metrics_response = fetch_data("analytics/zones", {"start_date": datetime.now() - timedelta(days=7), "end_date": datetime.now()})
    if zone_metrics_response and zone_metrics_response.get('success'):
        zones_data = zone_metrics_response['data']
        # st.write(pd.DataFrame(zones_data).columns.tolist())
        render_analytics_section(zones_data, stats_data)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.exception(e)
        st.info("Try refreshing the page or check if the API service is running.")
