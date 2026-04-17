import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime, timedelta
import time

# Set page config with custom theme
st.set_page_config(
    page_title="Engineering Hours and Cost Savings Calculator",
    page_icon="⏱️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.8rem;
        font-weight: 600;
        color: #1E3A8A;
        margin-top: 1.5rem;
        margin-bottom: 0.5rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #E5E7EB;
    }
    .card {
        background-color: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin-bottom: 1rem;
        border-left: 5px solid #1E3A8A;
    }
    .metric-card {
        background-color: #F3F4F6;
        border-radius: 8px;
        padding: 1rem;
        text-align: center;
        border: 1px solid #D1D5DB;
        transition: transform 0.3s ease;
    }
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 10px rgba(0, 0, 0, 0.1);
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1E3A8A;
    }
    .metric-label {
        font-size: 1rem;
        color: #6B7280;
    }
    .info-text {
        font-size: 1rem;
        color: #4B5563;
        line-height: 1.6;
    }
    .highlight {
        background-color: #DBEAFE;
        padding: 0.2rem 0.4rem;
        border-radius: 4px;
        font-weight: 500;
    }
    .sidebar .stSlider > div > div > div {
        background-color: #1E3A8A !important;
    }
    .stButton button {
        background-color: #1E3A8A;
        color: white;
        border-radius: 5px;
        padding: 0.5rem 1rem;
        font-weight: 500;
        border: none;
        transition: background-color 0.3s ease;
    }
    .stButton button:hover {
        background-color: #1E40AF;
    }
    .loading-spinner {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 100px;
    }
    .dataframe-container {
        border-radius: 10px;
        border: 1px solid #D1D5DB;
        overflow: hidden;
    }
    .stDateInput > div > div > input {
        border-radius: 5px !important;
        border-color: #D1D5DB !important;
    }
    .section-divider {
        height: 1px;
        background-color: #E5E7EB;
        margin: 1.5rem 0;
    }
    /* Custom selector styling */
    .stMultiSelect > div > div > div {
        border-radius: 5px !important;
        border-color: #D1D5DB !important;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state for tracking app state
if "initialized" not in st.session_state:
    st.session_state.initialized = False
    st.session_state.show_welcome = True
    st.session_state.query_executed = False
    # Initialize session state for dropdowns
    st.session_state.selected_destination_names = []
    st.session_state.selected_connector_types = []
    st.session_state.selected_connector_names = []
    st.session_state.selected_message_events = []
    st.session_state.connector_types_list = []
    st.session_state.connector_names_list = []
    st.session_state.message_events_list = []
    st.session_state.last_valid_state = {
        'destination_names': [],
        'connector_types': [],
        'connector_names': [],
        'message_events': []
    }

# Function to safely execute SQL queries
def safe_execute_query(query, error_message):
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"{error_message}: {str(e)}")
        return pd.DataFrame()

# Function to validate and update session state
def update_session_state(key, value, dependent_keys=None):
    try:
        st.session_state[key] = value
        if dependent_keys:
            for dep_key in dependent_keys:
                st.session_state[dep_key] = []
        st.session_state.last_valid_state[key] = value
    except Exception as e:
        st.error(f"Error updating state: {str(e)}")
        # Restore last valid state
        st.session_state[key] = st.session_state.last_valid_state[key]

# Function to display loading animation
def show_loading_animation():
    with st.spinner("Processing data..."):
        progress_bar = st.progress(0)
        for i in range(100):
            time.sleep(0.01)
            progress_bar.progress(i + 1)
        st.success("Data loaded successfully!")
        time.sleep(0.5)
        st.empty()

# Custom component for displaying metrics
def metric_card(title, value, prefix="", suffix=""):
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">{title}</div>
        <div class="metric-value">{prefix}{value}{suffix}</div>
    </div>
    """, unsafe_allow_html=True)

# Main header
st.markdown('<div class="main-header">Engineering Hours and Cost Savings Calculator</div>', unsafe_allow_html=True)

# Initialize Snowflake session
try:
    session = get_active_session()
except:
    st.error("Error connecting to Snowflake. Please check your connection settings.")
    st.stop()

# Initialize date range for today - 90 days to today
default_end_date = datetime.now()
default_start_date = default_end_date - timedelta(days=90)

with st.sidebar:
    st.markdown('<div class="sub-header">Filter Options</div>', unsafe_allow_html=True)
    
    # Date pickers for start and end date
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "**Start Date**",
            value=default_start_date,
            key="start_date"
        )
    with col2:
        end_date = st.date_input(
            "**End Date**",
            value=default_end_date,
            key="end_date"
        )
    
    # Format dates for SQL query
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Average Task Time Slider
    avg_task_time = st.slider(
        "**Average Task Time in hours**",
        min_value=0,
        max_value=48,
        value=6,  # Default value from customer file
        help="Use this to enter the average task time in hours per selected event",
    )
    
    # Hour Rate Slider
    hour_rate = st.slider(
        "**Hourly Rate**",
        min_value=0,
        max_value=250,
        value=55,  # Default value from dev file
        help="Use this to enter the engineering hourly rate",
    )

    # Apply button moved right below hour rate slider
    if st.button("Apply Filters", key="apply_filters"):
        st.session_state.show_welcome = False
        st.session_state.query_executed = True
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Get distinct destination names
    destination_names_query = """
    SELECT DISTINCT name 
    FROM DB.FIVETRAN_LOG.destination
    ORDER BY name
    """
    destination_names = safe_execute_query(destination_names_query, "Error fetching destination names")
    if not destination_names.empty:
        destination_names_list = destination_names['NAME'].tolist()
    else:
        destination_names_list = []
        st.warning("No destination names found. Please check your database connection.")
    
    # Collapsible and searchable multiselect for destinations
    with st.expander("Select Destination Names", expanded=False):
        selected_destination_names = st.multiselect(
            "Search and select destination names:",
            options=destination_names_list,
            default=st.session_state.selected_destination_names,
            key="destination_names_select"
        )
    
    # Update session state when destination selection changes
    if selected_destination_names != st.session_state.selected_destination_names:
        update_session_state(
            "selected_destination_names",
            selected_destination_names,
            dependent_keys=["selected_connector_types", "selected_connector_names", "selected_message_events"]
        )
        st.rerun()
    
    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
    # Get connector types based on selected destinations
    with st.spinner("Loading connector types..."):
        if selected_destination_names:
            destination_names_str = ', '.join(["'" + dn + "'" for dn in selected_destination_names])
            connector_types_query = f"""
            SELECT DISTINCT c.connector_type_id 
            FROM DB.FIVETRAN_LOG.connector c
            INNER JOIN DB.FIVETRAN_LOG.destination d ON c.destination_id = d.id
            WHERE d.name IN ({destination_names_str})
            ORDER BY connector_type_id
            """
            
            connector_types = safe_execute_query(connector_types_query, "Error fetching connector types")
            if not connector_types.empty:
                connector_types_list = connector_types['CONNECTOR_TYPE_ID'].tolist()
                st.session_state.connector_types_list = connector_types_list
            else:
                connector_types_list = []
                st.session_state.connector_types_list = []
                st.warning("No connector types found for selected destinations.")
        else:
            connector_types_list = []
            st.session_state.connector_types_list = []
    
    # Searchable and collapsible dropdown for connector types
    with st.expander("Select Connector Types", expanded=False):
        selected_connector_types = st.multiselect(
            "Search and select connector types:",
            options=connector_types_list,
            #default=st.session_state.selected_connector_types,
            default=connector_types_list if not st.session_state.selected_connector_types else st.session_state.selected_connector_types,
            key="connector_types_select",
            help="Search for specific connector types"
        )
    
    # Update session state when connector types selection changes
    if selected_connector_types != st.session_state.selected_connector_types:
        update_session_state(
            "selected_connector_types",
            selected_connector_types,
            dependent_keys=["selected_connector_names", "selected_message_events"]
        )
        st.rerun()
    
    # Get connector names based on selected destinations and connector types
    with st.spinner("Loading connector names..."):
        if selected_destination_names and selected_connector_types:
            destination_names_str = ', '.join(["'" + dn + "'" for dn in selected_destination_names])
            connector_types_str = ', '.join(["'" + ct + "'" for ct in selected_connector_types])
            
            connector_names_query = f"""
            SELECT DISTINCT c.connector_name 
            FROM DB.FIVETRAN_LOG.connector c
            INNER JOIN DB.FIVETRAN_LOG.destination d ON c.destination_id = d.id
            WHERE d.name IN ({destination_names_str})
            AND c.connector_type_id IN ({connector_types_str})
            ORDER BY connector_name
            """
            
            connector_names = safe_execute_query(connector_names_query, "Error fetching connector names")
            if not connector_names.empty:
                connector_names_list = connector_names['CONNECTOR_NAME'].tolist()
                st.session_state.connector_names_list = connector_names_list
            else:
                connector_names_list = []
                st.session_state.connector_names_list = []
                st.warning("No connector names found for selected destinations and connector types.")
        else:
            connector_names_list = []
            st.session_state.connector_names_list = []
    
    # Searchable and collapsible dropdown for connector names
    with st.expander("Select Connection Names", expanded=False):
        selected_connector_names = st.multiselect(
            "Choose Connection Names:",
            options=connector_names_list,
            default=connector_names_list if not st.session_state.selected_connector_names else st.session_state.selected_connector_names,
            key="connector_names_select",
            help="Search for specific Connection Names"
        )
    
    # Update session state when connector names selection changes
    if selected_connector_names != st.session_state.selected_connector_names:
        update_session_state(
            "selected_connector_names",
            selected_connector_names,
            dependent_keys=["selected_message_events"]
        )
        st.rerun()
    
    # Get message events based on selected destinations, connector types, and connector names
    with st.spinner("Loading message events..."):
        if selected_destination_names and selected_connector_types and selected_connector_names:
            destination_names_str = ', '.join(["'" + dn + "'" for dn in selected_destination_names])
            connector_types_str = ', '.join(["'" + ct + "'" for ct in selected_connector_types])
            connector_names_str = ', '.join(["'" + cn + "'" for cn in selected_connector_names])
            
            message_events_query = f"""
            SELECT DISTINCT l.message_event
            FROM DB.FIVETRAN_LOG.log l
            INNER JOIN DB.FIVETRAN_LOG.connector c ON l.connector_id = c.connector_id
            INNER JOIN DB.FIVETRAN_LOG.destination d ON c.destination_id = d.id
            WHERE d.name IN ({destination_names_str})
            AND c.connector_type_id IN ({connector_types_str})
            AND c.connector_name IN ({connector_names_str})
            ORDER BY l.message_event
            """
            
            message_events = safe_execute_query(message_events_query, "Error fetching message events")
            if not message_events.empty:
                message_events_list = message_events['MESSAGE_EVENT'].tolist()
                st.session_state.message_events_list = message_events_list
            else:
                message_events_list = []
                st.session_state.message_events_list = []
                st.warning("No message events found for selected filters.")
        else:
            message_events_list = []
            st.session_state.message_events_list = []
    
    # Searchable and collapsible dropdown for message events
    with st.expander("Select Message Events", expanded=False):
        selected_message_events = st.multiselect(
            "Choose Message Event types:",
            options=message_events_list,
            #default=st.session_state.selected_message_events,
            default=message_events_list if not st.session_state.selected_message_events else st.session_state.selected_message_events,
            key="message_events_select",
            help="Search for specific Message Events"
        )
    
    # Update session state when message events selection changes
    if selected_message_events != st.session_state.selected_message_events:
        update_session_state("selected_message_events", selected_message_events)
        st.rerun()
    
    # Display selected options
    if selected_message_events:
        selected_options_str = ', '.join(["'" + str(option) + "'" for option in selected_message_events])
        st.markdown(f"""
    <div class="info-text">
        Selected events: {selected_options_str}
    </div>
    """, unsafe_allow_html=True)
    else:
        st.warning("Please select at least one message event to proceed.")

    st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
    
# Main content area
if st.session_state.show_welcome:
    # Display welcome screen / ReadMe using Streamlit's markdown rendering
    st.markdown('<div class="sub-header">Welcome to the Engineering Hours and Cost Savings Calculator</div>', unsafe_allow_html=True)
    
    st.write("""
    This application helps you quantify the time and cost savings achieved by using Fivetran for your data integration needs. 
    By analyzing schema changes and other data integration events, we can estimate the engineering hours and costs saved.
    """)
    
    st.markdown("### How to use this app:")
    
    st.markdown("""
    1. Use the **date range selectors** in the sidebar to define your analysis period
    2. Adjust the **Average Task Time** slider to reflect how long your engineers typically spend[hours] handling the selected events
    3. Set the **Hourly Rate** slider to match your engineering team's cost
    4. Select the relevant **connector types** and **connector names** to include in your analysis
    5. Choose which **message events** to analyze (e.g., alter_table, create_table)
    6. Click the **Apply Filters** button to generate results
    """)
    
    st.markdown("### What this app calculates:")
    
    st.write("""
    The app analyzes your Fivetran logs to identify schema changes and other events that would require manual engineering 
    work if not automated by Fivetran. It then calculates:
    """)
    
    st.markdown("""
    - **Engineering Hours Saved:** Total time your team would have spent handling these changes manually
    - **Engineering Cost Saved:** The monetary value of this time based on your hourly rate
    - **Savings by Connector:** Breakdown of savings by connector type and name
    - **Savings by Category:** Analysis of savings across major categories (Large SaaS, Flat File, Database Systems)
    """)
    
    st.write("To begin your analysis, adjust the filters in the sidebar and click Apply Filters.")
    
elif st.session_state.query_executed:
    # Show loading animation
    show_loading_animation()

    # Run the main query with selected filters
    query = f"""
    WITH parse_json AS (
      SELECT
        DATE_TRUNC('DAY', l.time_stamp) AS date_day,
        l.id,
        l.time_stamp as event_time,
        l.message_event,
        d.id as destination_id,
        d.name as destination_name,
        d.type as destination_type,
        l.connector_id,
        c.connector_type_id,
        c.connector_name,
        PARSE_JSON(message_data) AS message_data
      FROM DB.FIVETRAN_LOG.log l
      INNER JOIN DB.FIVETRAN_LOG.connector c ON l.connector_id = c.connector_id
      INNER JOIN DB.FIVETRAN_LOG.destination d ON c.destination_id = d.id
      WHERE DATE_TRUNC('DAY', time_stamp) >= '{start_date_str}'
      AND DATE_TRUNC('DAY', time_stamp) <= '{end_date_str}'
    """

    # Add filter conditions
    if selected_connector_types:
        connector_types_str = ', '.join(["'" + ct + "'" for ct in selected_connector_types])
        query += f" AND c.connector_type_id IN ({connector_types_str})"
    if selected_connector_names:
        connector_names_str = ', '.join(["'" + cn + "'" for cn in selected_connector_names])
        query += f" AND c.connector_name IN ({connector_names_str})"

        # Add filter conditions
    if selected_destination_names:
        destination_names_str = ', '.join(["'" + dn + "'" for dn in selected_destination_names])
        query += f" AND d.name IN ({destination_names_str})"

    # Continue with the rest of the query
    query += """
    )
    , t as (
    SELECT 
      id,
      date_day,
      message_event,
      destination_id,
      destination_name,
      destination_type,
      connector_id,
      connector_type_id,
      connector_name,
      message_data:table AS logtable,
      message_data:count as rowsimpacted
    FROM parse_json
    GROUP BY date_day,id,destination_id,destination_name,destination_type,connector_id,connector_type_id,connector_name,message_event,logtable,rowsimpacted
    ORDER BY connector_id,logtable asc
    ), ec as(
    select
        date_day,
        message_event,
        destination_id,
        destination_name,
        destination_type,
        connector_id,
        connector_type_id,
        connector_name,
        count(id) as event_count
        from t
        group by date_day,destination_id,destination_name,destination_type,connector_id,connector_type_id,connector_name,message_event
    )
    select 
        date_day,
        message_event,
        destination_id,
        destination_name,
        destination_type,
        connector_id,
        connector_type_id,
        connector_name,
        event_count,
    """

    # Add the parameters
    query += f" round(sum(event_count * {avg_task_time} )) as eng_hours, "
    query += f" round(sum(event_count * {avg_task_time} * {hour_rate} )) as eng_cost_savings"

    # Continue with the rest of the query
    query += f"""
    from ec 
    where message_event in 
    ({selected_options_str}) 

    group by 
        date_day,
        destination_id,
        destination_name,
        destination_type,
        connector_id,
        connector_type_id,
        connector_name,
        message_event,
        event_count
    order by message_event
    """

    try:
        cd = session.sql(query).to_pandas()
        
        # Calculate overall totals
        total_events = cd['EVENT_COUNT'].sum()
        total_hours_saved = cd['ENG_HOURS'].sum()
        total_cost_saved = cd['ENG_COST_SAVINGS'].sum()
        date_range_days = (end_date - start_date).days
        
        # Display summary metrics in a prominent location
        st.markdown('<div class="sub-header">Overall Savings Summary</div>', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            metric_card("Date Range", f"{date_range_days} days")
        with col2:
            metric_card("Total Events", f"{total_events:,}")
        with col3:
            metric_card("Total Hours Saved", f"{total_hours_saved:,}")
        with col4:
            metric_card("Total Cost Saved", f"{total_cost_saved:,.2f}", prefix="$")
        
        # Summary per day
        avg_hours_per_day = total_hours_saved / max(date_range_days, 1)
        avg_cost_per_day = total_cost_saved / max(date_range_days, 1)

       
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Daily Averages</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            metric_card("Avg Hours Saved Per Day", f"{avg_hours_per_day:.2f}")
        with col2:
            metric_card("Avg Cost Saved Per Day", f"${avg_cost_per_day:,.2f}")
        
        # Process data for the different views
        # By Destination
        d = cd.groupby("DESTINATION_NAME")[["EVENT_COUNT", "ENG_HOURS", "ENG_COST_SAVINGS"]].sum()
        d["ENG_HOURS"] = d["ENG_HOURS"].astype(int)
        d["ENG_COST_SAVINGS"] = d["ENG_COST_SAVINGS"].astype(float).map("${:,.2f}".format)
        d = d.sort_values("ENG_HOURS", ascending=False)
        
        # By Connector Name
        c = cd.groupby("CONNECTOR_NAME")[["EVENT_COUNT", "ENG_HOURS", "ENG_COST_SAVINGS"]].sum()
        c["ENG_HOURS"] = c["ENG_HOURS"].astype(int)
        c["ENG_COST_SAVINGS"] = c["ENG_COST_SAVINGS"].astype(float).map("${:,.2f}".format)
        c = c.sort_values("ENG_HOURS", ascending=False)
        
        # By Connector Type
        ctid = cd.copy()
        cti = ctid.groupby("CONNECTOR_TYPE_ID")[["EVENT_COUNT", "ENG_HOURS", "ENG_COST_SAVINGS"]].sum()
        cti["ENG_HOURS"] = cti["ENG_HOURS"].astype(int)
        cti["ENG_COST_SAVINGS"] = cti["ENG_COST_SAVINGS"].astype(float).map("${:,.2f}".format)
        cti = cti.sort_values("CONNECTOR_TYPE_ID", ascending=True)
        
        # Display the data tables in tabs
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Detailed Savings Analysis</div>', unsafe_allow_html=True)
        st.markdown(f"During the selected period from **{start_date_str}** to **{end_date_str}**, Fivetran saved your team approximately **{total_hours_saved:,} engineering hours**, equivalent to **${total_cost_saved:,.2f}** in engineering costs.")
        tab1, tab2, tab3 = st.tabs(["By Destination", "By Connector Type", "By Connector Name"])
        
        with tab1:
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(d, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab2:
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(cti, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with tab3:
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(c, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Bar chart visualization
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Engineering Hours and Cost Savings Over Time</div>', unsafe_allow_html=True)
        st.bar_chart(cd, x="DATE_DAY", y=["ENG_HOURS", "ENG_COST_SAVINGS"])
        
        # Savings by connector type categories
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Savings by Connector Type Categories</div>', unsafe_allow_html=True)
        
        # Create a function to categorize connector types
        def categorize_connector_type(connector_type):
            if connector_type in ['workday', 'workday_raas', 'workday_hcm', 'salesforce']:
                return 'Large SaaS'
            elif connector_type in ['sharepoint', 's3']:
                return 'Flat File'
            elif 'sql' in connector_type.lower():
                return 'Database Systems'
            else:
                return 'Other'
        
        # Create a copy of the data with categories
        categorized_data = ctid.copy()
        categorized_data['CATEGORY'] = categorized_data['CONNECTOR_TYPE_ID'].apply(categorize_connector_type)
        
        # Group by category
        category_summary = categorized_data.groupby('CATEGORY')[['EVENT_COUNT', 'ENG_HOURS', 'ENG_COST_SAVINGS']].sum()
        category_summary["ENG_HOURS"] = category_summary["ENG_HOURS"].astype(int)
        category_summary["ENG_COST_SAVINGS"] = category_summary["ENG_COST_SAVINGS"].astype(float).map("${:,.2f}".format)
        category_summary = category_summary.sort_values('ENG_HOURS', ascending=False)
        
        # Display the category summary
        col1, col2 = st.columns([3, 7])
        
        with col1:
            st.markdown('<div class="dataframe-container">', unsafe_allow_html=True)
            st.dataframe(category_summary, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        
        with col2:
            # Prepare data for visualization
            hours_by_category = categorized_data.groupby('CATEGORY')['ENG_HOURS'].sum()
            st.bar_chart(hours_by_category)
        
        # Underlying data (collapsible)
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        with st.expander("View Underlying Data"):
            st.dataframe(cd, use_container_width=True)
        
        # Add an explanation of the savings - using Streamlit's native markdown/text rendering
        st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Fivetran Savings Explanation</div>', unsafe_allow_html=True)
        
        st.write("This dashboard demonstrates the significant time and cost savings achieved by using Fivetran for your data integration needs:")
        
        st.markdown("### Large SaaS Connectors:")
        st.write("Complex APIs like Workday and Salesforce require significant engineering effort to maintain. Fivetran handles schema changes, API version updates, and rate limiting automatically.")
        
        st.markdown("### Flat File Connectors:")
        st.write("Sharepoint and S3 file processing requires parsing, validation, and error handling. Fivetran standardizes this process and ensures data quality.")
        
        st.markdown("### Database Systems:")
        st.write("SQL database replication involves complex CDC (Change Data Capture) processes. Fivetran manages incremental updates efficiently with minimal impact on source systems.")
        
        st.write("The metrics above represent the engineering hours and costs that would have been required to build and maintain these data pipelines manually.")
        
    except Exception as e:
        st.error(f"Error executing query: {e}")
        st.warning("Please adjust your filters and try again.")
