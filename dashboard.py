import streamlit as st
import pandas as pd
import plotly.express as px
from database import ProjectDatabase
import subprocess
import os

st.set_page_config(page_title="Construction Project Tracker", layout="wide")

# Initialize database
db = ProjectDatabase()

# Auto-import initial data on first run
if len(db.get_all_projects()) == 0 and os.path.exists('initial_projects.csv'):
    db.import_from_csv('initial_projects.csv')

# Header
st.title("🏗️ Construction Project Tracker")
st.markdown("Track new construction projects across NC, SC, GA, FL, TN, AL, MS, AR, and LA")

# Sidebar
st.sidebar.header("Actions")

if st.sidebar.button("🔄 Run Scraper"):
    with st.spinner("Running scraper... This may take 3-4 hours"):
        try:
            result = subprocess.run(['node', 'scraper.js'], capture_output=True, text=True)
            st.sidebar.success("✅ Scraper completed!")
            
            # Import new data
            if os.path.exists('projects.csv'):
                db.import_from_csv('projects.csv')
                st.sidebar.success("✅ Data imported to database")
        except Exception as e:
            st.sidebar.error(f"❌ Error: {e}")

if st.sidebar.button("📥 Import CSV"):
    uploaded_file = st.sidebar.file_uploader("Choose CSV file", type="csv")
    if uploaded_file:
        db.import_from_csv(uploaded_file)
        st.sidebar.success("✅ CSV imported!")

if st.sidebar.button("📤 Export to CSV"):
    output_path = db.export_to_csv('construction_projects_export.csv')
    st.sidebar.success(f"✅ Exported to {output_path}")
    
    # Provide download button
    with open(output_path, 'rb') as f:
        st.sidebar.download_button(
            label="⬇️ Download CSV",
            data=f,
            file_name='construction_projects.csv',
            mime='text/csv'
        )

# Load data
df = db.get_all_projects()

# Filters
st.sidebar.header("Filters")
states = ['All'] + sorted(df['state'].dropna().unique().tolist())
selected_state = st.sidebar.selectbox("State", states)

if selected_state != 'All':
    df = df[df['state'] == selected_state]

# Metrics
st.metric("Total Projects", len(df))

# Charts
col1, col2 = st.columns(2)

# Data Table
st.subheader("All Projects")
# Reset index to start at 1 instead of 0
display_df = df[['project_name', 'customer', 'general_contractor', 'project_value', 'jobs_created', 
                 'city', 'state', 'announcement_date']].copy()
display_df.index = range(1, len(display_df) + 1)

st.dataframe(
    display_df,
    use_container_width=True,
    height=400
)

# Detailed view
with st.expander("View Project Details"):
    if len(df) > 0:
        selected_project = st.selectbox("Select Project", df['project_name'].tolist())
        project_details = df[df['project_name'] == selected_project].iloc[0]
        
        col1, col2 = st.columns(2)
        with col1:
            st.write("**Customer:**", project_details['customer'])
            st.write("**General Contractor:**", project_details['general_contractor'])
            st.write("**Value:**", project_details['project_value'])
            st.write("**Jobs:**", project_details['jobs_created'])
        with col2:
            st.write("**City:**", project_details['city'])
            st.write("**County:**", project_details['county'])
            st.write("**State:**", project_details['state'])
            st.write("**Date:**", project_details['announcement_date'])
        
        st.write("**Article:**", project_details['article_url'])
    else:
        st.warning("No projects available. Please import data first.")
    