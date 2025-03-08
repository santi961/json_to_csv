import streamlit as st
import pandas as pd
import json
import os
import zipfile
from io import BytesIO
from datetime import datetime

def format_duration(milliseconds):
    """Convert milliseconds to HH:MM:SS format, ensuring no '0 days' appears."""
    seconds = round(milliseconds / 1000)
    if seconds == 0:
        return ""
    return str(pd.to_timedelta(seconds, unit='s')).split()[-1]  # Extract only HH:MM:SS

def process_json(json_data):
    """Process JSON data and generate CSV data."""
    game_info = json_data["GameInfo"]
    logos = json_data["Logos"]
    shots = json_data["Shots"]

    # Format the filename
    game_date = datetime.strptime(game_info["Date"], "%Y-%m-%d %H:%M:%S.%f").strftime("%m.%d.%y")
    home_team = game_info["Home team"].replace(" ", "_")
    away_team = game_info["Away team"].replace(" ", "_")
    filename = f"{game_date}_{home_team}_vs_{away_team}_Exposure_Report.csv"

    # Track logo stats
    logo_data = {}
    
    for logo in logos:
        key = (logo["FileName"], logo["GroupId"])
        if key not in logo_data:
            logo_data[key] = {
                "Sponsor": os.path.splitext(logo["FileName"])[0], 
                "Placement": logo["Placement"], 
                "Total Shots": 0, 
                "Total Duration": 0, 
                "Q1": 0, "Q2": 0, "Q3": 0, "Q4": 0, "OT": 0
            }

    for shot in shots:
        key = (shot["FileName"], shot["GroupId"])
        if key in logo_data:
            period = shot["Period"].replace("Overtime", "OT")
            logo_data[key]["Total Shots"] += 1
            logo_data[key]["Total Duration"] += shot["Duration"]
            if period in logo_data[key]:
                logo_data[key][period] += shot["Duration"]

    # Convert to DataFrame and format data
    df = pd.DataFrame(logo_data.values())
    
    # Remove entries with 0 shots
    df = df[df["Total Shots"] > 0]

    # Convert duration columns
    df["Total Duration"] = df["Total Duration"].apply(format_duration)
    for period in ["Q1", "Q2", "Q3", "Q4", "OT"]:
        df[period] = df[period].apply(format_duration)

    return filename, df

def extract_json_from_zip(zip_file):
    """Extract report.json from a ZIP file."""
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if "report.json" in file:
                with zip_ref.open(file) as json_file:
                    return json.load(json_file)
    return None

def create_download_link(df_list):
    """Create a downloadable link for CSV or ZIP output."""
    if len(df_list) == 1:
        csv_filename, df = df_list[0]
        csv_data = df.to_csv(index=False).encode()
        st.download_button("Download CSV Report", csv_data, file_name=csv_filename, mime="text/csv")
    else:
        zip_buffer = BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zipf:
            for csv_filename, df in df_list:
                csv_data = df.to_csv(index=False)
                zipf.writestr(csv_filename, csv_data)
        zip_buffer.seek(0)
        st.download_button("Download All Reports (ZIP)", zip_buffer, file_name="Reports.zip", mime="application/zip")

# Streamlit UI
st.title("📊 JSON to CSV Exposure Report Generator")
st.write("Upload your `report.json` or ZIP files containing `report.json`, and get a CSV report.")

uploaded_files = st.file_uploader("Upload JSON or ZIP files", type=["json", "zip"], accept_multiple_files=True)

if uploaded_files:
    reports = []
    
    for uploaded_file in uploaded_files:
        if uploaded_file.name.endswith(".json"):
            json_data = json.load(uploaded_file)
        elif uploaded_file.name.endswith(".zip"):
            json_data = extract_json_from_zip(uploaded_file)
            if not json_data:
                st.error(f"Could not find `report.json` in {uploaded_file.name}. Skipping.")
                continue
        else:
            st.error(f"Unsupported file type: {uploaded_file.name}. Skipping.")
            continue

        csv_filename, report_df = process_json(json_data)
        reports.append((csv_filename, report_df))

    if reports:
        create_download_link(reports)
