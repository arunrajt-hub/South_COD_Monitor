"""
Amazon COD RTS Email Automation
Replicates the n8n workflow: runs analysis, reads Google Sheets, and sends category-based emails
"""

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import sys
import subprocess
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('amazon_cod_rts_email.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': os.getenv('GMAIL_SENDER_EMAIL', 'arunraj@loadshare.net'),
    'sender_password': os.getenv('GMAIL_APP_PASSWORD', 'ihczkvucdsayzrsu'),
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

# Google Sheets Configuration
SHEET_ID = "1W17PYZlZ09sCRtMYRtOIx5hvdf1CUz9pQZaDre6-O4Y"
SERVICE_ACCOUNT_FILE = 'service_account_key.json'

# Station Categories
STATION_CATEGORIES = {
    'Bangalore DSP Stations': ['BLRP', 'BLT3', 'BLRA', 'BLT1', 'BLT4', 'BLRL'],
    'Chennai DSP Stations': ['MAAL', 'MAAG', 'MAAI', 'MAAJ', 'MAAE', 'MAT1'],
    'Kerala eDSP Stations': ['KGQB', 'KLZE', 'KTYI', 'ERSA', 'TRVI', 'QLNB', 'TLAG'],
    'Chennai eDSP Stations': ['KELE', 'MASC']
}

# Station to Hub Type mapping
STATION_HUB_TYPE_MAPPING = {}
for category, stations in STATION_CATEGORIES.items():
    if 'DSP' in category:
        hub_type = 'DSP'
    elif 'eDSP' in category:
        hub_type = 'eDSP'
    else:
        hub_type = ''
    for station in stations:
        STATION_HUB_TYPE_MAPPING[station] = hub_type

# Fixed recipients for summary email
SUMMARY_EMAIL_RECIPIENTS = [
    'arunraj@loadshare.net',
    'sherin.kv@loadshare.net',
    'reagan@loadshare.net',
    'chandrakumar.r@loadshare.net',
    'ramesh@loadshare.net',
    'narendra@loadshare.net'
]


class AmazonCODRTSEmailAutomation:
    def __init__(self):
        """Initialize Google Sheets connection"""
        self.client = None
        self.setup_google_sheets()
    
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            credentials = Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=scope
            )
            self.client = gspread.authorize(credentials)
            logging.info("✅ Google Sheets connection established")
        except Exception as e:
            logging.error(f"❌ Failed to setup Google Sheets: {e}")
            raise
    
    def run_analysis_script(self):
        """Run the main Amazon COD RTS analysis script"""
        try:
            logging.info("🔄 Running Amazon COD RTS analysis script...")
            script_path = "Automatic_Amazon_COD_RTS_Reco.py"
            
            if not os.path.exists(script_path):
                logging.error(f"❌ Script not found: {script_path}")
                return False
            
            # Run the script
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            if result.returncode == 0:
                logging.info("✅ Analysis script completed successfully")
                return True
            else:
                logging.error(f"❌ Analysis script failed: {result.stderr}")
                return False
        except Exception as e:
            logging.error(f"❌ Error running analysis script: {e}")
            return False
    
    def get_station_email_mapping(self):
        """Get station-to-email mapping from Google Sheets (including Region)"""
        try:
            logging.info("📧 Fetching station-email mapping...")
            spreadsheet = self.client.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet("Email Mapping")
            
            # Get all data
            data = worksheet.get_all_records()
            
            # Build mapping dictionary with structure: {station: {'emails': [...], 'region': '...'}}
            station_email_mapping = {}
            for row in data:
                station = row.get('Station', '').strip()
                email = row.get('Email', '').strip()
                region = row.get('Region', '').strip()
                
                if station and email:
                    # Handle comma-separated emails
                    if ',' in email:
                        emails = [e.strip() for e in email.split(',') if e.strip()]
                    else:
                        emails = [email]
                    
                    if station not in station_email_mapping:
                        station_email_mapping[station] = {'emails': [], 'region': region}
                    
                    station_email_mapping[station]['emails'].extend(emails)
                    # Update region if not already set or if new row has region
                    if region and not station_email_mapping[station]['region']:
                        station_email_mapping[station]['region'] = region
            
            # Remove duplicate emails and ensure region is set
            for station in station_email_mapping:
                station_email_mapping[station]['emails'] = list(set(station_email_mapping[station]['emails']))
            
            logging.info(f"✅ Found {len(station_email_mapping)} stations with email mappings")
            return station_email_mapping
        except Exception as e:
            logging.error(f"❌ Error fetching email mapping: {e}")
            return {}
    
    def get_summary_data(self):
        """Get COD/RTS Analysis summary data"""
        try:
            logging.info("📊 Fetching summary data...")
            spreadsheet = self.client.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet("Amazon_COD_RTS_Analysis")
            
            # Get data from A1:L23
            data = worksheet.get('A1:L23')
            
            if not data or len(data) < 2:
                logging.warning("⚠️ No summary data found")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Ensure Hub Type column exists and populate missing values
            station_col = None
            hub_type_col = None
            
            # Find Station and Hub Type columns (case-insensitive)
            for col in df.columns:
                col_lower = str(col).lower()
                if 'station' in col_lower and station_col is None:
                    station_col = col
                if 'hub type' in col_lower or 'hubtype' in col_lower.replace(' ', ''):
                    hub_type_col = col
            
            # If Hub Type column doesn't exist, create it
            if hub_type_col is None:
                # Try to find it at index 1 (expected position)
                if len(df.columns) > 1:
                    df.insert(1, 'Hub Type', '')
                    hub_type_col = 'Hub Type'
                    logging.info("✅ Created 'Hub Type' column")
                else:
                    logging.warning("⚠️ Could not create Hub Type column")
            
            # Populate Hub Type for stations that have missing or empty values
            if station_col and hub_type_col:
                for idx, row in df.iterrows():
                    station = str(row.get(station_col, '')).strip()
                    hub_type = str(row.get(hub_type_col, '')).strip()
                    
                    # If Hub Type is missing or empty, populate from mapping
                    if station and (not hub_type or hub_type == '' or pd.isna(hub_type)):
                        if station in STATION_HUB_TYPE_MAPPING:
                            df.at[idx, hub_type_col] = STATION_HUB_TYPE_MAPPING[station]
                            logging.info(f"✅ Populated Hub Type '{STATION_HUB_TYPE_MAPPING[station]}' for station {station}")
            
            # Filter columns (1, 2, 8, 9, 10, 11, 12) - 0-indexed: 0, 1, 7, 8, 9, 10, 11
            if len(df.columns) >= 12:
                filtered_df = df.iloc[:, [0, 1, 7, 8, 9, 10, 11]].copy()
            else:
                filtered_df = df.copy()
            
            logging.info(f"✅ Fetched {len(filtered_df)} rows of summary data")
            return filtered_df
        except Exception as e:
            logging.error(f"❌ Error fetching summary data: {e}")
            return pd.DataFrame()
    
    def get_high_value_data(self):
        """Get High Value RTS/Ageing data"""
        try:
            logging.info("💰 Fetching high value data...")
            spreadsheet = self.client.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet("RTS_High_Value_Tracking")
            
            # Get data from A1:E70
            data = worksheet.get('A1:E70')
            
            if not data or len(data) < 2:
                logging.warning("⚠️ No high value data found")
                return []
            
            # Convert to list of dictionaries
            headers = data[0]
            records = []
            for row in data[1:]:
                if any(cell for cell in row):  # Skip empty rows
                    record = {headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))}
                    records.append(record)
            
            logging.info(f"✅ Fetched {len(records)} high value records")
            return records
        except Exception as e:
            logging.error(f"❌ Error fetching high value data: {e}")
            return []
    
    def get_high_default_agents_data(self):
        """Get High Default Agents data"""
        try:
            logging.info("👥 Fetching high default agents data...")
            spreadsheet = self.client.open_by_key(SHEET_ID)
            worksheet = spreadsheet.worksheet("High Default Agents")
            
            # Get data from A1:D50
            data = worksheet.get('A1:D50')
            
            if not data or len(data) < 2:
                logging.warning("⚠️ No high default agents data found")
                return []
            
            # Convert to list of dictionaries
            headers = data[0]
            records = []
            for row in data[1:]:
                if any(cell for cell in row):  # Skip empty rows
                    record = {headers[i]: row[i] if i < len(row) else '' for i in range(len(headers))}
                    records.append(record)
            
            logging.info(f"✅ Fetched {len(records)} high default agents records")
            return records
        except Exception as e:
            logging.error(f"❌ Error fetching high default agents data: {e}")
            return []
    
    def create_summary_html(self, df):
        """Create HTML table from summary DataFrame with 4D Active Email styling"""
        if df.empty:
            return ""
        
        # Get today's date
        today = datetime.now().strftime('%d-%m-%Y')
        
        # Build table HTML manually for better control
        html_table = '<table>\n                <tr>\n'
        
        # Header row
        for col in df.columns:
            html_table += f'                    <th>{col}</th>\n'
        html_table += '                </tr>\n'
        
        # Data rows
        for idx, row in df.iterrows():
            html_table += '                <tr>\n'
            for col in df.columns:
                value = row[col]
                # Format numbers
                if isinstance(value, (int, float)):
                    if value != int(value):
                        value_str = f"{value:,.2f}"
                    else:
                        value_str = f"{int(value):,}"
                else:
                    value_str = str(value) if pd.notna(value) else ""
                
                # Right-align numeric columns (4th, 5th, 6th, 7th)
                col_idx = list(df.columns).index(col)
                align_class = 'number-cell' if col_idx in [3, 4, 5, 6] else ''
                html_table += f'                    <td class="{align_class}">{value_str}</td>\n'
            html_table += '                </tr>\n'
        
        html_table += '            </table>'
        
        # Wrap in full email template with 4D Active Email styling
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            line-height: 1.6; 
            color: #2c3e50; 
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #FF6B35 0%, #F7931E 50%, #FFD23F 100%);
            color: white;
            padding: 10px 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 16px;
            font-weight: bold;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        .header p {{
            margin: 5px 0 0 0;
            font-size: 11px;
            opacity: 0.95;
        }}
        .content {{
            padding: 15px 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 8px 10px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 10px;
            letter-spacing: 0.3px;
            border: none;
        }}
        th:first-child {{
            border-top-left-radius: 8px;
        }}
        th:last-child {{
            border-top-right-radius: 8px;
        }}
        td {{
            padding: 8px 10px;
            border-bottom: 1px solid #e0e0e0;
            background: white;
            font-size: 11px;
        }}
        tr:nth-child(even) td {{
            background: #f8f9fa;
        }}
        tr:hover td {{
            background: #e3f2fd !important;
            transition: background 0.3s ease;
        }}
        .number-cell {{
            text-align: center;
            font-weight: 500;
        }}
        .footer {{
            background: #f5f5f5;
            padding: 12px 20px;
            text-align: center;
            color: #666;
            font-size: 10px;
            border-top: 3px solid #FF6B35;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Amazon COD/RTS Analysis</h1>
            <p>Report Date: {today}</p>
        </div>
        <div class="content">
            {html_table}
        </div>
        <div class="footer">
            <p>This email contains overall summary of Amazon COD/RTS Analysis.</p>
            <p>This email was automatically generated.</p>
        </div>
    </div>
</body>
</html>"""
        return html
    
    def group_data_by_category(self, data, station_email_mapping, station_field='Delivery_Station_Code'):
        """Group data by station category and recipients (including Region)"""
        category_items = []
        
        # Normalize email lists and regions for each station
        station_email_map = {}
        station_region_map = {}
        for station, station_data in station_email_mapping.items():
            if isinstance(station_data, dict):
                emails = station_data.get('emails', [])
                region = station_data.get('region', '')
            else:
                # Backward compatibility: if it's a list, treat as old format
                emails = station_data if isinstance(station_data, list) else [station_data]
                region = ''
            
            if isinstance(emails, list):
                email_str = ','.join(sorted(emails))
            else:
                email_str = str(emails)
            
            if email_str:
                station_email_map[station] = email_str
                if region:
                    station_region_map[station] = region
        
        # Group stations by category and recipient set
        category_recipient_map = {}
        
        for station, email_key in station_email_map.items():
            # Find which category this station belongs to
            category = None
            for cat_name, stations in STATION_CATEGORIES.items():
                if station in stations:
                    category = cat_name
                    break
            
            if category and email_key:
                # Get region for this station
                region = station_region_map.get(station, '')
                key = f"{category}|{email_key}|{region}"
                
                if key not in category_recipient_map:
                    category_recipient_map[key] = {
                        'category': category,
                        'emails': email_key.split(','),
                        'stations': [],
                        'region': region
                    }
                
                if station not in category_recipient_map[key]['stations']:
                    category_recipient_map[key]['stations'].append(station)
        
        # Create items per category-recipient group
        for key, group in category_recipient_map.items():
            emails = group['emails']
            stations = group['stations']
            category = group['category']
            
            # Filter data for stations in this category
            station_data = []
            for row in data:
                row_station = row.get(station_field, '')
                if row_station in stations:
                    station_data.append(row)
            
            if station_data and emails:
                region = group.get('region', '')
                category_items.append({
                    'category': category,
                    'emails': emails,
                    'emailsString': ', '.join(emails),
                    'stations': stations,
                    'stationsString': ', '.join(stations),
                    'data': station_data,
                    'stationCount': len(stations),
                    'emailCount': len(emails),
                    'region': region
                })
        
        return category_items
    
    def create_high_value_html(self, category_item):
        """Create HTML for High Value emails"""
        category = category_item['category']
        stations_string = category_item['stationsString']
        station_data = category_item['data']
        station_count = category_item['stationCount']
        
        # Build table rows
        table_rows = ''
        for row in station_data:
            tracking_id = row.get('Tracking_ID', '')
            delivery_station = row.get('Delivery_Station_Code', '')
            ageing_bucket = row.get('Ageing_Bucket', '')
            value = row.get('Value', '')
            data_source = row.get('Data_Source', '')
            
            table_rows += f"""<tr>
      <td>{tracking_id}</td>
      <td>{delivery_station}</td>
      <td>{ageing_bucket}</td>
      <td>{value}</td>
      <td>{data_source}</td>
    </tr>"""
        
        # Get today's date
        today = datetime.now().strftime('%d-%m-%Y')
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            line-height: 1.6; 
            color: #2c3e50; 
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #FF6B35 0%, #F7931E 50%, #FFD23F 100%);
            color: white;
            padding: 10px 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 16px;
            font-weight: bold;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        .header p {{
            margin: 5px 0 0 0;
            font-size: 11px;
            opacity: 0.95;
        }}
        .content {{
            padding: 15px 20px;
        }}
        .summary {{
            background: #f8f9fa;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 15px;
            font-size: 11px;
        }}
        .summary p {{
            margin: 5px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 8px 10px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 10px;
            letter-spacing: 0.3px;
            border: none;
        }}
        th:first-child {{
            border-top-left-radius: 8px;
        }}
        th:last-child {{
            border-top-right-radius: 8px;
        }}
        td {{
            padding: 8px 10px;
            border-bottom: 1px solid #e0e0e0;
            background: white;
            font-size: 11px;
        }}
        tr:nth-child(even) td {{
            background: #f8f9fa;
        }}
        tr:hover td {{
            background: #e3f2fd !important;
            transition: background 0.3s ease;
        }}
        .footer {{
            background: #f5f5f5;
            padding: 12px 20px;
            text-align: center;
            color: #666;
            font-size: 10px;
            border-top: 3px solid #FF6B35;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>💰 High Value Ageing/RTS Pending</h1>
            <p>Report Date: {today}</p>
        </div>
        <div class="content">
            <div class="summary">
                <p><strong>Category:</strong> {category}</p>
                <p><strong>Stations:</strong> {stations_string}</p>
                <p><strong>Total Records:</strong> {len(station_data)}</p>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Tracking ID</th>
                        <th>Delivery Station Code</th>
                        <th>Ageing Bucket</th>
                        <th>Value</th>
                        <th>Data Source</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
        <div class="footer">
            <p>This email contains data for {station_count} station(s) in {category}: {stations_string}</p>
            <p>This email was automatically generated.</p>
        </div>
    </div>
</body>
</html>"""
        return html
    
    def create_high_default_agents_html(self, category_item):
        """Create HTML for High Default Agents emails"""
        category = category_item['category']
        stations_string = category_item['stationsString']
        station_data = category_item['data']
        station_count = category_item['stationCount']
        
        # Build table rows
        table_rows = ''
        for row in station_data:
            employee_name = row.get('Employee_Name', '')
            station_code = row.get('Station_Code', '')
            type_val = row.get('Type', '')
            balance_due = row.get('Balance_Due', '')
            
            table_rows += f"""<tr>
      <td>{employee_name}</td>
      <td>{station_code}</td>
      <td>{type_val}</td>
      <td>{balance_due}</td>
    </tr>"""
        
        # Get today's date
        today = datetime.now().strftime('%d-%m-%Y')
        
        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            line-height: 1.6; 
            color: #2c3e50; 
            margin: 0;
            padding: 20px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #FF6B35 0%, #F7931E 50%, #FFD23F 100%);
            color: white;
            padding: 10px 20px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 16px;
            font-weight: bold;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }}
        .header p {{
            margin: 5px 0 0 0;
            font-size: 11px;
            opacity: 0.95;
        }}
        .content {{
            padding: 15px 20px;
        }}
        .summary {{
            background: #f8f9fa;
            padding: 12px;
            border-radius: 8px;
            margin-bottom: 15px;
            font-size: 11px;
        }}
        .summary p {{
            margin: 5px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 8px 10px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 10px;
            letter-spacing: 0.3px;
            border: none;
        }}
        th:first-child {{
            border-top-left-radius: 8px;
        }}
        th:last-child {{
            border-top-right-radius: 8px;
        }}
        td {{
            padding: 8px 10px;
            border-bottom: 1px solid #e0e0e0;
            background: white;
            font-size: 11px;
        }}
        tr:nth-child(even) td {{
            background: #f8f9fa;
        }}
        tr:hover td {{
            background: #e3f2fd !important;
            transition: background 0.3s ease;
        }}
        .footer {{
            background: #f5f5f5;
            padding: 12px 20px;
            text-align: center;
            color: #666;
            font-size: 10px;
            border-top: 3px solid #FF6B35;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>👥 High Default Agents - Collect the Cash</h1>
            <p>Report Date: {today}</p>
        </div>
        <div class="content">
            <div class="summary">
                <p><strong>Category:</strong> {category}</p>
                <p><strong>Stations:</strong> {stations_string}</p>
                <p><strong>Total Records:</strong> {len(station_data)}</p>
            </div>
            <table>
                <thead>
                    <tr>
                        <th>Employee Name</th>
                        <th>Station Code</th>
                        <th>Type</th>
                        <th>Balance Due</th>
                    </tr>
                </thead>
                <tbody>
                    {table_rows}
                </tbody>
            </table>
        </div>
        <div class="footer">
            <p>This email contains data for {station_count} station(s) in {category}: {stations_string}</p>
            <p>This email was automatically generated.</p>
        </div>
    </div>
</body>
</html>"""
        return html
    
    def send_email(self, to_emails, subject, html_body, cc_emails=None):
        """Send email"""
        try:
            if isinstance(to_emails, str):
                to_emails = [e.strip() for e in to_emails.split(',')]
            
            msg = MIMEMultipart('alternative')
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = ', '.join(to_emails)
            if cc_emails:
                msg['Cc'] = ', '.join(cc_emails) if isinstance(cc_emails, list) else cc_emails
            msg['Subject'] = subject
            
            msg.attach(MIMEText(html_body, 'html'))
            
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            
            recipients = to_emails.copy()
            if cc_emails:
                if isinstance(cc_emails, str):
                    recipients.extend([e.strip() for e in cc_emails.split(',')])
                else:
                    recipients.extend(cc_emails)
            
            server.sendmail(EMAIL_CONFIG['sender_email'], recipients, msg.as_string())
            server.quit()
            
            logging.info(f"✅ Email sent successfully to {', '.join(to_emails)}")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to send email: {e}")
            return False
    
    def send_summary_email(self, summary_df, station_email_mapping):
        """Send summary email to recipients mapped under station 'ALL'"""
        try:
            logging.info("📧 Sending summary email...")
            html = self.create_summary_html(summary_df)
            
            if not html:
                logging.warning("⚠️ No summary HTML to send")
                return False
            
            # Get recipients from station "ALL" in email mapping (case-insensitive)
            all_recipients = []
            for station, station_data in station_email_mapping.items():
                if station.upper() == 'ALL':
                    if isinstance(station_data, dict):
                        all_recipients = station_data.get('emails', [])
                    else:
                        all_recipients = station_data if isinstance(station_data, list) else [station_data]
                    break
            
            # If "ALL" station not found, fall back to fixed recipients
            if not all_recipients:
                logging.warning("⚠️ No 'ALL' station found in email mapping. Using fixed recipients.")
                all_recipients = SUMMARY_EMAIL_RECIPIENTS
            else:
                logging.info(f"✅ Found {len(all_recipients)} recipients for station 'ALL': {', '.join(all_recipients)}")
            
            subject = "South Amazon - COD/RTS Pendency"
            return self.send_email(all_recipients, subject, html)
        except Exception as e:
            logging.error(f"❌ Error sending summary email: {e}")
            return False
    
    def send_high_value_emails(self, high_value_data, station_email_mapping):
        """Send High Value emails grouped by category"""
        try:
            logging.info("📧 Sending high value emails...")
            
            if not high_value_data:
                logging.warning("⚠️ No high value data to send")
                return 0
            
            # Group data by category
            category_items = self.group_data_by_category(
                high_value_data,
                station_email_mapping,
                station_field='Delivery_Station_Code'
            )
            
            emails_sent = 0
            # Get today's date for subject
            today = datetime.now().strftime('%d-%m-%Y')
            
            for item in category_items:
                html = self.create_high_value_html(item)
                region = item.get('region', '')
                if region:
                    subject = f"Amazon - High Value - Ageing - {region} - {today}"
                else:
                    subject = f"Amazon - High Value - Ageing/Potential Loss - {item['category']} - {today}"
                
                if self.send_email(item['emails'], subject, html):
                    emails_sent += 1
            
            logging.info(f"✅ Sent {emails_sent} high value emails")
            return emails_sent
        except Exception as e:
            logging.error(f"❌ Error sending high value emails: {e}")
            return 0
    
    def send_high_default_agents_emails(self, agents_data, station_email_mapping):
        """Send High Default Agents emails grouped by category"""
        try:
            logging.info("📧 Sending high default agents emails...")
            
            if not agents_data:
                logging.warning("⚠️ No high default agents data to send")
                return 0
            
            # Group data by category
            category_items = self.group_data_by_category(
                agents_data,
                station_email_mapping,
                station_field='Station_Code'
            )
            
            emails_sent = 0
            # Get today's date for subject
            today = datetime.now().strftime('%d-%m-%Y')
            
            for item in category_items:
                html = self.create_high_default_agents_html(item)
                region = item.get('region', '')
                if region:
                    subject = f"Amazon - Defaulting Agents - {region} - {today}"
                else:
                    subject = f"Amazon - Defaulting Agents - {item['category']} - {today}"
                
                if self.send_email(item['emails'], subject, html):
                    emails_sent += 1
            
            logging.info(f"✅ Sent {emails_sent} high default agents emails")
            return emails_sent
        except Exception as e:
            logging.error(f"❌ Error sending high default agents emails: {e}")
            return 0
    
    def run(self):
        """Main execution function"""
        try:
            logging.info("=" * 60)
            logging.info("AMAZON COD RTS EMAIL AUTOMATION")
            logging.info("=" * 60)
            
            # Step 1: Run the analysis script
            if not self.run_analysis_script():
                logging.error("❌ Analysis script failed. Exiting.")
                return False
            
            # Step 2: Get station-email mapping
            station_email_mapping = self.get_station_email_mapping()
            
            # Step 3: Get summary data
            summary_df = self.get_summary_data()
            
            # Step 4: Get high value data
            high_value_data = self.get_high_value_data()
            
            # Step 5: Get high default agents data
            agents_data = self.get_high_default_agents_data()
            
            # Step 6: Send summary email (to recipients mapped under "ALL")
            self.send_summary_email(summary_df, station_email_mapping)
            
            # Step 7: Send high value emails
            self.send_high_value_emails(high_value_data, station_email_mapping)
            
            # Step 8: Send high default agents emails
            self.send_high_default_agents_emails(agents_data, station_email_mapping)
            
            logging.info("=" * 60)
            logging.info("✅ EMAIL AUTOMATION COMPLETED")
            logging.info("=" * 60)
            return True
            
        except Exception as e:
            logging.error(f"❌ Automation failed: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return False


def main():
    """Main entry point"""
    automation = AmazonCODRTSEmailAutomation()
    success = automation.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

