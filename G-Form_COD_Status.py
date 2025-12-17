"""
G-Form COD Status Email Automation Script
Reads Google Sheets data, filters by hub names, and sends styled HTML email with latest 4 days status.
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google Sheets Configuration
SERVICE_ACCOUNT_FILE = "service_account_key.json"

# Google Sheets Details
SPREADSHEET_ID = "1F5wmvARWLYwZHEwLpM3SxW9R_hbJdInnRDnOdRBp-L0"
SERVICE_ACCOUNT_EMAIL = "emo-reports-automation@single-frame-467107-i1.iam.gserviceaccount.com"

# Worksheet name (will auto-detect if not specified)
WORKSHEET_NAME = "Status"  # Main worksheet name

# Number of days to fetch (latest 4 days)
DAYS_TO_FETCH = 4

# Hub Names (Fixed - extracted from Automatic_CPD_Quick_Check_Googlesheet_Reports.py)
TARGET_HUB_NAMES = [
    "BagaluruMDH_BAG",
    "KoorieeSoukyaRdODH_BLR",
    "KoorieeSoukyaRdTempODH_BLR",
    "SulebeleMDH_SUL",
    "DommasandraSplitODH_DMN",
    "HulimavuHub_BLR",
    "ThavarekereMDH_THK",
    "KoorieeHayathnagarODH_HYD",
    "SaidabadSplitODH_HYD",
    "CABTSRNagarODH_HYD",
    "LargeLogicKuniyamuthurODH_CJB",
    "TTSPLKodaikanalODH_KDI",
    "LargeLogicDharapuramODH_DHP",
    "TTSPLBatlagunduODH_BGU",
    "VadipattiMDH_VDP",
    "SITICSWadiODH_WDI",
    "BidarFortHub_BDR",
    "ElasticRunBidarODH_BDR",
    "NaubadMDH_BDR",
    "LargeLogicRameswaramODH_RMS",
    "LargelogicChinnamanurODH_CNM"
]

# Mapping of Hub to CLM Name (Fixed - extracted from Automatic_CPD_Quick_Check_Googlesheet_Reports.py)
HUB_CLM_MAPPING = {
    "BagaluruMDH_BAG": "Kishore",
    "NaubadMDH_BDR": "Haseem",
    "SITICSWadiODH_WDI": "Haseem",
    "VadipattiMDH_VDP": "Madvesh",
    "TTSPLKodaikanalODH_KDI": "Madvesh",
    "LargeLogicRameswaramODH_RMS": "Madvesh",
    "CABTSRNagarODH_HYD": "Asif, Haseem",
    "LargeLogicKuniyamuthurODH_CJB": "Madvesh",
    "KoorieeHayathnagarODH_HYD": "Asif, Haseem",
    "SulebeleMDH_SUL": "Kishore",
    "KoorieeSoukyaRdODH_BLR": "Kishore",
    "KoorieeSoukyaRdTempODH_BLR": "Kishore",
    "ThavarekereMDH_THK": "Irappa",
    "SaidabadSplitODH_HYD": "Asif, Haseem",
    "LargelogicChinnamanurODH_CNM": "Madvesh",
    "LargeLogicDharapuramODH_DHP": "Madvesh",
    "HulimavuHub_BLR": "Kishore",
    "ElasticRunBidarODH_BDR": "Haseem",
    "DommasandraSplitODH_DMN": "Kishore",
    "TTSPLBatlagunduODH_BGU": "Madvesh",
    "BidarFortHub_BDR": "Haseem",
}

# CLM Email Mapping
CLM_EMAIL = {
    "Asif": "abdulasif@loadshare.net",
    "Kishore": "kishorkumar.m@loadshare.net",
    "Haseem": "hasheem@loadshare.net",
    "Madvesh": "madvesh@loadshare.net",
    "Irappa": "irappa.vaggappanavar@loadshare.net",
    "Lokesh": "lokeshh@loadshare.net",
    "Bharath": "bharath.s@loadshare.net"
}

# Hub Email Mapping
HUB_EMAIL = {
    "BagaluruMDH_BAG": "bagalurumdh_bag@loadshare.net",
    "KoorieeSoukyaRdODH_BLR": "koorieesoukyardodh_blr@loadshare.net",
    "KoorieeSoukyaRdTempODH_BLR": "soukyaodh_blr@loadshare.net",
    "SulebeleMDH_SUL": "sulebelemdh_sul@loadshare.net",
    "DommasandraSplitODH_DMN": "dommasandrasplitodh_dmn@loadshare.net",
    "HulimavuHub_BLR": "hulimavuhub_blr@loadshare.net",
    "ThavarekereMDH_THK": "thavarekeremdh_thk@loadshare.net",
    "KoorieeHayathnagarODH_HYD": "koorieeodh_hyd@loadshare.net",
    "SaidabadSplitODH_HYD": "saidabad.spiltodh@loadshare.net",
    "CABTSRNagarODH_HYD": "cabtsrnagarodh_hyd@loadshare.net",
    "LargeLogicKuniyamuthurODH_CJB": "kuniyamuthurodh_cjb@loadshare.net",
    "TTSPLKodaikanalODH_KDI": "kodaikanalODH_KDI@loadshare.net",
    "LargeLogicDharapuramODH_DHP": "dharapuramodh_dhp@loadshare.net",
    "TTSPLBatlagunduODH_BGU": "batlagunduODH_BGU@loadshare.net",
    "VadipattiMDH_VDP": "vadipattimdh_vdp@loadshare.net",
    "SITICSWadiODH_WDI": "wadiodh@loadshare.net",
    "BidarFortHub_BDR": "bidarfort_bdr@loadshare.net",
    "ElasticRunBidarODH_BDR": "bidarodh_bdr@loadshare.net",
    "NaubadMDH_BDR": "naubadmdh_brr@loadshare.net",
    "LargeLogicRameswaramODH_RMS": "rameswaramodh_rms@loadshare.net",
    "LargelogicChinnamanurODH_CNM": "chinnamannur_cnm@loadshare.net",
}

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': os.getenv('GMAIL_SENDER_EMAIL', 'arunraj@loadshare.net'),
    'sender_password': os.getenv('GMAIL_APP_PASSWORD', 'ihczkvucdsayzrsu'),
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

def get_email_recipients():
    """Build email recipient lists dynamically"""
    # Get all hub emails
    hub_emails = list(HUB_EMAIL.values())
    
    # Get all CLM emails (handle multiple CLMs per hub)
    clm_emails = set()
    for hub, clm_names in HUB_CLM_MAPPING.items():
        # Handle multiple CLMs separated by comma
        clm_list = [name.strip() for name in clm_names.split(',')]
        for clm_name in clm_list:
            if clm_name in CLM_EMAIL:
                clm_emails.add(CLM_EMAIL[clm_name])
    
    # Add Lokesh, Bharath, and Maligai Rasmeen to TO list
    additional_to = [
        CLM_EMAIL.get("Lokesh", "lokeshh@loadshare.net"),
        CLM_EMAIL.get("Bharath", "bharath.s@loadshare.net"),
        "maligai.rasmeen@loadshare.net"
    ]
    
    # Combine all TO recipients (hubs + CLMs + Lokesh + Bharath + Maligai Rasmeen)
    to_recipients = list(set(hub_emails + list(clm_emails) + additional_to))
    
    # CC list: Empty
    cc_recipients = []
    
    return to_recipients, cc_recipients

# Manual header row configuration
MANUAL_HEADER_ROW_INDEX = 0  # Headers are in first row (0-based index: 0) - Row 1 in sheet

# ============================================================================
# GOOGLE SHEETS FUNCTIONS
# ============================================================================

def get_google_sheets_client():
    """Initialize and return Google Sheets client"""
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
        client = gspread.authorize(creds)
        logger.info("✅ Google Sheets client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"❌ Error initializing Google Sheets client: {e}")
        raise

def read_sheet_data(client, spreadsheet_id, worksheet_name=None):
    """Read all data from Google Sheets worksheet"""
    try:
        logger.info(f"📊 Reading data from Google Sheets...")
        spreadsheet = client.open_by_key(spreadsheet_id)
        logger.info(f"✅ Opened spreadsheet: {spreadsheet.title}")
        
        # Get worksheet
        worksheet = None
        if worksheet_name:
            for ws in spreadsheet.worksheets():
                if ws.title.lower() == worksheet_name.lower():
                    worksheet = ws
                    break
            
            if not worksheet:
                logger.warning(f"⚠️ Worksheet '{worksheet_name}' not found. Available worksheets:")
                for ws in spreadsheet.worksheets():
                    logger.info(f"   - {ws.title}")
                raise ValueError(f"Worksheet '{worksheet_name}' not found")
        else:
            # Use first worksheet
            worksheet = spreadsheet.worksheets()[0]
            logger.info(f"✅ Using first worksheet: {worksheet.title}")
        
        logger.info(f"✅ Using worksheet: {worksheet.title}")
        values = worksheet.get_all_values()
        
        if not values:
            logger.warning("⚠️ No data found in worksheet")
            return [], worksheet.title
        
        logger.info(f"✅ Read {len(values)} rows from Google Sheets")
        return values, worksheet.title
    except PermissionError:
        logger.error("=" * 60)
        logger.error("❌ PERMISSION DENIED")
        logger.error("=" * 60)
        logger.error(f"Please share the Google Sheet with the service account:")
        logger.error(f"   Email: {SERVICE_ACCOUNT_EMAIL}")
        logger.error("")
        logger.error("Steps to share:")
        logger.error("1. Open the Google Sheet:")
        logger.error(f"   https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
        logger.error("2. Click the 'Share' button (top right)")
        logger.error(f"3. Add this email: {SERVICE_ACCOUNT_EMAIL}")
        logger.error("4. Set permission to 'Viewer' (minimum required) or 'Editor'")
        logger.error("5. Uncheck 'Notify people' if you don't want to send notification")
        logger.error("6. Click 'Share' or 'Send'")
        logger.error("")
        logger.error("Note: Even if you only have view access, you can still share it")
        logger.error("      with the service account to allow the script to read data.")
        logger.error("=" * 60)
        raise
    except Exception as e:
        logger.error(f"❌ Error reading Google Sheets data: {e}")
        raise

# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def parse_date(date_str):
    """Parse date string in various formats"""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    # Try Excel serial number first
    try:
        if date_str.replace('.', '').replace('-', '').isdigit():
            excel_date = float(date_str)
            if excel_date > 59:
                excel_date -= 1
            from datetime import datetime as dt
            excel_epoch = dt(1899, 12, 30)
            parsed_datetime = excel_epoch + timedelta(days=excel_date)
            return parsed_datetime.date()
    except:
        pass
    
    # Remove time portion if present
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    
    # Try common date formats
    date_formats = [
        '%d-%b-%Y', '%d-%b-%y', '%d-%B-%Y', '%d-%b',
        '%Y-%m-%d', '%d-%m-%Y', '%m/%d/%Y', '%d/%m/%Y',
        '%Y/%m/%d', '%d.%m.%Y', '%m-%d-%Y', '%d/%m/%y',
        '%d-%b-%25', '%d-%b-%y',  # Handle year 25 format
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            result_date = parsed_date.date()
            
            if fmt == '%d-%b' or fmt == '%d-%B':
                current_year = datetime.now().year
                if result_date.year == 1900:
                    try:
                        result_date = datetime(current_year, result_date.month, result_date.day).date()
                    except ValueError:
                        try:
                            result_date = datetime(current_year + 1, result_date.month, result_date.day).date()
                        except ValueError:
                            result_date = datetime(current_year, result_date.month, min(result_date.day, 28)).date()
            
            # Handle year 25 as 2025
            if result_date.year == 1925 or (result_date.year < 100 and result_date.year == 25):
                result_date = datetime(2025, result_date.month, result_date.day).date()
            
            return result_date
        except:
            continue
    
    # Try pandas as last resort
    try:
        parsed_date = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(parsed_date):
            return parsed_date.date()
    except:
        pass
    
    return None

def find_hub_name_column(headers):
    """Find the 'Hub Name' column index"""
    for idx, header in enumerate(headers):
        if not header:
            continue
        header_str = str(header).strip().lower()
        # Look for columns containing "hub" and "name"
        if "hub" in header_str and "name" in header_str:
            return idx
        # Also check for just "hub name"
        if header_str == "hub name":
            return idx
    return None

def normalize_hub_name(hub_name):
    """Normalize hub name for matching"""
    if not hub_name:
        return ""
    normalized = str(hub_name).strip().lower().replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())

def process_cod_status_data(data):
    """
    Process COD status data and extract Hub Name, CLM Name, and latest 4 days
    Returns: (filtered_headers, filtered_data_rows)
    """
    try:
        logger.info("🔍 Processing COD status data...")
        
        if not data or len(data) < 2:
            logger.warning("⚠️ Insufficient data")
            return [], []
        
        # Step 1: Find header row
        header_row_idx = MANUAL_HEADER_ROW_INDEX if MANUAL_HEADER_ROW_INDEX is not None else 0
        if header_row_idx >= len(data):
            header_row_idx = 0
        
        headers = data[header_row_idx]
        data_rows = data[header_row_idx + 1:]
        
        logger.info(f"📋 Using row {header_row_idx + 1} as headers")
        logger.info(f"📊 Processing {len(data_rows)} data rows")
        
        # Step 2: Find Hub Name column
        hub_name_col_idx = find_hub_name_column(headers)
        if hub_name_col_idx is None:
            logger.error("❌ Hub Name column not found!")
            logger.info("Available headers:")
            for i, h in enumerate(headers):
                if h:
                    logger.info(f"   Column {i}: '{h}'")
            return [], []
        
        logger.info(f"✅ Found Hub Name column at index {hub_name_col_idx}: '{headers[hub_name_col_idx]}'")
        
        # Step 3: Filter by hub names
        logger.info(f"🔍 Filtering by {len(TARGET_HUB_NAMES)} target hubs...")
        filtered_rows = []
        hub_lookup = {h.lower(): h for h in TARGET_HUB_NAMES}
        hub_lookup.update({normalize_hub_name(h): h for h in TARGET_HUB_NAMES})
        
        for row in data_rows:
            if hub_name_col_idx < len(row) and row[hub_name_col_idx]:
                hub_name = str(row[hub_name_col_idx]).strip()
                hub_name_lower = hub_name.lower()
                hub_name_normalized = normalize_hub_name(hub_name)
                
                # Check if this hub matches any target hub
                matched_hub = None
                if hub_name_lower in hub_lookup:
                    matched_hub = hub_lookup[hub_name_lower]
                elif hub_name_normalized in hub_lookup:
                    matched_hub = hub_lookup[hub_name_normalized]
                elif hub_name in TARGET_HUB_NAMES:
                    matched_hub = hub_name
                
                if matched_hub:
                    filtered_rows.append(row)
        
        data_rows = filtered_rows
        logger.info(f"✅ Filtered to {len(data_rows)} rows matching target hubs")
        
        # Step 4: Find date columns in headers
        date_columns = []
        for col_idx, header in enumerate(headers):
            if header:
                parsed_date = parse_date(str(header).strip())
                if parsed_date:
                    date_columns.append((col_idx, header, parsed_date))
        
        if not date_columns:
            logger.warning("⚠️ No date columns found in headers")
            logger.info("Sample headers:")
            for i, h in enumerate(headers[:20]):
                if h:
                    logger.info(f"   Column {i}: '{h}'")
            return [], []
        
        logger.info(f"📅 Found {len(date_columns)} date columns")
        
        # Step 5: Filter to latest 4 dates (up to today)
        today = datetime.now().date()
        valid_dates = [(c, h, d) for c, h, d in date_columns if d <= today]
        valid_dates.sort(key=lambda x: x[2], reverse=True)  # Sort descending to get latest first
        latest_dates_sorted_desc = valid_dates[:DAYS_TO_FETCH]  # Get latest 4
        
        # Reverse to chronological order (earliest to latest) for display
        latest_dates = list(reversed(latest_dates_sorted_desc))
        
        latest_date_col_indices = {c for c, _, _ in latest_dates}
        logger.info(f"✅ Selected latest {len(latest_dates)} date columns in chronological order")
        for col_idx, header, parsed_date in latest_dates:
            logger.info(f"   Date: {parsed_date.strftime('%d-%b-%Y')} (Column: '{header}')")
        
        # Step 6: Build column mapping - columns to keep
        filtered_headers = []
        column_mapping = {}  # Maps header_name -> original_column_index
        
        # Add Hub Name first
        hub_name_header = headers[hub_name_col_idx]
        filtered_headers.append("Hub Name")
        column_mapping["Hub Name"] = hub_name_col_idx
        
        # Add date columns in chronological order (earliest to latest)
        date_header_mapping = {}
        for col_idx, header, parsed_date in latest_dates:
            # Format date as "DD-MMM-YY" (e.g., "06-Dec-25")
            formatted_date = parsed_date.strftime('%d-%b-%y')
            filtered_headers.append(formatted_date)
            column_mapping[formatted_date] = col_idx
            date_header_mapping[header] = formatted_date
        
        logger.info(f"✅ Final headers: {filtered_headers}")
        
        # Step 7: Process rows using DICTIONARY approach
        filtered_data = []
        
        for row in data_rows:
            # Skip empty rows
            if not row:
                continue
            
            # Create row dictionary
            row_dict = {}
            
            # Extract Hub Name
            if hub_name_col_idx < len(row):
                hub_name = str(row[hub_name_col_idx]).strip() if row[hub_name_col_idx] else ""
                row_dict["Hub Name"] = hub_name
            else:
                row_dict["Hub Name"] = ""
            
            # Extract date column values using formatted header names
            for col_idx, header, _ in latest_dates:
                value = ""
                if col_idx < len(row) and row[col_idx]:
                    value = str(row[col_idx]).strip()
                
                # Use formatted date header as key (e.g., "06-Dec-25")
                formatted_date = date_header_mapping.get(header, header)
                row_dict[formatted_date] = value
            
            filtered_data.append(row_dict)
        
        logger.info(f"✅ Processed {len(filtered_data)} rows")
        
        # Step 8: Sort by Hub Name (alphabetical)
        filtered_data.sort(key=lambda x: x.get("Hub Name", ""))
        
        # Step 9: Convert dictionaries to lists in header order
        filtered_data_rows = []
        for row_dict in filtered_data:
            row_list = []
            for header in filtered_headers:
                row_list.append(row_dict.get(header, ""))
            filtered_data_rows.append(row_list)
        
        logger.info(f"✅ Converted to {len(filtered_data_rows)} rows")
        
        # Step 10: Add Compliance % row
        total_hubs = len(filtered_data)
        compliance_row = []
        
        for header in filtered_headers:
            if header == "Hub Name":
                compliance_row.append("Compliance %")
            else:
                # Calculate compliance % for date columns
                # Count hubs with "Uploaded-Accepted" status
                uploaded_accepted_count = 0
                total_counted = 0
                
                for row_dict in filtered_data:
                    value = row_dict.get(header, "")
                    if value:
                        value_str = str(value).strip().lower()
                        total_counted += 1
                        # Check if status is "Uploaded-Accepted" or contains "accepted"
                        if "uploaded-accepted" in value_str or "accepted" in value_str:
                            uploaded_accepted_count += 1
                
                # Calculate percentage
                if total_counted > 0:
                    compliance_pct = (uploaded_accepted_count / total_counted) * 100
                    compliance_row.append(f"{compliance_pct:.1f}%")
                else:
                    compliance_row.append("0%")
        
        # Insert Compliance % row at the beginning
        filtered_data_rows.insert(0, compliance_row)
        
        logger.info(f"✅ Added Compliance % row (Total hubs: {total_hubs})")
        
        return filtered_headers, filtered_data_rows
    
    except Exception as e:
        logger.error(f"❌ Error processing data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

# ============================================================================
# HTML EMAIL FUNCTIONS
# ============================================================================

def create_styled_html_table(headers, data):
    """Create styled HTML table"""
    try:
        logger.info("🎨 Creating HTML table...")
        
        # Get date range from headers (skip Hub Name)
        date_headers = [h for h in headers if h not in ["Hub Name", "TOTAL"]]
        if date_headers:
            date_range_text = f"Latest {len(date_headers)} Days: {', '.join(date_headers)}"
        else:
            date_range_text = "Latest 4 Days Status"
        
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
            padding: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.2);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #FF6B35 0%, #F7931E 50%, #FFD23F 100%);
            color: white;
            padding: 10px 15px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 16px;
            font-weight: bold;
        }}
        .header p {{
            margin: 5px 0 0 0;
            font-size: 11px;
        }}
        @media only screen and (max-width: 600px) {{
            body {{
                padding: 3px;
            }}
            .container {{
                border-radius: 8px;
            }}
            .header {{
                padding: 6px 8px;
            }}
            .header h1 {{
            font-size: 12px;
            }}
            .header p {{
                font-size: 9px;
        }}
        .content {{
                padding: 3px;
            overflow-x: auto;
                -webkit-overflow-scrolling: touch;
            }}
            table {{
                font-size: 9px;
                min-width: 100%;
            }}
            th, td {{
                padding: 4px 3px;
                font-size: 9px;
            }}
            th:first-child, td:first-child {{
                min-width: 80px;
                max-width: 100px;
                font-size: 9px;
            }}
            th:not(:first-child):not(:last-child), td:not(:first-child):not(:last-child) {{
                min-width: 45px;
                max-width: 55px;
                font-size: 9px;
            }}
            th:last-child, td:last-child {{
                min-width: 50px;
                max-width: 60px;
                font-size: 9px;
            }}
            th:nth-last-child(2), td:nth-last-child(2) {{
                min-width: 50px;
                max-width: 60px;
                font-size: 9px;
            }}
            th:nth-last-child(3), td:nth-last-child(3) {{
                min-width: 50px;
                max-width: 60px;
                font-size: 9px;
            }}
        }}
        .content {{
            padding: 10px;
            overflow-x: auto;
            -webkit-overflow-scrolling: touch;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
            table-layout: auto;
            min-width: 100%;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 6px 4px;
            text-align: left;
            font-weight: 600;
            white-space: nowrap;
            font-size: 10px;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        th:first-child {{
            min-width: 100px;
            max-width: 130px;
        }}
        th:not(:first-child):not(:last-child):not(:nth-last-child(2)):not(:nth-last-child(3)) {{
            min-width: 45px;
            max-width: 55px;
        }}
        th:nth-last-child(3), th:nth-last-child(2), th:last-child {{
            min-width: 50px;
            max-width: 65px;
        }}
        td {{
            padding: 6px 4px;
            border-bottom: 1px solid #e0e0e0;
            background: white;
            font-size: 10px;
            white-space: nowrap;
        }}
        td:first-child {{
            min-width: 100px;
            max-width: 130px;
            word-wrap: break-word;
            white-space: normal;
        }}
        td:not(:first-child):not(:last-child):not(:nth-last-child(2)):not(:nth-last-child(3)) {{
            min-width: 45px;
            max-width: 55px;
            text-align: center;
        }}
        td:nth-last-child(3), td:nth-last-child(2), td:last-child {{
            min-width: 50px;
            max-width: 65px;
            text-align: center;
        }}
        tr:nth-child(even) td {{
            background: #f8f9fa;
        }}
        .total-row td {{
            background: #e3f2fd !important;
            font-weight: bold;
            border-top: 3px solid #2196f3;
        }}
        .total-row td:first-child {{
            background: linear-gradient(135deg, #2196f3 0%, #1976d2 100%) !important;
            color: white;
        }}
        .footer {{
            background: #f5f5f5;
            padding: 12px 20px;
            text-align: center;
            font-size: 10px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📋 G-Form COD Status Report</h1>
            <p>{date_range_text}</p>
        </div>
        <div class="content">
            <table>
"""
        
        # Header row
        html += '                <tr>\n'
        for header in headers:
            html += f'                    <th>{header if header else ""}</th>\n'
        html += '                </tr>\n'
        
        # Data rows
        for row_idx, row in enumerate(data, 1):
            is_total_row = (row_idx == 1 and row[0] == "Compliance %")
            row_class = "total-row" if is_total_row else ""
            html += f'                <tr class="{row_class}">\n'
            
            for col_idx, header in enumerate(headers):
                value = row[col_idx] if col_idx < len(row) else ""
                value = str(value) if value else ""
                
                # Format numbers - don't format Hub Name column
                is_hub_or_clm_column = (header == "Hub Name")
                
                if not is_hub_or_clm_column and not is_total_row:
                    try:
                        if value and value.upper() != "TOTAL":
                            clean = value.replace(',', '').replace(' ', '')
                            if clean and clean.replace('.', '').replace('-', '').isdigit():
                                num_value = float(clean)
                                if num_value == int(num_value):
                                    value = f"{int(num_value):,}"
                                else:
                                    value = f"{num_value:,.2f}"
                    except:
                        pass
                
                # Replace status text for compact display
                display_value = str(value) if value else ""
                if display_value:
                    # Replace "Uploaded-Accepted" with "Up-Accepted"
                    if "uploaded-accepted" in display_value.lower():
                        display_value = re.sub(r'[Uu]ploaded-[Aa]ccepted', 'Up-Accepted', display_value)
                    # Replace "Not Uploaded" with "Not-Up"
                    if "not uploaded" in display_value.lower():
                        display_value = re.sub(r'[Nn]ot [Uu]ploaded', 'Not-Up', display_value)
                
                # Apply color coding based on status value
                cell_style = ""
                if not is_total_row and not is_hub_or_clm_column:
                    value_lower = value.lower().strip()
                    if "uploaded-accepted" in value_lower or "accepted" in value_lower:
                        cell_style = 'background-color: #4caf50; color: #ffffff; font-weight: 700; border: 2px solid #2e7d32;'  # Bright green
                    elif "not uploaded" in value_lower or "not" in value_lower:
                        cell_style = 'background-color: #f44336; color: #ffffff; font-weight: 700; border: 2px solid #c62828;'  # Bright red
                    elif "cms absent" in value_lower or "absent" in value_lower:
                        cell_style = 'background-color: #ffc107; color: #000000; font-weight: 700; border: 2px solid #f57f17;'  # Bright yellow
                    elif "pending" in value_lower:
                        cell_style = 'background-color: #ff9800; color: #ffffff; font-weight: 700; border: 2px solid #e65100;'  # Bright orange
                
                html += f'                    <td style="{cell_style}">{display_value}</td>\n'
            html += '                </tr>\n'
        
        html += """            </table>
        </div>
        <div class="footer">
            <p>This report is automatically generated by the G-Form COD Status Automation System</p>
        </div>
    </div>
</body>
</html>"""
        
        logger.info("✅ HTML table created")
        return html
    except Exception as e:
        logger.error(f"❌ Error creating HTML: {e}")
        raise

# ============================================================================
# EMAIL FUNCTIONS
# ============================================================================

def send_email(html_content):
    """Send email with HTML content"""
    try:
        logger.info("📧 Preparing email...")
        
        if not EMAIL_CONFIG['sender_password']:
            logger.error("❌ Gmail App Password not set!")
            logger.error("   Set it via environment variable: GMAIL_APP_PASSWORD")
            logger.warning("⚠️  Skipping email send. HTML content generated successfully.")
            return
        
        # Get email recipients dynamically
        to_recipients, cc_recipients = get_email_recipients()
        
        # Log detailed recipient information before sending
        logger.info(f"📧 Email recipients configured:")
        logger.info(f"\n{'='*60}")
        logger.info(f"📬 TO RECIPIENTS ({len(to_recipients)} total):")
        logger.info(f"{'='*60}")
        
        # Get hub emails
        hub_emails = list(HUB_EMAIL.values())
        logger.info(f"\n🏢 Hub Emails ({len(hub_emails)}):")
        for hub_name, hub_email in sorted(HUB_EMAIL.items()):
            logger.info(f"   • {hub_name}: {hub_email}")
        
        # Get CLM emails
        clm_emails = set()
        clm_details = {}
        for hub, clm_names in HUB_CLM_MAPPING.items():
            clm_list = [name.strip() for name in clm_names.split(',')]
            for clm_name in clm_list:
                if clm_name in CLM_EMAIL:
                    clm_email = CLM_EMAIL[clm_name]
                    clm_emails.add(clm_email)
                    if clm_name not in clm_details:
                        clm_details[clm_name] = clm_email
        
        logger.info(f"\n👤 CLM Emails ({len(clm_details)}):")
        for clm_name, clm_email in sorted(clm_details.items()):
            logger.info(f"   • {clm_name}: {clm_email}")
        
        # Additional TO recipients
        logger.info(f"\n➕ Additional TO Recipients:")
        logger.info(f"   • Lokesh: {CLM_EMAIL.get('Lokesh', 'lokeshh@loadshare.net')}")
        logger.info(f"   • Bharath: {CLM_EMAIL.get('Bharath', 'bharath.s@loadshare.net')}")
        logger.info(f"   • Maligai Rasmeen: maligai.rasmeen@loadshare.net")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📋 CC RECIPIENTS ({len(cc_recipients)} total):")
        logger.info(f"{'='*60}")
        for cc_email in cc_recipients:
            logger.info(f"   • {cc_email}")
        logger.info(f"{'='*60}\n")
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ', '.join(to_recipients)
        
        # Add CC recipients
        if cc_recipients:
            msg['Cc'] = ', '.join(cc_recipients)
        
        today_datetime = datetime.now()
        today_date = today_datetime.strftime('%d-%b-%Y')
        today_time = today_datetime.strftime('%H:%M')
        msg['Subject'] = f"South - COD (Gform) - Status - {today_date} {today_time}"
        
        msg.attach(MIMEText(html_content, 'html'))
        
        logger.info(f"🔗 Connecting to SMTP server: {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']}")
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        logger.info("🔐 Logging in...")
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        
        logger.info("📤 Sending email...")
        # All recipients (To + CC list)
        all_recipients = to_recipients + cc_recipients
        text = msg.as_string()
        server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, text)
        server.quit()
        
        logger.info("✅ Email sent successfully!")
        logger.info(f"\n{'='*60}")
        logger.info(f"📧 Email Summary:")
        logger.info(f"{'='*60}")
        logger.info(f"   Subject: {msg['Subject']}")
        logger.info(f"   To: {len(to_recipients)} recipients")
        logger.info(f"   CC: {len(cc_recipients)} recipients")
        logger.info(f"{'='*60}")
    except Exception as e:
        logger.error(f"❌ Error sending email: {e}")
        raise

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    try:
        logger.info("=" * 60)
        logger.info("🚀 Starting G-Form COD Status Email Automation")
        logger.info("=" * 60)
        
        # Step 1: Initialize Google Sheets client
        client = get_google_sheets_client()
        
        # Step 2: Read data from Google Sheet
        logger.info("\n📊 Step 1: Reading data from Google Sheet...")
        data, worksheet_title = read_sheet_data(client, SPREADSHEET_ID, WORKSHEET_NAME)
        
        if not data:
            logger.error("❌ No data to process")
            return
        
        logger.info(f"✅ Read {len(data)} rows from worksheet: {worksheet_title}")
        
        # Step 3: Process COD status data
        logger.info("\n🔍 Step 2: Processing COD status data...")
        headers, filtered_data = process_cod_status_data(data)
        
        if not headers or not filtered_data:
            logger.warning("⚠️ No data after processing")
            return
        
        # Step 4: Create HTML
        logger.info("\n🎨 Step 3: Creating HTML email...")
        html_content = create_styled_html_table(headers, filtered_data)
        
        # Step 5: Send email
        logger.info("\n📧 Step 4: Sending email...")
        send_email(html_content)
        
        logger.info("\n" + "=" * 60)
        logger.info("✅ G-Form COD Status Email Automation completed successfully!")
        logger.info("=" * 60)
        
    except PermissionError as e:
        logger.error("=" * 60)
        logger.error(f"❌ Permission Error: {e}")
        logger.error(f"Please share the Google Sheet with: {SERVICE_ACCOUNT_EMAIL}")
        logger.error("   Go to Google Sheet > Share > Add the service account email with Editor access")
        logger.error("=" * 60)
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Error: {e}")
        logger.error("=" * 60)
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
