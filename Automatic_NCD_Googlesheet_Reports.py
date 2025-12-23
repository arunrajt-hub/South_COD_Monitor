import time
import pandas as pd
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import string
from collections import Counter
import signal
import sys
import glob
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': 'arunraj@loadshare.net',  # LoadShare email
    'sender_password': 'ihczkvucdsayzrsu',  # Gmail App Password
    'recipient_email': 'arunraj@loadshare.net',  # Test recipient (same as sender)
    'smtp_server': 'smtp.gmail.com',  # Gmail SMTP
    'smtp_port': 587
}

# Path to your ChromeDriver
CHROMEDRIVER_PATH = r"C:\Users\Lsn-Arun\Downloads\chromedriver-win64\chromedriver.exe"

# Google Sheets Configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# You'll need to create a service account and download the JSON key file
# Place the JSON file in the same directory as this script
SERVICE_ACCOUNT_FILE = 'service_account_key.json'  # Update this to your JSON file name

# Global flag to track if tracking IDs have been processed
tracking_ids_processed = False

def upload_to_google_sheets(results):
    """Upload results to Google Sheets with EKL header formatting and grand total row"""
    try:
        # Authenticate with Google Sheets
        credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        gc = gspread.authorize(credentials)
        
        # Open the spreadsheet
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        
        # Clear existing data
        worksheet.clear()
        
        if not results:
            print_detailed_log("No data to upload to Google Sheets", "WARNING")
            return
        
        # Create DataFrame from results
        df = pd.DataFrame(results)
        
        # Calculate grand totals
        grand_total_row = {}
        for col in df.columns:
            if col == 'Hub Name':
                grand_total_row[col] = 'GRAND TOTAL'
            elif col in ['CLM Name', 'State']:
                grand_total_row[col] = ''  # Empty for CLM Name and State
            elif col in ncd_cpd_categories + ekl_cpd_categories + ['Total NCD Breaches']:
                grand_total_row[col] = df[col].sum()
            else:
                grand_total_row[col] = ''
        
        # Add grand total row to DataFrame
        grand_total_df = pd.DataFrame([grand_total_row])
        df_with_total = pd.concat([df, grand_total_df], ignore_index=True)
        
        # Upload data with grand total
        set_with_dataframe(worksheet, df_with_total, row=1, col=1, include_index=False, include_column_header=True)
        
        # Format EKL columns with light blue headers
        try:
            # Find EKL column indices
            ekl_0_col = None
            ekl_1_col = None
            
            for i, col in enumerate(df_with_total.columns):
                if col == 'EKL_0_Days':
                    ekl_0_col = i + 1  # Google Sheets is 1-indexed
                elif col == 'EKL_1_Days':
                    ekl_1_col = i + 1
            
            # Format EKL headers with light blue background
            if ekl_0_col:
                worksheet.format(f'{chr(64 + ekl_0_col)}1', {
                    'backgroundColor': {'red': 0.7, 'green': 0.9, 'blue': 1.0},  # Light blue
                    'textFormat': {'bold': True}
                })
            
            if ekl_1_col:
                worksheet.format(f'{chr(64 + ekl_1_col)}1', {
                    'backgroundColor': {'red': 0.7, 'green': 0.9, 'blue': 1.0},  # Light blue
                    'textFormat': {'bold': True}
                })
            
            print_detailed_log("✅ Successfully formatted EKL headers with light blue color", "SUCCESS")
            
        except Exception as format_error:
            print_detailed_log(f"⚠️ Could not format EKL headers: {format_error}", "WARNING")
        
        # Format grand total row with bold text
        try:
            total_row_num = len(df_with_total)  # Last row
            for col_idx, col in enumerate(df_with_total.columns):
                col_letter = chr(64 + col_idx + 1)  # Convert to column letter
                worksheet.format(f'{col_letter}{total_row_num}', {
                    'textFormat': {'bold': True}
                })
            
            print_detailed_log("✅ Successfully formatted grand total row with bold text", "SUCCESS")
            
        except Exception as total_format_error:
            print_detailed_log(f"⚠️ Could not format grand total row: {total_format_error}", "WARNING")
        
        # Add summary information below the data
        try:
            summary_start_row = len(df_with_total) + 3  # 2 rows gap
            
            # Add summary headers
            summary_data = [
                ['', ''],  # Empty row
                ['SUMMARY:', ''],  # Summary header
                ['Total Hubs Processed:', len(results)],
                ['Total NCD Breaches:', grand_total_row.get('Total NCD Breaches', 0)],
                ['Total EKL Breaches:', grand_total_row.get('EKL_0_Days', 0) + grand_total_row.get('EKL_1_Days', 0)],
                ['', ''],  # Empty row
                ['NCD BREAKDOWN:', ''],
                ['NCD_0_Days (Today):', grand_total_row.get('NCD_0_Days', 0)],
                ['NCD_1_Days (Yesterday):', grand_total_row.get('NCD_1_Days', 0)],
                ['NCD_2_Days (2 days ago):', grand_total_row.get('NCD_2_Days', 0)],
                ['NCD_3_Days (3 days ago):', grand_total_row.get('NCD_3_Days', 0)],
                ['NCD_>_3_Days (>3 days ago):', grand_total_row.get('NCD_>_3_Days', 0)],
                ['NCD_FDD (Future):', grand_total_row.get('NCD_FDD', 0)],
                ['', ''],  # Empty row
                ['EKL BREAKDOWN:', ''],
                ['EKL_0_Days (Today):', grand_total_row.get('EKL_0_Days', 0)],
                ['EKL_1_Days (Yesterday):', grand_total_row.get('EKL_1_Days', 0)],
                ['', ''],  # Empty row
                ['Last Updated:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
            ]
            
            # Upload summary data
            worksheet.update(values=summary_data, range_name=f'A{summary_start_row}:B{summary_start_row + len(summary_data) - 1}')
            
            # Format summary headers with bold text
            worksheet.format(f'A{summary_start_row + 1}:A{summary_start_row + 1}', {'textFormat': {'bold': True}})  # SUMMARY:
            worksheet.format(f'A{summary_start_row + 6}:A{summary_start_row + 6}', {'textFormat': {'bold': True}})  # NCD BREAKDOWN:
            worksheet.format(f'A{summary_start_row + 14}:A{summary_start_row + 14}', {'textFormat': {'bold': True}})  # EKL BREAKDOWN:
            
            print_detailed_log("✅ Successfully added summary information to Google Sheets", "SUCCESS")
            
        except Exception as summary_error:
            print_detailed_log(f"⚠️ Could not add summary information: {summary_error}", "WARNING")
        
        print_detailed_log("✅ Data successfully uploaded to Google Sheets with grand total and summary", "SUCCESS")
        
    except Exception as e:
        print_detailed_log(f"❌ Error uploading to Google Sheets: {e}", "ERROR")

def find_latest_csv_file():
    """
    Find the most recently downloaded CSV file from Downloads folder
    """
    try:
        import os
        import glob
        from datetime import datetime, timedelta
        
        # Look for CSV files in the Downloads folder
        download_dir = os.path.expanduser("~/Downloads")
        print_detailed_log(f"🔍 Looking for CSV files in: {download_dir}", "INFO")
        
        # Force refresh the directory listing to get latest files
        print_detailed_log(f"🔄 Refreshing Downloads folder to get latest files...", "INFO")
        try:
            # Force a directory refresh by listing the directory
            os.listdir(download_dir)
            time.sleep(1)  # Small delay to ensure file system is updated
        except Exception as refresh_error:
            print_detailed_log(f"⚠️ Could not refresh directory: {refresh_error}", "WARNING")
        
        # Look for files with "shipments" pattern specifically (case insensitive)
        # Priority: Look for exact pattern first, then broader patterns
        patterns = [
            # Exact pattern: shipments MM_DD_YYYY, H_MM_SS AM/PM.csv
            os.path.join(download_dir, "shipments *.csv"),
            os.path.join(download_dir, "Shipments *.csv"),
            os.path.join(download_dir, "SHIPMENTS *.csv"),
            # Broader patterns
            os.path.join(download_dir, "shipments*.csv"),
            os.path.join(download_dir, "Shipments*.csv"),
            os.path.join(download_dir, "SHIPMENTS*.csv"),
            os.path.join(download_dir, "*shipments*.csv"),
            os.path.join(download_dir, "*Shipments*.csv")
        ]
        
        csv_files = []
        for pattern in patterns:
            csv_files.extend(glob.glob(pattern))
        
        # Remove duplicates
        csv_files = list(set(csv_files))
        
        # Filter and prioritize files with the exact pattern: "shipments MM_DD_YYYY, H_MM_SS AM/PM.csv"
        import re
        exact_pattern_files = []
        other_files = []
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            # Check for exact pattern: "shipments M_D_YYYY, H_MM_SS AM/PM.csv"
            # Example: "shipments 9_17_2025, 7_17_35 PM.csv"
            if re.match(r'^shipments \d{1,2}_\d{1,2}_\d{4}, \d{1,2}_\d{2}_\d{2} [AP]M\.csv$', filename, re.IGNORECASE):
                exact_pattern_files.append(file_path)
            else:
                other_files.append(file_path)
        
        # Prioritize exact pattern files
        if exact_pattern_files:
            csv_files = exact_pattern_files + other_files
            print_detailed_log(f"📁 Found {len(exact_pattern_files)} files with exact pattern, {len(other_files)} other shipment files", "INFO")
        else:
            print_detailed_log(f"📁 Found {len(csv_files)} shipment CSV files (no exact pattern matches)", "INFO")
        
        if not csv_files:
            # If no shipments files, look for any CSV files
            csv_pattern = os.path.join(download_dir, "*.csv")
            csv_files = glob.glob(csv_pattern)
            print_detailed_log(f"📁 Found {len(csv_files)} total CSV files", "INFO")
        
        if not csv_files:
            print_detailed_log("❌ No CSV files found in Downloads folder", "ERROR")
            return None
        
        # Find the most recent file with detailed logging
        if csv_files:
            print_detailed_log(f"📋 All found CSV files:", "INFO")
            for i, file_path in enumerate(csv_files):
                file_time = datetime.fromtimestamp(os.path.getctime(file_path))
                print_detailed_log(f"  {i+1}. {os.path.basename(file_path)} (created: {file_time})", "INFO")
        
        latest_file = max(csv_files, key=os.path.getctime)
        file_time = datetime.fromtimestamp(os.path.getctime(latest_file))
        
        print_detailed_log(f"📁 Most recent CSV file: {os.path.basename(latest_file)}", "INFO")
        print_detailed_log(f"📅 File created at: {file_time}", "INFO")
        print_detailed_log(f"📂 Full path: {latest_file}", "INFO")
        
        # Check if this file matches the expected pattern
        filename = os.path.basename(latest_file)
        if re.match(r'^shipments \d{1,2}_\d{1,2}_\d{4}, \d{1,2}_\d{2}_\d{2} [AP]M\.csv$', filename, re.IGNORECASE):
            print_detailed_log(f"✅ Found file with exact expected pattern: {filename}", "SUCCESS")
        else:
            print_detailed_log(f"⚠️ File doesn't match exact pattern but will be used: {filename}", "WARNING")
        
        # Check if it was created in the last 3 minutes (more generous timeout)
        time_diff = datetime.now() - file_time
        if time_diff < timedelta(minutes=3):
            print_detailed_log(f"✅ Found recent CSV file: {os.path.basename(latest_file)} (created {time_diff.seconds}s ago)", "SUCCESS")
            return latest_file
        else:
            print_detailed_log(f"⚠️ CSV file is older than 3 minutes: {os.path.basename(latest_file)} (created {time_diff.seconds}s ago)", "WARNING")
            # Still return it as it might be the correct file, especially if it's the only one
            print_detailed_log(f"📄 Using this file anyway as it's the most recent available", "INFO")
            return latest_file

    except Exception as e:
        print_detailed_log(f"Error finding latest CSV file: {e}", "ERROR")
        return None

# Google Sheets ID - You'll need to create a Google Sheet and get its ID from the URL
SPREADSHEET_ID = '1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM'
# Set worksheet/tab name to 'breaches' only
WORKSHEET_NAME = 'breaches'  # Name of the worksheet to write data to

# List of all 21 hubs
HUBS = [
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

# Define the NCD breach CPD categories to track
ncd_cpd_categories = [
    'NCD_0_Days',    # CPD = Today
    'NCD_1_Days',    # CPD = Yesterday
    'NCD_2_Days',    # CPD = 2 days ago
    'NCD_3_Days',    # CPD = 3 days ago
    'NCD_>_3_Days',  # CPD > 3 days ago
    'NCD_FDD'        # CPD > Today (Future Delivery Date)
]

# Define the EKL breach CPD categories to track (only 3 EKL statuses)
ekl_cpd_categories = [
    'EKL_0_Days',    # CPD = Today (only 3 EKL statuses)
    'EKL_1_Days'     # CPD = Yesterday (only 3 EKL statuses)
]


# Define the specific NCD breach statuses to filter (only these will be processed)
breach_statuses = [
    'Undelivered_Heavy_Rain',
    'Undelivered_HeavyLoad',
    'Undelivered_Security_Instability', 
    'Undelivered_Shipment_Damage',
    'Undelivered_Not_Attended',
    'Undelivered_UntraceableFromHub',
    'Untraceable'
]

# Note: CPD_NCD_Breaches columns are based on customer_promise_date ageing (filtered NCD breach statuses)
#       CPD_EKL columns are based on customer_promise_date ageing (only 3 EKL statuses: Security_Instability, Heavy_Rain, Heavy_Load)

# Mapping of hub to CLM Name and State
HUB_INFO = {
    "BagaluruMDH_BAG": ("Kishore", "Karnataka"),
    "KoorieeSoukyaRdODH_BLR": ("Kishore", "Karnataka"),
    "KoorieeSoukyaRdTempODH_BLR": ("Kishore", "Karnataka"),
    "SulebeleMDH_SUL": ("Kishore", "Karnataka"),
    "DommasandraSplitODH_DMN": ("Kishore", "Karnataka"),
    "HulimavuHub_BLR": ("Kishore", "Karnataka"),
    "ThavarekereMDH_THK": ("Irappa", "Karnataka"),
    "KoorieeHayathnagarODH_HYD": ("Asif, Haseem", "Telengana"),
    "SaidabadSplitODH_HYD": ("Asif, Haseem", "Telengana"),
    "CABTSRNagarODH_HYD": ("Asif, Haseem", "Telengana"),
    "LargeLogicKuniyamuthurODH_CJB": ("Madvesh", "Tamil Nadu"),
    "TTSPLKodaikanalODH_KDI": ("Madvesh", "Tamil Nadu"),
    "LargeLogicDharapuramODH_DHP": ("Madvesh", "Tamil Nadu"),
    "TTSPLBatlagunduODH_BGU": ("Madvesh", "Tamil Nadu"),
    "VadipattiMDH_VDP": ("Madvesh", "Tamil Nadu"),
    "SITICSWadiODH_WDI": ("Haseem", "Karnataka"),
    "BidarFortHub_BDR": ("Haseem", "Karnataka"),
    "ElasticRunBidarODH_BDR": ("Haseem", "Karnataka"),
    "NaubadMDH_BDR": ("Haseem", "Karnataka"),
    "LargeLogicRameswaramODH_RMS": ("Madvesh", "Tamil Nadu"),
    "LargelogicChinnamanurODH_CNM": ("Madvesh", "Tamil Nadu"),
}

# Mapping of Hub to Email ID
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

# CLM Email Mapping
CLM_EMAIL = {
    "Asif": "abdulasif@loadshare.net",
    "Kishore": "kishorkumar.m@loadshare.net",
    "Haseem": "hasheem@loadshare.net",
    "Madvesh": "madvesh@loadshare.net",
    "Kannan": "kannan@loadshare.net",
    "Irappa": "irappa.vaggappanavar@loadshare.net",
    "Nithesh": "Nitheshkumar.a@loadshare.net",
    "Lokesh": "lokeshh@loadshare.net"
}

def print_detailed_log(message, level="INFO"):
    """Print detailed log with timestamp and level - only show summary data"""
    # Only show summary data, skip all progress logs
    if level == "DATA" and ("SUMMARY" in message or "AGEING BIFURCATION" in message or "Total hubs" in message or "Total NCD Breaches" in message or "Total Records" in message or "Days:" in message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] 📊 {message}")
    elif level == "SUCCESS" and ("Script completed" in message or "Data successfully uploaded" in message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] ✅ {message}")
    elif level == "ERROR":
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] ❌ {message}")
    # Skip all other logs (INFO, PROGRESS, WARNING)

def check_driver_health(driver):
    """Check if the WebDriver is still responsive and restart if needed"""
    try:
        # Try to get the current URL to test if driver is responsive
        current_url = driver.current_url
        return True
    except Exception as e:
        print_detailed_log(f"⚠️ WebDriver connection lost: {e}", "WARNING")
        return False

def restart_driver():
    """Restart the Chrome WebDriver with the same configuration"""
    try:
        print_detailed_log("🔄 Restarting Chrome WebDriver...", "PROGRESS")
        
        # Close existing driver if it exists
        try:
            if 'driver' in globals() and driver:
                driver.quit()
        except:
            pass
        
        # Create new driver with same configuration
        service = Service(CHROMEDRIVER_PATH)
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--start-maximized")
        
        # Add user data directory to maintain login state
        user_data_dir = os.path.expanduser("~\\AppData\\Local\\Google\\Chrome\\User Data")
        if os.path.exists(user_data_dir):
            chrome_options.add_argument(f"--user-data-dir={user_data_dir}")
        
        new_driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Navigate back to the portal
        new_driver.get("https://portal.delhivery.com/")
        print_detailed_log("✅ WebDriver restarted successfully", "SUCCESS")
        
        return new_driver
    except Exception as e:
        print_detailed_log(f"❌ Failed to restart WebDriver: {e}", "ERROR")
        return None

def send_email_report(results, filtered_tracking_counts, filtered_ekl_tracking_counts):
    """Send email report with breach data summary"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = EMAIL_CONFIG['recipient_email']
        msg['Subject'] = f"Breach Report - {datetime.now().strftime('%d %b %Y %H:%M')}"
        
        # Calculate totals
        total_hubs = len(results)
        total_ncd_0 = sum(result.get('NCD_0_Days', 0) for result in results)
        total_ncd_1 = sum(result.get('NCD_1_Days', 0) for result in results)
        total_ekl_0 = sum(result.get('EKL_0_Days', 0) for result in results)
        total_ekl_1 = sum(result.get('EKL_1_Days', 0) for result in results)
        
        # Calculate BagStatus filtered totals
        bagstatus_ncd_0 = sum(counts.get('0 Days', 0) for counts in filtered_tracking_counts.values())
        bagstatus_ncd_1 = sum(counts.get('1 Day', 0) for counts in filtered_tracking_counts.values())
        bagstatus_ekl_0 = sum(counts.get('0 Days', 0) for counts in filtered_ekl_tracking_counts.values())
        bagstatus_ekl_1 = sum(counts.get('1 Day', 0) for counts in filtered_ekl_tracking_counts.values())
        
        # Create HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .hub-data {{ background-color: #f9f9f9; padding: 10px; border-radius: 5px; margin: 5px 0; }}
                .total {{ background-color: #e3f2fd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .success {{ color: green; }}
                .warning {{ color: orange; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚚 Breach Report Summary</h2>
                <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                <p><strong>Total Hubs Processed:</strong> {total_hubs}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Main Data Summary (Before BagStatus Filtering)</h3>
                <table>
                    <tr>
                        <th>Hub Name</th>
                        <th>CPD_NCD (0 Day)</th>
                        <th>CPD_NCD (1 Day)</th>
                        <th>CPD_EKL (0 Days)</th>
                        <th>CPD_EKL (1 Days)</th>
                        <th>Total</th>
                    </tr>
        """
        
        # Add hub data
        for result in results:
            hub_name = result['Hub Name']
            ncd_0 = result.get('NCD_0_Days', 0)
            ncd_1 = result.get('NCD_1_Days', 0)
            ekl_0 = result.get('EKL_0_Days', 0)
            ekl_1 = result.get('EKL_1_Days', 0)
            total = ncd_0 + ncd_1 + ekl_0 + ekl_1
            
            html_body += f"""
                    <tr>
                        <td>{hub_name}</td>
                        <td>{ncd_0}</td>
                        <td>{ncd_1}</td>
                        <td>{ekl_0}</td>
                        <td>{ekl_1}</td>
                        <td>{total}</td>
                    </tr>
            """
        
        html_body += f"""
                </table>
                
                <h4>Grand Totals (Before BagStatus Filtering):</h4>
                <p><strong>CPD_NCD (0 Days):</strong> {total_ncd_0} | <strong>CPD_NCD (1 Day):</strong> {total_ncd_1}</p>
                <p><strong>CPD_EKL (0 Days):</strong> {total_ekl_0} | <strong>CPD_EKL (1 Days):</strong> {total_ekl_1}</p>
                <p><strong>Total Records:</strong> {total_ncd_0 + total_ncd_1 + total_ekl_0 + total_ekl_1}</p>
            </div>
            
            <div class="total">
                <h3>🔍 BagStatus Filtered Summary (After CLOSED Removal)</h3>
                <p><strong>CPD_NCD (0 Days):</strong> {bagstatus_ncd_0} | <strong>CPD_NCD (1 Day):</strong> {bagstatus_ncd_1}</p>
                <p><strong>CPD_EKL (0 Days):</strong> {bagstatus_ekl_0} | <strong>CPD_EKL (1 Days):</strong> {bagstatus_ekl_1}</p>
                <p><strong>Total Filtered Records:</strong> {bagstatus_ncd_0 + bagstatus_ncd_1 + bagstatus_ekl_0 + bagstatus_ekl_1}</p>
                <p><strong>CLOSED Cases Removed:</strong> {(total_ncd_0 + total_ncd_1 + total_ekl_0 + total_ekl_1) - (bagstatus_ncd_0 + bagstatus_ncd_1 + bagstatus_ekl_0 + bagstatus_ekl_1)}</p>
            </div>
            
            <div class="warning">
                <p><strong>Note:</strong> This report is automatically generated. The data has been uploaded to Google Sheets.</p>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML content
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send email
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        text = msg.as_string()
        server.sendmail(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['recipient_email'], text)
        server.quit()
        
        print_detailed_log(f"Email sent successfully to {EMAIL_CONFIG['recipient_email']}", "SUCCESS")
        
    except Exception as e:
        print_detailed_log(f"Error sending email: {e}", "ERROR")
        raise e

def send_ageing_10k_email_to_clms(ageing_10k_data):
    """Send consolidated Ageing > 5K data to all CLMs with subject 'Potential Fraud Cases - South - Date'"""
    try:
        if not ageing_10k_data:
            print_detailed_log("No Ageing > 5K data found - skipping email", "INFO")
            return True
        
        # Create DataFrame from the data
        df = pd.DataFrame(ageing_10k_data)
        
        # Get all unique CLMs from the data
        unique_clms = df['CLM Name'].unique()
        
        # Get all CLM emails (handle comma-separated CLM names)
        clm_emails = []
        for clm_name in unique_clms:
            # Handle comma-separated CLM names (e.g., "Asif, Haseem")
            clm_names_list = [name.strip() for name in clm_name.split(',')]
            
            for individual_clm_name in clm_names_list:
                clm_email = CLM_EMAIL.get(individual_clm_name, '')
                if clm_email:
                    if clm_email not in clm_emails:  # Avoid duplicates
                        clm_emails.append(clm_email)
                else:
                    print_detailed_log(f"⚠️ No email found for CLM: {individual_clm_name} - skipping", "WARNING")
        
        if not clm_emails:
            print_detailed_log("No valid CLM emails found - skipping email", "WARNING")
            return True
        
        # Get all hub emails from the data
        unique_hubs_in_data = df['Hub Name'].unique()
        hub_emails = []
        for hub_name in unique_hubs_in_data:
            hub_email = HUB_EMAIL.get(hub_name, '')
            if hub_email:
                hub_emails.append(hub_email)
            else:
                print_detailed_log(f"⚠️ No email found for hub: {hub_name} - skipping", "WARNING")
        
        # Combine CLM emails and hub emails for TO recipients
        to_recipients = clm_emails + hub_emails
        
        # Create consolidated message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ', '.join(to_recipients + ['maligai.rasmeen@loadshare.net'])
        msg['Cc'] = 'lokeshh@loadshare.net, arunraj@loadshare.net'
        msg['Subject'] = f"Potential Fraud Cases - South - {datetime.now().strftime('%d %b %Y')}"
                
        # Calculate consolidated totals
        total_records = len(df)
        total_amount = df['Amount'].sum()
        unique_hubs = df['Hub Name'].unique()
        hub_list = ', '.join(unique_hubs)
        clm_list = ', '.join(unique_clms)
        
        # Create HTML email body
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #f0f0f0; padding: 10px; border-radius: 5px; }}
                .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                .warning {{ background-color: #fff3cd; padding: 10px; border-radius: 5px; margin: 10px 0; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #4CAF50; color: white; }}
                .high-value {{ background-color: #ffebee; }}
                .medium-value {{ background-color: #fff3e0; }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>Potential Fraud Cases - South</h2>
                <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                <p><strong>CLMs:</strong> {clm_list}</p>
                <p><strong>Hubs:</strong> {hub_list}</p>
            </div>
            
            <div class="summary">
                <h3>Consolidated Summary</h3>
                <p><strong>Total Records:</strong> {total_records}</p>
                <p><strong>Total Amount:</strong> ₹{total_amount:,.0f}</p>
                <p><strong>CLMs Involved:</strong> {len(unique_clms)}</p>
                <p><strong>Hubs Involved:</strong> {len(unique_hubs)}</p>
            </div>
            
            <div class="warning">
                <h3>High-Value Ageing Cases - Action Required</h3>
                <table>
                    <tr>
                        <th>Hub Name</th>
                        <th>CLM Name</th>
                        <th>Tracking ID</th>
                        <th>Amount</th>
                        <th>Ageing (Days)</th>
                        <th>Status</th>
                        <th>Category</th>
                    </tr>
        """
                
        # Add tracking id data for all records
        for _, row in df.iterrows():
            amount = row['Amount']
            category = row['Category']
            row_class = 'high-value' if '>25K' in category else 'medium-value'
            
            html_body += f"""
                    <tr class="{row_class}">
                        <td>{row['Hub Name']}</td>
                        <td>{row['CLM Name']}</td>
                        <td>{row['Tracking ID']}</td>
                        <td>₹{amount:,.0f}</td>
                        <td>{row['Ageing (Days)']}</td>
                        <td>{row['Status']}</td>
                        <td>{category}</td>
                    </tr>
        """
        
        html_body += """
                </table>
            </div>
            
            <div class="warning">
                <p><strong>Action Required:</strong> Please review these high-value ageing cases and take appropriate action.</p>
                <p><strong>Note:</strong> This report is automatically generated from the NCD breach data.</p>
            </div>
        </body>
        </html>
        """
        
        # Attach HTML content
        msg.attach(MIMEText(html_body, 'html'))
        
        # Send consolidated email
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        text = msg.as_string()
        recipients = to_recipients + ['lokeshh@loadshare.net', 'arunraj@loadshare.net', 'maligai.rasmeen@loadshare.net']
        server.sendmail(EMAIL_CONFIG['sender_email'], recipients, text)
        server.quit()
        
        print_detailed_log(f"✅ Consolidated email sent successfully to all CLMs and hubs", "SUCCESS")
        print_detailed_log(f"TO (CLMs): {', '.join(clm_emails)}", "INFO")
        print_detailed_log(f"TO (Hubs): {', '.join(hub_emails)}", "INFO")
        print_detailed_log(f"CC: lokeshh@loadshare.net, arunraj@loadshare.net", "INFO")
        
        return True
        
    except Exception as e:
        print_detailed_log(f"Error in send_ageing_10k_email_to_clms: {e}", "ERROR")
        return False

# Global variable to store all tracking IDs from all hubs
all_tracking_ids = []

# Clear any existing tracking IDs at the start
print_detailed_log("🔄 Clearing any existing tracking IDs...", "INFO")
all_tracking_ids.clear()

# Delete existing temp_CPD.csv file to ensure fresh data
print_detailed_log("🗑️ Clearing temp_CPD.csv contents...", "INFO")
try:
    # Clear the contents of temp_CPD.csv (keep the file, just clear contents)
    if os.path.exists("temp_CPD.csv"):
        # Create an empty DataFrame with the correct columns and save it
        empty_df = pd.DataFrame(columns=['tracking_id', 'hub_name', 'clm_name', 'state', 'cpd_days', 'cpd_type', 'timestamp'])
        empty_df.to_csv("temp_CPD.csv", index=False)
        print_detailed_log("✅ Cleared contents of temp_CPD.csv file", "SUCCESS")
    else:
        # Create the file if it doesn't exist
        empty_df = pd.DataFrame(columns=['tracking_id', 'hub_name', 'clm_name', 'state', 'cpd_days', 'cpd_type', 'timestamp'])
        empty_df.to_csv("temp_CPD.csv", index=False)
        print_detailed_log("✅ Created new temp_CPD.csv file", "SUCCESS")
        
    print_detailed_log("📋 temp_CPD.csv is ready to accumulate tracking IDs from all hubs", "INFO")
            
except Exception as e:
    print_detailed_log(f"⚠️ Could not clear temp_CPD.csv: {e}", "WARNING")

# Global variable to store filtered tracking counts by hub
filtered_tracking_counts = {}
filtered_ekl_tracking_counts = {}

# Global variable to store NCD breach data for detailed analysis
ncd_breach_data = []

# Set up Chrome options to connect to existing session
chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "localhost:9222")

# Start the driver with better error handling
try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    wait = WebDriverWait(driver, 30)  # Reduced timeout to 30 seconds
    print_detailed_log("Successfully connected to existing browser session", "SUCCESS")

except Exception as e:
    print_detailed_log(f"Error connecting to browser session: {e}", "ERROR")
    print_detailed_log("Please make sure Chrome is running with remote debugging enabled", "WARNING")
    print_detailed_log("Run: chrome.exe --remote-debugging-port=9222", "WARNING")
    exit(1)

def switch_to_correct_tab(driver):
    """Switch to the correct tab (not DevTools)"""
    try:
        print_detailed_log("Looking for correct browser tab...", "PROGRESS")
        # Get all window handles
        handles = driver.window_handles
        print_detailed_log(f"Found {len(handles)} browser tabs", "INFO")
        
        # Switch to the first non-DevTools tab
        for i, handle in enumerate(handles):
            driver.switch_to.window(handle)
            if "DevTools" not in driver.title:
                print_detailed_log(f"Switched to tab {i+1}: {driver.title}", "SUCCESS")
                return True
        
        print_detailed_log("No non-DevTools tab found", "ERROR")
        return False
    except Exception as e:
        print_detailed_log(f"Error switching tabs: {e}", "ERROR")
        return False

def download_csv_for_hub(driver, hub_name, is_first_hub=False):
    """Download CSV data for a specific hub"""
    try:
        print_detailed_log(f"Starting CSV download for hub: {hub_name}", "PROGRESS")
        
        # Wait for page to load
        print_detailed_log("Waiting for page to load...", "INFO")
        time.sleep(2)
        
        # For all hubs, we need to select hub, type name, and press Enter
        print_detailed_log("Selecting hub, typing name, and pressing Enter", "INFO")
        
        # Try to find and click the dropdown to select hub
        try:
            print_detailed_log("Looking for hub selection dropdown...", "PROGRESS")
            # Look for dropdown
            dropdown_selectors = [
                "div.css-1uccc91-singleValue",
                "div[class*='singleValue']",
                "div[class*='dropdown']"
            ]
            
            dropdown_found = False
            for i, selector in enumerate(dropdown_selectors):
                try:
                    print_detailed_log(f"Trying dropdown selector {i+1}/{len(dropdown_selectors)}: {selector}", "INFO")
                    dropdown = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    dropdown.click()
                    print_detailed_log(f"Successfully clicked dropdown with selector: {selector}", "SUCCESS")
                    dropdown_found = True
                    break
                except Exception as e:
                    print_detailed_log(f"Dropdown selector {i+1} failed: {e}", "WARNING")
                    continue
            
            if not dropdown_found:
                print_detailed_log("Dropdown not found, trying to proceed...", "WARNING")
            
            # Enter hub name in input field
            print_detailed_log("Looking for hub name input field...", "PROGRESS")
            input_selectors = [
                "input[id^='react-select-'][type='text']",
                "input[type='text']",
                "input[placeholder*='hub']",
                "input[placeholder*='Hub']"
            ]
            
            input_found = False
            for i, selector in enumerate(input_selectors):
                try:
                    print_detailed_log(f"Trying input selector {i+1}/{len(input_selectors)}: {selector}", "INFO")
                    input_field = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    input_field.clear()
                    # Use the full hub name to ensure exact match
                    input_field.send_keys(hub_name)
                    print_detailed_log(f"Successfully entered hub name: {hub_name}", "SUCCESS")
                    
                    # Wait for dropdown to populate and filter
                    time.sleep(2)
                    
                    # Simple and fast: Just press Enter (old reliable method)
                    input_field.send_keys(Keys.ENTER)
                    print_detailed_log("Successfully pressed Enter key", "SUCCESS")
                    
                    # Wait for the table to load and verify the correct hub is selected
                    time.sleep(3)
                    print_detailed_log(f"Verifying that hub '{hub_name}' is correctly selected...", "INFO")
                    
                    input_found = True
                    break
                except Exception as e:
                    print_detailed_log(f"Input selector {i+1} failed: {e}", "WARNING")
                continue
        
            if not input_found:
                print_detailed_log("Input field not found, trying to proceed...", "WARNING")
            
            # Wait for the new table to appear after pressing Enter
            print_detailed_log("Waiting for new table to appear after pressing Enter...", "INFO")
            time.sleep(3)
            
            # Only try to click Show Data button for the first hub
            if is_first_hub:
                print_detailed_log("Looking for Show Data button...", "PROGRESS")
                button_selectors = [
                    "button.HubDashboard-showDataButton-1lZt5V8FT3Jdfw4weKyQLD",
                    "button[class*='showData']",
                    "button:contains('Show Data')",
                    "button:contains('Show')"
                ]
                
                button_found = False
                for i, selector in enumerate(button_selectors):
                    try:
                        print_detailed_log(f"Trying Show Data button selector {i+1}/{len(button_selectors)}: {selector}", "INFO")
                        button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )
                        button.click()
                        print_detailed_log(f"Successfully clicked Show Data button with selector: {selector}", "SUCCESS")
                        button_found = True
                        break
                    except Exception as e:
                        print_detailed_log(f"Show Data button selector {i+1} failed: {e}", "WARNING")
                        continue

                if not button_found:
                    print_detailed_log("Show Data button not found, trying to proceed...", "WARNING")
                
                # Wait for data to load
                print_detailed_log("Waiting for data to load...", "INFO")
                time.sleep(3)
            else:
                print_detailed_log("Subsequent hub - skipping SHOW DATA button check", "INFO")
        
        except Exception as e:
            print_detailed_log(f"Error in dropdown/button interaction: {e}", "WARNING")
        
        # Step 2: Look for and click the Forward value in the table (only for first hub)
        if is_first_hub:
            print_detailed_log("Looking for Forward value in table...", "PROGRESS")
            
            # Wait for table to appear
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
                print_detailed_log("Table found", "SUCCESS")
            except:
                print_detailed_log("Table not found", "ERROR")
                return False
            
            # Find the Forward value and click it
            forward_clicked = False
            max_retries = 3
            
            for retry in range(max_retries):
                try:
                    print_detailed_log(f"Attempting to click Forward value (attempt {retry + 1}/{max_retries})...", "PROGRESS")
                    
                    # Wait for page to be stable
                    time.sleep(2)
                    
                    # Find tables with fresh elements
                    tables = driver.find_elements(By.TAG_NAME, 'table')
                    
                    for table in tables:
                        rows = table.find_elements(By.TAG_NAME, 'tr')
                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, 'td')
                            if not cells or not cells[0].text.strip():
                                continue
                            
                            label = cells[0].text.strip().lower()
                            
                            # Check for various possible pendency table names
                            if any(keyword in label for keyword in ['pendency', 'pending', 'forward']):
                                # Look for Forward count in the Pendency table
                                for i, cell in enumerate(cells):
                                    cell_text = cell.text.strip().lower()
                                    if 'forward' in cell_text:
                                        # Get the count from the next cell (index 1)
                                        if len(cells) > 1:
                                            forward_value = cells[1].text.strip()
                                            print_detailed_log(f"Found Forward value: {forward_value}", "DATA")
                                            
                                            # If the cell contains an <a> tag, click it
                                            a_tags = cells[1].find_elements(By.TAG_NAME, 'a')
                                            if a_tags:
                                                print_detailed_log("Clicking Forward link...", "PROGRESS")
                                                # Use JavaScript click to avoid stale element issues
                                                driver.execute_script("arguments[0].click();", a_tags[0])
                                                forward_clicked = True
                                                print_detailed_log("Successfully clicked Forward link", "SUCCESS")
                                                break
                                break
                            if forward_clicked:
                                break
                    
                    if forward_clicked:
                        break
                        
                except Exception as e:
                    print_detailed_log(f"Error clicking Forward value (attempt {retry + 1}): {e}", "WARNING")
                    if retry < max_retries - 1:
                        print_detailed_log("Retrying...", "INFO")
                        time.sleep(3)  # Wait before retry
                    else:
                        print_detailed_log("All attempts failed to click Forward value", "ERROR")
            
            if not forward_clicked:
                print_detailed_log("Could not click Forward value", "ERROR")
                return False
            
            # Wait for the Forward page to load
            print_detailed_log("Waiting for Forward page to load...", "PROGRESS")
            time.sleep(5)
        else:
            print_detailed_log("Subsequent hub - table already visible, proceeding to CSV download", "INFO")
        
        # Step 3: Look for Download CSV button
        print_detailed_log("Looking for Download CSV button...", "PROGRESS")
        
        # Look for Download CSV button
        csv_button_selectors = [
            "//button[contains(text(), 'Download CSV')]",
            "//button[contains(text(), 'CSV')]",
            "//button[contains(text(), 'Download')]",
            "//div[contains(text(), 'Download CSV')]",
            "//span[contains(text(), 'Download CSV')]"
        ]
        
        csv_downloaded = False
        for i, selector in enumerate(csv_button_selectors):
            try:
                print_detailed_log(f"Trying CSV download button selector {i+1}/{len(csv_button_selectors)}: {selector}", "INFO")
                csv_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                csv_button.click()
                print_detailed_log(f"Successfully clicked Download CSV button with XPath: {selector}", "SUCCESS")
                csv_downloaded = True
                break
            except Exception as e:
                print_detailed_log(f"CSV download button selector {i+1} failed: {e}", "WARNING")
                continue
                    
        if not csv_downloaded:
            print_detailed_log("Download CSV button not found", "ERROR")
            return False
        
        # Wait for download to complete
        print_detailed_log("Waiting for CSV download to complete...", "INFO")
        time.sleep(5)
        
        # Check if any CSV files were actually downloaded
        download_dir = os.path.expanduser("~/Downloads")
        if os.path.exists(download_dir):
            csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
            if csv_files:
                # Sort by modification time
                csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
                latest_csv = csv_files[0]
                file_time = datetime.fromtimestamp(os.path.getmtime(os.path.join(download_dir, latest_csv)))
                time_diff = datetime.now() - file_time
                
                if time_diff.total_seconds() < 60:  # File created in last minute
                    print_detailed_log(f"✅ CSV download confirmed: {latest_csv} (created {time_diff.total_seconds():.0f}s ago)", "SUCCESS")
                else:
                    print_detailed_log(f"⚠️ Found CSV file but it's old: {latest_csv} (created {time_diff.total_seconds():.0f}s ago)", "WARNING")
            else:
                print_detailed_log(f"⚠️ No CSV files found in Downloads folder after download attempt", "WARNING")
        else:
            print_detailed_log(f"⚠️ Downloads folder not found: {download_dir}", "WARNING")
        
        return True
        
    except Exception as e:
        print_detailed_log(f"Error downloading CSV for {hub_name}: {e}", "ERROR")
        return False

def process_csv_data(csv_file_path, hub_name):
    """Process CSV data and extract breach information with CPD-based categorization (no filters)"""
    try:
        # Enable tracking ID collection for all hubs
        tracking_ids_enabled = True
        print_detailed_log(f"✅ Collecting tracking IDs for {hub_name}", "INFO")
        
        # Declare global variable at the beginning of the function
        global all_tracking_ids
        
        print_detailed_log("Reading CSV file...", "PROGRESS")
        
        # Read CSV file
        df = pd.read_csv(csv_file_path)
        print_detailed_log(f"Loaded {len(df)} total records from CSV", "DATA")
        print_detailed_log(f"CSV columns: {list(df.columns)}", "INFO")
        
        # Process ALL records (no status filtering)
        print_detailed_log("Processing ALL records (no status filters)...", "PROGRESS")
        
        # Get CLM name and state for this hub
        clm_name, state = HUB_INFO.get(hub_name, ("", ""))
        
        # Find tracking ID column (case insensitive)
        tracking_column = None
        print_detailed_log(f"DEBUG: Looking for tracking ID column in: {list(df.columns)}", "INFO")
        for col in df.columns:
            col_lower = col.lower()
            print_detailed_log(f"DEBUG: Checking column '{col}' (lowercase: '{col_lower}')", "INFO")
            if col_lower in ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']:
                tracking_column = col
                print_detailed_log(f"DEBUG: Found tracking ID column: '{col}'", "INFO")
                break
        
        if tracking_column is None:
            print_detailed_log(f"❌ CRITICAL: No tracking ID column found in CSV for {hub_name}", "ERROR")
            print_detailed_log(f"DEBUG: Available columns: {list(df.columns)}", "INFO")
            print_detailed_log(f"DEBUG: Searched for: ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']", "INFO")
            # Try to find any column that might contain tracking IDs
            for col in df.columns:
                if 'track' in col.lower() or 'id' in col.lower() or 'awb' in col.lower():
                    print_detailed_log(f"DEBUG: Potential tracking column found: '{col}'", "INFO")
        else:
            print_detailed_log(f"✅ Found tracking ID column: '{tracking_column}'", "SUCCESS")
        
        if tracking_column is not None:
            print_detailed_log(f"Using tracking ID column: {tracking_column}", "INFO")
        else:
            print_detailed_log(f"Tracking ID column not found for {hub_name}. Available columns: {list(df.columns)}", "WARNING")
            print_detailed_log(f"DEBUG: Searched for: ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']", "INFO")
        
        # Find status column (case insensitive)
        status_column = None
        for col in df.columns:
            if col.lower() in ['status', 'latest status', 'lateststatus']:
                status_column = col
                break
        
        if status_column is None:
            print_detailed_log(f"Status column not found. Available columns: {list(df.columns)}", "ERROR")
            return None
        
        print_detailed_log(f"Using status column: {status_column}", "INFO")
        
        # Filter data for breach statuses only
        breach_df = df[df[status_column].isin(breach_statuses)].copy()
        print_detailed_log(f"Filtered {len(breach_df)} records with breach statuses", "INFO")
        
        # Find amount column (case insensitive)
        amount_column = None
        for col in df.columns:
            if col.lower() in ['amount', 'order_value', 'order value', 'value', 'price']:
                amount_column = col
                break
        
        if amount_column is not None:
            print_detailed_log(f"Using amount column: {amount_column}", "INFO")
        else:
            print_detailed_log(f"Amount column not found for {hub_name}. Available columns: {list(df.columns)}", "WARNING")
        
        # Find customer_promise_date column (case insensitive)
        customer_promise_date_column = None
        print_detailed_log(f"DEBUG: Looking for customer_promise_date column in: {list(df.columns)}", "INFO")
        for col in df.columns:
            col_lower = col.lower()
            print_detailed_log(f"DEBUG: Checking column '{col}' (lowercase: '{col_lower}')", "INFO")
            if col_lower in ['customer_promise_date', 'customerpromisedate', 'customer promise date', 'promise_date', 'promisedate']:
                customer_promise_date_column = col
                print_detailed_log(f"DEBUG: Found customer_promise_date column: '{col}'", "INFO")
                break
                
        if customer_promise_date_column is None:
            print_detailed_log(f"❌ CRITICAL: No customer_promise_date column found in CSV for {hub_name}", "ERROR")
            print_detailed_log(f"DEBUG: Available columns: {list(df.columns)}", "INFO")
            print_detailed_log(f"DEBUG: Searched for: ['customer_promise_date', 'customerpromisedate', 'customer promise date', 'promise_date', 'promisedate']", "INFO")
            # Try to find any column that might contain dates
            for col in df.columns:
                if 'date' in col.lower() or 'promise' in col.lower() or 'cpd' in col.lower():
                    print_detailed_log(f"DEBUG: Potential date column found: '{col}'", "INFO")
            return None
        
        print_detailed_log(f"Using customer promise date column: {customer_promise_date_column}", "INFO")
        
        # Find last_updated column (case insensitive)
        last_updated_column = None
        for col in df.columns:
            if col.lower() in ['last_updated', 'lastupdated', 'last updated', 'updated', 'latest update time', 'latest update', 'update time']:
                last_updated_column = col
                break
                            
        if last_updated_column is None:
            print_detailed_log(f"Last updated column not found. Available columns: {list(df.columns)}", "ERROR")
            return None
        
        print_detailed_log(f"Using last_updated column: {last_updated_column}", "INFO")
        
        # Get current date for CPD calculation
        current_date = datetime.now().date()
        
        # Initialize NCD breach counter
        ncd_breach_counter = Counter()
        
        # Initialize EKL breach counter
        ekl_breach_counter = Counter()
        
        # Filter records based on breach_statuses
        print_detailed_log("Filtering records based on specified breach statuses...", "PROGRESS")
        print_detailed_log(f"DEBUG: breach_statuses to filter: {breach_statuses}", "INFO")
        print_detailed_log(f"DEBUG: status_column: '{status_column}'", "INFO")
        
        # Show unique status values in the data
        unique_statuses = df[status_column].unique()
        print_detailed_log(f"DEBUG: Unique status values in data: {list(unique_statuses)}", "INFO")
        
        filtered_df = df[df[status_column].isin(breach_statuses)].copy()
        print_detailed_log(f"Found {len(filtered_df)} records matching specified breach statuses", "DATA")
        
        if len(filtered_df) == 0:
            print_detailed_log(f"❌ CRITICAL: No records found matching breach statuses for {hub_name}", "ERROR")
            print_detailed_log(f"DEBUG: This means no tracking IDs will be collected", "INFO")
        else:
            print_detailed_log(f"✅ Found {len(filtered_df)} records to process for tracking IDs", "SUCCESS")
        
        # Define EKL statuses
        ekl_breach_statuses = [
            'Undelivered_Security_Instability',
            'Undelivered_Heavy_Rain', 
            'Undelivered_Heavy_Load'
        ]
        
        # Process filtered records with CPD-based categorization
        print_detailed_log("Processing filtered records with CPD-based categorization...", "PROGRESS")
        
        for _, row in filtered_df.iterrows():
            # Extract data from row
            tracking_id = row[tracking_column] if tracking_column else ""
            status_text = row[status_column] if status_column else ""
            cpd_text = row[customer_promise_date_column] if customer_promise_date_column else ""
            last_updated_text = row[last_updated_column] if last_updated_column else ""
            
            # Get amount if available
            try:
                amount = float(row[amount_column]) if amount_column and pd.notna(row[amount_column]) else 0
            except:
                amount = 0
            
            # Process filtered records
            # Check if we have valid data to process
            if tracking_id or status_text or cpd_text or last_updated_text:
                # Calculate CPD days difference
                cpd_days_diff = 0
                try:
                    if cpd_text:
                        cpd_date = pd.to_datetime(cpd_text, errors='coerce')
                        if pd.notna(cpd_date):
                            cpd_days_diff = (cpd_date.date() - current_date).days
                except:
                    cpd_days_diff = 0
                
                # Categorize by CPD for NCD (all breach statuses)
                if cpd_days_diff == 0:
                    ncd_breach_counter['NCD_0_Days'] += 1  # CPD = Today
                elif cpd_days_diff == -1:
                    ncd_breach_counter['NCD_1_Days'] += 1  # CPD = Yesterday
                elif cpd_days_diff == -2:
                    ncd_breach_counter['NCD_2_Days'] += 1  # CPD = 2 days ago
                elif cpd_days_diff == -3:
                    ncd_breach_counter['NCD_3_Days'] += 1  # CPD = 3 days ago
                elif cpd_days_diff < -3:
                    ncd_breach_counter['NCD_>_3_Days'] += 1  # CPD > 3 days ago
                elif cpd_days_diff > 0:
                    ncd_breach_counter['NCD_FDD'] += 1  # CPD > Today (Future)
                
                # Categorize by CPD for EKL (only 3 EKL statuses)
                if status_text in ekl_breach_statuses:
                    if cpd_days_diff == 0:
                        ekl_breach_counter['EKL_0_Days'] += 1  # CPD = Today
                    elif cpd_days_diff == -1:
                        ekl_breach_counter['EKL_1_Days'] += 1  # CPD = Yesterday
                
                # Store detailed data for analysis
                ncd_breach_data.append({
                    'Hub Name': hub_name,
                    'CLM Name': clm_name,
                    'State': state,
                    'Tracking ID': tracking_id,
                    'Amount': amount,
                    'Status': status_text,
                    'CPD Date': cpd_text,
                    'CPD Days Diff': cpd_days_diff,
                    'Last Updated': last_updated_text
                })
        
        print_detailed_log(f"{hub_name}: NCD Breach breakdown: {dict(ncd_breach_counter)}", "DATA")
        print_detailed_log(f"{hub_name}: EKL Breach breakdown: {dict(ekl_breach_counter)}", "DATA")
        
        # Return the NCD and EKL breach counts
        result_data = {}
        for category in ncd_cpd_categories:
            result_data[category] = ncd_breach_counter.get(category, 0)
        
        # Calculate Total NCD Breaches and add it after NCD_FDD
        total_ncd_breaches = sum(result_data.get(cat, 0) for cat in ncd_cpd_categories)
        result_data['Total NCD Breaches'] = total_ncd_breaches
        
        # Add EKL categories
        for category in ekl_cpd_categories:
            result_data[category] = ekl_breach_counter.get(category, 0)
        
        # Extract tracking IDs from breach records for CPD_EKL_Marking (0 Days) and (1 Days)
        tracking_column = None
        for col in df.columns:
            if col.lower() in ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']:
                tracking_column = col
                break
        
        if tracking_column is not None:
            print_detailed_log(f"Using tracking ID column: {tracking_column}", "INFO")
        else:
            print_detailed_log(f"Tracking ID column not found for {hub_name}. Available columns: {list(df.columns)}", "WARNING")
        
        # Find last_updated column (case insensitive)
        last_updated_column = None
        for col in df.columns:
            if col.lower() in ['last_updated', 'lastupdated', 'last updated', 'updated', 'latest update time', 'latest update', 'update time']:
                last_updated_column = col
                break
                            
        if last_updated_column is None:
            print_detailed_log(f"Last updated column not found. Available columns: {list(df.columns)}", "ERROR")
            return None
        
        print_detailed_log(f"Using last_updated column: {last_updated_column}", "INFO")
        
        # Find customer_promise_date column (case insensitive)
        customer_promise_date_column = None
        for col in df.columns:
            if col.lower() in ['customer_promise_date', 'customerpromisedate', 'customer promise date', 'promise_date', 'promisedate']:
                customer_promise_date_column = col
                break
                
        if customer_promise_date_column is None:
            print_detailed_log(f"Customer promise date column not found. Available columns: {list(df.columns)}", "WARNING")
            print_detailed_log("CPD_EKL_Marking columns will be set to 0", "WARNING")
        else:
            print_detailed_log(f"Using customer promise date column: {customer_promise_date_column}", "INFO")
        
        # Convert last_updated to datetime and calculate ageing
        print_detailed_log("Converting last_updated to datetime and calculating ageing...", "PROGRESS")
        current_date = datetime.now().date()  # Use date only, not datetime
        
        # Convert last_updated column to datetime with better error handling
        try:
            print_detailed_log(f"Original column dtype: {filtered_df[last_updated_column].dtype}", "INFO")
            print_detailed_log(f"Sample original values: {filtered_df[last_updated_column].head(3).tolist()}", "INFO")
            
            # Try multiple date parsing formats to handle various date formats
            date_formats = [
                '%Y-%m-%d %H:%M:%S',  # 2025-08-08 09:02:34
                '%d-%m-%Y %H:%M:%S',  # 08-08-2025 09:02:34
                '%Y-%m-%d',           # 2025-08-08
                '%d-%m-%Y',           # 08-08-2025
                '%m/%d/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                '%m/%d/%Y',           # 08/08/2025
                '%d/%m/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                '%d/%m/%Y'            # 08/08/2025
            ]
            
            # Try parsing with specific formats first
            parsed_dates = None
            for fmt in date_formats:
                try:
                    parsed_dates = pd.to_datetime(filtered_df[last_updated_column], format=fmt, errors='coerce')
                    if not parsed_dates.isna().all():
                        print_detailed_log(f"Successfully parsed with format: {fmt}", "INFO")
                        break
                except:
                    continue
            
            # If specific formats failed, try automatic parsing
            if parsed_dates is None or parsed_dates.isna().all():
                print_detailed_log("Specific formats failed, trying automatic parsing...", "INFO")
                parsed_dates = pd.to_datetime(filtered_df[last_updated_column], errors='coerce')
            
            filtered_df[last_updated_column] = parsed_dates
            print_detailed_log(f"Successfully converted {last_updated_column} to datetime", "INFO")
            print_detailed_log(f"Converted column dtype: {filtered_df[last_updated_column].dtype}", "INFO")
            print_detailed_log(f"Sample converted values: {filtered_df[last_updated_column].head(3).tolist()}", "INFO")
            
            # Show the actual date values (not just time)
            sample_dates = filtered_df[last_updated_column].head(3)
            print_detailed_log(f"Sample date values: {[str(d) for d in sample_dates if pd.notna(d)]}", "INFO")
            
        except Exception as e:
            print_detailed_log(f"Error converting {last_updated_column} to datetime: {e}", "ERROR")
            return None
        
        # Check if conversion was successful
        if filtered_df[last_updated_column].isna().all():
            print_detailed_log(f"All values in {last_updated_column} are null after conversion", "ERROR")
            return None
        
        # Check if we have any valid datetime values
        valid_dates = filtered_df[last_updated_column].notna()
        if not valid_dates.any():
            print_detailed_log(f"No valid dates found in {last_updated_column}", "ERROR")
            return None
        
        # Calculate ageing in days using date-only comparison with error handling
        try:
            # Filter to only valid dates for calculation
            valid_breach_df = filtered_df[valid_dates].copy()
            print_detailed_log(f"Processing {len(valid_breach_df)} records with valid dates", "INFO")
            
            # Debug: Check the actual data type and sample values before .dt.date
            print_detailed_log(f"Column dtype before .dt.date: {valid_breach_df[last_updated_column].dtype}", "INFO")
            sample_values = valid_breach_df[last_updated_column].head(3)
            print_detailed_log(f"Sample values before .dt.date: {sample_values.tolist()}", "INFO")
            print_detailed_log(f"Sample value types: {[type(v) for v in sample_values]}", "INFO")
            
            # Check if the column is actually datetime
            if not pd.api.types.is_datetime64_any_dtype(valid_breach_df[last_updated_column]):
                print_detailed_log(f"Column {last_updated_column} is not datetime type: {valid_breach_df[last_updated_column].dtype}", "ERROR")
                # Try to convert again with more explicit handling
                try:
                    valid_breach_df[last_updated_column] = pd.to_datetime(valid_breach_df[last_updated_column], errors='coerce')
                    print_detailed_log(f"Re-converted column dtype: {valid_breach_df[last_updated_column].dtype}", "INFO")
                except Exception as conv_e:
                    print_detailed_log(f"Failed to re-convert to datetime: {conv_e}", "ERROR")
                    return None
            
            # Try to get date component safely
            try:
                date_component = valid_breach_df[last_updated_column].dt.date
                print_detailed_log(f"Successfully extracted date component", "INFO")
            except Exception as date_e:
                print_detailed_log(f"Error extracting date component: {date_e}", "ERROR")
                print_detailed_log(f"Column dtype: {valid_breach_df[last_updated_column].dtype}", "ERROR")
                print_detailed_log(f"Sample values: {valid_breach_df[last_updated_column].head(3).tolist()}", "ERROR")
                return None
            
            # Calculate the difference and extract days
            timedelta_series = current_date - date_component
            print_detailed_log(f"Timedelta series type: {type(timedelta_series)}", "INFO")
            print_detailed_log(f"Timedelta series dtype: {getattr(timedelta_series, 'dtype', 'No dtype')}", "INFO")
            print_detailed_log(f"Sample timedelta values: {timedelta_series.head(3) if hasattr(timedelta_series, 'head') else timedelta_series}", "INFO")
            
            # Use a safer approach to extract days
            if hasattr(timedelta_series, 'dt'):
                ageing_days = timedelta_series.dt.days
            else:
                # Fallback: calculate manually
                ageing_days = [(current_date - date_val).days for date_val in date_component]
                ageing_days = pd.Series(ageing_days, index=date_component.index)
            
            valid_breach_df['ageing_days'] = ageing_days
            
            print_detailed_log(f"Successfully calculated ageing_days", "INFO")
            print_detailed_log(f"Sample ageing_days values: {ageing_days.head(3).tolist()}", "INFO")
            
            # Update the original dataframe with ageing_days
            filtered_df['ageing_days'] = np.nan  # Initialize with NaN
            filtered_df.loc[valid_dates, 'ageing_days'] = ageing_days
            
            # Update breach_df with ageing_days column
            breach_df['ageing_days'] = np.nan  # Initialize with NaN
            # Only update rows that exist in both dataframes
            common_indices = breach_df.index.intersection(filtered_df.index)
            breach_df.loc[common_indices, 'ageing_days'] = filtered_df.loc[common_indices, 'ageing_days']
            
        except Exception as e:
            print_detailed_log(f"Error calculating ageing_days: {e}", "ERROR")
            print_detailed_log(f"Column dtype: {filtered_df[last_updated_column].dtype}", "INFO")
            print_detailed_log(f"Sample values: {filtered_df[last_updated_column].head(3).tolist()}", "INFO")
            import traceback
            print_detailed_log(f"Full traceback: {traceback.format_exc()}", "ERROR")
            return None
        
        # Add debugging information
        print_detailed_log(f"Current date: {current_date}", "INFO")
        print_detailed_log(f"Sample last_updated values: {filtered_df[last_updated_column].head(3).tolist()}", "INFO")
        print_detailed_log(f"Sample ageing_days values: {filtered_df['ageing_days'].head(3).tolist()}", "INFO")
        
        # Count occurrences of each breach status
        status_counts = {}
        
        for status in breach_statuses:
            count = len(filtered_df[filtered_df[status_column] == status])
            status_counts[status] = count
            print_detailed_log(f"{status}: {count}", "DATA")
        
        # Calculate total fake marking
        total_fake_marking = sum(status_counts.values())
        
        # Ageing bifurcation for Total EKL Markings based on last_updated
        print_detailed_log("Calculating ageing bifurcation for Total EKL Markings based on last_updated...", "PROGRESS")
        
        ageing_categories = {
            '0_days': 0,
            '1_days': 0,
            '2_days': 0,
            '3_days': 0,
            'more_than_3_days': 0
        }
        
        # Count ageing for breach records only
        zero_day_count = 0
        one_day_count = 0
        
        for _, row in breach_df.iterrows():
            ageing_days = row['ageing_days']
            if pd.isna(ageing_days):
                continue
                            
            ageing_int = int(ageing_days)
            
            # Debug: Show some examples of 0-day records
            if ageing_int == 0:
                ageing_categories['0_days'] += 1
                zero_day_count += 1
                if zero_day_count <= 3:  # Show first 3 examples
                    last_updated_val = row[last_updated_column]
                    print_detailed_log(f"0-day example {zero_day_count}: last_updated={last_updated_val}, ageing_days={ageing_days}", "INFO")
            elif ageing_int == 1:
                ageing_categories['1_days'] += 1
                one_day_count += 1
                if one_day_count <= 3:  # Show first 3 examples
                    last_updated_val = row[last_updated_column]
                    print_detailed_log(f"1-day example {one_day_count}: last_updated={last_updated_val}, ageing_days={ageing_days}", "INFO")
            elif ageing_int == 2:
                ageing_categories['2_days'] += 1
            elif ageing_int == 3:
                ageing_categories['3_days'] += 1
            else:
                ageing_categories['more_than_3_days'] += 1
        
        # Print ageing breakdown
        print_detailed_log("Ageing breakdown for Total EKL Markings (based on last_updated):", "DATA")
        for category, count in ageing_categories.items():
            print_detailed_log(f"  {category}: {count}", "DATA")
        
        # Calculate CPD_NCD_Breaches based on customer_promise_date
        cpd_0_days = 0
        cpd_1_day = 0
        
        if customer_promise_date_column is not None:
            print_detailed_log("Calculating CPD_NCD_Breaches based on customer_promise_date...", "PROGRESS")
            
            # Convert customer_promise_date to datetime with error handling
            try:
                print_detailed_log(f"Original CPD column dtype: {breach_df[customer_promise_date_column].dtype}", "INFO")
                print_detailed_log(f"Sample original CPD values: {breach_df[customer_promise_date_column].head(3).tolist()}", "INFO")
                
                # Try multiple date parsing formats to handle various date formats
                date_formats = [
                    '%Y-%m-%d %H:%M:%S',  # 2025-08-08 09:02:34
                    '%d-%m-%Y %H:%M:%S',  # 08-08-2025 09:02:34
                    '%Y-%m-%d',           # 2025-08-08
                    '%d-%m-%Y',           # 08-08-2025
                    '%m/%d/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                    '%m/%d/%Y',           # 08/08/2025
                    '%d/%m/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                    '%d/%m/%Y'            # 08/08/2025
                ]
                
                # Try parsing with specific formats first
                parsed_cpd_dates = None
                for fmt in date_formats:
                    try:
                        parsed_cpd_dates = pd.to_datetime(filtered_df[customer_promise_date_column], format=fmt, errors='coerce')
                        if not parsed_cpd_dates.isna().all():
                            print_detailed_log(f"Successfully parsed CPD with format: {fmt}", "INFO")
                            break
                    except:
                        continue
                
                # If specific formats failed, try automatic parsing
                if parsed_cpd_dates is None or parsed_cpd_dates.isna().all():
                    print_detailed_log("Specific CPD formats failed, trying automatic parsing...", "INFO")
                    parsed_cpd_dates = pd.to_datetime(filtered_df[customer_promise_date_column], errors='coerce')
                
                filtered_df[customer_promise_date_column] = parsed_cpd_dates
                print_detailed_log(f"Successfully converted {customer_promise_date_column} to datetime", "INFO")
                print_detailed_log(f"Converted CPD column dtype: {filtered_df[customer_promise_date_column].dtype}", "INFO")
                print_detailed_log(f"Sample converted CPD values: {filtered_df[customer_promise_date_column].head(3).tolist()}", "INFO")
                
                # Show the actual date values (not just time)
                sample_cpd_dates = filtered_df[customer_promise_date_column].head(3)
                print_detailed_log(f"Sample CPD date values: {[str(d) for d in sample_cpd_dates if pd.notna(d)]}", "INFO")
                
            except Exception as e:
                print_detailed_log(f"Error converting {customer_promise_date_column} to datetime: {e}", "ERROR")
                print_detailed_log("CPD_NCD_Breaches columns will be 0", "WARNING")
                cpd_0_days = 0
                cpd_1_day = 0
            else:
                # Calculate days difference from customer promise date to current date
                try:
                    current_date = datetime.now().date()
                    
                    # Check if we have any valid datetime values for customer_promise_date
                    valid_cpd_dates = breach_df[customer_promise_date_column].notna()
                    if not valid_cpd_dates.any():
                        print_detailed_log(f"No valid dates found in {customer_promise_date_column}", "WARNING")
                        cpd_0_days = 0
                        cpd_1_day = 0
                    else:
                        # Filter to only valid dates for calculation
                        valid_cpd_df = filtered_df[valid_cpd_dates].copy()
                        print_detailed_log(f"Processing {len(valid_cpd_df)} records with valid CPD dates", "INFO")
                        
                        # Debug: Check the actual data type and sample values before .dt.date
                        print_detailed_log(f"CPD Column dtype before .dt.date: {valid_cpd_df[customer_promise_date_column].dtype}", "INFO")
                        cpd_sample_values = valid_cpd_df[customer_promise_date_column].head(3)
                        print_detailed_log(f"CPD Sample values before .dt.date: {cpd_sample_values.tolist()}", "INFO")
                        print_detailed_log(f"CPD Sample value types: {[type(v) for v in cpd_sample_values]}", "INFO")
                        
                        # Check if the CPD column is actually datetime
                        if not pd.api.types.is_datetime64_any_dtype(valid_cpd_df[customer_promise_date_column]):
                            print_detailed_log(f"CPD Column {customer_promise_date_column} is not datetime type: {valid_cpd_df[customer_promise_date_column].dtype}", "ERROR")
                            # Try to convert again with more explicit handling
                            try:
                                valid_cpd_df[customer_promise_date_column] = pd.to_datetime(valid_cpd_df[customer_promise_date_column], errors='coerce')
                                print_detailed_log(f"Re-converted CPD column dtype: {valid_cpd_df[customer_promise_date_column].dtype}", "INFO")
                            except Exception as conv_e:
                                print_detailed_log(f"Failed to re-convert CPD to datetime: {conv_e}", "ERROR")
                                cpd_0_days = 0
                                cpd_1_day = 0
                                return None
                        
                        # Try to get CPD date component safely
                        try:
                            cpd_date_component = valid_cpd_df[customer_promise_date_column].dt.date
                            print_detailed_log(f"Successfully extracted CPD date component", "INFO")
                        except Exception as cpd_date_e:
                            print_detailed_log(f"Error extracting CPD date component: {cpd_date_e}", "ERROR")
                            print_detailed_log(f"CPD Column dtype: {valid_cpd_df[customer_promise_date_column].dtype}", "ERROR")
                            print_detailed_log(f"CPD Sample values: {valid_cpd_df[customer_promise_date_column].head(3).tolist()}", "ERROR")
                            cpd_0_days = 0
                            cpd_1_day = 0
                            return None
                        
                        # Calculate the CPD difference and extract days
                        cpd_timedelta_series = current_date - cpd_date_component
                        print_detailed_log(f"CPD Timedelta series type: {type(cpd_timedelta_series)}", "INFO")
                        print_detailed_log(f"CPD Timedelta series dtype: {getattr(cpd_timedelta_series, 'dtype', 'No dtype')}", "INFO")
                        print_detailed_log(f"Sample CPD timedelta values: {cpd_timedelta_series.head(3) if hasattr(cpd_timedelta_series, 'head') else cpd_timedelta_series}", "INFO")
                        
                        # Use a safer approach to extract days
                        if hasattr(cpd_timedelta_series, 'dt'):
                            cpd_ageing_days = cpd_timedelta_series.dt.days
                        else:
                            # Fallback: calculate manually
                            cpd_ageing_days = [(current_date - date_val).days for date_val in cpd_date_component]
                            cpd_ageing_days = pd.Series(cpd_ageing_days, index=cpd_date_component.index)
                        
                        valid_cpd_df['cpd_ageing_days'] = cpd_ageing_days
                        
                        print_detailed_log(f"Successfully calculated cpd_ageing_days", "INFO")
                        print_detailed_log(f"Sample cpd_ageing_days values: {cpd_ageing_days.head(3).tolist()}", "INFO")
                        
                        # Update the original dataframe with cpd_ageing_days
                        filtered_df['cpd_ageing_days'] = np.nan  # Initialize with NaN
                        filtered_df.loc[valid_cpd_dates, 'cpd_ageing_days'] = cpd_ageing_days
                        
                        # Update breach_df with cpd_ageing_days column
                        breach_df['cpd_ageing_days'] = np.nan  # Initialize with NaN
                        # Only update rows that exist in both dataframes
                        common_indices = breach_df.index.intersection(filtered_df.index)
                        breach_df.loc[common_indices, 'cpd_ageing_days'] = filtered_df.loc[common_indices, 'cpd_ageing_days']
                        
                except Exception as e:
                    print_detailed_log(f"Error calculating cpd_ageing_days: {e}", "ERROR")
                    print_detailed_log("CPD_NCD_Breaches columns will be 0", "WARNING")
                    cpd_0_days = 0
                    cpd_1_day = 0
            
                # Count CPD ageing for breach records only
                for _, row in breach_df.iterrows():
                    cpd_ageing_days = row['cpd_ageing_days']
                    if pd.isna(cpd_ageing_days):
                        continue
                    
                    cpd_ageing_int = int(cpd_ageing_days)
                    if cpd_ageing_int == 0:
                        cpd_0_days += 1
                    elif cpd_ageing_int == 1:
                        cpd_1_day += 1
                
                print_detailed_log(f"CPD_NCD_Breaches (0 Day): {cpd_0_days}", "DATA")
                print_detailed_log(f"CPD_NCD_Breaches (1 Day): {cpd_1_day}", "DATA")
                
                # Extract tracking IDs for CPD_NCD_Breaches (0 Day) and (1 Day)
                print_detailed_log(f"DEBUG: Starting tracking ID extraction for {hub_name}", "INFO")
                print_detailed_log(f"DEBUG: tracking_column value: {tracking_column}", "INFO")
                print_detailed_log(f"DEBUG: breach_df columns: {list(breach_df.columns)}", "INFO")
                print_detailed_log(f"DEBUG: tracking_ids_enabled: {tracking_ids_enabled}", "INFO")
                print_detailed_log(f"DEBUG: breach_df shape: {breach_df.shape}", "INFO")
                print_detailed_log(f"DEBUG: breach_df head: {breach_df.head(3).to_dict() if not breach_df.empty else 'Empty DataFrame'}", "INFO")
                
                if tracking_column is not None and tracking_ids_enabled:
                    print_detailed_log(f"✅ Tracking ID collection ENABLED for {hub_name}", "INFO")
                    print_detailed_log(f"DEBUG: breach_df shape: {breach_df.shape}", "INFO")
                    print_detailed_log(f"DEBUG: breach_df columns: {list(breach_df.columns)}", "INFO")
                    print_detailed_log(f"DEBUG: tracking_column: '{tracking_column}'", "INFO")
                    
                    # Show sample data from breach_df
                    if not breach_df.empty:
                        print_detailed_log(f"DEBUG: breach_df head (first 3 rows):", "INFO")
                        for i, row in breach_df.head(3).iterrows():
                            tracking_id = str(row.get(tracking_column, 'N/A'))
                            cpd_ageing = row.get('cpd_ageing_days', 'N/A')
                            print_detailed_log(f"  Row {i}: tracking_id='{tracking_id}', cpd_ageing_days={cpd_ageing}", "INFO")
                    
                    # Filter for 0 days and 1 day CPD shipments
                    zero_day_shipments = filtered_df[filtered_df['cpd_ageing_days'] == 0]
                    one_day_shipments = filtered_df[filtered_df['cpd_ageing_days'] == 1]
                    
                    print_detailed_log(f"DEBUG: Zero day shipments count: {len(zero_day_shipments)}", "INFO")
                    print_detailed_log(f"DEBUG: One day shipments count: {len(one_day_shipments)}", "INFO")
                    
                    # Get tracking IDs from 0 day and 1 day CPD shipments
                    zero_day_tracking_ids = zero_day_shipments[tracking_column].dropna().astype(str).tolist()
                    one_day_tracking_ids = one_day_shipments[tracking_column].dropna().astype(str).tolist()
                    
                    print_detailed_log(f"DEBUG: Zero day tracking IDs extracted: {len(zero_day_tracking_ids)}", "INFO")
                    print_detailed_log(f"DEBUG: One day tracking IDs extracted: {len(one_day_tracking_ids)}", "INFO")
                    
                    # Show sample tracking IDs
                    if zero_day_tracking_ids:
                        print_detailed_log(f"DEBUG: Sample zero day tracking IDs: {zero_day_tracking_ids[:3]}", "INFO")
                    if one_day_tracking_ids:
                        print_detailed_log(f"DEBUG: Sample one day tracking IDs: {one_day_tracking_ids[:3]}", "INFO")
                    
                    print_detailed_log(f"CPD tracking IDs for {hub_name}: 0 days: {len(zero_day_tracking_ids)}, 1 day: {len(one_day_tracking_ids)}", "DATA")
                    print_detailed_log(f"DEBUG: Zero day tracking IDs: {zero_day_tracking_ids[:5]}...", "INFO")  # Show first 5
                    print_detailed_log(f"DEBUG: One day tracking IDs: {one_day_tracking_ids[:5]}...", "INFO")  # Show first 5
                    print_detailed_log(f"DEBUG: Before adding NCD tracking IDs, all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
                    
                    # Add tracking IDs to global list with NCD identifier
                    for tracking_id in zero_day_tracking_ids:
                        all_tracking_ids.append({
                            'tracking_id': tracking_id,
                            'hub_name': hub_name,
                            'clm_name': clm_name,
                            'state': state,
                            'cpd_days': '0 Days',
                            'cpd_type': 'NCD',  # Mark as NCD tracking ID
                            'timestamp': datetime.now().strftime("%d %b %H:%M")
                        })
                    
                    for tracking_id in one_day_tracking_ids:
                        all_tracking_ids.append({
                            'tracking_id': tracking_id,
                            'hub_name': hub_name,
                            'clm_name': clm_name,
                            'state': state,
                            'cpd_days': '1 Day',
                            'cpd_type': 'NCD',  # Mark as NCD tracking ID
                            'timestamp': datetime.now().strftime("%d %b %H:%M")
                        })
                    
                    print_detailed_log(f"DEBUG: Total tracking IDs collected so far: {len(all_tracking_ids)}", "INFO")
                    print_detailed_log(f"DEBUG: After adding NCD tracking IDs, all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
                    print_detailed_log(f"DEBUG: Sample NCD tracking IDs added: {all_tracking_ids[-3:] if len(all_tracking_ids) >= 3 else all_tracking_ids}", "INFO")
                else:
                    print_detailed_log(f"No tracking ID column found for {hub_name}, skipping tracking ID extraction", "WARNING")
                    print_detailed_log(f"DEBUG: Available columns for {hub_name}: {list(df.columns)}", "INFO")
        else:
            print_detailed_log("CPD_NCD_Breaches columns will be 0 (customer_promise_date column not found)", "WARNING")
        
        # Calculate CPD_EKL based on 3-status subset
        cpd_ekl_0_days = 0
        cpd_ekl_1_day = 0
        
        # Filter data for EKL breach statuses only
        ekl_breach_df = filtered_df[filtered_df[status_column].isin(ekl_breach_statuses)].copy()
        
        if customer_promise_date_column is not None and len(ekl_breach_df) > 0:
            print_detailed_log("Calculating CPD_EKL based on 3-status subset...", "PROGRESS")
            
            # Convert customer_promise_date for EKL dataframe
            try:
                # Try multiple date parsing formats to handle various date formats
                date_formats = [
                    '%Y-%m-%d %H:%M:%S',  # 2025-08-08 09:02:34
                    '%d-%m-%Y %H:%M:%S',  # 08-08-2025 09:02:34
                    '%Y-%m-%d',           # 2025-08-08
                    '%d-%m-%Y',           # 08-08-2025
                    '%m/%d/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                    '%m/%d/%Y',           # 08/08/2025
                    '%d/%m/%Y %H:%M:%S',  # 08/08/2025 09:02:34
                    '%d/%m/%Y'            # 08/08/2025
                ]
                
                # Try parsing with specific formats first
                parsed_ekl_cpd_dates = None
                for fmt in date_formats:
                    try:
                        parsed_ekl_cpd_dates = pd.to_datetime(ekl_breach_df[customer_promise_date_column], format=fmt, errors='coerce')
                        if not parsed_ekl_cpd_dates.isna().all():
                            print_detailed_log(f"Successfully parsed EKL CPD with format: {fmt}", "INFO")
                            break
                    except:
                        continue
                
                # If specific formats failed, try automatic parsing
                if parsed_ekl_cpd_dates is None or parsed_ekl_cpd_dates.isna().all():
                    print_detailed_log("Specific EKL CPD formats failed, trying automatic parsing...", "INFO")
                    parsed_ekl_cpd_dates = pd.to_datetime(ekl_breach_df[customer_promise_date_column], errors='coerce')
                
                ekl_breach_df[customer_promise_date_column] = parsed_ekl_cpd_dates
                
                # Calculate CPD ageing for EKL records
                current_date = datetime.now().date()
                valid_ekl_cpd_dates = ekl_breach_df[customer_promise_date_column].notna()
                
                if valid_ekl_cpd_dates.any():
                    valid_ekl_df = ekl_breach_df[valid_ekl_cpd_dates].copy()
                    
                    # Calculate ageing days for EKL dataframe
                    if pd.api.types.is_datetime64_any_dtype(valid_ekl_df[customer_promise_date_column]):
                        cpd_date_component = valid_ekl_df[customer_promise_date_column].dt.date
                        cpd_timedelta_series = current_date - cpd_date_component
                        
                        if hasattr(cpd_timedelta_series, 'dt'):
                            cpd_ageing_days = cpd_timedelta_series.dt.days
                        else:
                            cpd_ageing_days = [(current_date - date_val).days for date_val in cpd_date_component]
                            cpd_ageing_days = pd.Series(cpd_ageing_days, index=cpd_date_component.index)
                        
                        # Add cpd_ageing_days column to valid_ekl_df
                        valid_ekl_df['cpd_ageing_days'] = cpd_ageing_days
                        
                        # Count 0 and 1 day records
                        for ageing_days in cpd_ageing_days:
                            if pd.notna(ageing_days):
                                ageing_int = int(ageing_days)
                                if ageing_int == 0:
                                    cpd_ekl_0_days += 1
                                elif ageing_int == 1:
                                    cpd_ekl_1_day += 1
                
                print_detailed_log(f"CPD_EKL (0 Days): {cpd_ekl_0_days}", "DATA")
                print_detailed_log(f"CPD_EKL (1 Days): {cpd_ekl_1_day}", "DATA")
                
                # Extract tracking IDs for CPD_EKL (0 Days) and (1 Days)
                if tracking_column is not None:
                    # Filter for 0 days and 1 day CPD shipments in EKL dataframe
                    ekl_zero_day_shipments = valid_ekl_df[valid_ekl_df['cpd_ageing_days'] == 0]
                    ekl_one_day_shipments = valid_ekl_df[valid_ekl_df['cpd_ageing_days'] == 1]
                    
                    print_detailed_log(f"DEBUG: EKL zero day shipments count: {len(ekl_zero_day_shipments)}", "INFO")
                    print_detailed_log(f"DEBUG: EKL one day shipments count: {len(ekl_one_day_shipments)}", "INFO")
                    
                    # Get tracking IDs from 0 day and 1 day CPD shipments for EKL
                    ekl_zero_day_tracking_ids = ekl_zero_day_shipments[tracking_column].dropna().astype(str).tolist()
                    ekl_one_day_tracking_ids = ekl_one_day_shipments[tracking_column].dropna().astype(str).tolist()
                    
                    print_detailed_log(f"DEBUG: EKL zero day tracking IDs extracted: {len(ekl_zero_day_tracking_ids)}", "INFO")
                    print_detailed_log(f"DEBUG: EKL one day tracking IDs extracted: {len(ekl_one_day_tracking_ids)}", "INFO")
                    
                    # Show sample EKL tracking IDs
                    if ekl_zero_day_tracking_ids:
                        print_detailed_log(f"DEBUG: Sample EKL zero day tracking IDs: {ekl_zero_day_tracking_ids[:3]}", "INFO")
                    if ekl_one_day_tracking_ids:
                        print_detailed_log(f"DEBUG: Sample EKL one day tracking IDs: {ekl_one_day_tracking_ids[:3]}", "INFO")
                    
                    print_detailed_log(f"CPD_EKL tracking IDs for {hub_name}: 0 days: {len(ekl_zero_day_tracking_ids)}, 1 day: {len(ekl_one_day_tracking_ids)}", "DATA")
                    print_detailed_log(f"DEBUG: Before adding EKL tracking IDs, all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
                    
                    # Add EKL tracking IDs to global list with EKL identifier (only if enabled)
                    if tracking_ids_enabled:
                        for tracking_id in ekl_zero_day_tracking_ids:
                            all_tracking_ids.append({
                                'tracking_id': tracking_id,
                                'hub_name': hub_name,
                                'clm_name': clm_name,
                                'state': state,
                                'cpd_days': '0 Days',
                                'cpd_type': 'EKL',  # Mark as EKL tracking ID
                                'timestamp': datetime.now().strftime("%d %b %H:%M")
                            })
                        
                        for tracking_id in ekl_one_day_tracking_ids:
                            all_tracking_ids.append({
                                'tracking_id': tracking_id,
                                'hub_name': hub_name,
                                'clm_name': clm_name,
                                'state': state,
                                'cpd_days': '1 Day',
                                'cpd_type': 'EKL',  # Mark as EKL tracking ID
                                'timestamp': datetime.now().strftime("%d %b %H:%M")
                            })
                    print_detailed_log(f"DEBUG: After adding EKL tracking IDs, all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
                    print_detailed_log(f"DEBUG: Sample EKL tracking IDs added: {all_tracking_ids[-3:] if len(all_tracking_ids) >= 3 else all_tracking_ids}", "INFO")
                else:
                    print_detailed_log(f"No tracking ID column found for {hub_name}, skipping EKL tracking ID extraction", "WARNING")
                
            except Exception as e:
                print_detailed_log(f"Error calculating CPD_EKL: {e}", "ERROR")
                cpd_ekl_0_days = 0
                cpd_ekl_1_day = 0
        else:
            print_detailed_log("CPD_EKL columns will be 0 (no 3-status records or customer_promise_date column not found)", "WARNING")
        
        # Debug: Check if tracking IDs were collected
        print_detailed_log(f"DEBUG: At end of process_csv_data for {hub_name}, all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
        print_detailed_log(f"DEBUG: Summary for {hub_name}: NCD_0_Days={cpd_0_days}, NCD_1_Days={cpd_1_day}, EKL_0_Days={cpd_ekl_0_days}, EKL_1_Days={cpd_ekl_1_day}", "INFO")
        print_detailed_log(f"DEBUG: Expected tracking IDs for {hub_name}: {cpd_0_days + cpd_1_day + cpd_ekl_0_days + cpd_ekl_1_day}", "INFO")
        
        # Count actual tracking IDs collected for this hub
        hub_tracking_ids = [tid for tid in all_tracking_ids if tid['hub_name'] == hub_name]
        hub_ncd_0 = len([tid for tid in hub_tracking_ids if tid['cpd_type'] == 'NCD' and tid['cpd_days'] == '0 Days'])
        hub_ncd_1 = len([tid for tid in hub_tracking_ids if tid['cpd_type'] == 'NCD' and tid['cpd_days'] == '1 Day'])
        hub_ekl_0 = len([tid for tid in hub_tracking_ids if tid['cpd_type'] == 'EKL' and tid['cpd_days'] == '0 Days'])
        hub_ekl_1 = len([tid for tid in hub_tracking_ids if tid['cpd_type'] == 'EKL' and tid['cpd_days'] == '1 Day'])
        
        print_detailed_log(f"📊 TRACKING ID COLLECTION SUMMARY FOR {hub_name}:", "INFO")
        print_detailed_log(f"   Expected: NCD_0_Days={cpd_0_days}, NCD_1_Days={cpd_1_day}, EKL_0_Days={cpd_ekl_0_days}, EKL_1_Days={cpd_ekl_1_day}", "INFO")
        print_detailed_log(f"   Collected: NCD_0_Days={hub_ncd_0}, NCD_1_Days={hub_ncd_1}, EKL_0_Days={hub_ekl_0}, EKL_1_Days={hub_ekl_1}", "INFO")
        print_detailed_log(f"   Total collected for {hub_name}: {len(hub_tracking_ids)}", "INFO")
        print_detailed_log(f"   Total in all_tracking_ids: {len(all_tracking_ids)}", "INFO")
        
        # Wait a moment for the CSV download to complete
        print_detailed_log(f"⏳ Waiting for CSV download to complete for {hub_name}...", "INFO")
        time.sleep(2)  # Wait 2 seconds for download to complete
        
        # Extract tracking IDs directly from the downloaded CSV file for this hub
        print_detailed_log(f"🔍 Extracting tracking IDs directly from downloaded CSV for {hub_name}", "INFO")
        
        # Call separate tracking ID extraction function
        print_detailed_log(f"🔧 ABOUT TO CALL extract_tracking_ids_from_csv for {hub_name}", "INFO")
        try:
            extract_tracking_ids_from_csv(hub_name)
            print_detailed_log(f"✅ FINISHED CALLING extract_tracking_ids_from_csv for {hub_name}", "INFO")
        except Exception as func_e:
            print_detailed_log(f"❌ ERROR calling extract_tracking_ids_from_csv for {hub_name}: {func_e}", "ERROR")
            import traceback
            print_detailed_log(f"DEBUG: Function call traceback: {traceback.format_exc()}", "INFO")
        
        return result_data
    except Exception as e:
        print_detailed_log(f"Error processing CSV data for {hub_name}: {e}", "ERROR")
        return None

def extract_tracking_ids_from_csv(hub_name, csv_file_path=None):
    """Extract tracking IDs from the specified CSV file or the most recent CSV file in Downloads folder"""
    print_detailed_log(f"🚀 STARTING extract_tracking_ids_from_csv for {hub_name}", "INFO")
    try:
        # If csv_file_path is provided, use it; otherwise find the most recent CSV file
        if csv_file_path is None:
            print_detailed_log(f"🔍 Looking for most recent CSV file for {hub_name}", "INFO")
            
            # Check Downloads folder for the most recent CSV file
            download_dir = os.path.expanduser("~/Downloads")
            
            if os.path.exists(download_dir):
                # Get all CSV files in Downloads folder
                csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
                print_detailed_log(f"📁 Found {len(csv_files)} CSV files in Downloads folder", "INFO")
                print_detailed_log(f"🔍 DEBUG: CSV files found: {csv_files[:5]}", "INFO")  # Show first 5 files
                
                if csv_files:
                    # Sort by modification time to get the most recent
                    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
                    most_recent_csv = csv_files[0]
                    csv_file_path = os.path.join(download_dir, most_recent_csv)
                    
                    print_detailed_log(f"✅ Found most recent CSV file: {most_recent_csv}", "SUCCESS")
                    print_detailed_log(f"📁 Full path: {csv_file_path}", "INFO")
                    
                    # Show file modification time
                    file_time = datetime.fromtimestamp(os.path.getmtime(csv_file_path))
                    print_detailed_log(f"📅 File modified: {file_time.strftime('%H:%M:%S')}", "INFO")
                    
                    # Check if file is recent (within last 2 minutes)
                    time_diff = datetime.now() - file_time
                    if time_diff.total_seconds() > 120:  # 2 minutes
                        print_detailed_log(f"⚠️ Warning: CSV file is {time_diff.total_seconds():.0f} seconds old", "WARNING")
                else:
                    print_detailed_log(f"❌ No CSV files found in Downloads folder", "ERROR")
            else:
                print_detailed_log(f"❌ Downloads folder not found: {download_dir}", "ERROR")
        else:
            print_detailed_log(f"📁 Using provided CSV file: {csv_file_path}", "INFO")
        
        if csv_file_path is None:
            print_detailed_log(f"❌ No CSV file found for {hub_name}", "ERROR")
            return None
        
        if csv_file_path and os.path.exists(csv_file_path):
                print_detailed_log(f"📁 Found downloaded CSV file: {csv_file_path}", "INFO")
                print_detailed_log(f"🔍 DEBUG: About to read CSV file and extract tracking IDs", "INFO")
                
                # Read the downloaded CSV file
                downloaded_df = pd.read_csv(csv_file_path)
                print_detailed_log(f"📊 Downloaded CSV has {len(downloaded_df)} records", "INFO")
                print_detailed_log(f"📋 Downloaded CSV columns: {list(downloaded_df.columns)}", "INFO")
                print_detailed_log(f"🔍 DEBUG: CSV file loaded successfully", "INFO")
                
                # Show sample data
                if len(downloaded_df) > 0:
                    print_detailed_log(f"📋 Sample data (first 3 rows):", "INFO")
                    for i, row in downloaded_df.head(3).iterrows():
                        print_detailed_log(f"  Row {i}: {dict(row)}", "INFO")
                
                # Find tracking_id column
                tracking_column = None
                print_detailed_log(f"🔍 Looking for tracking_id column...", "INFO")
                print_detailed_log(f"🔍 Searching for: ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']", "INFO")
                
                for col in downloaded_df.columns:
                    col_lower = col.lower()
                    print_detailed_log(f"🔍 Checking column: '{col}' (lowercase: '{col_lower}')", "INFO")
                    if col_lower in ['tracking_id', 'trackingid', 'tracking id', 'awb', 'awb_no', 'awbno', 'consignment_id', 'consignmentid']:
                        tracking_column = col
                        print_detailed_log(f"✅ Found tracking_id column: '{col}'", "SUCCESS")
                        break
                
                if tracking_column is None:
                    print_detailed_log(f"❌ No tracking_id column found in {csv_file_path}", "ERROR")
                    return None
                
                print_detailed_log(f"✅ Found tracking_id column: '{tracking_column}'", "SUCCESS")
                
                # Find customer_promise_date column
                cpd_column = None
                print_detailed_log(f"🔍 Looking for customer_promise_date column...", "INFO")
                print_detailed_log(f"🔍 Searching for: ['customer_promise_date', 'customerpromisedate', 'customer promise date', 'cpd', 'promise_date', 'promisedate']", "INFO")
                
                for col in downloaded_df.columns:
                    col_lower = col.lower()
                    print_detailed_log(f"🔍 Checking column: '{col}' (lowercase: '{col_lower}')", "INFO")
                    if col_lower in ['customer_promise_date', 'customerpromisedate', 'customer promise date', 'cpd', 'promise_date', 'promisedate']:
                        cpd_column = col
                        print_detailed_log(f"✅ Found customer_promise_date column: '{col}'", "SUCCESS")
                        break
                
                if cpd_column is None:
                    print_detailed_log(f"❌ No customer_promise_date column found in {csv_file_path}", "ERROR")
                    return None
                
                print_detailed_log(f"✅ Found customer_promise_date column: '{cpd_column}'", "SUCCESS")
                
                # Calculate CPD ageing for each record
                current_date = datetime.now().date()
                tracking_ids_to_save = []
                
                # Convert CPD column to datetime for efficient processing
                print_detailed_log(f"🔄 Converting {cpd_column} to datetime for processing...", "INFO")
                print_detailed_log(f"🔍 DEBUG: Starting tracking ID extraction process", "INFO")
                downloaded_df[cpd_column] = pd.to_datetime(downloaded_df[cpd_column], errors='coerce')
                
                # Calculate CPD ageing for all records at once
                current_date = datetime.now().date()
                downloaded_df['cpd_date_only'] = downloaded_df[cpd_column].dt.date
                
                # Calculate days difference properly
                downloaded_df['cpd_days_diff'] = (downloaded_df['cpd_date_only'] - current_date).apply(lambda x: x.days)
                
                # Define the 7 NCD statuses
                ncd_statuses = [
                    'Undelivered_Security_Instability',
                    'Undelivered_Heavy_Load',
                    'Undelivered_Heavy_Rain',
                    'Undelivered_Not_Attended',
                    'Undelivered_SameStateMisroute',
                    'Undelivered_Shipment_Damage',
                    'Undelivered_UntraceableFromHub'
                ]
                
                # Find status column
                status_column = None
                for col in downloaded_df.columns:
                    if col.lower() in ['status', 'latest status', 'lateststatus']:
                        status_column = col
                        break
                
                if status_column is None:
                    print_detailed_log(f"❌ No status column found for {hub_name}", "ERROR")
                    return None
                
                print_detailed_log(f"✅ Using status column: {status_column}", "INFO")
                print_detailed_log(f"🔍 Filtering for NCD statuses: {ncd_statuses}", "INFO")
                
                # Filter records for NCD_0_Days and NCD_1_Days with NCD statuses only
                ncd_0_days_mask = (downloaded_df['cpd_days_diff'] == 0) & (downloaded_df[status_column].isin(ncd_statuses))
                ncd_1_days_mask = (downloaded_df['cpd_days_diff'] == -1) & (downloaded_df[status_column].isin(ncd_statuses))
                
                ncd_0_days_records = downloaded_df[ncd_0_days_mask]
                ncd_1_days_records = downloaded_df[ncd_1_days_mask]
                
                print_detailed_log(f"📊 Found {len(ncd_0_days_records)} records with CPD = Today (0 days)", "INFO")
                print_detailed_log(f"📊 Found {len(ncd_1_days_records)} records with CPD = Yesterday (-1 days)", "INFO")
                
                # Collect tracking IDs for NCD_0_Days
                for _, row in ncd_0_days_records.iterrows():
                    tracking_id = str(row[tracking_column]).strip()
                    if tracking_id and tracking_id != 'nan':
                        tracking_ids_to_save.append({
                            'tracking_id': tracking_id,
                            'hub_name': hub_name,
                            'clm_name': HUB_INFO.get(hub_name, ("", ""))[0],
                            'state': HUB_INFO.get(hub_name, ("", ""))[1],
                            'cpd_days': '0 Days',
                            'cpd_type': 'NCD',
                            'timestamp': datetime.now().strftime("%d %b %H:%M")
                        })
                
                # Collect tracking IDs for NCD_1_Days
                for _, row in ncd_1_days_records.iterrows():
                    tracking_id = str(row[tracking_column]).strip()
                    if tracking_id and tracking_id != 'nan':
                        tracking_ids_to_save.append({
                            'tracking_id': tracking_id,
                            'hub_name': hub_name,
                            'clm_name': HUB_INFO.get(hub_name, ("", ""))[0],
                            'state': HUB_INFO.get(hub_name, ("", ""))[1],
                            'cpd_days': '1 Day',
                            'cpd_type': 'NCD',
                            'timestamp': datetime.now().strftime("%d %b %H:%M")
                        })
                
                print_detailed_log(f"📊 Extracted {len(tracking_ids_to_save)} tracking IDs from {csv_file_path}", "INFO")
                print_detailed_log(f"🔍 DEBUG: Tracking ID extraction completed", "INFO")
                if len(tracking_ids_to_save) > 0:
                    print_detailed_log(f"🔍 DEBUG: Sample tracking IDs: {tracking_ids_to_save[:3]}", "INFO")
                
                # Count by CPD days
                ncd_0_count = len([tid for tid in tracking_ids_to_save if tid['cpd_days'] == '0 Days'])
                ncd_1_count = len([tid for tid in tracking_ids_to_save if tid['cpd_days'] == '1 Day'])
                print_detailed_log(f"📊 Breakdown - NCD_0_Days: {ncd_0_count}, NCD_1_Days: {ncd_1_count}", "INFO")
                
                # Save to temp_CPD.csv (always append mode)
                if tracking_ids_to_save:
                    print_detailed_log(f"💾 Appending {len(tracking_ids_to_save)} tracking IDs to temp_CPD.csv", "INFO")
                    
                    tracking_df = pd.DataFrame(tracking_ids_to_save)
                    tracking_df.to_csv("temp_CPD.csv", mode='a', header=False, index=False)
                    
                    print_detailed_log(f"✅ Successfully saved {len(tracking_ids_to_save)} tracking IDs for {hub_name}", "SUCCESS")
                    
                    # Verify the file contents
                    if os.path.exists("temp_CPD.csv"):
                        temp_df = pd.read_csv("temp_CPD.csv")
                        print_detailed_log(f"📋 temp_CPD.csv now contains {len(temp_df)} total tracking IDs", "INFO")
                        if len(temp_df) > 0:
                            print_detailed_log(f"📋 Sample tracking IDs: {temp_df['tracking_id'].head(3).tolist()}", "INFO")
                            
                            # Count by hub
                            hub_counts = temp_df['hub_name'].value_counts()
                            print_detailed_log(f"📊 Hubs in temp_CPD.csv: {dict(hub_counts)}", "INFO")
                            
                            # Count by CPD days
                            ncd_0_total = len(temp_df[(temp_df['cpd_type'] == 'NCD') & (temp_df['cpd_days'] == '0 Days')])
                            ncd_1_total = len(temp_df[(temp_df['cpd_type'] == 'NCD') & (temp_df['cpd_days'] == '1 Day')])
                            print_detailed_log(f"📊 Total counts - NCD_0_Days: {ncd_0_total}, NCD_1_Days: {ncd_1_total}", "INFO")
                else:
                    print_detailed_log(f"⚠️ No tracking IDs found for {hub_name} that meet NCD_0_Days and NCD_1_Days conditions", "WARNING")
                
                # NOTE: CSV file will be deleted by the CSV processing function after it's done
                print_detailed_log(f"📁 temp_CPD.csv saved in Automate Reports folder with {len(tracking_ids_to_save)} tracking IDs", "INFO")
                print_detailed_log(f"📁 CSV file {os.path.basename(csv_file_path)} will be deleted after CSV processing", "INFO")
                    
        else:
            print_detailed_log(f"❌ No downloaded CSV file found for {hub_name}", "ERROR")
        
        # Show final temp_CPD.csv summary after each hub
        if os.path.exists("temp_CPD.csv"):
            try:
                final_df = pd.read_csv("temp_CPD.csv")
                print_detailed_log(f"📊 FINAL temp_CPD.csv SUMMARY after {hub_name}:", "INFO")
                print_detailed_log(f"   Total tracking IDs: {len(final_df)}", "INFO")
                
                # Count by hub
                hub_counts = final_df['hub_name'].value_counts()
                print_detailed_log(f"   By hub: {dict(hub_counts)}", "INFO")
                
                # Count by CPD days
                ncd_0_total = len(final_df[(final_df['cpd_type'] == 'NCD') & (final_df['cpd_days'] == '0 Days')])
                ncd_1_total = len(final_df[(final_df['cpd_type'] == 'NCD') & (final_df['cpd_days'] == '1 Day')])
                print_detailed_log(f"   NCD_0_Days: {ncd_0_total}, NCD_1_Days: {ncd_1_total}", "INFO")
                
                if len(final_df) > 0:
                    print_detailed_log(f"   Sample tracking IDs: {final_df['tracking_id'].head(3).tolist()}", "INFO")
            except Exception as summary_e:
                print_detailed_log(f"⚠️ Error reading final temp_CPD.csv: {summary_e}", "WARNING")
        else:
            print_detailed_log(f"⚠️ temp_CPD.csv not found after processing {hub_name}", "WARNING")
    except Exception as e:
        print_detailed_log(f"❌ Error in extract_tracking_ids_from_csv for {hub_name}: {e}", "ERROR")
        import traceback
        print_detailed_log(f"DEBUG: Full traceback: {traceback.format_exc()}", "INFO")
    
    print_detailed_log(f"🏁 FINISHED extract_tracking_ids_from_csv for {hub_name}", "INFO")

def navigate_to_tracking_and_select_multiple():
    """Navigate to Tracking tab and select Multiple Shipment Tracking"""
    try:
        from selenium.webdriver.common.by import By
        print_detailed_log("🔍 DEBUG: Starting navigate_to_tracking_and_select_multiple function", "INFO")
        print_detailed_log("🔍 DEBUG: Current URL: " + str(driver.current_url), "INFO")
        print_detailed_log("🔍 DEBUG: Current page title: " + str(driver.title), "INFO")
        print_detailed_log("Navigating to Tracking tab...", "PROGRESS")
        
        # Wait for page to load
        time.sleep(3)
        
        # Find and click on "tracking" tab
        tracking_tab = None
        try:
            # Simple debug: Check what links are available
            all_links = driver.find_elements(By.TAG_NAME, "a")
            print_detailed_log(f"Found {len(all_links)} links on page", "INFO")
            
            # Show first 5 links to see what's available
            for i, link in enumerate(all_links[:5]):
                try:
                    link_text = link.text.strip()
                    if link_text:
                        print_detailed_log(f"Link {i+1}: '{link_text}'", "INFO")
                except:
                    continue
            
            # Try different selectors for tracking tab
            selectors = [
                "//a[contains(text(), 'tracking') or contains(text(), 'Tracking')]",
                "//span[contains(text(), 'tracking') or contains(text(), 'Tracking')]",
                "//div[contains(text(), 'tracking') or contains(text(), 'Tracking')]",
                "//li[contains(text(), 'tracking') or contains(text(), 'Tracking')]",
                "//button[contains(text(), 'tracking') or contains(text(), 'Tracking')]"
            ]

            for selector in selectors:
                try:
                    tracking_tab = driver.find_element(By.XPATH, selector)
                    if tracking_tab.is_displayed():
                        print_detailed_log(f"Found tracking tab with selector: {selector}", "SUCCESS")
                        break
                except:
                    continue

            if tracking_tab:
                tracking_tab.click()
                print_detailed_log("✅ Clicked on tracking tab", "SUCCESS")
                time.sleep(3)
            else:
                print_detailed_log("❌ Could not find tracking tab", "ERROR")
                return False

        except Exception as e:
            print_detailed_log(f"Error clicking tracking tab: {e}", "ERROR")
            return False
        
        # Find and click dropdown to select "Multiple Shipment Tracking" option
        print_detailed_log("Looking for dropdown box to select Multiple Shipment Tracking...", "PROGRESS")
        
        # Debug: Check current page state
        print_detailed_log(f"Current URL: {driver.current_url}", "INFO")
        print_detailed_log(f"Page title: {driver.title}", "INFO")
        
        # Wait a bit more for page to load
        time.sleep(5)
        
        # First, find the dropdown box (which shows "Single Shipment Tracking" by default)
        dropdown_found = False
        try:
            # Try different selectors for dropdown
            dropdown_selectors = [
                "//select",
                "//div[@role='combobox']",
                "//div[contains(@class, 'dropdown')]",
                "//div[contains(@class, 'select')]",
                "//button[contains(@class, 'dropdown')]",
                "//div[contains(text(), 'Single Shipment Tracking')]",
                "//span[contains(text(), 'Single Shipment Tracking')]",
                "//*[contains(text(), 'Single Shipment Tracking')]"
            ]
            
            for i, selector in enumerate(dropdown_selectors):
                try:
                    print_detailed_log(f"Trying dropdown selector {i+1}/{len(dropdown_selectors)}: {selector}", "INFO")
                    dropdown = driver.find_element(By.XPATH, selector)
                    if dropdown.is_displayed():
                        print_detailed_log(f"Found dropdown with selector: {selector}", "SUCCESS")
                        dropdown_found = True
                        break
                except Exception as sel_e:
                    print_detailed_log(f"Dropdown selector {i+1} failed: {sel_e}", "WARNING")
                    continue
            
            if dropdown_found:
                # Click on the dropdown to open it
                print_detailed_log("Clicking on dropdown to open options...", "PROGRESS")
                dropdown.click()
                time.sleep(2)
                
                # Debug: Show all dropdown options that are now visible
                print_detailed_log("🔍 DEBUG: Showing all dropdown options after clicking...", "INFO")
                try:
                    # Look for all visible options
                    all_options = driver.find_elements(By.XPATH, "//option | //div[contains(@class, 'option')] | //div[contains(@class, 'item')] | //li | //span[contains(@class, 'option')]")
                    print_detailed_log(f"Found {len(all_options)} potential dropdown options", "INFO")
                    
                    for i, option in enumerate(all_options):
                        try:
                            option_text = option.text.strip()
                            option_class = option.get_attribute("class")
                            if option_text and len(option_text) < 100:
                                print_detailed_log(f"Option {i+1}: text='{option_text}', class='{option_class}'", "INFO")
                        except:
                            continue
                    
                    # Also look for any text containing "single" or "tracking"
                    single_tracking_elements = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'single') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tracking')]")
                    print_detailed_log(f"Found {len(single_tracking_elements)} elements with 'single' or 'tracking' text", "INFO")
                    
                    for i, element in enumerate(single_tracking_elements[:5]):
                        try:
                            element_text = element.text.strip()
                            element_class = element.get_attribute("class")
                            if element_text:
                                print_detailed_log(f"Single/Tracking element {i+1}: text='{element_text}', class='{element_class}'", "INFO")
                        except:
                            continue
                            
                except Exception as debug_e:
                    print_detailed_log(f"Debug search failed: {debug_e}", "WARNING")
                
                # Now look for "Multiple Shipment Tracking" option in the dropdown
                print_detailed_log("Looking for Multiple Shipment Tracking option in dropdown...", "PROGRESS")
                multiple_tracking = None
                
                multiple_selectors = [
                    "//option[contains(text(), 'Multiple Shipment Tracking')]",
                    "//div[contains(text(), 'Multiple Shipment Tracking')]",
                    "//span[contains(text(), 'Multiple Shipment Tracking')]",
                    "//li[contains(text(), 'Multiple Shipment Tracking')]",
                    "//*[contains(text(), 'Multiple Shipment Tracking')]"
                ]
                
                for i, selector in enumerate(multiple_selectors):
                    try:
                        print_detailed_log(f"Trying multiple tracking selector {i+1}/{len(multiple_selectors)}: {selector}", "INFO")
                        multiple_tracking = driver.find_element(By.XPATH, selector)
                        if multiple_tracking.is_displayed():
                            print_detailed_log(f"Found Multiple Shipment Tracking with selector: {selector}", "SUCCESS")
                            break
                    except Exception as sel_e:
                        print_detailed_log(f"Multiple tracking selector {i+1} failed: {sel_e}", "WARNING")
                        continue
                
                if multiple_tracking:
                    multiple_tracking.click()
                    print_detailed_log("✅ Clicked on Multiple Shipment Tracking option", "SUCCESS")
                    time.sleep(3)
                    dropdown_success = True
                else:
                    print_detailed_log("❌ Could not find Multiple Shipment Tracking option in dropdown", "ERROR")
                    
                    # Fallback: Try to find any option with "Multiple" in the text
                    print_detailed_log("Trying fallback: looking for any option with 'Multiple' text...", "INFO")
                    try:
                        multiple_fallback = driver.find_element(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'multiple')]")
                        if multiple_fallback.is_displayed():
                            print_detailed_log("✅ Found option with 'Multiple' using fallback", "SUCCESS")
                            multiple_fallback.click()
                            print_detailed_log("✅ Clicked on Multiple option", "SUCCESS")
                            time.sleep(3)
                            dropdown_success = True
                        else:
                            print_detailed_log("❌ Multiple option found but not displayed", "ERROR")
                            dropdown_success = False
                    except Exception as fallback_e:
                        print_detailed_log(f"Fallback search failed: {fallback_e}", "WARNING")
                        dropdown_success = False
            else:
                print_detailed_log("❌ Could not find dropdown box", "ERROR")
                
                # Try to find any elements with "single" or "tracking" in text
                print_detailed_log("Trying to find any single/tracking related elements...", "INFO")
                try:
                    all_elements = driver.find_elements(By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'single') or contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'tracking')]")
                    
                    print_detailed_log(f"Found {len(all_elements)} elements with 'single' or 'tracking' in text", "INFO")
                    
                    for i, element in enumerate(all_elements[:10]):  # Show first 10
                        try:
                            element_text = element.text.strip()
                            if element_text:
                                print_detailed_log(f"Element {i}: {element_text[:100]}...", "INFO")
                        except:
                            continue
                            
                except Exception as debug_e:
                    print_detailed_log(f"Debug search failed: {debug_e}", "WARNING")
                
                dropdown_success = False
                
        except Exception as e:
            print_detailed_log(f"Error handling dropdown: {e}", "ERROR")
            dropdown_success = False
        
        # Only proceed with tracking ID pasting if dropdown selection was successful
        if not dropdown_success:
            print_detailed_log("❌ Dropdown selection failed - cannot proceed with tracking ID pasting", "ERROR")
            return False
            
        print_detailed_log("✅ Successfully navigated to Multiple Shipment Tracking page", "SUCCESS")
        
        # Step 3: Copy and paste tracking IDs from temp CSV file
        global tracking_ids_processed
        
        # Check if tracking IDs have already been processed
        if tracking_ids_processed:
            print_detailed_log("⚠️ Tracking IDs have already been processed - skipping to avoid duplicate pasting", "WARNING")
            return True
        
        print_detailed_log("Reading tracking IDs from temp_CPD.csv...", "PROGRESS")
        try:
            # Read tracking IDs from the temp CSV file
            tracking_df = pd.read_csv("temp_CPD.csv")
            tracking_ids = tracking_df['tracking_id'].astype(str).tolist()
            print_detailed_log(f"Loaded {len(tracking_ids)} tracking IDs from temp_CPD.csv", "INFO")
            
            # Check if we need to batch the tracking IDs (limit is ~2800)
            BATCH_SIZE = 2800
            if len(tracking_ids) > BATCH_SIZE:
                print_detailed_log(f"Large dataset detected ({len(tracking_ids)} IDs), using batching with {BATCH_SIZE} IDs per batch", "INFO")
                result = process_tracking_ids_in_batches(driver, tracking_ids, BATCH_SIZE)
                if result:
                    tracking_ids_processed = True
                    print_detailed_log("✅ Tracking IDs processing completed - flag set to prevent duplicate pasting", "SUCCESS")
                return result
            else:
                print_detailed_log(f"Small dataset ({len(tracking_ids)} IDs), processing in single batch", "INFO")
                # Prepare tracking IDs for pasting with multiple separator options
                # Try different separators based on what the portal expects
                separators = [' ', ', ', '\n', '\t']  # Prioritize space as it works better
                tracking_ids_text = '\n'.join(tracking_ids)  # Default to newlines
                print_detailed_log(f"Prepared {len(tracking_ids)} tracking IDs with newline separators", "INFO")
            
            # Find textarea or input field for pasting tracking IDs
            print_detailed_log("Looking for input field to paste tracking IDs...", "PROGRESS")
            input_field = None
            try:
                # Target ONLY the specific Multiple Shipment Tracking field - NO OTHER PLACE
                selectors = [
                    # ONLY target the exact input field with ID react-select-4-input
                    "//input[@id='react-select-4-input']"
                ]
                
                for i, selector in enumerate(selectors):
                    try:
                        print_detailed_log(f"Trying input field selector {i+1}/{len(selectors)}: {selector}", "INFO")
                        input_field = driver.find_element(By.XPATH, selector)
                        if input_field.is_displayed():
                            # Since we're targeting only the specific ID, just verify it's the right field
                            field_id = input_field.get_attribute("id")
                            if field_id == "react-select-4-input":
                                print_detailed_log(f"✅ Found EXACT target field: {field_id} - This is the ONLY place to paste tracking IDs", "SUCCESS")
                                break
                            else:
                                print_detailed_log(f"❌ Wrong field found - ID: {field_id}, expected: react-select-4-input", "ERROR")
                                continue
                    except Exception as sel_e:
                        print_detailed_log(f"Input field selector {i+1} failed: {sel_e}", "WARNING")
                        continue
                
                if input_field:
                    # Clear the field first
                    input_field.clear()
                    time.sleep(1)
                    
                    # Debug: Show sample of tracking IDs being pasted
                    sample_ids = tracking_ids[:5] if len(tracking_ids) >= 5 else tracking_ids
                    print_detailed_log(f"Sample tracking IDs: {sample_ids}", "INFO")
                    print_detailed_log(f"Separator used: newline (\\n)", "INFO")
                    
                    # Check if we need to chunk the data for very large datasets
                    if len(tracking_ids) > 1000:
                        print_detailed_log(f"Large dataset detected ({len(tracking_ids)} IDs), using chunked approach...", "INFO")
                        chunk_size = 500
                        chunks = [tracking_ids[i:i + chunk_size] for i in range(0, len(tracking_ids), chunk_size)]
                        
                        for chunk_num, chunk in enumerate(chunks, 1):
                            chunk_text = '\n'.join(chunk)
                            print_detailed_log(f"Pasting chunk {chunk_num}/{len(chunks)} ({len(chunk)} IDs)...", "INFO")
                            
                            try:
                                # Use JavaScript to set value directly (much faster)
                                driver.execute_script("arguments[0].value = arguments[1];", input_field, chunk_text)
                                
                                # Trigger input event to ensure the page recognizes the change
                                driver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", input_field)
                                driver.execute_script("arguments[0].dispatchEvent(new Event('change', { bubbles: true }));", input_field)
                                
                                print_detailed_log(f"✅ Pasted chunk {chunk_num}/{len(chunks)}", "SUCCESS")
                                time.sleep(0.5)  # Brief pause between chunks
                                
                            except Exception as chunk_error:
                                print_detailed_log(f"❌ Error pasting chunk {chunk_num}: {chunk_error}", "ERROR")
                                return False
                        
                        print_detailed_log(f"✅ Successfully pasted all {len(tracking_ids)} tracking IDs in {len(chunks)} chunks", "SUCCESS")
                    else:
                        # Fast paste tracking IDs using JavaScript with different separator options
                        max_retries = 3
                        separator_attempts = 0
                        
                        for retry in range(max_retries):
                            try:
                                # Try different separators if needed
                                if retry > 0 and separator_attempts < len(separators) - 1:
                                    separator_attempts += 1
                                    current_separator = separators[separator_attempts]
                                    tracking_ids_text = current_separator.join(tracking_ids)
                                    print_detailed_log(f"Retrying with separator: '{current_separator}' (attempt {retry + 1}/{max_retries})", "INFO")
                                else:
                                    current_separator = '\n'
                                
                                print_detailed_log(f"Fast pasting {len(tracking_ids)} tracking IDs with comma separation (attempt {retry + 1}/{max_retries})...", "INFO")
                                
                                # Method 1: React Select specific approach (try this first)
                                try:
                                    # Clear the field first
                                    input_field.clear()
                                    time.sleep(0.5)
                                    
                                    # Focus on the input field
                                    driver.execute_script("arguments[0].focus();", input_field)
                                    time.sleep(0.5)
                                    
                                    # For React Select, we need to type each ID individually with proper separation
                                    space_separated_text = ' '.join(tracking_ids)
                                    print_detailed_log(f"React Select method - typing: '{space_separated_text[:100]}...'", "INFO")
                                    
                                    # Type the text directly with verification
                                    input_field.send_keys(space_separated_text)
                                    time.sleep(2)
                                    
                                    # Verify what was actually typed
                                    actual_value = driver.execute_script("return arguments[0].value;", input_field)
                                    space_count = actual_value.count(' ')
                                    expected_spaces = len(tracking_ids) - 1
                                    
                                    print_detailed_log(f"React Select method - Pasted {len(tracking_ids)} IDs with {space_count} spaces (expected: {expected_spaces})", "INFO")
                                    print_detailed_log(f"React Select actual value: '{actual_value[:100]}...'", "INFO")
                                    
                                    # If spaces are missing, try clipboard pasting (much faster)
                                    if space_count < expected_spaces * 0.8:
                                        print_detailed_log("Spaces missing, trying clipboard pasting method", "WARNING")
                                        
                                        try:
                                            import pyperclip
                                            
                                            # Clear and retry with clipboard
                                            input_field.clear()
                                            time.sleep(1)
                                            driver.execute_script("arguments[0].focus();", input_field)
                                            time.sleep(1)
                                            
                                            # Copy space-separated text to clipboard
                                            pyperclip.copy(space_separated_text)
                                            
                                            # Paste using Ctrl+V (much faster)
                                            from selenium.webdriver.common.keys import Keys
                                            from selenium.webdriver.common.action_chains import ActionChains
                                            
                                            actions = ActionChains(driver)
                                            actions.key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL).perform()
                                            
                                            time.sleep(2)
                                            
                                            # Verify again
                                            actual_value = driver.execute_script("return arguments[0].value;", input_field)
                                            space_count = actual_value.count(' ')
                                            print_detailed_log(f"Clipboard pasting - Pasted {len(tracking_ids)} IDs with {space_count} spaces", "INFO")
                                            
                                        except ImportError:
                                            print_detailed_log("pyperclip not available, skipping clipboard method", "WARNING")
                                        except Exception as clipboard_error:
                                            print_detailed_log(f"Clipboard method failed: {clipboard_error}", "WARNING")
                                    
                                    print_detailed_log(f"✅ Successfully pasted {len(tracking_ids)} tracking IDs using React Select method", "SUCCESS")
                                    break
                                    
                                except Exception as react_error:
                                    print_detailed_log(f"React Select method failed: {react_error}", "WARNING")
                                    
                                    # Method 2: Use clipboard-based pasting (most reliable for React components)
                                try:
                                    import pyperclip
                                    
                                    # Clear the field first
                                    input_field.clear()
                                    time.sleep(0.5)
                                    
                                    # Focus on the input field using JavaScript to avoid click interception
                                    try:
                                        driver.execute_script("arguments[0].focus();", input_field)
                                        time.sleep(0.5)
                                    except:
                                        # Fallback to click if JavaScript focus fails
                                        input_field.click()
                                        time.sleep(0.5)
                                    
                                    # Copy tracking IDs to clipboard with space formatting (works better)
                                    clipboard_text = ' '.join(tracking_ids)
                                    pyperclip.copy(clipboard_text)
                                    
                                    # Debug: Show what we're trying to paste
                                    print_detailed_log(f"Clipboard content (first 100 chars): '{clipboard_text[:100]}...'", "INFO")
                                    
                                    # Paste using Ctrl+V
                                    from selenium.webdriver.common.keys import Keys
                                    from selenium.webdriver.common.action_chains import ActionChains
                                    
                                    actions = ActionChains(driver)
                                    actions.key_down(Keys.CONTROL).send_keys('v').key_up(Keys.CONTROL).perform()
                                    
                                    # Verify what was actually pasted
                                    time.sleep(1)
                                    actual_value = driver.execute_script("return arguments[0].value;", input_field)
                                    print_detailed_log(f"Actual value in field (first 100 chars): '{actual_value[:100]}...'", "INFO")
                                    
                                    print_detailed_log(f"✅ Successfully pasted {len(tracking_ids)} tracking IDs using clipboard", "SUCCESS")
                                    
                                except ImportError:
                                    print_detailed_log("pyperclip not available, using fallback method", "WARNING")
                                    
                                    # Fallback: Use send_keys with proper formatting
                                    try:
                                        # Clear the field
                                        input_field.clear()
                                        time.sleep(0.5)
                                        
                                        # Focus and paste with proper newlines
                                        try:
                                            driver.execute_script("arguments[0].focus();", input_field)
                                            time.sleep(0.5)
                                        except:
                                            input_field.click()
                                            time.sleep(0.5)
                                        
                                        # Use send_keys with space separation
                                        space_separated_text = ' '.join(tracking_ids)
                                        input_field.send_keys(space_separated_text)
                                        
                                        print_detailed_log(f"✅ Successfully pasted {len(tracking_ids)} tracking IDs using send_keys", "SUCCESS")
                                        
                                    except Exception as fallback_error:
                                        print_detailed_log(f"Fallback method failed: {fallback_error}", "ERROR")
                                        raise Exception("All paste methods failed")
                                        
                                except Exception as clipboard_error:
                                    print_detailed_log(f"Clipboard method failed: {clipboard_error}", "WARNING")
                                    
                                    # Final fallback: Direct JavaScript with proper formatting
                                    try:
                                        input_field.clear()
                                        time.sleep(0.5)
                                        
                                        # Set value with space separation
                                        space_separated_text = ' '.join(tracking_ids)
                                        driver.execute_script("arguments[0].value = arguments[1];", input_field, space_separated_text)
                                        
                                        # Trigger events
                                        driver.execute_script("""
                                            arguments[0].dispatchEvent(new Event('input', { bubbles: true }));
                                            arguments[0].dispatchEvent(new Event('change', { bubbles: true }));
                                        """, input_field)
                                        
                                        print_detailed_log(f"✅ Successfully pasted {len(tracking_ids)} tracking IDs using JavaScript", "SUCCESS")
                                        
                                    except Exception as js_error:
                                        print_detailed_log(f"JavaScript method failed: {js_error}", "ERROR")
                                        raise Exception("All paste methods failed")
                                
                                print_detailed_log(f"✅ Fast pasted {len(tracking_ids)} tracking IDs with separator '{current_separator}'", "SUCCESS")
                                time.sleep(1)  # Reduced wait time since JavaScript is faster
                                break
                            except Exception as paste_error:
                                print_detailed_log(f"Fast paste attempt {retry + 1} failed: {paste_error}", "WARNING")
                            if retry < max_retries - 1:
                                print_detailed_log("Retrying in 2 seconds...", "INFO")
                                time.sleep(2)
                                # Try to refresh the input field
                                try:
                                    input_field = driver.find_element(By.XPATH, "//textarea | //input[@type='text'] | //input[not(@type)]")
                                except:
                                    pass
                            else:
                                print_detailed_log("❌ All fast paste attempts failed, trying slow method...", "WARNING")
                                # Fallback to slow method
                                try:
                                    input_field.send_keys(tracking_ids_text)
                                    print_detailed_log(f"✅ Slow pasted {len(tracking_ids)} tracking IDs", "SUCCESS")
                                except Exception as slow_error:
                                    print_detailed_log(f"❌ Slow paste also failed: {slow_error}", "ERROR")
                                    return False
                else:
                    print_detailed_log("❌ Could not find input field for tracking IDs", "ERROR")
                    return False
                    
            except Exception as e:
                print_detailed_log(f"Error pasting tracking IDs: {e}", "ERROR")
                return False
            
            # Step 4: Find and click download/submit button
            print_detailed_log("Looking for download/submit button...", "PROGRESS")
            download_button = None
            try:
                # Target ONLY the specific Download button - NO OTHER BUTTON
                selectors = [
                    # ONLY target the exact Download button with specific classes
                    "//button[@class='sc-fzXfNg gHtILN' and text()='Download']",
                    "//button[@class='sc-fzXfNg gHtILN' and text()='DOWNLOAD']",
                    "//button[contains(@class, 'sc-fzXfNg') and contains(@class, 'gHtILN') and text()='Download']",
                    "//button[contains(@class, 'sc-fzXfNg') and contains(@class, 'gHtILN') and text()='DOWNLOAD']",
                    # More specific selector for the exact button you provided
                    "//button[contains(@class, 'sc-fzXfNg') and contains(@class, 'gHtILN') and normalize-space(text())='Download']"
                ]
                
                for i, selector in enumerate(selectors):
                    try:
                        print_detailed_log(f"Trying download button selector {i+1}/{len(selectors)}: {selector}", "INFO")
                        download_button = driver.find_element(By.XPATH, selector)
                        if download_button.is_displayed():
                            # Verify this is the exact Download button we want
                            button_text = download_button.text.strip()
                            button_class = download_button.get_attribute("class")
                            if (button_text == "Download" or button_text.upper() == "DOWNLOAD") and "sc-fzXfNg" in button_class and "gHtILN" in button_class:
                                print_detailed_log(f"✅ Found EXACT Download button: '{button_text}' with classes: {button_class}", "SUCCESS")
                                break
                            else:
                                print_detailed_log(f"❌ Wrong button found - Text: '{button_text}', Classes: {button_class}", "ERROR")
                                continue
                    except Exception as sel_e:
                        print_detailed_log(f"Download button selector {i+1} failed: {sel_e}", "WARNING")
                        continue
                
                if download_button:
                    # Get current window handles before clicking
                    current_windows = driver.window_handles
                    print_detailed_log(f"📋 Current windows before click: {len(current_windows)}", "INFO")
                    
                    download_button.click()
                    print_detailed_log("✅ Clicked download/submit button", "SUCCESS")
                    
                    # Wait a moment for new tab to open
                    time.sleep(3)
                    
                    # Check if a new tab opened
                    new_windows = driver.window_handles
                    print_detailed_log(f"📋 Windows after click: {len(new_windows)}", "INFO")
                    
                    if len(new_windows) > len(current_windows):
                        print_detailed_log("🆕 New tab opened for download - switching to it", "INFO")
                        # Switch to the new tab
                        driver.switch_to.window(new_windows[-1])
                        time.sleep(5)  # Wait for download to complete in new tab
                        
                        # Close the new tab and switch back to original
                        driver.close()
                        driver.switch_to.window(current_windows[0])
                        print_detailed_log("✅ Switched back to original tab", "SUCCESS")
                    else:
                        print_detailed_log("📥 Download in same tab - waiting for completion", "INFO")
                        time.sleep(8)  # Wait for download to complete
                else:
                    print_detailed_log("⚠️ Could not find download/submit button - checking for automatic download", "WARNING")
                    time.sleep(5)  # Wait for potential automatic download
                
                # Always check for CSV file after attempting download - this is the primary success indicator
                try:
                    csv_file = find_latest_csv_file()
                    if csv_file:
                        print_detailed_log(f"✅ Download successful - Found CSV: {os.path.basename(csv_file)}", "SUCCESS")
                    else:
                        # If no file found, wait a bit more and try again
                        print_detailed_log("⏳ No CSV file found immediately, waiting 5 seconds and retrying...", "INFO")
                        time.sleep(5)
                        csv_file = find_latest_csv_file()
                        
                        if csv_file:
                            print_detailed_log(f"✅ Download successful - Found CSV on retry: {os.path.basename(csv_file)}", "SUCCESS")
                        else:
                            print_detailed_log("❌ No CSV file found in Downloads folder - download failed", "ERROR")
                            return False
                except NameError:
                    print_detailed_log("❌ find_latest_csv_file function not available", "ERROR")
                    return False
                    
            except Exception as e:
                print_detailed_log(f"Error clicking download button: {e}", "ERROR")
                return False
            
            print_detailed_log("✅ Successfully completed tracking download process", "SUCCESS")
            
            # Set flag to prevent duplicate tracking ID pasting
            tracking_ids_processed = True
            print_detailed_log("✅ Tracking IDs processing completed - flag set to prevent duplicate pasting", "SUCCESS")
            
            print_detailed_log("🔍 DEBUG: About to start Continue button search...", "INFO")
            print_detailed_log("🔍 DEBUG: Script should continue to Continue button section...", "INFO")
            print_detailed_log("🔍 DEBUG: New screen should appear with Continue button...", "INFO")
            
            # Step 7: Click the Continue button
            print_detailed_log("🔍 DEBUG: ENTERING Continue button section", "INFO")
            print_detailed_log("Looking for Continue button...", "PROGRESS")
            continue_button = None
            try:
                # Wait a bit for the page to load after download
                    time.sleep(2)
                    
                    # Wait for the download completion message to appear
                    print_detailed_log("Waiting for download completion message...", "PROGRESS")
                    try:
                        from selenium.webdriver.support.ui import WebDriverWait
                        from selenium.webdriver.support import expected_conditions as EC
                        from selenium.webdriver.common.by import By
                        
                        # Wait for the modal container to appear
                        print_detailed_log("Waiting for modal container...", "PROGRESS")
                        modal_container = wait.until(
                            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ModalComponent-modal-container')]"))
                        )
                        print_detailed_log("✅ Modal container found", "SUCCESS")
                        
                        # Wait for the download completion message inside the modal
                        download_complete_element = modal_container.find_element(By.XPATH, ".//div[contains(text(), 'Download Complete')]")
                        print_detailed_log("✅ Download completion message found in modal", "SUCCESS")
                        
                        # Wait a bit more for the button to appear
                        time.sleep(3)
                        
                        # Try to find the Continue button inside the modal
                        print_detailed_log("Looking for Continue button inside modal...", "PROGRESS")
                        try:
                            # Look for the Continue button inside the modal container
                            continue_button = modal_container.find_element(By.XPATH, ".//button[contains(text(), 'Continue')]")
                            if continue_button.is_displayed():
                                print_detailed_log("✅ Found Continue button inside modal", "SUCCESS")
                                continue_button.click()
                                print_detailed_log("✅ Clicked Continue button", "SUCCESS")
                                time.sleep(3)
                                return True
                        except Exception as rel_e:
                            print_detailed_log(f"Modal button search failed: {rel_e}", "WARNING")
                        
                        # Debug: Show all elements on the page to understand what's available
                        print_detailed_log("🔍 DEBUG: Showing all elements on page after download...", "INFO")
                        try:
                            # Show all divs with text content
                            all_divs = driver.find_elements(By.TAG_NAME, "div")
                            print_detailed_log(f"Found {len(all_divs)} divs on page", "INFO")
                            
                            for i, div in enumerate(all_divs[:10]):  # Show first 10 divs
                                try:
                                    div_text = div.text.strip()
                                    div_class = div.get_attribute("class")
                                    if div_text and len(div_text) < 100:  # Show short text only
                                        print_detailed_log(f"Div {i+1}: text='{div_text[:50]}...', class='{div_class}'", "INFO")
                                except:
                                    continue
                            
                            # Show all buttons
                            all_buttons = driver.find_elements(By.TAG_NAME, "button")
                            print_detailed_log(f"Found {len(all_buttons)} buttons on page", "INFO")
                            
                            for i, button in enumerate(all_buttons):
                                try:
                                    button_text = button.text.strip()
                                    button_class = button.get_attribute("class")
                                    if button_text:
                                        print_detailed_log(f"Button {i+1}: text='{button_text}', class='{button_class}'", "INFO")
                                except:
                                    continue
                                    
                        except Exception as debug_e:
                            print_detailed_log(f"Debug search failed: {debug_e}", "WARNING")
                        
                    except Exception as wait_e:
                        print_detailed_log(f"⚠️ Download completion message not found: {wait_e}", "WARNING")
                    
                    # Try different selectors for Continue button - based on exact HTML structure
                    selectors = [
                        "//div[contains(@class, 'dnoTmY')]//div[@class='hViphV']//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(@class, 'ebtjII')]//div[@class='hViphV']//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(@class, 'hfKXCZ')]//div[@class='hViphV']//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[@class='hViphV']//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(@class, 'hViphV')]//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(text(), 'Download Complete')]//div[@class='hViphV']//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(text(), 'Download Complete')]//button[@class='sc-fzXfNg fFaqcP']",
                        "//button[@class='sc-fzXfNg fFaqcP' and contains(@style, 'width: 50%')]",
                        "//button[@class='sc-fzXfNg fFaqcP']",
                        "//div[contains(text(), 'Download Complete')]//button[contains(@class, 'fFaqcP')]",
                        "//div[contains(text(), 'Download Complete')]//button",
                        "//button[contains(@class, 'sc-fzXfNg') and contains(@class, 'fFaqcP')]",
                        "//button[contains(@style, 'width: 50%')]",
                        "//button[contains(text(), 'Continue')]",
                        "//button[contains(text(), 'continue')]",
                        "//button[contains(@class, 'fFaqcP')]",
                        "//button[contains(@class, 'sc-fzXfNg')]"
                    ]
                    
                    # Simple fallback: Try to find any button with "Continue" text
                    print_detailed_log("Trying simple fallback: looking for any button with 'Continue' text...", "INFO")
                    try:
                        simple_continue_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Continue')]")
                        if simple_continue_button.is_displayed():
                            print_detailed_log("✅ Found Continue button using simple fallback", "SUCCESS")
                            simple_continue_button.click()
                            print_detailed_log("✅ Clicked Continue button", "SUCCESS")
                            time.sleep(3)
                            return True
                    except Exception as simple_e:
                        print_detailed_log(f"Simple fallback failed: {simple_e}", "WARNING")
                    
                    # Debug: Show current page state
                    print_detailed_log(f"Current URL: {driver.current_url}", "INFO")
                    print_detailed_log(f"Page title: {driver.title}", "INFO")
                    
                    # Look for any buttons on the page for debugging
                    all_buttons = driver.find_elements(By.TAG_NAME, "button")
                    print_detailed_log(f"Found {len(all_buttons)} buttons on the page", "INFO")
                    
                    # Show ALL buttons for debugging
                    for i, button in enumerate(all_buttons):
                        try:
                            button_text = button.text.strip()
                            button_class = button.get_attribute("class")
                            button_style = button.get_attribute("style")
                            if button_text or button_class:
                                print_detailed_log(f"Button {i+1}: Text='{button_text}', Class='{button_class}', Style='{button_style}'", "INFO")
                        except Exception as btn_e:
                            print_detailed_log(f"Error getting button {i+1} info: {btn_e}", "WARNING")
                            continue
                    
                    for i, selector in enumerate(selectors):
                        try:
                            print_detailed_log(f"Trying Continue button selector {i+1}/{len(selectors)}: {selector}", "INFO")
                            continue_button = driver.find_element(By.XPATH, selector)
                            if continue_button.is_displayed():
                                print_detailed_log(f"Found Continue button with selector: {selector}", "SUCCESS")
                                break
                        except Exception as sel_e:
                            print_detailed_log(f"Continue button selector {i+1} failed: {sel_e}", "WARNING")
                            continue
                    
                    if continue_button:
                        # Scroll to the button to make sure it's visible
                        driver.execute_script("arguments[0].scrollIntoView(true);", continue_button)
                        time.sleep(1)
                        
                        continue_button.click()
                        print_detailed_log("✅ Clicked Continue button", "SUCCESS")
                        time.sleep(3)  # Wait for page to respond
                        
                        # Step 8: Click the Dashboard button
                        print_detailed_log("Looking for Dashboard button...", "PROGRESS")
                        dashboard_button = None
                        try:
                            # Wait a bit for the page to load after Continue
                            time.sleep(2)
                            
                            # Try different selectors for Dashboard button
                            dashboard_selectors = [
                                "//a[contains(text(), 'Dashboard')]",
                                "//button[contains(text(), 'Dashboard')]",
                                "//span[contains(text(), 'Dashboard')]",
                                "//div[contains(text(), 'Dashboard')]",
                                "//a[contains(@href, 'dashboard')]",
                                "//a[contains(@href, 'Dashboard')]",
                                "//li[contains(text(), 'Dashboard')]",
                                "//nav//a[contains(text(), 'Dashboard')]"
                            ]
                            
                            # Debug: Show current page state after Continue
                            print_detailed_log(f"Current URL after Continue: {driver.current_url}", "INFO")
                            print_detailed_log(f"Page title after Continue: {driver.title}", "INFO")
                            
                            # Look for any links/buttons on the page for debugging
                            all_links = driver.find_elements(By.TAG_NAME, "a")
                            all_buttons = driver.find_elements(By.TAG_NAME, "button")
                            print_detailed_log(f"Found {len(all_links)} links and {len(all_buttons)} buttons on the page", "INFO")
                            
                            for i, link in enumerate(all_links[:5]):  # Show first 5 links
                                try:
                                    link_text = link.text.strip()
                                    link_href = link.get_attribute("href")
                                    if link_text:
                                        print_detailed_log(f"Link {i+1}: Text='{link_text}', Href='{link_href}'", "INFO")
                                except:
                                    continue
                            
                            for i, selector in enumerate(dashboard_selectors):
                                try:
                                    print_detailed_log(f"Trying Dashboard selector {i+1}/{len(dashboard_selectors)}: {selector}", "INFO")
                                    dashboard_button = driver.find_element(By.XPATH, selector)
                                    if dashboard_button.is_displayed():
                                        print_detailed_log(f"Found Dashboard button with selector: {selector}", "SUCCESS")
                                        break
                                except Exception as sel_e:
                                    print_detailed_log(f"Dashboard selector {i+1} failed: {sel_e}", "WARNING")
                                    continue
                            
                            if dashboard_button:
                                # Scroll to the button to make sure it's visible
                                driver.execute_script("arguments[0].scrollIntoView(true);", dashboard_button)
                                time.sleep(1)
                                
                                dashboard_button.click()
                                print_detailed_log("✅ Clicked Dashboard button", "SUCCESS")
                                time.sleep(3)  # Wait for page to respond
                            else:
                                print_detailed_log("⚠️ Could not find Dashboard button", "WARNING")
                                print_detailed_log("Available links on page:", "INFO")
                                for i, link in enumerate(all_links):
                                    try:
                                        link_text = link.text.strip()
                                        if link_text:
                                            print_detailed_log(f"  Link {i+1}: '{link_text}'", "INFO")
                                    except:
                                        continue
                                
                        except Exception as e:
                            print_detailed_log(f"Error clicking Dashboard button: {e}", "ERROR")
                    else:
                        print_detailed_log("⚠️ Could not find Continue button", "WARNING")
                        print_detailed_log("Available buttons on page:", "INFO")
                        for i, button in enumerate(all_buttons):
                            try:
                                button_text = button.text.strip()
                                if button_text:
                                    print_detailed_log(f"  Button {i+1}: '{button_text}'", "INFO")
                            except:
                                continue
                        
            except Exception as e:
                print_detailed_log(f"Error clicking Continue button: {e}", "ERROR")
            
            return True
                
        except Exception as e:
            print_detailed_log(f"Error reading tracking IDs from CSV: {e}", "ERROR")
            return False
        
        return True
        
    except Exception as e:
        print_detailed_log(f"Error in tracking navigation: {e}", "ERROR")
        return False

def save_tracking_ids_to_file():
    """Save all collected tracking IDs to a CSV file"""
    global all_tracking_ids
    try:
        print_detailed_log(f"DEBUG: save_tracking_ids_to_file called. all_tracking_ids length: {len(all_tracking_ids)}", "INFO")
        print_detailed_log(f"DEBUG: all_tracking_ids content: {all_tracking_ids[:3] if all_tracking_ids else 'Empty'}", "INFO")
        print_detailed_log(f"DEBUG: all_tracking_ids type: {type(all_tracking_ids)}", "INFO")
        print_detailed_log(f"DEBUG: Current working directory: {os.getcwd()}", "INFO")
        
        if all_tracking_ids:
            # Remove duplicates based on tracking_id to avoid portal duplicate removal issues
            unique_tracking_ids = []
            seen_tracking_ids = set()
            
            for tracking_data in all_tracking_ids:
                tracking_id = tracking_data['tracking_id']
                if tracking_id not in seen_tracking_ids:
                    seen_tracking_ids.add(tracking_id)
                    unique_tracking_ids.append(tracking_data)
                else:
                    # If duplicate found, keep the NCD version (more comprehensive)
                    existing_index = next(i for i, data in enumerate(unique_tracking_ids) if data['tracking_id'] == tracking_id)
                    if tracking_data['cpd_type'] == 'NCD' and unique_tracking_ids[existing_index]['cpd_type'] == 'EKL':
                        # Replace EKL with NCD version
                        unique_tracking_ids[existing_index] = tracking_data
            
            print_detailed_log(f"DEBUG: Removed duplicates. Original: {len(all_tracking_ids)}, Unique: {len(unique_tracking_ids)}", "INFO")
            
            # Create DataFrame from unique tracking IDs
            print_detailed_log(f"DEBUG: Creating DataFrame from {len(unique_tracking_ids)} unique tracking IDs", "INFO")
            tracking_df = pd.DataFrame(unique_tracking_ids)
            print_detailed_log(f"DEBUG: DataFrame created successfully. Shape: {tracking_df.shape}", "INFO")
            print_detailed_log(f"DEBUG: DataFrame columns: {list(tracking_df.columns)}", "INFO")

            # Use fixed filename - replace existing file each time
            filename = "temp_CPD.csv"

            # Try to save with better error handling
            try:
                # Check if file is locked by another process
                if os.path.exists(filename):
                    try:
                        # Try to open the file in write mode to check if it's locked
                        with open(filename, 'w') as test_file:
                            pass
                    except PermissionError:
                        print_detailed_log(f"⚠️ File {filename} is locked by another process. Trying to delete and recreate...", "WARNING")
                        try:
                            os.remove(filename)
                            time.sleep(1)  # Wait a moment
                        except Exception as del_e:
                            print_detailed_log(f"⚠️ Could not delete locked file: {del_e}", "WARNING")
                            # Try with a different filename
                            filename = f"temp_CPD_{int(time.time())}.csv"
                            print_detailed_log(f"⚠️ Using alternative filename: {filename}", "WARNING")

                # Save to CSV
                tracking_df.to_csv(filename, index=False)

                print_detailed_log(f"✅ Saved {len(unique_tracking_ids)} unique tracking IDs to {filename}", "SUCCESS")
                print_detailed_log(f"📊 Tracking IDs Summary:", "DATA")
                print_detailed_log(f"   • Total Unique Tracking IDs: {len(unique_tracking_ids)}", "DATA")
                print_detailed_log(f"   • 0 Days CPD: {len([tid for tid in unique_tracking_ids if tid['cpd_days'] == '0 Days'])}", "DATA")
                print_detailed_log(f"   • 1 Day CPD: {len([tid for tid in unique_tracking_ids if tid['cpd_days'] == '1 Day'])}", "DATA")

                # Count by hub
                hub_counts = tracking_df['hub_name'].value_counts()
                print_detailed_log(f"   • By Hub:", "DATA")
                for hub, count in hub_counts.items():
                    print_detailed_log(f"     - {hub}: {count} tracking IDs", "DATA")

                return filename
                
            except PermissionError as pe:
                print_detailed_log(f"❌ Permission denied saving {filename}: {pe}", "ERROR")
                # Try with a timestamped filename
                timestamped_filename = f"temp_CPD_{int(time.time())}.csv"
                tracking_df.to_csv(timestamped_filename, index=False)
                print_detailed_log(f"✅ Saved to alternative file: {timestamped_filename}", "SUCCESS")
                return timestamped_filename
        else:
            print_detailed_log("No tracking IDs collected", "WARNING")
            print_detailed_log(f"DEBUG: all_tracking_ids is empty or None. Length: {len(all_tracking_ids) if all_tracking_ids else 'None'}", "INFO")
            return None
    except Exception as e:
        print_detailed_log(f"❌ Failed to save CPD tracking IDs file: {e}", "ERROR")
        print_detailed_log(f"DEBUG: Exception type: {type(e)}", "INFO")
        import traceback
        print_detailed_log(f"DEBUG: Full traceback: {traceback.format_exc()}", "INFO")
        return None

def paste_tracking_ids_from_file():
    """Read tracking IDs from temp_CPD.csv and paste them into Multiple Shipment Tracking"""
    try:
        print_detailed_log("🔍 DEBUG: Starting paste_tracking_ids_from_file function", "INFO")
        print_detailed_log("🔍 DEBUG: Current URL: " + str(driver.current_url), "INFO")
        print_detailed_log("🔍 DEBUG: Current page title: " + str(driver.title), "INFO")
        print_detailed_log("Reading tracking IDs from temp_CPD.csv...", "PROGRESS")
        
        # Check if temp_CPD.csv exists
        if not os.path.exists("temp_CPD.csv"):
            print_detailed_log("❌ temp_CPD.csv file not found", "ERROR")
            return False
        
        # Read tracking IDs from temp_CPD.csv
        tracking_df = pd.read_csv("temp_CPD.csv")
        tracking_ids = tracking_df['tracking_id'].astype(str).tolist()
        
        print_detailed_log(f"Loaded {len(tracking_ids)} tracking IDs from temp_CPD.csv", "INFO")
        
        if not tracking_ids:
            print_detailed_log("❌ No tracking IDs found in temp_CPD.csv", "ERROR")
            return False
        
        # Paste tracking IDs into the portal
        print_detailed_log("Pasting tracking IDs into Multiple Shipment Tracking portal...", "PROGRESS")
        
        # Find the input field for tracking IDs
        input_field = find_tracking_input_field(driver)
        if not input_field:
            print_detailed_log("❌ Could not find input field for tracking IDs", "ERROR")
            return False
        
        # Prepare tracking IDs for pasting (space-separated)
        space_separated_text = ' '.join(tracking_ids)
        
        # Clear and paste the tracking IDs
        input_field.clear()
        time.sleep(1)
        
        # Focus the field
        driver.execute_script("arguments[0].focus();", input_field)
        time.sleep(1)
        
        # Paste the tracking IDs
        input_field.send_keys(space_separated_text)
        print_detailed_log(f"✅ Pasted {len(tracking_ids)} tracking IDs into portal", "SUCCESS")
        
        # Wait for processing
        time.sleep(3)
        
        # Find and click the download button
        print_detailed_log("🔍 DEBUG: About to call find_download_button", "INFO")
        download_button = find_download_button(driver)
        print_detailed_log(f"🔍 DEBUG: find_download_button returned: {download_button}", "INFO")
        if download_button:
            # Store the current tab handle
            original_tab = driver.current_window_handle
            print_detailed_log(f"📋 Original tab handle: {original_tab}", "INFO")
            
            # Click the download button (this may open a new tab)
            download_button.click()
            print_detailed_log("✅ Clicked download button", "SUCCESS")
            
            # Wait a moment for new tab to open
            time.sleep(3)
            
            # Check if a new tab opened
            all_tabs = driver.window_handles
            print_detailed_log(f"📋 All tab handles after click: {all_tabs}", "INFO")
            
            if len(all_tabs) > 1:
                # New tab opened - switch to it
                new_tab = [tab for tab in all_tabs if tab != original_tab][0]
                print_detailed_log(f"🔄 Switching to new tab: {new_tab}", "INFO")
                driver.switch_to.window(new_tab)
                
                # Wait for download to complete in new tab
                print_detailed_log("⏳ Waiting for download to complete in new tab...", "INFO")
                time.sleep(5)
                
                # Check if download completed
                try:
                    downloaded_file = find_latest_csv_file()
                    if downloaded_file:
                        print_detailed_log(f"✅ Download completed: {downloaded_file}", "SUCCESS")
                except NameError:
                    print_detailed_log("⚠️ find_latest_csv_file function not available", "WARNING")
                
                # Close the new tab and switch back to original
                print_detailed_log("🔄 Closing new tab and switching back to original", "INFO")
                driver.close()
                driver.switch_to.window(original_tab)
                print_detailed_log("✅ Switched back to original tab", "SUCCESS")
                
                return True
            else:
                # No new tab opened - download happened in current tab
                print_detailed_log("📋 No new tab opened, download in current tab", "INFO")
                time.sleep(5)
                
                # Check if download completed
                try:
                    downloaded_file = find_latest_csv_file()
                    if downloaded_file:
                        print_detailed_log(f"✅ Download completed: {downloaded_file}", "SUCCESS")
                        return True
                    else:
                        print_detailed_log("⚠️ Download may not have completed", "WARNING")
                except NameError:
                    print_detailed_log("⚠️ find_latest_csv_file function not available", "WARNING")
                    return True  # Still return True as the paste was successful
        else:
            print_detailed_log("❌ Could not find download button", "ERROR")
            return False
            
    except Exception as e:
        print_detailed_log(f"❌ Error pasting tracking IDs: {e}", "ERROR")
        return False

def find_tracking_input_field(driver):
    """Find the input field for tracking IDs"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        # Try different selectors for the input field
        input_selectors = [
            "//textarea",
            "//input[@type='text']",
            "//input[contains(@placeholder, 'tracking')]",
            "//input[contains(@placeholder, 'Tracking')]",
            "//textarea[contains(@placeholder, 'tracking')]",
            "//textarea[contains(@placeholder, 'Tracking')]",
            "//div[@contenteditable='true']"
        ]
        
        for selector in input_selectors:
            try:
                input_field = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                if input_field.is_displayed():
                    print_detailed_log(f"Found input field with selector: {selector}", "SUCCESS")
                    return input_field
            except:
                continue
        
        print_detailed_log("❌ Could not find input field for tracking IDs", "ERROR")
        return None
        
    except Exception as e:
        print_detailed_log(f"Error finding input field: {e}", "ERROR")
        return None

def find_download_button(driver):
    """Find the download button"""
    try:
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        
        print_detailed_log("🔍 DEBUG: Starting find_download_button function", "INFO")
        print_detailed_log("🔍 DEBUG: Current URL: " + str(driver.current_url), "INFO")
        print_detailed_log("🔍 DEBUG: Current page title: " + str(driver.title), "INFO")
        
        # Try different selectors for the download button with better debugging
        button_selectors = [
            "//button[contains(text(), 'Download')]",
            "//a[contains(text(), 'Download')]",
            "//div[contains(@class, 'btn') and contains(text(), 'Download')]",
            "//span[contains(text(), 'Download')]",
            "//button[contains(text(), 'Submit')]",
            "//button[contains(text(), 'Search')]",
            "//button[contains(text(), 'Track')]",
            "//button[contains(text(), 'Get')]",
            "//button[contains(text(), 'Fetch')]",
            "//button[contains(text(), 'Process')]",
            "//input[@type='submit']",
            "//button[@type='submit']",
            "//button[contains(@class, 'btn')]",
            "//button[contains(@class, 'button')]",
            "//a[contains(text(), 'Submit')]",
            "//div[contains(@class, 'btn') and contains(text(), 'Submit')]",
        ]
        
        print_detailed_log("🔍 Searching for download button with WebDriverWait...", "INFO")
        
        # First, let's see what buttons are actually on the page
        try:
            all_buttons = driver.find_elements(By.TAG_NAME, "button")
            print_detailed_log(f"🔍 DEBUG: Found {len(all_buttons)} button elements on the page", "INFO")
            for i, btn in enumerate(all_buttons[:10]):  # Show first 10 buttons
                try:
                    btn_text = btn.text.strip()
                    btn_class = btn.get_attribute("class")
                    btn_id = btn.get_attribute("id")
                    print_detailed_log(f"🔍 DEBUG: Button {i+1}: text='{btn_text}', class='{btn_class}', id='{btn_id}'", "INFO")
                except:
                    print_detailed_log(f"🔍 DEBUG: Button {i+1}: Could not get details", "INFO")
        except Exception as debug_e:
            print_detailed_log(f"🔍 DEBUG: Error getting button list: {debug_e}", "WARNING")
        
        for i, selector in enumerate(button_selectors):
            try:
                print_detailed_log(f"Trying selector {i+1}: {selector}", "INFO")
                button = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, selector))
                )
                if button.is_displayed():
                    button_text = button.text.strip()
                    print_detailed_log(f"✅ Found download button: '{button_text}' with selector: {selector}", "SUCCESS")
                    return button
                else:
                    print_detailed_log(f"Button found but not visible with selector: {selector}", "WARNING")
            except Exception as e:
                print_detailed_log(f"Selector {i+1} failed: {str(e)[:100]}...", "WARNING")
                continue
        
        print_detailed_log("❌ Could not find download button with any selector", "ERROR")
        return None
        
    except Exception as e:
        print_detailed_log(f"Error finding download button: {e}", "ERROR")
        return None

def process_downloaded_tracking_file():
    """Process the downloaded tracking file and filter out CLOSED status"""
    try:
        print_detailed_log("Processing downloaded tracking file...", "PROGRESS")
        print_detailed_log("🔧 Starting BagStatus filtering process...", "INFO")
        print_detailed_log("⏰ Setting 30-second timeout for BagStatus filtering...", "INFO")
        
        # Wait for download to complete and find the downloaded file
        print_detailed_log("⏳ Waiting for download to complete...", "INFO")
        time.sleep(3)
        download_dir = os.path.expanduser("~/Downloads")  # Default download directory
        
        print_detailed_log(f"📁 Looking for CSV files in: {download_dir}", "INFO")
        # Look for the most recent CSV file in downloads
        csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
        if csv_files:
            print_detailed_log(f"📁 Found {len(csv_files)} CSV files in Downloads", "INFO")
            # Sort by modification time to get the most recent
            csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
            downloaded_file = os.path.join(download_dir, csv_files[0])
            print_detailed_log(f"✅ Found downloaded file: {csv_files[0]}", "SUCCESS")
            
            # Read the downloaded tracking file for BagStatus information
            print_detailed_log(f"📖 Reading CSV file: {csv_files[0]}", "INFO")
            try:
                print_detailed_log(f"🔍 Starting CSV read operation...", "INFO")
                tracking_results_df = pd.read_csv(downloaded_file)
                print_detailed_log(f"✅ Loaded tracking results: {len(tracking_results_df)} rows", "SUCCESS")
                print_detailed_log(f"📋 CSV file size: {os.path.getsize(downloaded_file)} bytes", "INFO")
            except Exception as read_error:
                print_detailed_log(f"❌ Error reading CSV file: {read_error}", "ERROR")
                return False
            
            # Debug: Show available columns
            print_detailed_log(f"Available columns in downloaded file: {list(tracking_results_df.columns)}", "INFO")
            
            # Check for Bagstatus column with different possible names
            bagstatus_column = None
            possible_names = ['BagStatus', 'Bagstatus', 'bagstatus', 'Bag Status', 'bag status', 'Status', 'status', 'Bag_Status', 'bag_status']
            
            for col_name in possible_names:
                if col_name in tracking_results_df.columns:
                    bagstatus_column = col_name
                    break
            
            if bagstatus_column:
                print_detailed_log(f"Found bag status column: '{bagstatus_column}', creating BagStatus mapping...", "INFO")
                
                # Show unique values in the status column
                unique_statuses = tracking_results_df[bagstatus_column].unique()
                print_detailed_log(f"Unique status values: {list(unique_statuses)}", "INFO")
                
                # Create a mapping of tracking_id to BagStatus from the downloaded file
                bagstatus_mapping = {}
                tracking_id_column = None
                tracking_id_possibilities = ['tracking_id', 'Tracking Id', 'Tracking ID', 'tracking id', 'TrackingId', 'trackingId', 'AWB', 'awb', 'AWB No', 'awb no', 'Consignment ID', 'consignment id']
                
                for col_name in tracking_id_possibilities:
                    if col_name in tracking_results_df.columns:
                        tracking_id_column = col_name
                        break
                
                if tracking_id_column:
                    for _, row in tracking_results_df.iterrows():
                        tracking_id = str(row[tracking_id_column])
                        bagstatus = row[bagstatus_column]
                        bagstatus_mapping[tracking_id] = bagstatus
                    
                    print_detailed_log(f"Created BagStatus mapping for {len(bagstatus_mapping)} tracking IDs", "INFO")
                    
                    # Filter the downloaded CSV file directly (remove CLOSED status)
                    print_detailed_log("Filtering downloaded CSV file to remove CLOSED status...", "INFO")
                    
                    # Filter out CLOSED status from the downloaded file
                    filtered_downloaded_df = tracking_results_df[tracking_results_df[bagstatus_column] != 'CLOSED']
                    closed_count = len(tracking_results_df) - len(filtered_downloaded_df)
                    
                    print_detailed_log(f"Removed {closed_count} tracking IDs with CLOSED status from downloaded file", "INFO")
                    print_detailed_log(f"Remaining tracking IDs in downloaded file: {len(filtered_downloaded_df)}", "INFO")
                    
                    # Show BagStatus distribution in downloaded file
                    bagstatus_counts = tracking_results_df[bagstatus_column].value_counts()
                    print_detailed_log(f"BagStatus distribution in downloaded file: {dict(bagstatus_counts)}", "INFO")
                    
                    # Now we need to map the filtered tracking IDs back to their hub information
                    # Load temp_CPD.csv to get hub mapping
                    print_detailed_log("Loading temp_CPD.csv to get hub mapping for filtered tracking IDs...", "INFO")
                    temp_tracking_df = pd.read_csv("temp_CPD.csv")
                    print_detailed_log(f"Loaded temp_CPD.csv: {len(temp_tracking_df)} rows", "INFO")
                    
                    # Create mapping from tracking_id to hub information
                    print_detailed_log(f"🔍 Creating tracking ID to hub mapping...", "INFO")
                    tracking_to_hub_mapping = {}
                    mapping_count = 0
                    for _, row in temp_tracking_df.iterrows():
                        tracking_id = str(row['tracking_id'])
                        tracking_to_hub_mapping[tracking_id] = {
                            'hub_name': row['hub_name'],
                            'cpd_days': row['cpd_days'],
                            'cpd_type': row.get('cpd_type', 'NCD')
                        }
                        mapping_count += 1
                        if mapping_count % 100 == 0:  # Progress update every 100 mappings
                            print_detailed_log(f"📊 Created {mapping_count} mappings...", "INFO")
                    
                    print_detailed_log(f"✅ Created tracking ID to hub mapping for {len(tracking_to_hub_mapping)} tracking IDs", "SUCCESS")
                    
                    # Apply hub mapping to filtered downloaded file
                    print_detailed_log(f"🔍 Applying hub mapping to {len(filtered_downloaded_df)} filtered records...", "INFO")
                    filtered_downloaded_df['tracking_id_str'] = filtered_downloaded_df[tracking_id_column].astype(str)
                    print_detailed_log(f"✅ Converted tracking IDs to string", "INFO")
                    
                    filtered_downloaded_df['hub_name'] = filtered_downloaded_df['tracking_id_str'].map(lambda x: tracking_to_hub_mapping.get(x, {}).get('hub_name', 'Unknown'))
                    print_detailed_log(f"✅ Applied hub_name mapping", "INFO")
                    
                    filtered_downloaded_df['cpd_days'] = filtered_downloaded_df['tracking_id_str'].map(lambda x: tracking_to_hub_mapping.get(x, {}).get('cpd_days', 'Unknown'))
                    print_detailed_log(f"✅ Applied cpd_days mapping", "INFO")
                    
                    filtered_downloaded_df['cpd_type'] = filtered_downloaded_df['tracking_id_str'].map(lambda x: tracking_to_hub_mapping.get(x, {}).get('cpd_type', 'NCD'))
                    print_detailed_log(f"✅ Applied cpd_type mapping", "INFO")
                    
                    print_detailed_log("✅ Applied hub mapping to filtered downloaded file", "SUCCESS")
                
                # Count by hub, CPD days, and CPD type (NCD vs EKL) from filtered downloaded file
                print_detailed_log(f"🔍 Counting filtered records by hub and CPD type...", "INFO")
                hub_cpd_counts = {}
                hub_ekl_counts = {}
                count_progress = 0
                
                for _, row in filtered_downloaded_df.iterrows():
                    count_progress += 1
                    if count_progress % 100 == 0:  # Progress update every 100 records
                        print_detailed_log(f"📊 Processed {count_progress}/{len(filtered_downloaded_df)} records...", "INFO")
                    hub_name = row['hub_name']
                    cpd_days = row['cpd_days']
                    cpd_type = row.get('cpd_type', 'NCD')  # Default to NCD if not specified
                    
                    if cpd_type == 'NCD':
                        if hub_name not in hub_cpd_counts:
                            hub_cpd_counts[hub_name] = {'0 Days': 0, '1 Day': 0}
                        
                        if cpd_days == '0 Days':
                            hub_cpd_counts[hub_name]['0 Days'] += 1
                        elif cpd_days == '1 Day':
                            hub_cpd_counts[hub_name]['1 Day'] += 1
                    elif cpd_type == 'EKL':
                        if hub_name not in hub_ekl_counts:
                            hub_ekl_counts[hub_name] = {'0 Days': 0, '1 Day': 0}
                        
                        if cpd_days == '0 Days':
                            hub_ekl_counts[hub_name]['0 Days'] += 1
                        elif cpd_days == '1 Day':
                            hub_ekl_counts[hub_name]['1 Day'] += 1
                

                
                print_detailed_log("Filtered NCD tracking ID counts by hub:", "DATA")
                for hub, counts in hub_cpd_counts.items():
                    print_detailed_log(f"  {hub}: 0 Days: {counts['0 Days']}, 1 Day: {counts['1 Day']}", "DATA")
                
                print_detailed_log("Filtered EKL tracking ID counts by hub:", "DATA")
                for hub, counts in hub_ekl_counts.items():
                    print_detailed_log(f"  {hub}: 0 Days: {counts['0 Days']}, 1 Day: {counts['1 Day']}", "DATA")
                
                # Store the filtered counts for Google Sheets upload
                global filtered_tracking_counts
                global filtered_ekl_tracking_counts
                filtered_tracking_counts = hub_cpd_counts
                filtered_ekl_tracking_counts = hub_ekl_counts
                
                # Display PowerShell statistics after tracking file processing
                print_detailed_log("📊 Displaying PowerShell statistics after tracking file processing...", "PROGRESS")
                try:
                    import subprocess
                    
                    # Calculate statistics
                    total_original = len(temp_tracking_df)  # Total from temp_CPD.csv
                    total_filtered = len(filtered_downloaded_df)  # After BagStatus filtering from downloaded file
                    closed_cases_removed = total_original - total_filtered
                    
                    # Get the actual count from temp_CPD.csv
                    try:
                        temp_cpd_df = pd.read_csv('temp_CPD.csv')
                        total_in_temp_cpd = len(temp_cpd_df)
                    except:
                        total_in_temp_cpd = "Unknown"
                    
                    # Calculate totals for each hub
                    hub_stats = {}
                    
                    # Initialize all hubs with 0 values
                    for hub in HUBS:
                        hub_stats[hub] = {
                            'CPD_NCD_0_Days': 0,
                            'CPD_NCD_1_Day': 0,
                            'CPD_EKL_0_Days': 0,
                            'CPD_EKL_1_Day': 0
                        }
                    
                    # Update with actual data for hubs that have tracking data
                    for hub, counts in hub_cpd_counts.items():
                        if hub in hub_stats:
                            hub_stats[hub]['CPD_NCD_0_Days'] = counts.get('0 Days', 0)
                            hub_stats[hub]['CPD_NCD_1_Day'] = counts.get('1 Day', 0)
                    
                    for hub, counts in hub_ekl_counts.items():
                        if hub in hub_stats:
                            hub_stats[hub]['CPD_EKL_0_Days'] = counts.get('0 Days', 0)
                            hub_stats[hub]['CPD_EKL_1_Day'] = counts.get('1 Day', 0)
                    
                    # Create PowerShell command
                    powershell_command = f'''
                    Write-Host "=" * 80 -ForegroundColor Cyan
                    Write-Host "📊 TRACKING FILE PROCESSING STATISTICS (AFTER Multiple Shipment Tracking)" -ForegroundColor Green
                    Write-Host "=" * 80 -ForegroundColor Cyan
                    
                    Write-Host "[SUCCESS] Paste and download completed successfully!" -ForegroundColor Green
                    Write-Host ""
                    Write-Host "BAG STATUS FILTERING RESULTS (FROM DOWNLOADED CSV):" -ForegroundColor Yellow
                    Write-Host "   • Total tracking IDs in temp_CPD.csv: {total_in_temp_cpd}" -ForegroundColor Cyan
                    Write-Host "   • Total tracking IDs processed by portal: {total_original}" -ForegroundColor White
                    Write-Host "   • Tracking IDs removed (CLOSED + not found): {closed_cases_removed}" -ForegroundColor Red
                    Write-Host "   • Final filtered count: {total_filtered}" -ForegroundColor Green
                    Write-Host ""
                    Write-Host "📊 RESULTS BY HUB (BagStatus Filtered):" -ForegroundColor Yellow
                    '''
                    
                    # Add results for all hubs
                    for hub in HUBS:
                        ncd_0 = filtered_tracking_counts.get(hub, {}).get('0 Days', 0)
                        ncd_1 = filtered_tracking_counts.get(hub, {}).get('1 Day', 0)
                        ekl_0 = filtered_ekl_tracking_counts.get(hub, {}).get('0 Days', 0)
                        ekl_1 = filtered_ekl_tracking_counts.get(hub, {}).get('1 Day', 0)
                        total = ncd_0 + ncd_1 + ekl_0 + ekl_1
                        
                        powershell_command += f'''
                    Write-Host "   📍 {hub}: NCD(0)={ncd_0}, NCD(1)={ncd_1}, EKL(0)={ekl_0}, EKL(1)={ekl_1}, Total={total}" -ForegroundColor White
                    '''
                    
                    powershell_command += f'''
                    Write-Host ""
                    '''
                    
                    # Calculate grand totals from the same data source used for Google Sheets
                    total_ncd_0 = sum(counts.get('0 Days', 0) for counts in filtered_tracking_counts.values())
                    total_ncd_1 = sum(counts.get('1 Day', 0) for counts in filtered_tracking_counts.values())
                    total_ekl_0 = sum(counts.get('0 Days', 0) for counts in filtered_ekl_tracking_counts.values())
                    total_ekl_1 = sum(counts.get('1 Day', 0) for counts in filtered_ekl_tracking_counts.values())
                    
                    powershell_command += f'''
                    Write-Host "📊 GRAND TOTALS:" -ForegroundColor Magenta
                    Write-Host "   • CPD_NCD (0 Days): {total_ncd_0}" -ForegroundColor Green
                    Write-Host "   • CPD_NCD (1 Day): {total_ncd_1}" -ForegroundColor Green
                    Write-Host "   • CPD_EKL (0 Days): {total_ekl_0}" -ForegroundColor Green
                    Write-Host "   • CPD_EKL (1 Day): {total_ekl_1}" -ForegroundColor Green
                    Write-Host "   • Total Final Count: {total_ncd_0 + total_ncd_1 + total_ekl_0 + total_ekl_1}" -ForegroundColor Green
                    Write-Host "=" * 80 -ForegroundColor Cyan
                    '''
                    
                    result = subprocess.run(['powershell', '-Command', powershell_command], 
                                          capture_output=True, text=True, cwd=os.getcwd())
                    
                    if result.returncode == 0:
                        print_detailed_log("✅ PowerShell statistics displayed successfully", "SUCCESS")
                        # Print the PowerShell output
                        if result.stdout.strip():
                            print(result.stdout.strip())
                    else:
                        print_detailed_log(f"⚠️ PowerShell command had issues: {result.stderr}", "WARNING")
                        
                except Exception as e:
                    print_detailed_log(f"⚠️ Could not execute PowerShell statistics: {e}", "WARNING")
                    # Fallback: Display statistics in Python
                    print_detailed_log("📊 Python Fallback Statistics:", "INFO")
                    print_detailed_log(f"   • Total tracking IDs processed: {total_original}", "INFO")
                    print_detailed_log(f"   • Bag closed cases removed: {closed_cases_removed}", "INFO")
                    print_detailed_log(f"   • Final filtered count: {total_filtered}", "INFO")
                    for hub, stats in hub_stats.items():
                        print_detailed_log(f"   • {hub}: NCD(0)={stats['CPD_NCD_0_Days']}, NCD(1)={stats['CPD_NCD_1_Day']}, EKL(0)={stats.get('CPD_EKL_0_Days', 0)}, EKL(1)={stats.get('CPD_EKL_1_Day', 0)}", "INFO")
                
                return True
            else:
                print_detailed_log("❌ Bag status column not found in downloaded file", "ERROR")
                print_detailed_log("Available columns: " + ", ".join(tracking_results_df.columns), "ERROR")
                return False
        else:
            print_detailed_log("❌ No CSV files found in downloads directory", "ERROR")
            return False
            
    except Exception as e:
        print_detailed_log(f"Error processing downloaded tracking file: {e}", "ERROR")
        return False

# Note: temp CSV file will be saved AFTER data processing but BEFORE BagStatus filtering

def find_and_process_csv(hub_name):
    """Find the latest CSV file and process it"""
    try:
        print_detailed_log(f"Looking for downloaded CSV file for {hub_name}...", "PROGRESS")
        # Common download paths
        download_paths = [
            os.path.expanduser("~/Downloads"),
            "C:/Users/Lsn-Arun/Downloads",
            os.getcwd()
        ]
        
        csv_files = []
        for path in download_paths:
            if os.path.exists(path):
                pattern = os.path.join(path, "*.csv")
                files = glob.glob(pattern)
                csv_files.extend(files)
                if files:
                    print_detailed_log(f"Found {len(files)} CSV files in {path}", "INFO")
        
        if not csv_files:
            print_detailed_log(f"No CSV files found for {hub_name}", "ERROR")
            return None
        
        # Get the most recent CSV file
        latest_csv = max(csv_files, key=os.path.getctime)
        print_detailed_log(f"Found CSV file: {latest_csv}", "SUCCESS")
        
        # Process the CSV file
        breach_data = process_csv_data(latest_csv, hub_name)
        
        # NOTE: CSV file deletion is now handled by the tracking ID extraction logic
        # to ensure tracking IDs are extracted before the file is deleted
        print_detailed_log(f"CSV file {latest_csv} will be deleted after tracking ID extraction", "INFO")
        
        return breach_data
        
    except Exception as e:
        print_detailed_log(f"Error finding/processing CSV for {hub_name}: {e}", "ERROR")
        return None

def process_hub_with_csv_download(hub_name, clm_name, state, driver, is_first_hub=False):
    """Process a single hub by downloading CSV and extracting data"""
    try:
        print_detailed_log(f"Starting processing for hub: {hub_name}", "PROGRESS")
        print_detailed_log(f"CLM: {clm_name}, State: {state}", "INFO")
        
        # Download CSV for this hub
        download_success = download_csv_for_hub(driver, hub_name, is_first_hub)
        if not download_success:
            print_detailed_log(f"Failed to download CSV for {hub_name}", "ERROR")
            return None
        
        # CSV download success message
        print(f"📥 CSV DOWNLOAD: {hub_name} - SUCCESS")
        
        # Extract tracking IDs from the downloaded CSV BEFORE processing
        print_detailed_log(f"🔧 EXTRACTING TRACKING IDs for {hub_name} after successful download", "INFO")
        try:
            # Find the most recent CSV file that was just downloaded for this hub
            download_dir = os.path.expanduser("~/Downloads")
            if os.path.exists(download_dir):
                csv_files = [f for f in os.listdir(download_dir) if f.endswith('.csv')]
                if csv_files:
                    # Sort by modification time to get the most recent
                    csv_files.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
                    most_recent_csv = csv_files[0]
                    csv_file_path = os.path.join(download_dir, most_recent_csv)
                    
                    print_detailed_log(f"📁 Found CSV file for {hub_name}: {most_recent_csv}", "INFO")
                    extract_tracking_ids_from_csv(hub_name, csv_file_path)
                    print_detailed_log(f"✅ TRACKING ID EXTRACTION COMPLETED for {hub_name}", "SUCCESS")
                else:
                    print_detailed_log(f"❌ No CSV files found for {hub_name}", "ERROR")
            else:
                print_detailed_log(f"❌ Downloads folder not found for {hub_name}", "ERROR")
        except Exception as extract_e:
            print_detailed_log(f"❌ ERROR in tracking ID extraction for {hub_name}: {extract_e}", "ERROR")
            import traceback
            print_detailed_log(f"DEBUG: Tracking extraction traceback: {traceback.format_exc()}", "INFO")
        
        # Process the downloaded CSV using the same file path
        print_detailed_log(f"🔧 PROCESSING CSV for {hub_name} using file: {csv_file_path}", "INFO")
        breach_data = process_csv_data(csv_file_path, hub_name) # Use the same CSV file path
        print_detailed_log(f"DEBUG: breach_data from process_csv_data: {breach_data}", "DEBUG")
        if not breach_data:
            print_detailed_log(f"Failed to process CSV for {hub_name}", "ERROR")
            return None
        
        # Clean up the CSV file after both tracking ID extraction and CSV processing are complete
        try:
            if os.path.exists(csv_file_path):
                os.remove(csv_file_path)
                print_detailed_log(f"🗑️ Deleted CSV file from Downloads: {os.path.basename(csv_file_path)}", "SUCCESS")
            else:
                print_detailed_log(f"⚠️ CSV file {os.path.basename(csv_file_path)} was already deleted", "WARNING")
        except Exception as cleanup_e:
            print_detailed_log(f"⚠️ Could not delete {csv_file_path}: {cleanup_e}", "WARNING")
        
        # Data processing success message
        print(f"📊 DATA PROCESSING: {hub_name} - SUCCESS")
        
        # Add hub information
        breach_data['clm_name'] = clm_name
        breach_data['state'] = state
        
        print(f"[SUCCESS] Successfully processed {hub_name}")
        print_detailed_log(f"DEBUG: Returning breach_data: {breach_data}", "DEBUG")
        return breach_data
                
    except Exception as e:
        print(f"[ERROR] Error processing {hub_name}: {e}")
        return None

# Switch to correct tab
print_detailed_log("Initializing browser connection...", "PROGRESS")
if not switch_to_correct_tab(driver):
    print_detailed_log("Could not switch to correct tab.", "ERROR")
    driver.quit()
    exit(1)

results = []
failed_hubs = []  # Track failed hubs for retry

print_detailed_log(f"Starting processing of all {len(HUBS)} hubs...", "PROGRESS")
print_detailed_log("=" * 80, "INFO")

# First pass: Process all hubs
for i, hub in enumerate(HUBS, 1):  # Process all hubs
    clm_name, state = HUB_INFO.get(hub, ("", ""))
    
    print_detailed_log(f"Processing hub {i}/{len(HUBS)}: {hub}", "PROGRESS")
    print_detailed_log(f"CLM: {clm_name}, State: {state}", "INFO")
    print_detailed_log("=" * 60, "INFO")
    
    # Check WebDriver health before processing each hub
    if not check_driver_health(driver):
        print_detailed_log("⚠️ WebDriver connection lost, attempting to restart...", "WARNING")
        new_driver = restart_driver()
        if new_driver:
            driver = new_driver
            # Switch to correct tab again
            if not switch_to_correct_tab(driver):
                print_detailed_log("❌ Could not switch to correct tab after restart", "ERROR")
                failed_hubs.append((hub, clm_name, state, i))
                continue
        else:
            print_detailed_log("❌ Failed to restart WebDriver", "ERROR")
            failed_hubs.append((hub, clm_name, state, i))
            continue
    
    # Add immediate status update
    print_detailed_log(f"🔄 Starting data extraction for {hub}...", "PROGRESS")
    
    # Process hub with CSV download
    # First hub (i=1) needs SHOW DATA, subsequent hubs just need hub selection
    is_first_hub = (i == 1)
    breach_data = process_hub_with_csv_download(hub, clm_name, state, driver, is_first_hub)
    
    # Immediate status update - PROMINENT DISPLAY
    print("-" * 80)
    if breach_data:
        total_ncd_breaches = sum(breach_data.get(category, 0) for category in ncd_cpd_categories)
        print(f"[SUCCESS] HUB {i}/{len(HUBS)} COMPLETED: {hub} - SUCCESS - {total_ncd_breaches} records extracted")
    else:
        print(f"[FAILED] HUB {i}/{len(HUBS)} COMPLETED: {hub} - FAILED - No data extracted")
        print_detailed_log(f"DEBUG: breach_data is None or empty for {hub}", "DEBUG")
    print("-" * 80)
    
    if breach_data:
        print_detailed_log(f"DEBUG: breach_data for {hub}: {breach_data}", "DEBUG")
        print_detailed_log(f"DEBUG: breach_data type: {type(breach_data)}", "DEBUG")
        # Create result data for this hub
        result_data = {
            'Hub Name': hub,
            'CLM Name': clm_name,
            'State': state,
        }
        
        # Add NCD breach counts to result_data
        for category in ncd_cpd_categories:
            result_data[category] = breach_data.get(category, 0)
        
        # Calculate Total NCD Breaches and add it after NCD_FDD
        total_ncd_breaches = sum(breach_data.get(category, 0) for category in ncd_cpd_categories)
        result_data['Total NCD Breaches'] = total_ncd_breaches
        
        # Add EKL breach counts to result_data
        for category in ekl_cpd_categories:
            result_data[category] = breach_data.get(category, 0)
        results.append(result_data)
        print_detailed_log(f"DEBUG: Added result_data to results list. Results length: {len(results)}", "DEBUG")
        
        # Show individual hub summary
        print_detailed_log("=" * 50, "INFO")
        print_detailed_log(f"HUB {i} SUMMARY: {hub}", "DATA")
        print_detailed_log("=" * 50, "INFO")
        print_detailed_log(f"NCD_0_Days: {result_data.get('NCD_0_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_1_Days: {result_data.get('NCD_1_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_2_Days: {result_data.get('NCD_2_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_3_Days: {result_data.get('NCD_3_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_>_3_Days: {result_data.get('NCD_>_3_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_FDD: {result_data.get('NCD_FDD', 0)}", "DATA")
        print_detailed_log(f"Total NCD Breaches: {result_data.get('Total NCD Breaches', 0)}", "DATA")
        print_detailed_log(f"EKL_0_Days: {result_data.get('EKL_0_Days', 0)}", "DATA")
        print_detailed_log(f"EKL_1_Days: {result_data.get('EKL_1_Days', 0)}", "DATA")
        print_detailed_log("=" * 50, "INFO")
        
        print_detailed_log(f"✅ Hub {i}/{len(HUBS)} completed successfully - Data extracted: {total_ncd_breaches} records", "SUCCESS")
    else:
        # Track failed hub for retry
        failed_hubs.append((hub, clm_name, state, i))
        print_detailed_log(f"❌ Hub {i}/{len(HUBS)} failed - will retry later", "ERROR")
        
        # Add placeholder data for now (will be replaced if retry succeeds)
        result_data = {
            'Hub Name': hub,
            'CLM Name': clm_name,
            'State': state,
        }
        
        # Add NCD breach counts to result_data (set to 0 if error)
        for category in ncd_cpd_categories:
            result_data[category] = 0
        
        # Calculate Total NCD Breaches (will be 0 since all categories are 0) and add it after NCD_FDD
        total_ncd_breaches = sum(result_data.get(category, 0) for category in ncd_cpd_categories)
        result_data['Total NCD Breaches'] = total_ncd_breaches
        
        # Add EKL breach counts to result_data (set to 0 if error)
        for category in ekl_cpd_categories:
            result_data[category] = 0
        results.append(result_data)
        
        # Show individual hub summary for retry
        print_detailed_log("=" * 50, "INFO")
        print_detailed_log(f"HUB RETRY SUMMARY: {hub}", "DATA")
        print_detailed_log("=" * 50, "INFO")
        print_detailed_log(f"NCD_0_Days: {result_data.get('NCD_0_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_1_Days: {result_data.get('NCD_1_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_2_Days: {result_data.get('NCD_2_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_3_Days: {result_data.get('NCD_3_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_>_3_Days: {result_data.get('NCD_>_3_Days', 0)}", "DATA")
        print_detailed_log(f"NCD_FDD: {result_data.get('NCD_FDD', 0)}", "DATA")
        print_detailed_log(f"Total NCD Breaches: {result_data.get('Total NCD Breaches', 0)}", "DATA")
        print_detailed_log(f"EKL_0_Days: {result_data.get('EKL_0_Days', 0)}", "DATA")
        print_detailed_log(f"EKL_1_Days: {result_data.get('EKL_1_Days', 0)}", "DATA")
        print_detailed_log("=" * 50, "INFO")

# Second pass: Retry failed hubs
if failed_hubs:
    print_detailed_log("=" * 80, "INFO")
    print_detailed_log(f"RETRYING {len(failed_hubs)} FAILED HUBS", "PROGRESS")
    print_detailed_log("=" * 80, "INFO")
    
    for retry_count, (hub, clm_name, state, original_index) in enumerate(failed_hubs, 1):
        print_detailed_log(f"Retry {retry_count}/{len(failed_hubs)}: {hub}", "PROGRESS")
        print_detailed_log(f"CLM: {clm_name}, State: {state}", "INFO")
        print_detailed_log("=" * 60, "INFO")
        
        # Check WebDriver health before retry
        if not check_driver_health(driver):
            print_detailed_log("⚠️ WebDriver connection lost during retry, attempting to restart...", "WARNING")
            new_driver = restart_driver()
            if new_driver:
                driver = new_driver
                # Switch to correct tab again
                if not switch_to_correct_tab(driver):
                    print_detailed_log("❌ Could not switch to correct tab after restart during retry", "ERROR")
                    continue
            else:
                print_detailed_log("❌ Failed to restart WebDriver during retry", "ERROR")
                continue
        
        # Retry processing (not first hub for retries)
        print_detailed_log(f"🔄 Retrying data extraction for {hub}...", "PROGRESS")
        breach_data = process_hub_with_csv_download(hub, clm_name, state, driver, is_first_hub=False)
        
        # Immediate retry status update - PROMINENT DISPLAY
        print("-" * 80)
        if breach_data:
            total_ncd_breaches = sum(breach_data.get(category, 0) for category in ncd_cpd_categories)
            print(f"[SUCCESS] HUB RETRY SUCCESS: {hub} - {total_ncd_breaches} records extracted")
        else:
            print(f"[FAILED] HUB RETRY FAILED: {hub} - No data extracted")
        print("-" * 80)
        
        if breach_data:
            # Update the result data with successful retry
            result_data = {
                'Hub Name': hub,
                'CLM Name': clm_name,
                'State': state,
            }
            
            # Add NCD breach counts to result_data
            for category in ncd_cpd_categories:
                result_data[category] = breach_data.get(category, 0)
            
            # Calculate Total NCD Breaches and add it after NCD_FDD
            total_ncd_breaches = sum(result_data.get(category, 0) for category in ncd_cpd_categories)
            result_data['Total NCD Breaches'] = total_ncd_breaches
            
            # Add EKL breach counts to result_data
            for category in ekl_cpd_categories:
                result_data[category] = breach_data.get(category, 0)
            # Replace the placeholder data with real data
            results[original_index - 1] = result_data  # original_index is 1-based, results is 0-based
            print_detailed_log(f"✅ Retry successful for {hub}", "SUCCESS")
        else:
            print_detailed_log(f"❌ Retry failed for {hub} - keeping placeholder data", "ERROR")
else:
    print_detailed_log("All hubs processed successfully on first attempt!", "SUCCESS")

# Create DataFrame
print_detailed_log("Creating results DataFrame...", "PROGRESS")
df = pd.DataFrame(results)

# Add timestamp to the Hub Name column header (like in EMO report)
current_timestamp = datetime.now().strftime("%d %b %H:%M")
df.columns = [f"{col} - {current_timestamp}" if col == 'Hub Name' else col for col in df.columns]

# Add Grand Total row
print_detailed_log("Calculating grand totals...", "PROGRESS")
# Get the actual Hub Name column name (with timestamp)
hub_name_col = [col for col in df.columns if 'Hub Name' in col][0]
grand_total_row = {hub_name_col: 'GRAND TOTAL'}
for col in df.columns:
    if 'Hub Name' in col:
        continue
    elif col in ['CLM Name', 'State', 'Timestamp']:
        grand_total_row[col] = ''
    else:
        # Sum numeric columns
        grand_total_row[col] = df[col].apply(pd.to_numeric, errors='coerce').sum()

# Add Grand Total row to DataFrame
df = pd.concat([df, pd.DataFrame([grand_total_row])], ignore_index=True)

# Print summary
print_detailed_log("=" * 80, "INFO")
print_detailed_log("SUMMARY OF ALL NCD BREACH DATA COLLECTED", "DATA")
print_detailed_log("=" * 80, "INFO")
print_detailed_log(f"Total hubs processed: {len(results)}", "DATA")
print_detailed_log(f"Total NCD breaches found: {len(ncd_breach_data)}", "DATA")
print_detailed_log(f"DEBUG: Results list contents: {results}", "DEBUG")

# Calculate grand totals from accumulated results
if len(results) > 0:
    grand_total_row = {}
    # Initialize grand totals
    for category in ncd_cpd_categories + ekl_cpd_categories + ['Total NCD Breaches']:
        grand_total_row[category] = sum(result.get(category, 0) for result in results)
    
    print_detailed_log(f"DEBUG: Calculated grand_total_row: {grand_total_row}", "DEBUG")
    
    # Show final grand totals only
    print_detailed_log(f"Total NCD_0_Days: {grand_total_row.get('NCD_0_Days', 0)}", "DATA")
    print_detailed_log(f"Total NCD_1_Days: {grand_total_row.get('NCD_1_Days', 0)}", "DATA")
    print_detailed_log(f"Total NCD_2_Days: {grand_total_row.get('NCD_2_Days', 0)}", "DATA")
    print_detailed_log(f"Total NCD_3_Days: {grand_total_row.get('NCD_3_Days', 0)}", "DATA")
    print_detailed_log(f"Total NCD_>_3_Days: {grand_total_row.get('NCD_>_3_Days', 0)}", "DATA")
    print_detailed_log(f"Total NCD_FDD: {grand_total_row.get('NCD_FDD', 0)}", "DATA")
    print_detailed_log(f"Total NCD Breaches: {grand_total_row.get('Total NCD Breaches', 0)}", "DATA")
    print_detailed_log(f"Total EKL_0_Days: {grand_total_row.get('EKL_0_Days', 0)}", "DATA")
    print_detailed_log(f"Total EKL_1_Days: {grand_total_row.get('EKL_1_Days', 0)}", "DATA")
    
else:
    grand_total_row = {}
    for category in ncd_cpd_categories + ekl_cpd_categories + ['Total NCD Breaches']:
        grand_total_row[category] = 0

# Check if results is empty
if len(results) == 0:
    print_detailed_log("⚠️ WARNING: No data was processed successfully!", "WARNING")
    print_detailed_log("This could be due to:", "INFO")
    print_detailed_log("1. CSV files not found in Downloads folder", "INFO")
    print_detailed_log("2. Data processing errors", "INFO")
    print_detailed_log("3. WebDriver connection issues", "INFO")
    print_detailed_log("4. Portal navigation problems", "INFO")
    print_detailed_log("=" * 50, "INFO")
else:
    print_detailed_log("=" * 50, "INFO")

# Process Ageing > 5K data for email
print_detailed_log("🔍 Processing Ageing > 5K data for email...", "PROGRESS")
ageing_10k_data = []

if ncd_breach_data:
    for record in ncd_breach_data:
        amount = record.get('Amount', 0)
        cpd_days_diff = record.get('CPD Days Diff', 0)
        
        # Filter for Ageing > 5K: amount > 5000 AND ageing > 2 days - extract all Tracking IDs > 5K
        if amount > 5000 and cpd_days_diff < -2:  # Negative means past due
            # Determine category based on amount
            if amount > 25000:
                category = "NCD (>25K)"
            else:
                category = "NCD (5K-25K)"
            
            # Calculate ageing in days (convert negative to positive)
            ageing_days = abs(cpd_days_diff)
            
            ageing_10k_data.append({
                'Hub Name': record['Hub Name'],
                'CLM Name': record['CLM Name'],
                'State': record['State'],
                'Tracking ID': record['Tracking ID'],
                'Amount': amount,
                'Ageing (Days)': ageing_days,
                'Status': record['Status'],
                'Category': category
            })

print_detailed_log(f"Found {len(ageing_10k_data)} Ageing > 5K records", "DATA")

# Send email to CLMs with Ageing > 5K data
if ageing_10k_data:
    print_detailed_log("📧 Sending Ageing > 5K email to CLMs...", "PROGRESS")
    send_ageing_10k_email_to_clms(ageing_10k_data)
else:
    print_detailed_log("No Ageing > 5K data found - skipping email", "INFO")

# Upload results to Google Sheets with EKL header formatting
print_detailed_log("📤 Uploading results to Google Sheets...", "PROGRESS")
upload_to_google_sheets(results)

print_detailed_log('Closing browser...', 'PROGRESS')
driver.quit()
print('[DONE] Script completed successfully!')
