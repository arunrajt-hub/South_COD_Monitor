import time
import pandas as pd
import numpy as np
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import os
from collections import defaultdict
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
import string
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': 'arunraj@loadshare.net',  # LoadShare email
    'sender_password': 'ihczkvucdsayzrsu',  # Gmail App Password
    'recipient_email': 'lokeshh@loadshare.net',  # Recipient email
    'smtp_server': 'smtp.gmail.com',  # Gmail SMTP
    'smtp_port': 587
}

# ChromeDriver will be automatically managed by webdriver-manager

# Google Sheets Configuration
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# You'll need to create a service account and download the JSON key file
# Place the JSON file in the same directory as this script
SERVICE_ACCOUNT_FILE = 'service_account_key.json'  # Update this to your JSON file name

# Google Sheets ID - You'll need to create a Google Sheet and get its ID from the URL
SPREADSHEET_ID = '1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM'
WORKSHEET_NAME = 'EMO_Reports'  # Name of the worksheet to write data to

# List of hubs
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

# Mapping of hub to CLM Name, State, and BBD AOP
HUB_INFO = {
    "BagaluruMDH_BAG": ("Kishore", "Karnataka", "26"),
    "NaubadMDH_BDR": ("Haseem", "Karnataka", "13"),
    "SITICSWadiODH_WDI": ("Haseem", "Karnataka", "19"),
    "VadipattiMDH_VDP": ("Madvesh", "Tamil Nadu", "14"),
    "TTSPLKodaikanalODH_KDI": ("Madvesh", "Tamil Nadu", "9"),
    "LargeLogicRameswaramODH_RMS": ("Madvesh", "Tamil Nadu", "15"),
    "CABTSRNagarODH_HYD": ("Asif, Haseem", "Telengana", "41"),
    "LargeLogicKuniyamuthurODH_CJB": ("Madvesh", "Tamil Nadu", "48"),
    "KoorieeHayathnagarODH_HYD": ("Asif, Haseem", "Telengana", "32"),
    "SulebeleMDH_SUL": ("Kishore", "Karnataka", "34"),
    "KoorieeSoukyaRdODH_BLR": ("Kishore", "Karnataka", "88"),
    "KoorieeSoukyaRdTempODH_BLR": ("Kishore", "Karnataka", "89"),
    "ThavarekereMDH_THK": ("Irappa", "Karnataka", "24"),
    "SaidabadSplitODH_HYD": ("Asif, Haseem", "Telengana", "35"),
    "LargelogicChinnamanurODH_CNM": ("Madvesh", "Tamil Nadu", "20"),
    "LargeLogicDharapuramODH_DHP": ("Madvesh", "Tamil Nadu", "23"),
    "HulimavuHub_BLR": ("Kishore", "Karnataka", "64"),
    "ElasticRunBidarODH_BDR": ("Haseem", "Karnataka", "31"),
    "DommasandraSplitODH_DMN": ("Kishore", "Karnataka", "51"),
    "TTSPLBatlagunduODH_BGU": ("Madvesh", "Tamil Nadu", "20"),
    "BidarFortHub_BDR": ("Haseem", "Karnataka", "44"),
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

# Mapping of CLM Name to Email ID
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

def send_email_report(results, df=None):
    """Send email report with EMO data summary"""
    try:
        # Create message
        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = f"{EMAIL_CONFIG['recipient_email']}, maligai.rasmeen@loadshare.net, bharath.s@loadshare.net"
        msg['Cc'] = 'arunraj@loadshare.net'  # CC recipient
        msg['Subject'] = f"South Flipkart ODH Dashboard - {datetime.now().strftime('%d %b %Y %H:%M')}"
        
        # Calculate totals with proper type handling
        # Count only actual hubs, not Grand Total row
        total_hubs = len([r for r in results if 'Grand Total' not in str(r.get('Hub Name', ''))])
        
        def safe_sum(field_name):
            total = 0
            for result in results:
                # Skip Grand Total row to avoid double counting
                if 'Grand Total' in str(result.get('Hub Name', '')):
                    continue
                    
                value = result.get(field_name, 0)
                if isinstance(value, (int, float)):
                    total += value
                elif isinstance(value, str) and value.replace(',', '').isdigit():
                    total += int(value.replace(',', ''))
            return total
        
        total_ageing = safe_sum('Ageing')
        total_cpd = safe_sum('CPD-FWD')  # Using CPD-FWD instead of CPD
        total_rvp = safe_sum('CPD-RVP')  # Using CPD-RVP instead of RVP
        total_future_cpd = safe_sum('FDD')  # Using FDD instead of Future CPD
        total_untraceable = safe_sum('Untraceable')
        total_brsnr = safe_sum('BRSNR')
        total_ofd = safe_sum('OFD')
        total_fe_live = safe_sum("FE's Live")
        total_p0 = safe_sum('P0')
        total_p1 = safe_sum('P1')
        
        # Create HTML email body
        # Sort results by CPD-FWD in descending order (separate Grand Total row)
        sorted_results = []
        grand_total_result = None
        
        for result in results:
            if 'Grand Total' in str(result.get('Hub Name', '')):
                grand_total_result = result
            else:
                sorted_results.append(result)
        
        # Sort by CPD-FWD in descending order
        def get_cpd_fwd_value(result):
            cpd_value = result.get('CPD-FWD', 0)
            if isinstance(cpd_value, (int, float)):
                return cpd_value
            elif isinstance(cpd_value, str) and cpd_value.replace(',', '').isdigit():
                return int(cpd_value.replace(',', ''))
            return 0
        
        sorted_results.sort(key=get_cpd_fwd_value, reverse=True)
        
        # Add Grand Total row back at the end if it exists
        if grand_total_result:
            sorted_results.append(grand_total_result)
        
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ 
                    font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
                    margin: 0; 
                    padding: 20px; 
                    background-color: #f5f5f5; 
                    font-size: 14px; 
                }}
                .header {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 25px; 
                    border-radius: 10px 10px 0 0; 
                    color: white;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                .header h2 {{ 
                    margin: 0 0 10px 0; 
                    font-size: 20px; 
                    font-weight: 600;
                    text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
                }}
                .header p {{ 
                    margin: 5px 0; 
                    font-size: 13px;
                    opacity: 0.95;
                }}
                .summary {{ 
                    background-color: #ffffff; 
                    padding: 20px; 
                    border-radius: 0 0 10px 10px; 
                    margin: 0; 
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                .summary h3 {{ 
                    margin-top: 0; 
                    color: #333; 
                    font-size: 18px; 
                    border-bottom: 3px solid #667eea; 
                    padding-bottom: 10px;
                    font-weight: 600;
                }}
                table {{ 
                    border-collapse: collapse; 
                    width: 100%; 
                    font-size: 12px; 
                    margin: 15px 0;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
                    text-align: left;
                }}
                th, td {{ 
                    border: 1px solid #e0e0e0; 
                    padding: 12px 8px; 
                    text-align: left; 
                }}
                th {{ 
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    color: white; 
                    font-size: 13px; 
                    font-weight: 600;
                    text-transform: uppercase;
                    letter-spacing: 0.5px;
                    text-shadow: 1px 1px 2px rgba(0,0,0,0.2);
                }}
                tr:nth-child(even) {{ 
                    background-color: #f8f9fa; 
                }}
                tr:hover {{ 
                    background-color: #e3e7eb; 
                    transition: background-color 0.2s;
                }}
                .grand-total {{ 
                    background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                    color: white; 
                    font-weight: bold; 
                    text-shadow: 1px 1px 2px rgba(0,0,0,0.2);
                }}
                .grand-total td {{
                    border-color: #f5576c;
                }}
                .warning {{ 
                    background-color: #fff3cd; 
                    border-left: 4px solid #ffc107; 
                    padding: 15px; 
                    margin: 20px 0; 
                    border-radius: 5px;
                    color: #856404;
                }}
                .grand-totals-section {{
                    margin-top: 20px;
                    background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);
                    padding: 20px;
                    border-radius: 8px;
                }}
                .grand-totals-section h4 {{
                    margin: 0 0 15px 0;
                    color: #2c3e50;
                    font-size: 18px;
                    font-weight: 600;
                }}
                .grand-totals-section p {{
                    margin: 8px 0;
                    font-size: 14px;
                    color: #2c3e50;
                    font-weight: 500;
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h2>🚚 EMO Report Summary</h2>
                <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                <p><strong>Total Hubs Processed:</strong> {total_hubs}</p>
            </div>
            
            <div class="summary">
                <h3>📊 Flipkart ODH - South Dashboard</h3>
                <table>
                    <tr>
                        <th>Hub Name</th>
                        <th>State</th>
                        <th>CPD-FWD</th>
                        <th>CPD-RVP</th>
                        <th>UTR</th>
                        <th>BRSNR</th>
                        <th>OFD</th>
                        <th>FE's Live</th>
                        <th>P0</th>
                        <th>P1</th>
                    </tr>
        """
        
        # Add hub data
        for result in sorted_results:
             hub_name = result.get('Hub Name', '')
             state = result.get('State', '')
             cpd = result.get('CPD-FWD', 0)
             rvp = result.get('CPD-RVP', 0)
             untraceable = result.get('Untraceable', 0)
             brsnr = result.get('BRSNR', 0)
             ofd = result.get('OFD', 0)
             fe_live = result.get("FE's Live", 0)
             p0 = result.get('P0', 0)
             p1 = result.get('P1', 0)
             
             # Check if this is the Grand Total row
             is_grand_total = 'Grand Total' in str(hub_name)
             row_class = 'grand-total' if is_grand_total else ''
             
             html_body += f"""
                     <tr class="{row_class}">
                         <td>{hub_name}</td>
                         <td>{state}</td>
                         <td>{cpd}</td>
                         <td>{rvp}</td>
                         <td>{untraceable}</td>
                         <td>{brsnr}</td>
                         <td>{ofd}</td>
                         <td>{fe_live}</td>
                         <td>{p0}</td>
                         <td>{p1}</td>
                     </tr>
             """
        
        html_body += f"""
                </table>
                
                <div class="grand-totals-section">
                    <h4>Grand Totals Summary</h4>
                    <p><strong>Total CPD-FWD:</strong> {total_cpd} | <strong>Total CPD-RVP:</strong> {total_rvp}</p>
                    <p><strong>Total UTR:</strong> {total_untraceable} | <strong>Total BRSNR:</strong> {total_brsnr}</p>
                    <p><strong>Total OFD:</strong> {total_ofd} | <strong>Total FE's Live:</strong> {total_fe_live}</p>
                    <p><strong>Total P0:</strong> {total_p0} | <strong>Total P1:</strong> {total_p1}</p>
                </div>
            </div>
            
            <div class="warning">
                <p><strong>ℹ️ Note:</strong> This report is automatically generated. The data has been uploaded to Google Sheets.</p>
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
        # Send to both To and CC recipients
        recipients = [EMAIL_CONFIG['recipient_email'], 'maligai.rasmeen@loadshare.net', 'bharath.s@loadshare.net', 'arunraj@loadshare.net']
        server.sendmail(EMAIL_CONFIG['sender_email'], recipients, text)
        server.quit()
        
        print(f"Email sent successfully to {EMAIL_CONFIG['recipient_email']}, bharath.s@loadshare.net")
        print(f"Email CC'd to: arunraj@loadshare.net")
        
    except Exception as e:
        print(f"Error sending email: {e}")
        raise e

# Set up Chrome options to connect to existing browser
chrome_options = Options()
chrome_options.add_experimental_option("debuggerAddress", "localhost:9222")

# Start the driver to connect to existing Chrome instance
def create_driver():
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def restart_driver():
    global driver, wait
    try:
        if 'driver' in globals() and driver:
            driver.quit()
    except:
        pass
    driver = create_driver()
    wait = WebDriverWait(driver, 60)
    return driver

driver = create_driver()
wait = WebDriverWait(driver, 60)

results = []

# Remove or comment out any line that restricts HUBS to a subset
# HUBS = HUBS[:2]  # Only process the first 2 hubs for simulation

for hub in HUBS:
    clm_name, state, bbd_aop = HUB_INFO.get(hub, ("", "", ""))
    try:
        # Select the hub from the dropdown
        single_value = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-1uccc91-singleValue')))
        dropdown_control = single_value.find_element(By.XPATH, './ancestor::div[contains(@class, "-control")]')
        dropdown_control.click()
        time.sleep(0.5)
        input_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[id^="react-select-"][type="text"]')))
        input_box.clear()
        # Use the full hub name to ensure exact match
        input_box.send_keys(hub)
        time.sleep(2)  # Wait for dropdown to populate and filter
        
        # Simple and fast: Just press Enter (old reliable method)
        input_box.send_keys(Keys.ENTER)
        
        time.sleep(3)  # Wait for table to load
        print(f"Verifying that hub '{hub}' is correctly selected...")

        # Click the Show Data button
        show_data_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.HubDashboard-showDataButton-1lZt5V8FT3Jdfw4weKyQLD')))
        show_data_btn.click()
        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
        tables = driver.find_elements(By.TAG_NAME, 'table')
        if tables:
            first_table_rows = tables[0].find_elements(By.TAG_NAME, 'tr')
            if first_table_rows:
                first_row_cells = [c.text.strip() for c in first_table_rows[0].find_elements(By.TAG_NAME, 'td')]
            else:
                print("First table has no rows.")
        else:
            print("No tables found after wait.")

        # --- Debug: Print all tables, headers, and first 2 rows ---
        # tables = driver.find_elements(By.TAG_NAME, 'table')
        # print(f"Hub: {hub} - Tables found: {len(tables)}")
        # print(f"Attempting to extract data from tables for hub: {hub}")

        # Extract summary table values
        ageing = cpd = rvp = future_cpd = None
        try:
            table = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'table.pcm-table.HubDashboard-dashboard-3WQAbhqmb9SxXZhnxYECVK'))
            )
            rows = table.find_elements(By.TAG_NAME, 'tr')
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, 'td')
                if len(cells) >= 3:
                    if 'must attempt today - breached shipments' in cells[0].text.lower():
                        ageing = cells[1].text
                    elif 'must attempt today - today cpd' in cells[0].text.lower():
                        cpd = cells[1].text
                        rvp = cells[2].text
                    elif 'future cpd' in cells[0].text.lower():
                        future_cpd = cells[1].text
            def to_int(val):
                try:
                    return int(str(val).replace(',', '')) if val and str(val).strip().replace(',', '').isdigit() else 0
                except:
                    return 0
            # --- Calculate row total as sum of all relevant numeric columns ---
            mr_untraceable = None
            brsnr = None
            mh_rto = None
            mh_rvp = None
            p0_priority = None
            p1_priority = None
            for table in tables:
                rows_mr = table.find_elements(By.TAG_NAME, 'tr')
                for row in rows_mr:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if cells:
                        label_raw = cells[0].text.strip()
                        label = label_raw.lower().replace(' ', '').replace('-', '').replace('_', '')
                        if label == 'untraceable':
                            mr_untraceable = cells[1].text.strip() if len(cells) > 1 else None
                            # Look for 'BRSNR' in the same row and fetch its value
                            for i, val in enumerate([c.text.strip() for c in cells]):
                                if val.lower() == 'brsnr' and i + 1 < len(cells):
                                    brsnr = cells[i + 1].text.strip()
                        elif label in ['mhreturnrto']:
                            # Check for <a> tag in the cell
                            if len(cells) > 1:
                                a_tags = cells[1].find_elements(By.TAG_NAME, 'a')
                                if a_tags:
                                    mh_rto = a_tags[0].text.strip()
                                else:
                                    mh_rto = cells[1].text.strip()
                        elif label in ['mhreturnrvp']:
                            # Check for <a> tag in the cell
                            if len(cells) > 1:
                                a_tags = cells[1].find_elements(By.TAG_NAME, 'a')
                                if a_tags:
                                    mh_rvp = a_tags[0].text.strip()
                                else:
                                    mh_rvp = cells[1].text.strip()
                        elif 'priorityshipments[p0]' in label:
                            p0_priority = cells[1].text.strip() if len(cells) > 1 else None
                        elif 'priorityshipments[p1]' in label:
                            p1_priority = cells[1].text.strip() if len(cells) > 1 else None
                # Only break if all are found
                if mr_untraceable is not None and brsnr is not None and mh_rto is not None and mh_rvp is not None:
                    break

            # --- Calculate RTO/RVP Pending ---
            try:
                rto_val = to_int(mh_rto)
                rvp_val = to_int(mh_rvp)
                rto_rvp_pending = rto_val + rvp_val
            except Exception:
                rto_rvp_pending = 'No Data'

            # --- Extract 'OFD', 'Attempted', 'Delivered' from any table ---
            ofd = attempted = delivered = None
            for table in tables:
                rows_of = table.find_elements(By.TAG_NAME, 'tr')
                for row in rows_of:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if not cells or not cells[0].text.strip():
                        continue
                    label = cells[0].text.strip().lower()
                    if 'total ofd' == label or 'ofd' == label:
                        ofd = cells[1].text.strip() if len(cells) > 1 else None
                    elif 'attempted' == label:
                        attempted = cells[1].text.strip() if len(cells) > 1 else None
                    elif 'delivered' == label:
                        delivered = cells[1].text.strip() if len(cells) > 1 else None

            # --- Calculate row total as sum of all relevant numeric columns ---
            row_total = (
                to_int(ageing) + to_int(cpd) + to_int(rvp) + to_int(future_cpd) +
                to_int(mr_untraceable) + to_int(brsnr) + to_int(ofd) + to_int(attempted) + to_int(delivered) + rto_rvp_pending
            )

            # Calculate OFD%
            try:
                denominator = row_total - to_int(rvp)
                if denominator > 0:
                    ofd_percent = (to_int(ofd) + to_int(delivered) + to_int(attempted)) / denominator
                    ofd_percent = f"{ofd_percent * 100:.2f}%"
                else:
                    ofd_percent = "N/A"
            except Exception:
                ofd_percent = "N/A"

            # Calculate Conversion%
            try:
                conv_denominator = to_int(ofd) + to_int(attempted) + to_int(delivered)
                if conv_denominator > 0:
                    conversion_percent = to_int(delivered) / conv_denominator
                    conversion_percent = f"{conversion_percent * 100:.2f}%"
                else:
                    conversion_percent = "N/A"
            except Exception:
                conversion_percent = "N/A"

            # Calculate AOP Achievement %
            try:
                bbd_aop_val = int(bbd_aop) if bbd_aop and bbd_aop.isdigit() else 0
                if bbd_aop_val > 0:
                    # Note: FE's Present will be updated later, so we'll calculate this after FE's Present is extracted
                    aop_ach_percent = "Pending"
                else:
                    aop_ach_percent = "N/A"
            except Exception:
                aop_ach_percent = "N/A"
            
            # Create the result dictionary
            result_data = {
                 'Hub Name': hub,
                 'CLM Name': clm_name,
                 'State': state,
                 'BBD AOP': bbd_aop,
                 'Ageing': ageing,
                 'CPD-FWD': cpd,
                 'CPD-RVP': rvp,
                                   'FDD': future_cpd,
                 'Untraceable': mr_untraceable,
                 'BRSNR': brsnr,
                 'OFD': ofd,
                 'Attempted': attempted,
                 'Delivered': delivered,
                 'Total': row_total,
                 'OFD%': ofd_percent,
                 'CONV%': conversion_percent,
                                   'FE\'s Live': 'No Data', # Initialize to default
                 'AOP Ach%': aop_ach_percent,
                                   'RTO/RVP': rto_rvp_pending,
                 'P0': p0_priority,
                 'P1': p1_priority,
                 'Status': 'Success'
             }
            
            # Print detailed data for this hub
            print(f"\n{'='*60}")
            print(f"📊 DATA EXTRACTED FOR HUB: {hub}")
            print(f"{'='*60}")
            print(f"📍 Location: {clm_name}, {state}")
            print(f"📈 Metrics:")
            print(f"   • Ageing: {ageing}")
            print(f"   • CPD-FWD: {cpd}")
            print(f"   • CPD-RVP: {rvp}")
            print(f"   • FDD: {future_cpd}")
            print(f"   • Untraceable: {mr_untraceable}")
            print(f"   • BRSNR: {brsnr}")
            print(f"   • OFD: {ofd}")
            print(f"   • Attempted: {attempted}")
            print(f"   • Delivered: {delivered}")
            print(f"   • Total: {row_total}")
            print(f"   • OFD%: {ofd_percent}")
            print(f"   • CONV%: {conversion_percent}")
            print(f"   • MH-RTO/RVP: {rto_rvp_pending}")
            print(f"   • P0: {p0_priority}")
            print(f"   • P1: {p1_priority}")
            print(f"   • Status: [SUCCESS] Success")
            print(f"{'='*60}")
            
            results.append(result_data)
        except Exception as e:
            print(f"\n{'='*60}")
            print(f"⚠️  NO DATA FOUND FOR HUB: {hub}")
            print(f"{'='*60}")
            print(f"📍 Location: {clm_name}, {state}")
            print(f"❌ Error: {str(e)}")
            print(f"📊 All metrics: No Data")
            print(f"   • Status: ⚠️  No Data")
            print(f"{'='*60}")
            
            results.append({
                'Hub Name': hub,
                'CLM Name': clm_name,
                'State': state,
                'BBD AOP': bbd_aop,
                'Ageing': 'No Data',
                'CPD-FWD': 'No Data',
                'CPD-RVP': 'No Data',
                'FDD': 'No Data',
                'Untraceable': 'No Data',
                'BRSNR': 'No Data',
                'OFD': 'No Data',
                'Attempted': 'No Data',
                'Delivered': 'No Data',
                'Total': 0,
                'OFD%': 'N/A',
                'CONV%': 'N/A',
                                 'FE\'s Live': 'No Data', # Ensure it's set to default
                'AOP Ach%': 'N/A',
                                 'RTO/RVP': 'No Data', # Ensure it's set to default
                'P0': 'No Data',
                'P1': 'No Data',
                'Status': '[WARNING] No Data'
            })
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"[ERROR] ERROR PROCESSING HUB: {hub}")
        print(f"{'='*60}")
        print(f"[LOCATION] Location: {clm_name}, {state}")
        print(f"[FAIL] Error: {str(e)}")
        print(f"[DATA] All metrics: Error")
        print(f"   • Status: [ERROR] Error")
        print(f"{'='*60}")
        
        results.append({
            'Hub Name': hub,
            'CLM Name': clm_name,
            'State': state,
            'BBD AOP': bbd_aop,
            'Ageing': 'Error',
            'CPD-FWD': 'Error',
            'CPD-RVP': 'Error',
            'FDD': 'Error',
            'Untraceable': 'Error',
            'BRSNR': 'Error',
            'OFD': 'Error',
            'Attempted': 'Error',
            'Delivered': 'Error',
            'Total': 0,
            'OFD%': 'N/A',
            'CONV%': 'N/A',
                         'FE\'s Live': 'Error', # Ensure it's set to default
            'AOP Ach%': 'N/A',
                         'RTO/RVP': 'Error', # Ensure it's set to default
            'P0': 'Error',
            'P1': 'Error',
                'Status': '[ERROR] Error'
        })

    # After extracting main data for this hub:
    # Click Agents summary
    try:
        print(f"Looking for Agents summary tab for {hub}...")
        agents_summary_div = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@title="Agents summary"]')))
        agents_summary_div.click()
        print(f"Clicked Agents summary for {hub}")
        
        # Wait for agents table with shorter timeout
        try:
            agents_table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            rows = agents_table.find_elements(By.TAG_NAME, 'tr')
            
            # Count agents (exclude header row)
            agent_count = len(rows) - 1 if len(rows) > 1 else 0
            print(f"{hub}: Agent count extracted from Agents Summary: {agent_count}")
            results[-1]['FE\'s Live'] = agent_count
        except Exception as table_error:
            print(f"Could not find agents table for {hub}: {table_error}")
            print(f"Setting FE's Live to 0 for {hub}")
            results[-1]['FE\'s Live'] = 0
            
    except Exception as agents_error:
        print(f"Could not click Agents summary for {hub}: {agents_error}")
        print(f"Setting FE's Live to 0 for {hub}")
        results[-1]['FE\'s Live'] = 0
    
    # Calculate AOP Achievement % now that we have FE's Live
    try:
        bbd_aop_val = int(bbd_aop) if bbd_aop and bbd_aop.isdigit() else 0
        if bbd_aop_val > 0 and agent_count > 0:
            aop_ach_percent = (agent_count / bbd_aop_val) * 100
            results[-1]['AOP Ach%'] = f"{aop_ach_percent:.2f}%"
        else:
            results[-1]['AOP Ach%'] = "N/A"
    except Exception:
        results[-1]['AOP Ach%'] = "N/A"
    
    # Return to dashboard/main page
    dashboard_div = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@title="Dashboard"]')))
    dashboard_div.click()
    # Continue to next hub

# After collecting all results, retry failed hubs before exporting
print(f"\n{'='*80}")
print(f"🔄 RETRYING FAILED HUBS")
print(f"{'='*80}")

# Identify failed hubs
failed_hubs = []
for i, result in enumerate(results):
    if result['Status'] in ['⚠️  No Data', '🚨 Error']:
        failed_hubs.append((i, result['Hub Name']))

if failed_hubs:
    print(f"Found {len(failed_hubs)} failed hubs. Retrying...")
    
    for idx, hub_name in failed_hubs:
        print(f"\n🔄 Retrying hub: {hub_name}")
        clm_name, state, bbd_aop = HUB_INFO.get(hub_name, ("", "", ""))
        
        try:
            # Select the hub from the dropdown
            single_value = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div.css-1uccc91-singleValue')))
            dropdown_control = single_value.find_element(By.XPATH, './ancestor::div[contains(@class, "-control")]')
            dropdown_control.click()
            time.sleep(0.5)
            input_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[id^="react-select-"][type="text"]')))
            input_box.clear()
            # Use the full hub name to ensure exact match
            input_box.send_keys(hub_name)
            time.sleep(2)  # Wait for dropdown to populate and filter
            
            # Simple and fast: Just press Enter (old reliable method)
            input_box.send_keys(Keys.ENTER)
            
            time.sleep(3)  # Wait for table to load
            print(f"Verifying that hub '{hub_name}' is correctly selected...")

            # Click the Show Data button
            show_data_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.HubDashboard-showDataButton-1lZt5V8FT3Jdfw4weKyQLD')))
            show_data_btn.click()
            wait.until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
            tables = driver.find_elements(By.TAG_NAME, 'table')
            
            # Extract data (same logic as before)
            ageing = cpd = rvp = future_cpd = None
            try:
                table = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'table.pcm-table.HubDashboard-dashboard-3WQAbhqmb9SxXZhnxYECVK'))
                )
                rows = table.find_elements(By.TAG_NAME, 'tr')
                for row in rows:
                    cells = row.find_elements(By.TAG_NAME, 'td')
                    if len(cells) >= 3:
                        if 'must attempt today - breached shipments' in cells[0].text.lower():
                            ageing = cells[1].text
                        elif 'must attempt today - today cpd' in cells[0].text.lower():
                            cpd = cells[1].text
                            rvp = cells[2].text
                        elif 'future cpd' in cells[0].text.lower():
                            future_cpd = cells[1].text
                
                def to_int(val):
                    try:
                        return int(str(val).replace(',', '')) if val and str(val).strip().replace(',', '').isdigit() else 0
                    except:
                        return 0
                
                # Extract other data
                mr_untraceable = None
                brsnr = None
                mh_rto = None
                mh_rvp = None
                p0_priority = None
                p1_priority = None
                for table in tables:
                    rows_mr = table.find_elements(By.TAG_NAME, 'tr')
                    for row in rows_mr:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if cells:
                            label_raw = cells[0].text.strip()
                            label = label_raw.lower().replace(' ', '').replace('-', '').replace('_', '')
                            if label == 'untraceable':
                                mr_untraceable = cells[1].text.strip() if len(cells) > 1 else None
                                for i, val in enumerate([c.text.strip() for c in cells]):
                                    if val.lower() == 'brsnr' and i + 1 < len(cells):
                                        brsnr = cells[i + 1].text.strip()
                            elif label in ['mhreturnrto']:
                                if len(cells) > 1:
                                    a_tags = cells[1].find_elements(By.TAG_NAME, 'a')
                                    if a_tags:
                                        mh_rto = a_tags[0].text.strip()
                                    else:
                                        mh_rto = cells[1].text.strip()
                            elif label in ['mhreturnrvp']:
                                if len(cells) > 1:
                                    a_tags = cells[1].find_elements(By.TAG_NAME, 'a')
                                    if a_tags:
                                        mh_rvp = a_tags[0].text.strip()
                                    else:
                                        mh_rvp = cells[1].text.strip()
                            elif 'priorityshipments[p0]' in label:
                                p0_priority = cells[1].text.strip() if len(cells) > 1 else None
                            elif 'priorityshipments[p1]' in label:
                                p1_priority = cells[1].text.strip() if len(cells) > 1 else None
                    if mr_untraceable is not None and brsnr is not None and mh_rto is not None and mh_rvp is not None:
                        break

                # Calculate RTO/RVP Pending
                try:
                    rto_val = to_int(mh_rto)
                    rvp_val = to_int(mh_rvp)
                    rto_rvp_pending = rto_val + rvp_val
                except Exception:
                    rto_rvp_pending = 'No Data'

                # Extract OFD, Attempted, Delivered
                ofd = attempted = delivered = None
                for table in tables:
                    rows_of = table.find_elements(By.TAG_NAME, 'tr')
                    for row in rows_of:
                        cells = row.find_elements(By.TAG_NAME, 'td')
                        if not cells or not cells[0].text.strip():
                            continue
                        label = cells[0].text.strip().lower()
                        if 'total ofd' == label or 'ofd' == label:
                            ofd = cells[1].text.strip() if len(cells) > 1 else None
                        elif 'attempted' == label:
                            attempted = cells[1].text.strip() if len(cells) > 1 else None
                        elif 'delivered' == label:
                            delivered = cells[1].text.strip() if len(cells) > 1 else None

                # Calculate totals and percentages
                row_total = (
                    to_int(ageing) + to_int(cpd) + to_int(rvp) + to_int(future_cpd) +
                    to_int(mr_untraceable) + to_int(brsnr) + to_int(ofd) + to_int(attempted) + to_int(delivered) + rto_rvp_pending
                )

                # Calculate OFD%
                try:
                    denominator = row_total - to_int(rvp)
                    if denominator > 0:
                        ofd_percent = (to_int(ofd) + to_int(delivered) + to_int(attempted)) / denominator
                        ofd_percent = f"{ofd_percent * 100:.2f}%"
                    else:
                        ofd_percent = "N/A"
                except Exception:
                    ofd_percent = "N/A"

                # Calculate Conversion%
                try:
                    conv_denominator = to_int(ofd) + to_int(attempted) + to_int(delivered)
                    if conv_denominator > 0:
                        conversion_percent = to_int(delivered) / conv_denominator
                        conversion_percent = f"{conversion_percent * 100:.2f}%"
                    else:
                        conversion_percent = "N/A"
                except Exception:
                    conversion_percent = "N/A"

                # Calculate AOP Achievement %
                try:
                    bbd_aop_val = int(bbd_aop) if bbd_aop and bbd_aop.isdigit() else 0
                    if bbd_aop_val > 0:
                        aop_ach_percent = "Pending"
                    else:
                        aop_ach_percent = "N/A"
                except Exception:
                    aop_ach_percent = "N/A"

                # Update the result with retry data
                results[idx] = {
                    'Hub Name': hub_name,
                    'CLM Name': clm_name,
                    'State': state,
                    'BBD AOP': bbd_aop,
                    'Ageing': ageing,
                    'CPD-FWD': cpd,
                    'CPD-RVP': rvp,
                    'FDD': future_cpd,
                    'Untraceable': mr_untraceable,
                    'BRSNR': brsnr,
                    'OFD': ofd,
                    'Attempted': attempted,
                    'Delivered': delivered,
                    'Total': row_total,
                    'OFD%': ofd_percent,
                    'CONV%': conversion_percent,
                    'FE\'s Live': 'No Data',  # Will be updated after agents extraction
                    'AOP Ach%': aop_ach_percent,
                    'RTO/RVP': rto_rvp_pending,
                    'P0': p0_priority,
                    'P1': p1_priority,
                    'Status': 'Success'
                }

                # Extract agents data
                try:
                    print(f"Looking for Agents summary tab for {hub_name} (retry)...")
                    agents_summary_div = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@title="Agents summary"]')))
                    agents_summary_div.click()
                    print(f"Clicked Agents summary for {hub_name} (retry)")
                    
                    # Wait for agents table with shorter timeout
                    try:
                        agents_table = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'table')))
                        rows = agents_table.find_elements(By.TAG_NAME, 'tr')
                        agent_count = len(rows) - 1 if len(rows) > 1 else 0
                        results[idx]['FE\'s Live'] = agent_count
                    except Exception as table_error:
                        print(f"Could not find agents table for {hub_name} (retry): {table_error}")
                        print(f"Setting FE's Live to 0 for {hub_name}")
                        results[idx]['FE\'s Live'] = 0
                        
                except Exception as agents_error:
                    print(f"Could not click Agents summary for {hub_name} (retry): {agents_error}")
                    print(f"Setting FE's Live to 0 for {hub_name}")
                    results[idx]['FE\'s Live'] = 0

                # Calculate AOP Achievement % now that we have FE's Live
                try:
                    bbd_aop_val = int(bbd_aop) if bbd_aop and bbd_aop.isdigit() else 0
                    if bbd_aop_val > 0 and agent_count > 0:
                        aop_ach_percent = (agent_count / bbd_aop_val) * 100
                        results[idx]['AOP Ach%'] = f"{aop_ach_percent:.2f}%"
                    else:
                        results[idx]['AOP Ach%'] = "N/A"
                except Exception:
                    results[idx]['AOP Ach%'] = "N/A"

                # Return to dashboard
                dashboard_div = wait.until(EC.element_to_be_clickable((By.XPATH, '//div[@title="Dashboard"]')))
                dashboard_div.click()

                print(f"✅ Successfully retried {hub_name}")
                
            except Exception as e:
                print(f"❌ Retry failed for {hub_name}: {str(e)}")
                # Keep the original failed result
        except Exception as e:
            print(f"❌ Error during retry for {hub_name}: {str(e)}")
            # Keep the original failed result
else:
    print("No failed hubs to retry.")

# Ensure 'RTO/RVP' is present in every result and in the DataFrame
for r in results:
    if 'RTO/RVP' not in r:
        r['RTO/RVP'] = 'No Data'

# Use double quotes for all keys/columns with apostrophes
for r in results:
    if 'FE\'s Live' in r:
        r["FE's Live"] = r.pop('FE\'s Live')
    elif "FE's Live" not in r:
        r["FE's Live"] = 'No Data'

# Save to Excel
output_file = 'hub_report.xlsx'
df = pd.DataFrame(results)

# Ensure RTO/RVP is always in the column order and DataFrame
column_order = [
    'Hub Name', 'CLM Name', 'State', 'BBD AOP', 'Ageing', 'CPD-FWD', 'CPD-RVP', 'FDD',
    'Untraceable', 'BRSNR', 'OFD', 'Attempted', 'Delivered', 'Total', 'OFD%', 'CONV%', "FE's Live", 'AOP Ach%', 'RTO/RVP', 'P0', 'P1', 'Status'
]
for col in column_order:
    if col not in df.columns:
        df[col] = 'No Data'
df = df[column_order]

# Sort by 'OFD%' in ascending order (convert to numeric for sorting, ignore '%' and handle 'N/A')
def ofd_percent_to_float(val):
    try:
        if isinstance(val, str) and val.endswith('%'):
            return float(val.replace('%', ''))
        else:
            return float(val)
    except:
        return float('inf')  # Place 'N/A' or invalid at the end

# Exclude Grand Total row from sorting
is_grand_total = df['Hub Name'] == 'Grand Total'
df_main = df[~is_grand_total].copy()
df_total = df[is_grand_total].copy()

df_main['OFD%_num'] = df_main['OFD%'].apply(ofd_percent_to_float)
df_main = df_main.sort_values(by='OFD%_num', ascending=True)
df_main = df_main.drop(columns=['OFD%_num'])

df = pd.concat([df_main, df_total], ignore_index=True)

# Calculate totals for numeric columns
numeric_cols = ['Ageing', 'CPD-FWD', 'CPD-RVP', 'FDD', 'Untraceable', 'BRSNR', 'OFD', 'Attempted', 'Delivered', 'Total', 'P0', 'P1']
total_row = {'Hub Name': 'Grand Total', 'CLM Name': '', 'State': '', 'BBD AOP': ''}
for col in numeric_cols:
    total_row[col] = pd.to_numeric(df[col], errors='coerce').sum()

# Calculate Grand Total for BBD AOP
bbd_aop_sum = pd.to_numeric(df['BBD AOP'], errors='coerce').sum()
total_row['BBD AOP'] = int(bbd_aop_sum) if not pd.isna(bbd_aop_sum) else 0

# Handle FE's Live separately to ensure it's treated as a number sum, not percentage
fe_live_sum = pd.to_numeric(df["FE's Live"], errors='coerce').sum()
total_row["FE's Live"] = int(fe_live_sum) if not pd.isna(fe_live_sum) else 0
# Calculate overall OFD% for all hubs
try:
    sum_ofd = pd.to_numeric(df['OFD'], errors='coerce').sum()
    sum_delivered = pd.to_numeric(df['Delivered'], errors='coerce').sum()
    sum_attempted = pd.to_numeric(df['Attempted'], errors='coerce').sum()
    sum_total = pd.to_numeric(df['Total'], errors='coerce').sum()
    sum_cpd_rvp = pd.to_numeric(df['CPD-RVP'], errors='coerce').sum()
    denominator = sum_total - sum_cpd_rvp
    if denominator > 0:
        total_ofd_percent = (sum_ofd + sum_delivered + sum_attempted) / denominator
        total_row['OFD%'] = f"{total_ofd_percent * 100:.2f}%"
    else:
        total_row['OFD%'] = 'N/A'
except Exception:
    total_row['OFD%'] = 'N/A'
# Calculate overall CONV% for all hubs
try:
    sum_attempted = pd.to_numeric(df['Attempted'], errors='coerce').sum()
    conv_denominator = sum_ofd + sum_attempted + sum_delivered
    if conv_denominator > 0:
        total_conversion_percent = sum_delivered / conv_denominator
        total_row['CONV%'] = f"{total_conversion_percent * 100:.2f}%"
    else:
        total_row['CONV%'] = 'N/A'
except Exception:
    total_row['CONV%'] = 'N/A'

# Calculate overall AOP Achievement % for all hubs
try:
    total_bbd_aop = pd.to_numeric(df['BBD AOP'], errors='coerce').sum()
    total_fe_live = pd.to_numeric(df["FE's Live"], errors='coerce').sum()
    if total_bbd_aop > 0 and total_fe_live > 0:
        total_aop_ach_percent = (total_fe_live / total_bbd_aop) * 100
        total_row['AOP Ach%'] = f"{total_aop_ach_percent:.2f}%"
    else:
        total_row['AOP Ach%'] = 'N/A'
except Exception:
    total_row['AOP Ach%'] = 'N/A'
# Only include columns that exist in the DataFrame
total_row = {col: total_row.get(col, None) for col in df.columns}
df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

# Also add Grand Total row to results list for email
results.append(total_row)

# FE's Present is now handled in the numeric_cols loop above

# Set Grand Total for RTO/RVP as sum
if 'RTO/RVP' in df.columns:
    rto_rvp_sum = pd.to_numeric(df.loc[df['Hub Name'] != 'Grand Total', 'RTO/RVP'], errors='coerce').sum()
    df.loc[df['Hub Name'] == 'Grand Total', 'RTO/RVP'] = rto_rvp_sum

# Set Grand Total for P0 and P1 as sum
if 'P0' in df.columns:
    p0_sum = pd.to_numeric(df.loc[df['Hub Name'] != 'Grand Total', 'P0'], errors='coerce').sum()
    df.loc[df['Hub Name'] == 'Grand Total', 'P0'] = p0_sum

if 'P1' in df.columns:
    p1_sum = pd.to_numeric(df.loc[df['Hub Name'] != 'Grand Total', 'P1'], errors='coerce').sum()
    df.loc[df['Hub Name'] == 'Grand Total', 'P1'] = p1_sum

# Print summary of all data collected
print(f"\n{'='*80}")
print(f"[SUMMARY] SUMMARY OF ALL DATA COLLECTED")
print(f"{'='*80}")
print(f"Total hubs processed: {len(results)}")
success_count = len([r for r in results if r['Status'] == 'Success'])
no_data_count = len([r for r in results if r['Status'] == '[WARNING] No Data'])
error_count = len([r for r in results if r['Status'] == '[ERROR] Error'])
print(f"[SUCCESS] Successful extractions: {success_count}")
print(f"[WARNING] No data found: {no_data_count}")
print(f"[ERROR] Errors: {error_count}")
print(f"{'='*80}")

# Add timestamp to the header
current_timestamp = datetime.now().strftime("%d %b %H:%M")
timestamp_header = f"EMO Reports - {current_timestamp}"

# Update column headers to include timestamp
df.columns = [f"{col} - {current_timestamp}" if col == 'Hub Name' else col for col in df.columns]

# Initialize Google Sheets connection
try:
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    gc = gspread.authorize(creds)
    
    # Open the spreadsheet
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    
    # Get or create the worksheet
    try:
        worksheet = spreadsheet.worksheet(WORKSHEET_NAME)
        # Clear existing data
        worksheet.clear()
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=WORKSHEET_NAME, rows=1000, cols=20)
    
    # Convert all columns to standard Python types to avoid JSON serialization errors
    def convert_to_serializable(obj):
        if pd.isna(obj) or obj is None:
            return None
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return obj
    
    # Apply conversion to all DataFrame values (using map to avoid deprecation warning)
    df = df.map(convert_to_serializable)

    # Write data to Google Sheets starting from row 1
    set_with_dataframe(worksheet, df, row=1)
    
    # Apply basic formatting (Google Sheets has limited formatting options compared to Excel)
    
    # Note: Column widths can be manually adjusted in Google Sheets using "Fit to data"
    print("💡 Tip: Use 'Fit to data' in Google Sheets to adjust column widths automatically")
    
    # Find the column letter for the last column (Status)
    num_columns = len(column_order)
    last_col_letter = string.ascii_uppercase[num_columns - 1]
    
    # Apply green formatting to the column headers row (row 1) only
    worksheet.format(f'A1:{last_col_letter}1', {
        'backgroundColor': {'red': 0.2, 'green': 0.8, 'blue': 0.2},
        'textFormat': {'bold': True},
        'horizontalAlignment': 'CENTER'
    })
    
    # Set left alignment for Hub Name, CLM Name, State, and BBD AOP columns (first 4 columns)
    worksheet.format(f'A1:D1', {
        'backgroundColor': {'red': 0.2, 'green': 0.8, 'blue': 0.2},
        'textFormat': {'bold': True},
        'horizontalAlignment': 'LEFT'
    })
    
    # Format the Hub Name column (which contains the timestamp) with normal black text
    worksheet.format(f'A1', {
        'backgroundColor': {'red': 0.2, 'green': 0.8, 'blue': 0.2},
        'textFormat': {'bold': True},
        'horizontalAlignment': 'LEFT'
    })
    
    # Replace the previous Grand Total formatting logic with a search for the row where 'Hub Name' == 'Grand Total'.
    # Only format that row in yellow and bold.

    # Find the row number where 'Hub Name' == 'Grand Total'
    grand_total_row = None
    hub_name_col = [col for col in df.columns if 'Hub Name' in col][0]  # Get the actual column name with timestamp
    for idx, val in enumerate(df[hub_name_col], start=2):  # +2 because column headers is row 1
        if val == 'Grand Total':
            grand_total_row = idx
            break

    if grand_total_row:
        worksheet.format(f'A{grand_total_row}:{last_col_letter}{grand_total_row}', {
            'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0},  # Yellow color
            'textFormat': {'bold': True}
        })
    
    # Batch formatting to reduce API calls and avoid rate limits
    print("Applying formatting (this may take a moment to avoid rate limits)...")
    
    # Get all cell values at once to reduce API calls (including row 2 which is the first data row)
    data_range = f'A2:{last_col_letter}{len(df) + 1}'
    all_cells = worksheet.get(data_range)
    
    # Prepare batch formatting requests
    batch_requests = []
    
    # Debug: Print available columns
    # print(f"Debug: Available columns in DataFrame: {list(df.columns)}")
    
    # Format OFD% column - apply individual cell formatting
    try:
         ofd_col_idx = list(df.columns).index('OFD%')
         ofd_col_letter = string.ascii_uppercase[ofd_col_idx]
         ofd_col_data = [row[ofd_col_idx] for row in all_cells]
         
         for i, cell_value in enumerate(ofd_col_data, start=2):
             if cell_value and isinstance(cell_value, str) and cell_value.endswith('%'):
                 try:
                     num = float(cell_value.replace('%', ''))
                     if num < 50:
                         worksheet.format(f'{ofd_col_letter}{i}', {
                             'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.4}
                         })
                     elif num < 80:
                         worksheet.format(f'{ofd_col_letter}{i}', {
                             'backgroundColor': {'red': 1.0, 'green': 0.7, 'blue': 0.3}
                         })
                     else:
                         worksheet.format(f'{ofd_col_letter}{i}', {
                             'backgroundColor': {'red': 0.6, 'green': 0.9, 'blue': 0.6}
                         })
                 except Exception:
                     pass
    except ValueError:
        print("Warning: OFD% column not found, skipping OFD% formatting")
    
    # Format CONV% column - apply individual cell formatting
    try:
         conv_col_idx = list(df.columns).index('CONV%')
         conv_col_letter = string.ascii_uppercase[conv_col_idx]
         conv_col_data = [row[conv_col_idx] for row in all_cells]
         
         for i, cell_value in enumerate(conv_col_data, start=2):
             if cell_value and isinstance(cell_value, str) and cell_value.endswith('%'):
                 try:
                     num = float(cell_value.replace('%', ''))
                     if num < 50:
                         worksheet.format(f'{conv_col_letter}{i}', {
                             'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.4}
                         })
                     elif num < 80:
                         worksheet.format(f'{conv_col_letter}{i}', {
                             'backgroundColor': {'red': 1.0, 'green': 0.7, 'blue': 0.3}
                         })
                     else:
                         worksheet.format(f'{conv_col_letter}{i}', {
                             'backgroundColor': {'red': 0.6, 'green': 0.9, 'blue': 0.6}
                         })
                 except Exception:
                     pass
    except ValueError:
        print("Warning: CONV% column not found, skipping CONV% formatting")
    
    # Format CPD-FWD column - make cells red if > 50
    try:
        cpd_fwd_col_idx = list(df.columns).index('CPD-FWD')
        cpd_fwd_col_letter = string.ascii_uppercase[cpd_fwd_col_idx]
        cpd_fwd_col_data = [row[cpd_fwd_col_idx] for row in all_cells]
        
        for i, cell_value in enumerate(cpd_fwd_col_data, start=2):
            if cell_value and isinstance(cell_value, (int, float, str)):
                try:
                    num = float(cell_value) if isinstance(cell_value, str) else float(cell_value)
                    if num > 50:
                        worksheet.format(f'{cpd_fwd_col_letter}{i}', {
                            'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.4}
                        })
                except Exception:
                    pass
    except ValueError:
        print("Warning: CPD-FWD column not found, skipping CPD-FWD formatting")
    
    # Total column - apply formatting to entire column at once (more efficient)
    try:
        total_col_idx = list(df.columns).index('Total')
        total_col_letter = string.ascii_uppercase[total_col_idx]
        # Apply white background to entire Total column to reset any existing formatting
        batch_requests.append({
            'range': f'{total_col_letter}2:{total_col_letter}{len(df) + 1}',
            'format': {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},  # White background
                'numberFormat': {'type': 'TEXT'}  # Force text format to remove any number formatting
            }
        })
    except ValueError:
        pass
    
    # P1 column - apply white background to remove any existing color formatting (excluding Grand Total row)
    try:
        p1_col_idx = list(df.columns).index('P1')
        p1_col_letter = string.ascii_uppercase[p1_col_idx]
        # Apply white background to P1 column data rows only (excluding Grand Total)
        if grand_total_row:
            # Format data rows (row 2 to Grand Total row - 1)
            batch_requests.append({
                'range': f'{p1_col_letter}2:{p1_col_letter}{grand_total_row - 1}',
                'format': {
                    'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},  # White background
                    'numberFormat': {'type': 'NUMBER', 'pattern': '0'}  # Force number format
                }
            })
            # Format rows after Grand Total (if any)
            if grand_total_row < len(df) + 1:
                batch_requests.append({
                    'range': f'{p1_col_letter}{grand_total_row + 1}:{p1_col_letter}{len(df) + 1}',
                    'format': {
                        'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},  # White background
                        'numberFormat': {'type': 'NUMBER', 'pattern': '0'}  # Force number format
                    }
                })
        else:
            # If no Grand Total row found, format entire column
            batch_requests.append({
                'range': f'{p1_col_letter}2:{p1_col_letter}{len(df) + 1}',
                'format': {
                    'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 1.0},  # White background
                    'numberFormat': {'type': 'NUMBER', 'pattern': '0'}  # Force number format
                }
            })
    except ValueError:
        pass
    
    # Format FE's Live column as number
    try:
        fe_live_col_idx = list(df.columns).index("FE's Live")
        fe_live_col_letter = string.ascii_uppercase[fe_live_col_idx]
    except ValueError:
        print("Warning: FE's Live column not found, skipping FE's Live formatting")
        fe_live_col_idx = None
        fe_live_col_letter = None
    
    # Apply number formatting to entire FE's Live column at once (more efficient)
    if fe_live_col_letter:
        batch_requests.append({
            'range': f'{fe_live_col_letter}2:{fe_live_col_letter}{len(df) + 1}',
            'format': {
                'numberFormat': {'type': 'NUMBER', 'pattern': '0'}
            }
        })
    
    # Format Grand Total row FE's Live cell
    if grand_total_row and fe_live_col_letter:
        batch_requests.append({
            'range': f'{fe_live_col_letter}{grand_total_row}',
            'format': {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0},
                'textFormat': {'bold': True},
                'numberFormat': {'type': 'NUMBER', 'pattern': '0'}
            }
        })
    
    # Format Grand Total row Total column as number (not percentage)
    try:
        total_col_idx = list(df.columns).index('Total')
        total_col_letter = string.ascii_uppercase[total_col_idx]
        if grand_total_row and total_col_letter:
            batch_requests.append({
                'range': f'{total_col_letter}{grand_total_row}',
                'format': {
                    'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0},
                    'textFormat': {'bold': True},
                    'numberFormat': {'type': 'NUMBER', 'pattern': '0'}
                }
            })
    except ValueError:
        pass
    
    # Apply batch formatting in chunks to avoid rate limits
    chunk_size = 5  # Process 5 formatting requests at a time (increased since we have fewer requests now)
    # print(f"Debug: Total batch requests to apply: {len(batch_requests)}")
    for i in range(0, len(batch_requests), chunk_size):
        chunk = batch_requests[i:i + chunk_size]
        # print(f"Debug: Applying chunk {i//chunk_size + 1}, requests {i} to {min(i+chunk_size, len(batch_requests))}")
        for request in chunk:
            try:
                worksheet.format(request['range'], request['format'])
            except Exception as e:
                # print(f"Debug: Error formatting {request['range']}: {e}")
                pass
        time.sleep(1)  # Reduced wait time since we have fewer API calls

    print(f"Data successfully uploaded to Google Sheets!")
    print(f"Spreadsheet URL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    
    # Send email report
    print("[EMAIL] Sending email report...")
    try:
        send_email_report(results, df)
        print("[SUCCESS] Email report sent successfully!")
    except Exception as e:
        print(f"[ERROR] Failed to send email report: {e}")
    
except Exception as e:
    print(f"Error uploading to Google Sheets: {e}")
    print("Falling back to Excel output...")
    
    # Fallback to Excel if Google Sheets fails
    output_file = 'Automatic_EMO_Googlesheet_Reports.xlsx'
    df.to_excel(output_file, index=False)
    print(f"Data saved to Excel file: {output_file}")

driver.quit()