"""
4D Active Email Automation Script
Replicates the n8n workflow: Reads Google Sheets data, filters columns, calculates Attrition, and sends styled HTML email.
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import os

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

# Google Sheets Details (from n8n workflow)
SPREADSHEET_ID = "1yexwDDUJn6frYet7AaoKkJRjqFQp_F40MtrxpOCKkv8"
SHEET_ID = "851267488"  # Sheet ID (gid) - can also use sheet name if ID doesn't work
SHEET_NAME = None  # Optional: Use sheet name instead of ID (e.g., "Sheet1")
RANGE = "A1:V23"  # Range to read (22 columns, rows 1-23)

# Columns to fetch by index (from Code2 in n8n workflow)
# Indices: [0, 18, 19, 20, 21, 22]
# Note: Index 0 = Hub Name, Index 1 (State) is removed, Index 2 (Peak HC) is removed, replaced with FE AOP
COLUMNS_TO_FETCH = [0, 18, 19, 20, 21, 22]  # Hub Name, removed State (index 1) and Peak HC (index 2)

# Column names to find dynamically
FE_AOP_COLUMN_NAME = "FE AOP"  # Column to replace "Peak HC"
LATEST_HC_COLUMN_NAME = "4D Active (30th)"  # Column for Latest HC (30/11/2025) - might also be "Latest HC (30/11/2025)"

# CLM Email Mapping (from Automatic_Untraceable_BRSNR_Googlesheet_Reports.py)
CLM_EMAIL = {
    "Asif": "abdulasif@loadshare.net",
    "Kishore": "kishorkumar.m@loadshare.net",
    "Haseem": "hasheem@loadshare.net",
    "Madvesh": "madvesh@loadshare.net",
    "Irappa": "irappa.vaggappanavar@loadshare.net",
    "Bharath": "bharath.s@loadshare.net",
    "Lokesh": "lokeshh@loadshare.net"
}

# Email Configuration (same as Flipkart Myntra DN Analysis script)
EMAIL_CONFIG = {
    'sender_email': os.getenv('GMAIL_SENDER_EMAIL', 'arunraj@loadshare.net'),
    'sender_password': os.getenv('GMAIL_APP_PASSWORD', 'ihczkvucdsayzrsu'),  # Gmail App Password (same as Flipkart Myntra DN Analysis)
    'recipient_email': 'arunraj@loadshare.net',  # Will be updated with all CLM emails
    'cc_list': ['maligai.rasmeen@loadshare.net', 'rakib@loadshare.net'],
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

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
        
        # Extract and display service account email for sharing
        try:
            import json as json_module
            with open(SERVICE_ACCOUNT_FILE, 'r') as f:
                service_account_data = json_module.load(f)
                service_account_email = service_account_data.get('client_email', 'Not found')
                logger.info("✅ Google Sheets client initialized successfully")
                logger.info(f"📧 Service Account Email: {service_account_email}")
                logger.info("=" * 60)
                logger.info("⚠️  IMPORTANT: Share your Google Sheet with this email!")
                logger.info(f"   1. Open: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit")
                logger.info(f"   2. Click 'Share' button")
                logger.info(f"   3. Add email: {service_account_email}")
                logger.info(f"   4. Set permission to 'Viewer' (or 'Editor' if needed)")
                logger.info(f"   5. Click 'Send'")
                logger.info("=" * 60)
        except Exception as e:
            logger.info("✅ Google Sheets client initialized successfully")
            logger.warning(f"⚠️  Could not extract service account email: {e}")
        
        return client
    except Exception as e:
        logger.error(f"❌ Error initializing Google Sheets client: {e}")
        raise

def read_sheet_data(client, spreadsheet_id, sheet_id, range_name):
    """Read data from Google Sheets"""
    try:
        logger.info(f"📊 Reading data from Google Sheets...")
        logger.info(f"   Spreadsheet ID: {spreadsheet_id}")
        logger.info(f"   Sheet ID: {sheet_id}")
        logger.info(f"   Range: {range_name}")
        
        # Open spreadsheet
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        # Get sheet by ID (gid) or by name
        sheet = None
        if SHEET_NAME:
            # Use sheet name if provided
            logger.info(f"   Using sheet name: {SHEET_NAME}")
            sheet = spreadsheet.worksheet(SHEET_NAME)
        else:
            # Try to get by ID (gid)
            try:
                logger.info(f"   Using sheet ID (gid): {sheet_id}")
                sheet = spreadsheet.get_worksheet_by_id(int(sheet_id))
            except Exception as e:
                logger.warning(f"⚠️ Could not get sheet by ID {sheet_id}: {e}")
                logger.info("   Attempting to list all sheets...")
                # List all worksheets to find the one we need
                all_sheets = spreadsheet.worksheets()
                logger.info(f"   Found {len(all_sheets)} sheets: {[s.title for s in all_sheets]}")
                if all_sheets:
                    # Use first sheet as fallback
                    sheet = all_sheets[0]
                    logger.warning(f"   Using first sheet '{sheet.title}' as fallback")
                else:
                    raise ValueError(f"Could not find any sheets in spreadsheet")
        
        logger.info(f"✅ Using sheet: {sheet.title} (ID: {sheet.id})")
        
        # Read range
        values = sheet.get(range_name)
        
        if not values:
            logger.warning("⚠️ No data found in the specified range")
            return []
        
        logger.info(f"✅ Read {len(values)} rows from Google Sheets")
        return values
    
    except Exception as e:
        logger.error(f"❌ Error reading Google Sheets data: {e}")
        raise

# ============================================================================
# DATA PROCESSING FUNCTIONS
# ============================================================================

def filter_columns_and_calculate_gap(data):
    """
    Filter specific columns and calculate GAP
    Updated: Replaces Peak HC with FE AOP, calculates GAP = FE AOP - Latest HC (30/11/2025)
    """
    try:
        logger.info("🔍 Filtering columns and calculating GAP...")
        
        if not data or len(data) < 2:
            logger.warning("⚠️ Insufficient data to process")
            return []
        
        # First row is headers
        headers = data[0]
        data_rows = data[1:]
        
        # Find FE AOP column index dynamically
        fe_aop_idx = None
        for idx, header in enumerate(headers):
            if header and FE_AOP_COLUMN_NAME.lower() in str(header).lower():
                fe_aop_idx = idx
                logger.info(f"✅ Found '{FE_AOP_COLUMN_NAME}' at column index {idx}")
                break
        
        if fe_aop_idx is None:
            logger.warning(f"⚠️ Could not find '{FE_AOP_COLUMN_NAME}' column. Available headers: {headers[:10]}...")
            # Fallback: assume it's at a specific index if not found
            fe_aop_idx = 2  # Default to index 2 (where Peak HC was)
            logger.warning(f"⚠️ Using default index {fe_aop_idx} for FE AOP")
        
        # Find Latest HC column index (4D Active 30th or similar)
        latest_hc_idx = None
        for idx, header in enumerate(headers):
            if header:
                header_lower = str(header).lower()
                if LATEST_HC_COLUMN_NAME.lower() in header_lower or "30th" in header_lower or "30/11" in header_lower:
                    latest_hc_idx = idx
                    logger.info(f"✅ Found Latest HC column '{header}' at index {idx}")
                    break
        
        if latest_hc_idx is None:
            # Fallback: use index 18 (4D Active 30th)
            latest_hc_idx = 18
            logger.warning(f"⚠️ Could not find Latest HC column. Using default index {latest_hc_idx}")
        
        # Filter headers by index, adding FE AOP after Hub Name (removed Peak HC at index 2)
        filtered_headers = []
        column_mapping = {}  # Maps header name to column index
        
        # Build column mapping first (excluding Peak HC and FE AOP)
        temp_mapping = {}
        fe_aop_header = None
        
        # Get FE AOP column info
        if fe_aop_idx is not None and fe_aop_idx < len(headers):
            fe_aop_header = headers[fe_aop_idx]
            logger.info(f"✅ Found FE AOP column: {fe_aop_header} at index {fe_aop_idx}")
        
        # Build mapping for other columns from COLUMNS_TO_FETCH
        for idx in COLUMNS_TO_FETCH:
            if idx < len(headers):
                header_name = headers[idx]
                # Explicitly skip "Peak HC" column (remove it completely)
                if "Peak HC" in str(header_name) or "peak hc" in str(header_name).lower():
                    logger.info(f"⏭️  Skipping Peak HC column at index {idx}: {header_name}")
                    continue
                # Skip if this is the FE AOP column (will add it separately after Hub Name)
                if idx != fe_aop_idx:
                    temp_mapping[header_name] = idx
        
        # Now build headers in correct order: Hub Name (index 0), then FE AOP, then others
        # Add Hub Name first (index 0)
        if 0 in COLUMNS_TO_FETCH and 0 < len(headers):
            hub_name = headers[0]
            if "Peak HC" not in str(hub_name) and "peak hc" not in str(hub_name).lower() and 0 != fe_aop_idx:
                filtered_headers.append(hub_name)
                column_mapping[hub_name] = 0
                logger.info(f"✅ Added Hub Name: {hub_name}")
        
        # Add FE AOP after Hub Name
        if fe_aop_header:
            filtered_headers.append(fe_aop_header)
            column_mapping[fe_aop_header] = fe_aop_idx
            logger.info(f"✅ Added FE AOP after Hub Name: {fe_aop_header}")
        
        # Add remaining columns in order (excluding Hub Name and State which are already handled)
        for idx in COLUMNS_TO_FETCH:
            if idx == 0:  # Skip Hub Name, already added
                continue
            if idx == 1:  # Skip State column (removed)
                logger.info(f"⏭️  Skipping State column at index {idx}")
                continue
            if idx < len(headers):
                header_name = headers[idx]
                # Skip Peak HC
                if "Peak HC" in str(header_name) or "peak hc" in str(header_name).lower():
                    continue
                # Skip State column
                if "State" in str(header_name) or "state" in str(header_name).lower():
                    logger.info(f"⏭️  Skipping State column: {header_name}")
                    continue
                # Skip FE AOP (already added)
                if idx == fe_aop_idx:
                    continue
                # Skip if already in mapping
                if header_name not in column_mapping:
                    filtered_headers.append(header_name)
                    column_mapping[header_name] = idx
        
        # Add GAP column (must be last)
        if "GAP" not in filtered_headers:
            filtered_headers.append("GAP")
            logger.info("✅ Added GAP column to headers")
        
        # Final check: Remove "Peak HC" and "State" from headers if they somehow got through (but keep GAP)
        filtered_headers = [h for h in filtered_headers if (("Peak HC" not in str(h) and "peak hc" not in str(h).lower()) and 
                                                           ("State" not in str(h) or h == "GAP")) or h == "GAP"]
        # Also remove from column_mapping
        column_mapping = {k: v for k, v in column_mapping.items() if ("Peak HC" not in str(k) and "peak hc" not in str(k).lower() and 
                                                                      "State" not in str(k) and "state" not in str(k).lower())}
        
        # Ensure GAP is in headers (final safety check)
        if "GAP" not in filtered_headers:
            filtered_headers.append("GAP")
            logger.warning("⚠️ GAP was missing, re-added to headers")
        
        logger.info(f"📋 Filtered headers (Peak HC removed, GAP included): {filtered_headers}")
        logger.info(f"📊 Processing {len(data_rows)} data rows...")
        logger.debug(f"   FE AOP column index: {fe_aop_idx}, Latest HC column index: {latest_hc_idx}")
        
        # Process each row
        filtered_data = []
        for row_idx, row in enumerate(data_rows, start=2):  # start=2 because row 1 is header
            # Skip empty rows
            if not row or (len(row) == 1 and not row[0]):
                continue
            
            filtered_row = {}
            
            # Get filtered columns (excluding Peak HC)
            for header_name, col_idx in column_mapping.items():
                # Skip Peak HC if it somehow got through
                if "Peak HC" in str(header_name) or "peak hc" in str(header_name).lower():
                    continue
                # Get value if column exists, otherwise empty string
                if col_idx < len(row):
                    value = row[col_idx] if row[col_idx] else ""
                else:
                    value = ""
                filtered_row[header_name] = value
            
            # Calculate GAP = FE AOP - Latest HC (30/11/2025)
            try:
                # Get FE AOP value
                fe_aop_value = 0
                if fe_aop_idx is not None and fe_aop_idx < len(row):
                    fe_aop_str = str(row[fe_aop_idx]).strip() if row[fe_aop_idx] else "0"
                    try:
                        fe_aop_value = float(fe_aop_str) if fe_aop_str else 0
                    except (ValueError, TypeError):
                        fe_aop_value = 0
                
                # Get Latest HC value
                latest_hc_value = 0
                if latest_hc_idx is not None and latest_hc_idx < len(row):
                    latest_hc_str = str(row[latest_hc_idx]).strip() if row[latest_hc_idx] else "0"
                    try:
                        latest_hc_value = float(latest_hc_str) if latest_hc_str else 0
                    except (ValueError, TypeError):
                        latest_hc_value = 0
                
                # Calculate GAP
                gap = fe_aop_value - latest_hc_value
                filtered_row["GAP"] = gap
            except (ValueError, TypeError) as e:
                logger.debug(f"⚠️ Could not calculate GAP for row {row_idx}: {e}")
                filtered_row["GAP"] = 0
            
            # Only add row if it has at least some data
            if filtered_row:
                filtered_data.append(filtered_row)
            else:
                logger.debug(f"   Skipped empty row {row_idx}")
        
        logger.info(f"✅ Processed {len(filtered_data)} rows (from {len(data_rows)} total rows)")
        if len(filtered_data) == 0:
            logger.warning("⚠️ No data rows were processed. Check if:")
            logger.warning("   1. Data rows are not empty")
            logger.warning("   2. Column indices match your data structure")
            logger.warning(f"   3. First data row: {data_rows[0] if data_rows else 'No rows'}")
        
        # Sort by GAP in descending order (highest GAP first)
        try:
            filtered_data.sort(key=lambda x: float(x.get("GAP", 0)) if isinstance(x.get("GAP"), (int, float)) else 0, reverse=True)
            logger.info("✅ Sorted data by GAP (descending order)")
        except Exception as e:
            logger.warning(f"⚠️ Could not sort by GAP: {e}")
        
        return filtered_headers, filtered_data
    
    except Exception as e:
        logger.error(f"❌ Error filtering columns: {e}")
        raise

def create_styled_html_table(headers, data):
    """
    Create colorful and vibrant styled HTML table
    """
    try:
        logger.info("🎨 Creating colorful styled HTML table...")
        
        # Get today's date for header
        today = datetime.now().strftime('%d-%m-%Y')
        
        # Start HTML with vibrant styling
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
        .gap-positive {{
            background: #ffcdd2 !important;
            color: #c62828;
            font-weight: bold;
        }}
        .gap-negative {{
            background: #c8e6c9 !important;
            color: #2e7d32;
            font-weight: bold;
        }}
        .gap-zero {{
            background: #fff9c4 !important;
            color: #f57f17;
            font-weight: bold;
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
            <h1>📊 4D Active Report - South Flipkart ODH</h1>
            <p>Report Date: {today}</p>
        </div>
        <div class="content">
            <table>
"""
        
        # Header row with vibrant gradient
        html += '                <tr>\n'
        for header in headers:
            html += f'                    <th>{header}</th>\n'
        html += '                </tr>\n'
        
        # Data rows with color coding
        for row_idx, row in enumerate(data, 1):
            html += '                <tr>\n'
            for header in headers:
                value = row.get(header, "")
                
                # Format numbers if they're numeric
                cell_class = ""
                if isinstance(value, (int, float)):
                    value_formatted = f"{value:,.2f}" if value != int(value) else f"{int(value):,}"
                    cell_class = "number-cell"
                    
                    # Color code GAP column
                    if header == "GAP":
                        if value > 0:
                            cell_class += " gap-positive"
                        elif value < 0:
                            cell_class += " gap-negative"
                        else:
                            cell_class += " gap-zero"
                else:
                    value_formatted = str(value) if value else ""
                
                html += f'                    <td class="{cell_class}">{value_formatted}</td>\n'
            html += '                </tr>\n'
        
        html += """            </table>
        </div>
        <div class="footer">
            <p>This report is automatically generated by the 4D Active Email Automation System</p>
            <p>For questions or issues, please contact arunraj@loadshare.net</p>
        </div>
    </div>
</body>
</html>"""
        
        logger.info("✅ Colorful HTML table created successfully")
        return html
    
    except Exception as e:
        logger.error(f"❌ Error creating HTML table: {e}")
        raise

# ============================================================================
# EMAIL FUNCTIONS
# ============================================================================

def get_clm_emails_from_data(data):
    """
    Extract unique CLM emails from the data based on CLM column
    Returns list of CLM email addresses (always includes all CLMs)
    """
    # Always use all CLM emails (as per user requirement: send 1 mail with all CLMs)
    all_clm_emails = list(CLM_EMAIL.values())
    
    logger.info(f"📧 Using all CLM emails ({len(all_clm_emails)} total):")
    for clm_name, email in sorted(CLM_EMAIL.items()):
        logger.info(f"   - {clm_name}: {email}")
    
    return all_clm_emails

def send_email(html_content, clm_emails=None):
    """
    Send email with HTML content to all CLM emails
    Replicates Send a message1 from n8n workflow
    """
    try:
        logger.info("📧 Preparing email...")
        
        # Check if password is set
        if not EMAIL_CONFIG['sender_password']:
            logger.error("❌ Gmail App Password not set!")
            logger.error("   Set it via environment variable: GMAIL_APP_PASSWORD")
            logger.error("   Or in GitHub Actions: Add GMAIL_APP_PASSWORD secret")
            logger.warning("⚠️  Skipping email send. HTML content generated successfully.")
            logger.info("=" * 60)
            logger.info("📄 HTML Content Preview (first 500 chars):")
            logger.info("-" * 60)
            logger.info(html_content[:500] + "..." if len(html_content) > 500 else html_content)
            logger.info("=" * 60)
            return
        
        # Use CLM emails if provided, otherwise use all CLM emails
        if clm_emails:
            recipient_emails = clm_emails
        else:
            # Fallback: use all CLM emails
            recipient_emails = list(CLM_EMAIL.values())
            logger.info("📧 Using all CLM emails as recipients")
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ', '.join(recipient_emails)  # All CLM emails in To field
        msg['Cc'] = ', '.join(EMAIL_CONFIG['cc_list'])
        
        logger.info(f"📧 Email recipients configured:")
        logger.info(f"   To ({len(recipient_emails)} CLMs): {', '.join(recipient_emails)}")
        logger.info(f"   CC ({len(EMAIL_CONFIG['cc_list'])}): {', '.join(EMAIL_CONFIG['cc_list'])}")
        
        # Subject with date (matching n8n format)
        today = datetime.now().strftime('%d-%m-%Y')
        msg['Subject'] = f"Today's 4D Active - South Flipkart ODH - {today}"
        
        # Attach HTML content
        msg.attach(MIMEText(html_content, 'html'))
        
        # Send email
        logger.info(f"🔗 Connecting to SMTP server: {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']}")
        server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
        server.starttls()
        
        logger.info("🔐 Logging in...")
        server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
        
        # All recipients (To + CC)
        recipients = [EMAIL_CONFIG['recipient_email']] + EMAIL_CONFIG['cc_list']
        
        logger.info("📤 Sending email...")
        text = msg.as_string()
        # All recipients (CLM emails + CC list)
        all_recipients = recipient_emails + EMAIL_CONFIG['cc_list']
        server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, text)
        server.quit()
        
        logger.info("✅ Email sent successfully!")
        logger.info(f"   To: {', '.join(recipient_emails)}")
        logger.info(f"   CC: {', '.join(EMAIL_CONFIG['cc_list'])}")
        logger.info(f"   Subject: {msg['Subject']}")
        
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
        logger.info("🚀 Starting 4D Active Email Automation")
        logger.info("=" * 60)
        
        # Step 1: Initialize Google Sheets client
        client = get_google_sheets_client()
        
        # Step 2: Read data from Google Sheets
        data = read_sheet_data(client, SPREADSHEET_ID, SHEET_ID, RANGE)
        
        if not data:
            logger.error("❌ No data to process. Exiting.")
            return
        
        # Step 3: Filter columns and calculate GAP
        headers, filtered_data = filter_columns_and_calculate_gap(data)
        
        if not filtered_data:
            logger.error("❌ No filtered data to send. Exiting.")
            return
        
        # Step 4: Create styled HTML table
        html_content = create_styled_html_table(headers, filtered_data)
        
        # Step 5: Extract CLM emails from data
        clm_emails = get_clm_emails_from_data(filtered_data)
        
        # Step 6: Send email to all CLM emails
        send_email(html_content, clm_emails)
        
        logger.info("=" * 60)
        logger.info("✅ 4D Active Email Automation completed successfully!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ 4D Active Email Automation failed: {e}")
        logger.error("=" * 60)
        raise

if __name__ == "__main__":
    main()

