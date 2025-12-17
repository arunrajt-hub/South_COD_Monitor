"""
Reservations Email Automation Script
Reads Google Sheets data from "Reservations" worksheet, filters last 5 days data, and sends styled HTML email.
Similar to 4D Active Email automation.
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
SPREADSHEET_ID = "1MECqYXOwZhA903-dnjZKgo-z5q14CindqgiLiAvtpxI"
WORKSHEET_NAME = "Reservations"
OFD_WORKSHEET_NAME = "OFD"
DAYS_TO_FETCH = 5
OFD_DAYS_TO_FETCH = 3  # Latest 3 days for OFD

# Hub Names to Filter
TARGET_HUB_NAMES = [
    "LargelogicChinnamanurODH_CNM",
    "LargeLogicKuniyamuthurODH_CJB",
    "LargeLogicDharapuramODH_DHP",
    "KoorieeSoukyaRdODH_BLR",
    "HulimavuHub_BLR",
    "SulebeleMDH_SUL",
    "CABTSRNagarODH_HYD",
    "DommasandraSplitODH_DMN",
    "VadipattiMDH_VDP",
    "BagaluruMDH_BAG",
    "TTSPLKodaikanalODH_KDI",
    "ThavarekereMDH_THK",
    "LargeLogicRameswaramODH_RMS",
    "KoorieeSoukyaRdTempODH_BLR",
    "SaidabadSplitODH_HYD",
    "KoorieeHayathnagarODH_HYD",
    "BidarFortHub_BDR",
    "TTSPLBatlagunduODH_BGU",
    "ElasticRunBidarODH_BDR",
    "SITICSWadiODH_WDI",
    "NaubadMDH_BDR"
]

# Manual header row configuration
MANUAL_HEADER_ROW_INDEX = 1  # Headers are in 2nd row (0-based index: 1) for Reservations
OFD_MANUAL_HEADER_ROW_INDEX = 0  # Headers are in 1st row (0-based index: 0) for OFD

# CLM Email Mapping (same as 4D Active Email)
CLM_EMAIL = {
    "Asif": "abdulasif@loadshare.net",
    "Kishore": "kishorkumar.m@loadshare.net",
    "Haseem": "hasheem@loadshare.net",
    "Madvesh": "madvesh@loadshare.net",
    "Irappa": "irappa.vaggappanavar@loadshare.net",
    "Bharath": "bharath.s@loadshare.net",
    "Lokesh": "lokeshh@loadshare.net"
}

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': os.getenv('GMAIL_SENDER_EMAIL', 'arunraj@loadshare.net'),
    'sender_password': os.getenv('GMAIL_APP_PASSWORD', 'ihczkvucdsayzrsu'),
    'recipient_email': 'arunraj@loadshare.net',
    'cc_list': ['maligai.rasmeen@loadshare.net'],
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
        logger.info("✅ Google Sheets client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"❌ Error initializing Google Sheets client: {e}")
        raise

def read_sheet_data(client, spreadsheet_id, worksheet_name):
    """Read all data from Google Sheets worksheet"""
    try:
        logger.info(f"📊 Reading data from Google Sheets...")
        spreadsheet = client.open_by_key(spreadsheet_id)
        
        worksheet = None
        for ws in spreadsheet.worksheets():
            if ws.title.lower() == worksheet_name.lower():
                worksheet = ws
                break
        
        if not worksheet:
            raise ValueError(f"Worksheet '{worksheet_name}' not found")
        
        logger.info(f"✅ Using worksheet: {worksheet.title}")
        values = worksheet.get_all_values()
        
        if not values:
            logger.warning("⚠️ No data found in worksheet")
            return []
        
        logger.info(f"✅ Read {len(values)} rows from Google Sheets")
        return values
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
        header_str = str(header).strip()
        header_lower = header_str.lower()
        # Look for columns containing both "hub" and "name"
        if "hub" in header_lower and "name" in header_lower:
            return idx
    return None

def normalize_hub_name(hub_name):
    """Normalize hub name for matching"""
    if not hub_name:
        return ""
    normalized = str(hub_name).strip().lower().replace("_", " ").replace("-", " ")
    return " ".join(normalized.split())

def process_reservations_data(data, ofd_averages=None):
    """
    Main data processing function - rewritten from scratch
    Args:
        data: Reservations worksheet data
        ofd_averages: Dictionary mapping hub_name -> ofd_average (optional)
    Returns: (filtered_headers, filtered_data_rows, last_3_dates)
        last_3_dates: List of last 3 date objects from the 5 dates used
    """
    try:
        logger.info("🔍 Processing reservations data...")
        
        if not data or len(data) < 2:
            logger.warning("⚠️ Insufficient data")
            return [], []
        
        # Step 1: Find header row
        header_row_idx = MANUAL_HEADER_ROW_INDEX if MANUAL_HEADER_ROW_INDEX is not None else 1
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
            return [], []
        
        logger.info(f"✅ Found Hub Name column at index {hub_name_col_idx}: '{headers[hub_name_col_idx]}'")
        
        # Step 3: Filter by hub names
        if TARGET_HUB_NAMES:
            logger.info(f"🔍 Filtering by {len(TARGET_HUB_NAMES)} target hubs...")
            filtered_rows = []
            hub_lookup = {h.lower(): h for h in TARGET_HUB_NAMES}
            hub_lookup.update({normalize_hub_name(h): h for h in TARGET_HUB_NAMES})
            
            for row in data_rows:
                if hub_name_col_idx < len(row) and row[hub_name_col_idx]:
                    hub_name = str(row[hub_name_col_idx]).strip()
                    if hub_name.lower() in hub_lookup or normalize_hub_name(hub_name) in hub_lookup:
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
            return [], [], []
        
        logger.info(f"📅 Found {len(date_columns)} date columns")
        
        # Step 5: Filter to latest 5 dates (up to yesterday)
        yesterday = datetime.now().date() - timedelta(days=1)
        valid_dates = [(c, h, d) for c, h, d in date_columns if d <= yesterday]
        valid_dates.sort(key=lambda x: x[2], reverse=True)  # Sort descending to get latest first
        latest_dates_sorted_desc = valid_dates[:DAYS_TO_FETCH]  # Get latest 5
        
        # Reverse to chronological order (earliest to latest) for display
        latest_dates = list(reversed(latest_dates_sorted_desc))
        
        # Extract last 3 dates for OFD (as date objects only)
        last_3_dates = [d for _, _, d in latest_dates[-3:]] if len(latest_dates) >= 3 else [d for _, _, d in latest_dates]
        
        latest_date_col_indices = {c for c, _, _ in latest_dates}
        logger.info(f"✅ Selected latest {len(latest_dates)} date columns in chronological order")
        logger.info(f"📅 Last 3 dates for OFD: {[d.strftime('%d-%b-%Y') for d in last_3_dates]}")
        
        # Step 6: Build column mapping - columns to keep
        columns_to_remove = ["OPS SPOC", "ZONE", "ZONAL HEAD", "STATE"]
        columns_to_remove_lower = [c.lower() for c in columns_to_remove]
        
        # Build final headers and column mapping
        filtered_headers = []
        column_mapping = {}  # Maps header_name -> original_column_index
        
        # Add Hub Name first
        hub_name_header = headers[hub_name_col_idx]
        filtered_headers.append(hub_name_header)
        column_mapping[hub_name_header] = hub_name_col_idx
        
        # Add date columns in chronological order (earliest to latest)
        # Create mapping: original_header -> formatted_date
        date_header_mapping = {}
        for col_idx, header, parsed_date in latest_dates:
            # Format date as "DD-MMM" (e.g., "03-Dec") without year
            formatted_date = parsed_date.strftime('%d-%b')
            filtered_headers.append(formatted_date)
            column_mapping[formatted_date] = col_idx
            date_header_mapping[header] = formatted_date
        
        # Add AVG column
        filtered_headers.append("AVG")
        
        # Add OFD Capacity column if OFD data is available
        if ofd_averages:
            filtered_headers.append("OFD Cap")
            # Add Capacity Gap column (OFD Capacity - AVG)
            filtered_headers.append("OFDvsRES")
        
        logger.info(f"✅ Final headers: {filtered_headers}")
        
        # Step 7: Process rows using DICTIONARY approach (like 4d_active_email.py)
        filtered_data = []
        
        for row in data_rows:
            # Skip empty rows
            if not row:
                continue
            
            # Create row dictionary
            row_dict = {}
            date_values = []
            
            # Extract Hub Name
            if hub_name_col_idx < len(row):
                hub_name = str(row[hub_name_col_idx]).strip() if row[hub_name_col_idx] else ""
                row_dict[hub_name_header] = hub_name
            else:
                row_dict[hub_name_header] = ""
            
            # Extract date column values using formatted header names
            for col_idx, header, _ in latest_dates:
                value = ""
                if col_idx < len(row) and row[col_idx]:
                    value = str(row[col_idx]).strip()
                
                # Use formatted date header as key (e.g., "03-Dec")
                formatted_date = date_header_mapping.get(header, header)
                row_dict[formatted_date] = value
                
                # Collect for AVG calculation
                try:
                    if value:
                        clean = value.replace(',', '').replace(' ', '')
                        if clean:
                            date_values.append(float(clean))
                except:
                    pass
            
            # Calculate AVG
            avg_val = round(sum(date_values) / len(date_values)) if date_values else 0
            row_dict["AVG"] = avg_val
            
            # Add OFD Capacity if available
            if ofd_averages:
                hub_name_for_ofd = row_dict.get(hub_name_header, "")
                # Try exact match first
                ofd_capacity = ofd_averages.get(hub_name_for_ofd, None)
                
                # If not found, try case-insensitive and normalized matching
                if ofd_capacity is None:
                    hub_name_lower = hub_name_for_ofd.lower()
                    hub_name_normalized = normalize_hub_name(hub_name_for_ofd)
                    
                    # Try case-insensitive match
                    for key, value in ofd_averages.items():
                        if key.lower() == hub_name_lower:
                            ofd_capacity = value
                            break
                        if normalize_hub_name(key) == hub_name_normalized and hub_name_normalized:
                            ofd_capacity = value
                            break
                
                # Default to 0 if still not found
                if ofd_capacity is None:
                    ofd_capacity = 0
                
                row_dict["OFD Cap"] = ofd_capacity
                
                # Calculate Capacity Gap = OFD Capacity - AVG
                capacity_gap = ofd_capacity - avg_val
                row_dict["OFDvsRES"] = capacity_gap
            
            filtered_data.append(row_dict)
        
        logger.info(f"✅ Processed {len(filtered_data)} rows")
        
        # Log OFD capacity values - check for specific hub and all hubs
        if ofd_averages and filtered_data:
            logger.info(f"   📊 OFD Capacity lookup verification:")
            for row_dict in filtered_data:
                hub = row_dict.get(hub_name_header, "UNKNOWN")
                ofd_cap = row_dict.get("OFD Cap", 0)
                # Check if this is the hub the user mentioned
                if "rameswaram" in hub.lower():
                    logger.info(f"      🔍 {hub}: OFD Cap = {ofd_cap} (matched from OFD averages: {hub in ofd_averages})")
            
            # Show summary
            hubs_with_ofd = sum(1 for row_dict in filtered_data if row_dict.get("OFD Cap", 0) > 0)
            hubs_without_ofd = len(filtered_data) - hubs_with_ofd
            logger.info(f"      Summary: {hubs_with_ofd} hubs with OFD Cap > 0, {hubs_without_ofd} hubs with 0")
        
        # Step 8: Sort by AVG (descending)
        filtered_data.sort(key=lambda x: float(x.get("AVG", 0)), reverse=True)
        
        # Step 9: Convert dictionaries to lists in header order
        filtered_data_rows = []
        for row_dict in filtered_data:
            row_list = []
            for header in filtered_headers:
                row_list.append(row_dict.get(header, ""))
            filtered_data_rows.append(row_list)
        
        logger.info(f"✅ Converted to {len(filtered_data_rows)} rows")
        
        # Step 10: Add TOTAL row
        total_row = []
        for header in filtered_headers:
            if header == hub_name_header:
                total_row.append("TOTAL")
            elif header == "AVG":
                # Sum all AVG values
                total_avg = sum(float(row_dict.get("AVG", 0)) for row_dict in filtered_data)
                total_row.append(round(total_avg))
            elif header == "OFD Cap":
                # Sum all OFD Cap values
                total_ofd = sum(float(row_dict.get("OFD Cap", 0)) for row_dict in filtered_data)
                total_row.append(round(total_ofd))
            elif header == "OFDvsRES":
                # Sum all OFDvsRES values
                total_gap = sum(float(row_dict.get("OFDvsRES", 0)) for row_dict in filtered_data)
                total_row.append(round(total_gap))
            else:
                # Sum numeric values for date columns
                col_sum = 0
                for row_dict in filtered_data:
                    value = row_dict.get(header, "")
                    try:
                        if value:
                            clean = str(value).strip().replace(',', '').replace(' ', '')
                            if clean:
                                col_sum += float(clean)
                    except:
                        pass
                total_row.append(round(col_sum))
        
        # Insert TOTAL row at the beginning
        filtered_data_rows.insert(0, total_row)
        
        logger.info("✅ Added TOTAL row")
        
        # Verify Hub Names
        if filtered_data_rows:
            logger.info(f"✅ Sample Hub Names (first 5 rows):")
            for i, row in enumerate(filtered_data_rows[:5]):
                if len(row) > 0:
                    hub_name = str(row[0]).strip()
                    logger.info(f"   Row {i+1}: '{hub_name}'")
        
        # Extract last 3 dates for OFD calculation (just date objects)
        last_3_dates = [d for _, _, d in latest_dates[-3:]] if len(latest_dates) >= 3 else [d for _, _, d in latest_dates]
        
        return filtered_headers, filtered_data_rows, last_3_dates
    
    except Exception as e:
        logger.error(f"❌ Error processing data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise

def process_ofd_data(data, reservation_hub_names, reservation_dates=None):
    """
    Process OFD worksheet data and calculate average of latest 3 days per hub
    Args:
        data: OFD worksheet data
        reservation_hub_names: List of hub names from reservations
        reservation_dates: Optional list of date objects - if provided, use last 3 of these dates
    Returns: dictionary mapping hub_name -> ofd_average
    """
    try:
        logger.info("🔍 Processing OFD data...")
        
        if not data or len(data) < 2:
            logger.warning("⚠️ Insufficient OFD data")
            return {}
        
        # Find header row - OFD uses row 1 (index 0)
        header_row_idx = OFD_MANUAL_HEADER_ROW_INDEX if OFD_MANUAL_HEADER_ROW_INDEX is not None else 0
        if header_row_idx >= len(data):
            header_row_idx = 0
        
        headers = data[header_row_idx]
        data_rows = data[header_row_idx + 1:]
        
        logger.info(f"📋 OFD: Using row {header_row_idx + 1} (index {header_row_idx}) as headers")
        # Log all non-empty headers to help debug
        non_empty_headers = [(i, str(h).strip()) for i, h in enumerate(headers) if h and str(h).strip()]
        logger.info(f"📋 OFD: Found {len(non_empty_headers)} non-empty headers")
        logger.info(f"   First 20 headers: {[f'{i}:{h[:40]}' for i, h in non_empty_headers[:20]]}")
        
        # Find Hub Name column - try multiple strategies
        hub_name_col_idx = None
        
        # Strategy 1: Look for exact "Hub Name" header
        for idx, header in enumerate(headers):
            if header:
                header_str = str(header).strip()
                header_lower = header_str.lower()
                if header_str == "Hub Name" or header_lower == "hub name":
                    hub_name_col_idx = idx
                    logger.info(f"✅ OFD: Found exact match 'Hub Name' at index {idx}")
                    break
        
        # Strategy 2: If not found, check if column 1 contains hub names (common structure)
        if hub_name_col_idx is None:
            # Check if column 1 looks like it contains hub names (has hub-like patterns)
            if len(headers) > 1 and headers[1]:
                col1_value = str(headers[1]).strip()
                # Check if it looks like a hub name (contains common hub patterns)
                if any(pattern in col1_value.lower() for pattern in ['hub', 'mdh', 'odh', 'split', 'kooriee', 'largelogic']):
                    # Check first few data rows to confirm column 1 has hub names
                    hub_like_count = 0
                    for row in data_rows[:5]:
                        if len(row) > 1 and row[1]:
                            val = str(row[1]).strip().lower()
                            if any(pattern in val for pattern in ['hub', 'mdh', 'odh', 'split', 'kooriee', 'largelogic']):
                                hub_like_count += 1
                    if hub_like_count >= 3:  # At least 3 out of 5 rows look like hub names
                        hub_name_col_idx = 1
                        logger.info(f"✅ OFD: Detected Hub Name column at index 1 (contains hub-like values)")
        
        # Strategy 3: Look for columns containing both "hub" and "name" in header
        if hub_name_col_idx is None:
            for idx, header in enumerate(headers):
                if header:
                    header_str = str(header).strip()
                    header_lower = header_str.lower()
                    if "hub" in header_lower and "name" in header_lower:
                        hub_name_col_idx = idx
                        logger.info(f"✅ OFD: Found hub name column '{header_str}' at index {idx}")
                        break
        
        if hub_name_col_idx is None:
            logger.warning("⚠️ OFD: Hub Name column not found!")
            logger.warning("   Tried: exact 'Hub Name' match, column 1 detection, and 'hub'+'name' search")
            logger.warning(f"   Available headers (showing all): {non_empty_headers[:30]}")
            # Try to find similar columns
            similar_cols = []
            for idx, header_str in non_empty_headers:
                header_lower = header_str.lower()
                if "hub" in header_lower or "name" in header_lower:
                    similar_cols.append(f"Index {idx}: '{header_str}'")
            if similar_cols:
                logger.warning(f"   Similar columns found: {similar_cols[:10]}")
            logger.warning("   💡 Please check if the column name is different in OFD worksheet")
            return {}
        
        logger.info(f"✅ OFD: Using Hub Name column at index {hub_name_col_idx}: '{headers[hub_name_col_idx] if hub_name_col_idx < len(headers) else 'N/A'}'")
        
        # Find date columns in headers
        date_columns = []
        for col_idx, header in enumerate(headers):
            if header:
                parsed_date = parse_date(str(header).strip())
                if parsed_date:
                    date_columns.append((col_idx, header, parsed_date))
        
        if not date_columns:
            logger.warning("⚠️ OFD: No date columns found")
            return {}
        
        logger.info(f"📅 OFD: Found {len(date_columns)} date columns")
        
        # Always find latest 3 dates in OFD independently (up to yesterday)
        # This ensures we get the actual last 3 days of data from OFD
        yesterday = datetime.now().date() - timedelta(days=1)
        valid_dates = [(c, h, d) for c, h, d in date_columns if d <= yesterday]
        valid_dates.sort(key=lambda x: x[2], reverse=True)  # Sort descending (newest first)
        
        # Take latest dates - we'll use the last 3 that have data
        # Start with more dates in case some are empty
        latest_dates_desc = valid_dates[:OFD_DAYS_TO_FETCH + 2] if len(valid_dates) >= OFD_DAYS_TO_FETCH + 2 else valid_dates
        latest_dates_desc.sort(key=lambda x: x[2], reverse=False)  # Sort ascending (oldest first) for easier selection
        
        # Use dates 2, 3, 4 from the available dates (to skip date 1 which might be empty/today)
        # This gives us: 30-Nov, 01-Dec, 02-Dec if dates are: 29-Nov, 30-Nov, 01-Dec, 02-Dec, 03-Dec
        if len(latest_dates_desc) >= 4:
            latest_3_dates = latest_dates_desc[1:4]  # Use positions 1, 2, 3 (30-Nov, 01-Dec, 02-Dec)
        elif len(latest_dates_desc) >= 3:
            latest_3_dates = latest_dates_desc[-3:]  # Use last 3
        else:
            latest_3_dates = latest_dates_desc
        
        logger.info(f"📅 OFD: Finding latest 3 dates independently (up to yesterday: {yesterday.strftime('%d-%b-%Y')})")
        
        latest_date_col_indices = {c for c, _, _ in latest_3_dates}
        logger.info(f"✅ OFD: Selected latest {len(latest_3_dates)} date columns")
        # Log which dates are being used (in chronological order)
        for col_idx, header, parsed_date in latest_3_dates:
            logger.info(f"      Date: {parsed_date.strftime('%d-%b-%Y')} (Column {col_idx}: '{header}')")
        
        # Build hub lookup for matching
        hub_lookup = {}
        for hub in reservation_hub_names:
            hub_lower = hub.lower()
            hub_normalized = normalize_hub_name(hub)
            hub_lookup[hub_lower] = hub
            hub_lookup[hub] = hub
            if hub_normalized:
                hub_lookup[hub_normalized] = hub
        
        # Process rows and calculate averages
        ofd_averages = {}  # Maps hub_name -> average_value
        matched_hubs_set = set()
        unmatched_ofd_hubs = []
        
        for row in data_rows:
            if not row or hub_name_col_idx >= len(row) or not row[hub_name_col_idx]:
                continue
            
            hub_name = str(row[hub_name_col_idx]).strip()
            if not hub_name:
                continue
            
            # Try to match with reservation hubs
            matched_hub = None
            match_type = "NONE"
            hub_name_lower = hub_name.lower()
            hub_name_normalized = normalize_hub_name(hub_name)
            
            # Strategy 1: Exact case-sensitive match (highest priority)
            if hub_name in hub_lookup:
                matched_hub = hub_lookup[hub_name]
                match_type = "EXACT_CASE_SENSITIVE"
            # Strategy 2: Exact case-insensitive match
            elif hub_name_lower in hub_lookup:
                matched_hub = hub_lookup[hub_name_lower]
                match_type = "EXACT_CASE_INSENSITIVE"
            # Strategy 3: Normalized match (only if no exact match) - but log this as it might be incorrect
            elif hub_name_normalized and hub_name_normalized in hub_lookup:
                matched_hub = hub_lookup[hub_name_normalized]
                match_type = "NORMALIZED"
                # Log normalized matches as they might be incorrect
                logger.info(f"   ⚠️  Using normalized match: OFD hub '{hub_name}' matched '{matched_hub}' via normalization")
            
            # Skip if no match found
            if not matched_hub:
                # Log unmatched hubs that might be similar to BagaluruMDH_BAG for debugging
                if "bagaluru" in hub_name_lower or "bageshwar" in hub_name_lower or "bagicha" in hub_name_lower:
                    logger.info(f"   ⚠️  OFD hub '{hub_name}' did not match any reservation hub")
                    logger.info(f"      Tried: exact '{hub_name}', lowercase '{hub_name_lower}', normalized '{hub_name_normalized}'")
                unmatched_ofd_hubs.append(hub_name)
                continue
            
            # IMPORTANT: If we already have a value for this hub, check match type priority
            # Exact matches should never be overwritten, but warn if we're about to overwrite
            if matched_hub in ofd_averages:
                existing_value = ofd_averages[matched_hub]
                logger.warning(f"   ⚠️  CONFLICT: OFD hub '{hub_name}' matched '{matched_hub}' but value already exists ({existing_value})")
                logger.warning(f"      Match type: {match_type}")
                logger.warning(f"      This might indicate multiple OFD hubs matching the same reservation hub")
                # Only overwrite if this is a better match (exact vs normalized)
                # But for now, skip to prevent incorrect averaging
                continue
            
            matched_hubs_set.add(matched_hub)
            
            # Extract date values for latest 3 days
            date_values = []
            date_info = []  # Store date and value for logging
            raw_extractions = []  # Store raw extraction info for debugging
            for col_idx, header, parsed_date in latest_3_dates:
                # Check if column index is within row bounds
                if col_idx >= len(row):
                    raw_extractions.append(f"Col{col_idx}({header}): SKIPPED - Column index {col_idx} >= row length {len(row)}")
                    continue
                
                raw_value = row[col_idx]
                # Check if value exists (even if it's empty string or None)
                if raw_value is None or (isinstance(raw_value, str) and not raw_value.strip()):
                    raw_extractions.append(f"Col{col_idx}({header}): raw='{raw_value}' -> EMPTY/NULL")
                    continue
                
                try:
                    value_str = str(raw_value).strip().replace(',', '').replace(' ', '')
                    if value_str:
                        value_float = float(value_str)
                        date_values.append(value_float)
                        date_info.append((parsed_date.strftime('%d-%b'), value_float))
                        raw_extractions.append(f"Col{col_idx}({header}): raw='{raw_value}' -> {value_float}")
                    else:
                        raw_extractions.append(f"Col{col_idx}({header}): raw='{raw_value}' -> EMPTY after strip")
                except Exception as e:
                    raw_extractions.append(f"Col{col_idx}({header}): raw='{raw_value}' -> ERROR: {e}")
                    pass
            
            # Calculate average
            if date_values:
                avg_value = round(sum(date_values) / len(date_values))
                
                # IMPORTANT: Only store if not already set to prevent overwriting from duplicate matches
                if matched_hub in ofd_averages:
                    logger.warning(f"   ⚠️  OFD hub '{hub_name}' matched to '{matched_hub}' but value already exists ({ofd_averages[matched_hub]}). Skipping to avoid overwrite.")
                    continue
                
                # Store the average value
                ofd_averages[matched_hub] = avg_value
                
                # Log detailed info for specific hubs mentioned by user
                if "soukya" in matched_hub.lower() or "bagaluru" in matched_hub.lower():
                    logger.info(f"   📊 OFD: {matched_hub}:")
                    logger.info(f"      Dates & Values: {date_info}")
                    logger.info(f"      Raw extractions: {raw_extractions}")
                    logger.info(f"      Raw values list: {date_values}")
                    if date_values:
                        logger.info(f"      Sum: {sum(date_values):.2f}, Count: {len(date_values)}")
                        logger.info(f"      Average (before round): {sum(date_values) / len(date_values):.6f}")
                    logger.info(f"      Average (rounded): {avg_value}")
                    
                    # Also check what the OFD hub name was in the worksheet
                    ofd_hub_name = str(row[hub_name_col_idx]).strip() if hub_name_col_idx < len(row) else "N/A"
                    logger.info(f"      OFD worksheet hub name: '{ofd_hub_name}'")
                    logger.info(f"      Matched to reservation hub: '{matched_hub}'")
                    logger.info(f"      Match type: {match_type}")
                
                # Always log for BagaluruMDH_BAG to debug the issue
                if matched_hub == "BagaluruMDH_BAG":
                    logger.info(f"   🔍 DEBUG BagaluruMDH_BAG:")
                    logger.info(f"      OFD worksheet hub name: '{hub_name}'")
                    logger.info(f"      Match type: {match_type}")
                    logger.info(f"      Dates & Values: {date_info}")
                    logger.info(f"      Average: {avg_value}")
        
        logger.info(f"✅ OFD: Calculated averages for {len(ofd_averages)} hubs")
        
        # Log matched and unmatched hubs for debugging
        if matched_hubs_set:
            logger.info(f"   ✅ Matched hubs ({len(matched_hubs_set)}): {sorted(matched_hubs_set)}")
        
        missing_reservation_hubs = set(reservation_hub_names) - matched_hubs_set
        if missing_reservation_hubs:
            logger.warning(f"   ⚠️  Reservation hubs NOT found in OFD ({len(missing_reservation_hubs)}):")
            for hub in sorted(missing_reservation_hubs):
                logger.warning(f"      - {hub}")
        
        if unmatched_ofd_hubs:
            unique_unmatched = list(set(unmatched_ofd_hubs))[:10]  # Show first 10
            logger.info(f"   ℹ️  OFD hubs not matching reservation list (showing first 10): {unique_unmatched}")
        
        return ofd_averages
    
    except Exception as e:
        logger.error(f"❌ Error processing OFD data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}

# ============================================================================
# HTML EMAIL FUNCTIONS
# ============================================================================

def create_styled_html_table(headers, data, latest_dates=None):
    """Create styled HTML table"""
    try:
        logger.info("🎨 Creating HTML table...")
        
        if latest_dates and len(latest_dates) > 0:
            sorted_dates = sorted([d for _, _, d in latest_dates])
            earliest = sorted_dates[0].strftime('%d-%m-%Y')
            latest = sorted_dates[-1].strftime('%d-%m-%Y')
            date_range_text = f"{earliest} to {latest} ({len(latest_dates)} dates)"
        else:
            date_range_text = f"Latest {DAYS_TO_FETCH} Dates"
        
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
        .avg-cell {{
            background: #fff3cd !important;
            font-weight: bold;
            text-align: center;
        }}
        th.avg-header {{
            background: linear-gradient(135deg, #ffc107 0%, #ff9800 100%) !important;
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
            <h1>📋 Reservations Vs Capacity</h1>
            <p>{date_range_text}</p>
        </div>
        <div class="content">
            <table>
"""
        
        # Header row
        html += '                <tr>\n'
        for header in headers:
            header_class = "avg-header" if str(header).strip().upper() == "AVG" else ""
            html += f'                    <th class="{header_class}">{header if header else ""}</th>\n'
        html += '                </tr>\n'
        
        # Data rows
        for row_idx, row in enumerate(data, 1):
            is_total_row = (row_idx == 1)
            row_class = "total-row" if is_total_row else ""
            html += f'                <tr class="{row_class}">\n'
            
            for col_idx, header in enumerate(headers):
                value = row[col_idx] if col_idx < len(row) else ""
                value = str(value) if value else ""
                
                cell_class = ""
                is_avg_column = str(header).strip().upper() == "AVG"
                is_hub_name_column = (col_idx == 0)  # First column is always hub name
                
                if is_avg_column and not is_total_row:
                    cell_class = "avg-cell"
                
                # Format numbers - NEVER format hub name column (first column)
                if not is_hub_name_column:
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
                
                html += f'                    <td class="{cell_class}">{value}</td>\n'
            html += '                </tr>\n'
        
        html += """            </table>
        </div>
        <div class="footer">
            <p>This report is automatically generated by the Reservations Email Automation System</p>
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

def get_clm_emails():
    """
    Get all CLM emails for sending the report
    Returns list of CLM email addresses
    """
    all_clm_emails = list(CLM_EMAIL.values())
    
    logger.info(f"📧 Using all CLM emails ({len(all_clm_emails)} total):")
    for clm_name, email in sorted(CLM_EMAIL.items()):
        logger.info(f"   - {clm_name}: {email}")
    
    return all_clm_emails

def send_email(html_content, clm_emails=None, max_retries=3, timeout=60):
    """
    Send email with HTML content to all CLM emails
    Args:
        html_content: HTML content to send
        clm_emails: List of CLM email addresses (optional)
        max_retries: Maximum number of retry attempts (default: 3)
        timeout: Connection timeout in seconds (default: 30)
    """
    if not EMAIL_CONFIG['sender_password']:
        logger.error("❌ Gmail App Password not set!")
        logger.error("   Set it via environment variable: GMAIL_APP_PASSWORD")
        logger.warning("⚠️  Skipping email send. HTML content generated successfully.")
        return
    
    # Use CLM emails if provided, otherwise use all CLM emails
    if clm_emails:
        recipient_emails = clm_emails
    else:
        recipient_emails = get_clm_emails()
    
    # Create message
    msg = MIMEMultipart('alternative')
    msg['From'] = EMAIL_CONFIG['sender_email']
    msg['To'] = ', '.join(recipient_emails)  # All CLM emails in To field
    msg['Cc'] = ', '.join(EMAIL_CONFIG['cc_list'])
    
    logger.info(f"📧 Email recipients configured:")
    logger.info(f"   To ({len(recipient_emails)} CLMs): {', '.join(recipient_emails)}")
    logger.info(f"   CC ({len(EMAIL_CONFIG['cc_list'])}): {', '.join(EMAIL_CONFIG['cc_list'])}")
    
    today_date = datetime.now().strftime('%d-%b-%Y')
    msg['Subject'] = f"South - Reservations Dashboard - {today_date}"
    
    msg.attach(MIMEText(html_content, 'html'))
    
    # All recipients (CLM emails + CC list)
    all_recipients = recipient_emails + EMAIL_CONFIG['cc_list']
    text = msg.as_string()
    
    # Retry logic with timeout
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🔗 Attempt {attempt}/{max_retries}: Connecting to SMTP server: {EMAIL_CONFIG['smtp_server']}:{EMAIL_CONFIG['smtp_port']} (timeout: {timeout}s)")
            
            # Create SMTP connection with timeout
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'], timeout=timeout)
            
            # Set additional timeout for operations
            server.timeout = timeout
            
            logger.info("🔐 Starting TLS...")
            server.starttls()
            
            logger.info("🔐 Logging in...")
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            
            logger.info("📤 Sending email...")
            server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, text)
            server.quit()
            
            logger.info("✅ Email sent successfully!")
            logger.info(f"   To: {', '.join(recipient_emails)}")
            logger.info(f"   CC: {', '.join(EMAIL_CONFIG['cc_list'])}")
            logger.info(f"   Subject: {msg['Subject']}")
            return  # Success, exit function
            
        except smtplib.SMTPConnectError as e:
            last_error = e
            logger.warning(f"⚠️  Attempt {attempt}/{max_retries} failed: Connection error - {e}")
            if attempt < max_retries:
                wait_time = attempt * 5  # Exponential backoff: 5s, 10s, 15s
                logger.info(f"   ⏳ Waiting {wait_time} seconds before retry...")
                import time
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Failed to connect after {max_retries} attempts")
                
        except smtplib.SMTPAuthenticationError as e:
            last_error = e
            logger.error(f"❌ Authentication failed: {e}")
            logger.error("   Please check your Gmail App Password")
            raise  # Don't retry authentication errors
            
        except smtplib.SMTPException as e:
            last_error = e
            logger.warning(f"⚠️  Attempt {attempt}/{max_retries} failed: SMTP error - {e}")
            if attempt < max_retries:
                wait_time = attempt * 5
                logger.info(f"   ⏳ Waiting {wait_time} seconds before retry...")
                import time
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Failed to send email after {max_retries} attempts")
                
        except (ConnectionError, TimeoutError, OSError) as e:
            last_error = e
            error_msg = str(e)
            logger.warning(f"⚠️  Attempt {attempt}/{max_retries} failed: Network error - {error_msg}")
            if attempt < max_retries:
                wait_time = attempt * 5
                logger.info(f"   ⏳ Waiting {wait_time} seconds before retry...")
                import time
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Network connection failed after {max_retries} attempts")
                logger.error(f"   Last error: {error_msg}")
                logger.error("   Possible causes:")
                logger.error("   - Network connectivity issues")
                logger.error("   - Firewall blocking SMTP connection")
                logger.error("   - Gmail server temporarily unavailable")
                logger.error("   - VPN or proxy interfering with connection")
                
        except Exception as e:
            last_error = e
            logger.error(f"❌ Attempt {attempt}/{max_retries} failed with unexpected error: {e}")
            if attempt < max_retries:
                wait_time = attempt * 5
                logger.info(f"   ⏳ Waiting {wait_time} seconds before retry...")
                import time
                time.sleep(wait_time)
            else:
                logger.error(f"❌ Failed after {max_retries} attempts")
                raise
    
    # If we get here, all retries failed
    if last_error:
        logger.error(f"❌ Failed to send email after {max_retries} attempts. Last error: {last_error}")
        raise last_error

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function"""
    try:
        logger.info("=" * 60)
        logger.info("🚀 Starting Reservations Email Automation")
        logger.info("=" * 60)
        
        # Step 1: Initialize Google Sheets client
        client = get_google_sheets_client()
        
        # Step 2: Read Reservations data
        logger.info("📊 Step 2: Reading Reservations worksheet...")
        reservations_data = read_sheet_data(client, SPREADSHEET_ID, WORKSHEET_NAME)
        
        if not reservations_data:
            logger.error("❌ No Reservations data to process")
            return
        
        # Step 3: Extract dates from Reservations worksheet first (to use same dates in OFD)
        logger.info("=" * 60)
        logger.info("📊 Step 3: Extracting dates from Reservations worksheet...")
        reservation_dates = None
        try:
            # Quick extraction of dates from Reservations headers
            header_row_idx = MANUAL_HEADER_ROW_INDEX if MANUAL_HEADER_ROW_INDEX is not None else 1
            if header_row_idx < len(reservations_data):
                headers = reservations_data[header_row_idx]
                date_columns = []
                for col_idx, header in enumerate(headers):
                    if header:
                        parsed_date = parse_date(str(header).strip())
                        if parsed_date:
                            date_columns.append((col_idx, header, parsed_date))
                
                if date_columns:
                    yesterday = datetime.now().date() - timedelta(days=1)
                    valid_dates = [(c, h, d) for c, h, d in date_columns if d <= yesterday]
                    valid_dates.sort(key=lambda x: x[2], reverse=True)
                    latest_5_dates = valid_dates[:DAYS_TO_FETCH]
                    latest_5_dates = list(reversed(latest_5_dates))  # Chronological order
                    
                    # Extract all 5 dates as date objects (to allow OFD to pick last 3 with data)
                    reservation_dates = [d for _, _, d in latest_5_dates] if len(latest_5_dates) >= 5 else [d for _, _, d in latest_5_dates]
                    logger.info(f"✅ Extracted {len(reservation_dates)} dates from Reservations: {[d.strftime('%d-%b-%Y') for d in reservation_dates]}")
        except Exception as e:
            logger.warning(f"⚠️ Could not extract dates from Reservations: {e}")
            reservation_dates = None
        
        # Step 4: Read OFD data (optional - continue even if it fails)
        ofd_averages = {}
        try:
            logger.info("=" * 60)
            logger.info("📊 Step 4: Reading OFD worksheet...")
            logger.info(f"   Worksheet name: '{OFD_WORKSHEET_NAME}'")
            ofd_data = read_sheet_data(client, SPREADSHEET_ID, OFD_WORKSHEET_NAME)
            
            if ofd_data and len(ofd_data) > 0:
                logger.info(f"✅ OFD worksheet read successfully ({len(ofd_data)} rows)")
                # Get hub names from target list for matching
                logger.info("🔍 Processing OFD data with reservation hub names...")
                ofd_averages = process_ofd_data(ofd_data, TARGET_HUB_NAMES, reservation_dates)
                if ofd_averages:
                    logger.info(f"✅ Successfully calculated OFD averages for {len(ofd_averages)} hubs")
                else:
                    logger.warning("⚠️ No OFD averages calculated (may be empty or no matching hubs)")
            else:
                logger.warning("⚠️ OFD worksheet is empty - continuing without OFD data")
        except ValueError as e:
            logger.warning(f"⚠️ OFD worksheet '{OFD_WORKSHEET_NAME}' not found: {e}")
            logger.warning("   Continuing without OFD data - email will be sent without OFD Cap column")
            ofd_averages = {}
        except Exception as e:
            logger.warning(f"⚠️ Error reading/processing OFD worksheet: {type(e).__name__}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            logger.warning("   Continuing without OFD data - email will be sent without OFD Cap column")
            ofd_averages = {}
        
        logger.info("=" * 60)
        
        # Step 5: Process Reservations data (with or without OFD averages)
        logger.info("🔍 Step 5: Processing Reservations data...")
        headers, filtered_data, _ = process_reservations_data(reservations_data, ofd_averages if ofd_averages else None)
        
        if not headers or not filtered_data:
            logger.warning("⚠️ No data after processing")
            return
        
        # Step 6: Create HTML
        logger.info("🎨 Step 6: Creating HTML email...")
        # Create fresh copies of all rows to ensure no reference issues
        filtered_data_copy = [list(row) for row in filtered_data]
        html_content = create_styled_html_table(headers, filtered_data_copy)
        
        # Step 7: Get CLM emails and send email
        logger.info("📧 Step 7: Sending email...")
        clm_emails = get_clm_emails()
        send_email(html_content, clm_emails)
        
        logger.info("=" * 60)
        logger.info("✅ Reservations Email Automation completed successfully!")
        if ofd_averages:
            logger.info(f"   ✅ OFD Cap column included ({len(ofd_averages)} hubs matched)")
        else:
            logger.info("   ℹ️  OFD Cap column not included (OFD data unavailable)")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"❌ Error: {e}")
        logger.error("=" * 60)
        import traceback
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()
