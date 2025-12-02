"""
Flipkart & Myntra Q2 DN Data Analysis
Analyzes Q2 DN Data from Google Sheets and generates insights (South Zone Hub-wise)
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import Counter, defaultdict
import logging
from typing import Dict, List, Optional
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Google Sheets Configuration
SERVICE_ACCOUNT_FILE = "service_account_key.json"

# Multiple Google Sheets with their worksheets
SHEET_CONFIGS = [
    {
        'sheet_id': "1vEXO1TGn2S9gJ8kzSkCO9M-eiZoyCYFDwjMmVskkERo",
        'worksheets': [
            "IMD Myntra 02"  # IMD Myntra Master Tracker December 2025
        ]
    },
    {
        'sheet_id': "1FFa2Vp5QB8Hx7klp6vGD-hcwj9a3OnbQ0JBKBK2Fa4c",
        'worksheets': [
            "BRSNR Data"  # BRSNR Data sheet
        ]
    }
]

# Legacy configuration for backward compatibility
GOOGLE_SHEET_ID = "1vEXO1TGn2S9gJ8kzSkCO9M-eiZoyCYFDwjMmVskkERo"
WORKSHEET_NAMES = [
    "IMD Myntra 02",
    "BRSNR Data"
]

# Analysis Configuration
OUTPUT_SHEET_ID = "1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM"  # Output sheet for results
OUTPUT_WORKSHEET_NAME = "Q2 DN Analysis Results"  # Base name - timestamp will be added automatically

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': 'arunraj@loadshare.net',
    'sender_password': 'ihczkvucdsayzrsu',  # Gmail App Password
    'recipient_email': 'lokeshh@loadshare.net',
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

# South Zone Hub List (from Automatic_EMO_Googlesheet_Reports.py)
SOUTH_ZONE_HUBS = [
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

# Hub Info Mapping (from Automatic_EMO_Googlesheet_Reports.py)
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

# ============================================================================
# GOOGLE SHEETS SETUP
# ============================================================================

class Q2DNAnalyzer:
    def __init__(self):
        """Initialize the Q2 DN Analyzer with Google Sheets connection"""
        self.sheets_client = None
        self.setup_google_sheets()
    
    def setup_google_sheets(self):
        """Setup Google Sheets connection using service account"""
        try:
            logging.info(f"Loading service account key from: {SERVICE_ACCOUNT_FILE}")
            
            credentials = Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE,
                scopes=[
                    'https://spreadsheets.google.com/feeds',
                    'https://www.googleapis.com/auth/drive'
                ]
            )
            self.sheets_client = gspread.authorize(credentials)
            
            # Extract and display service account email for sharing
            try:
                import json as json_module
                with open(SERVICE_ACCOUNT_FILE, 'r') as f:
                    service_account_data = json_module.load(f)
                    service_account_email = service_account_data.get('client_email', 'Not found')
                    logging.info("Google Sheets connection established")
                    logging.info(f"Service Account Email: {service_account_email}")
                    logging.info("IMPORTANT: Share your Google Sheet with this email address (with Editor access)")
            except Exception as e:
                logging.info("Google Sheets connection established")
                logging.warning(f"Could not extract service account email: {e}")
        except Exception as e:
            logging.error(f"Failed to setup Google Sheets: {e}")
            raise
    
    # ============================================================================
    # DATA EXTRACTION
    # ============================================================================
    
    def pull_data_from_sheet(self, sheet_id: str, worksheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Pull data from Google Sheet
        """
        try:
            logging.info(f"Opening spreadsheet: {sheet_id}")
            spreadsheet = self.sheets_client.open_by_key(sheet_id)
            
            # List all worksheets
            all_worksheets = spreadsheet.worksheets()
            logging.info(f"Available worksheets ({len(all_worksheets)}):")
            for i, ws in enumerate(all_worksheets, 1):
                logging.info(f"   {i}. {ws.title}")
            
            # Get the worksheet
            if worksheet_name:
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                    logging.info(f"Found worksheet: {worksheet_name}")
                except gspread.exceptions.WorksheetNotFound:
                    logging.warning(f"Worksheet '{worksheet_name}' not found. Using first worksheet.")
                    worksheet = all_worksheets[0]
            else:
                worksheet = all_worksheets[0]
            
            # Get all data
            logging.info(f"Reading data from '{worksheet.title}'...")
            
            # Handle duplicate/empty headers by getting raw values first
            try:
                data = worksheet.get_all_records()
                if not data:
                    logging.warning("No data found in worksheet")
                    return pd.DataFrame()
                df = pd.DataFrame(data)
            except gspread.exceptions.GSpreadException as e:
                if "duplicates" in str(e).lower() or "duplicate" in str(e).lower():
                    logging.warning(f"Duplicate headers detected in worksheet. Using alternative method to read data.")
                    # Get raw values and process manually
                    all_values = worksheet.get_all_values()
                    if not all_values or len(all_values) < 2:
                        logging.warning("No data found in worksheet")
                        return pd.DataFrame()
                    
                    # Get headers from first row
                    headers = all_values[0]
                    # Remove duplicate/empty headers by adding index suffix
                    cleaned_headers = []
                    header_counts = {}
                    for idx, header in enumerate(headers):
                        header_str = str(header).strip() if header else ''
                        if not header_str:
                            header_str = f'Unnamed_{idx}'
                        if header_str in header_counts:
                            header_counts[header_str] += 1
                            header_str = f'{header_str}_{header_counts[header_str]}'
                        else:
                            header_counts[header_str] = 0
                        cleaned_headers.append(header_str)
                    
                    # Get data rows
                    data_rows = all_values[1:]
                    
                    # Create DataFrame
                    df = pd.DataFrame(data_rows, columns=cleaned_headers)
                    logging.info(f"Successfully read {len(df)} records using alternative method (duplicate headers handled)")
                else:
                    raise
            
            logging.info(f"Successfully read {len(df)} records")
            logging.info(f"Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error pulling data from sheet: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
            raise
    
    # ============================================================================
    # DATA CLEANING AND PREPARATION
    # ============================================================================
    
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and prepare the data for analysis
        """
        logging.info("Cleaning data...")
        
        # Make a copy
        df_clean = df.copy()
        
        # Handle different column names for Value (Value, Final Amount, TotalPrice)
        if 'Value' in df_clean.columns:
            df_clean['Value'] = pd.to_numeric(
                df_clean['Value'], 
                errors='coerce'
            ).fillna(0)
        elif 'Final Amount' in df_clean.columns:
            df_clean['Value'] = pd.to_numeric(
                df_clean['Final Amount'], 
                errors='coerce'
            ).fillna(0)
        elif 'TotalPrice' in df_clean.columns:
            df_clean['Value'] = pd.to_numeric(
                df_clean['TotalPrice'], 
                errors='coerce'
            ).fillna(0)
        
        # Handle different column names for Tracking ID (TrackingID, TID, Tracking Number, ShipmentID, ShipmentId)
        if 'TrackingID' not in df_clean.columns:
            if 'TID' in df_clean.columns:
                df_clean['TrackingID'] = df_clean['TID']
            elif 'Tracking Number' in df_clean.columns:
                df_clean['TrackingID'] = df_clean['Tracking Number']
            elif 'ShipmentId' in df_clean.columns:
                df_clean['TrackingID'] = df_clean['ShipmentId']
            elif 'ShipmentID' in df_clean.columns:
                df_clean['TrackingID'] = df_clean['ShipmentID']
        
        # Handle different column names for Hub Name (Hub Name, Hub Name as per ERP, Mapped hub, CurrentHub)
        if 'Hub Name' not in df_clean.columns:
            if 'Hub Name as per ERP' in df_clean.columns:
                df_clean['Hub Name'] = df_clean['Hub Name as per ERP']
            elif 'Mapped hub' in df_clean.columns:
                df_clean['Hub Name'] = df_clean['Mapped hub']
            elif 'CurrentHub' in df_clean.columns:
                df_clean['Hub Name'] = df_clean['CurrentHub']
        
        # Handle different column names for Ops Remarks (Ops Remarks, Remarks)
        # For BRSNR Data: "Form filled status" = "NO" means missing Ops Remarks
        if 'Ops Remarks' not in df_clean.columns:
            if 'Remarks' in df_clean.columns:
                df_clean['Ops Remarks'] = df_clean['Remarks']
            elif 'Form filled status' in df_clean.columns:
                # Convert "Form filled status" to Ops Remarks: "NO" = missing, others = filled
                df_clean['Ops Remarks'] = df_clean['Form filled status'].apply(
                    lambda x: '' if str(x).strip().upper() == 'NO' else 'Filled'
                )
            else:
                # Create empty Ops Remarks column if none exists
                df_clean['Ops Remarks'] = ''
        
        # Create Image Proof column if it doesn't exist (for sheets that don't have it)
        if 'Image Proof' not in df_clean.columns:
            df_clean['Image Proof'] = ''
        
        # Clean text columns (including alternative column names)
        text_columns = ['Loss Type', 'Reject Reason', 'Hub Name', 'Hub Name as per ERP', 'Mapped hub', 'CurrentHub',
                        'Region', 'LSN State', 'Pln_Owner', 'TrackingID', 'TID', 'Tracking Number', 'ShipmentID', 'ShipmentId',
                        'Ops Remarks', 'Remarks', 'Image Proof', 'Form filled status']
        for col in text_columns:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
                df_clean[col] = df_clean[col].replace(['nan', 'None', ''], np.nan)
        
        logging.info(f"Data cleaned. Total records: {len(df_clean)}")
        return df_clean
    
    def filter_south_zone_hubs(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter data to include only South zone hubs based on Hub Name column
        Handles case-insensitive matching (source sheet may have different case)
        """
        if 'Hub Name' not in df.columns:
            logging.warning("Hub Name column not found. Returning all data.")
            return df
        
        logging.info(f"Filtering for South zone hubs...")
        logging.info(f"Total records before filtering: {len(df)}")
        
        # Create uppercase versions of South zone hubs for matching
        south_zone_hubs_upper = [hub.upper() for hub in SOUTH_ZONE_HUBS]
        
        # Create a case-insensitive matching function
        def match_hub(hub_name):
            """Match hub name case-insensitively"""
            if pd.isna(hub_name):
                return False
            hub_name_str = str(hub_name).strip()
            hub_name_upper = hub_name_str.upper()
            
            # Check against uppercase list
            if hub_name_upper in south_zone_hubs_upper:
                return True
            
            # Check case-insensitive match with original list
            for south_hub in SOUTH_ZONE_HUBS:
                if hub_name_upper == south_hub.upper():
                    return True
            
            return False
        
        # Apply matching
        df['is_south_zone'] = df['Hub Name'].apply(match_hub)
        df_south = df[df['is_south_zone'] == True].copy()
        
        # Drop the temporary column
        if 'is_south_zone' in df_south.columns:
            df_south = df_south.drop('is_south_zone', axis=1)
        
        logging.info(f"Total records after filtering: {len(df_south)}")
        logging.info(f"South zone hubs found: {df_south['Hub Name'].nunique()}")
        
        # Log which hubs have data
        if len(df_south) > 0:
            hub_counts = df_south['Hub Name'].value_counts()
            logging.info(f"Hubs with cases:")
            for hub, count in hub_counts.items():
                logging.info(f"   {hub}: {count} cases")
        else:
            logging.warning("No matching South zone hubs found!")
            unique_hubs = df['Hub Name'].dropna().unique()
            logging.info(f"Sample hub names from data (first 10): {list(unique_hubs[:10])}")
        
        return df_south
    
    # ============================================================================
    # ANALYSIS FUNCTIONS
    # ============================================================================
    
    def analyze_data(self, df: pd.DataFrame) -> Dict:
        """
        Analyze Q2 DN data patterns
        """
        logging.info("Analyzing Q2 DN data patterns...")
        
        analysis = {}
        
        # 1. Total cases and amount
        total_cases = len(df)
        total_amount = df['Value'].sum() if 'Value' in df.columns else 0
        avg_amount = df['Value'].mean() if 'Value' in df.columns else 0
        
        analysis['summary'] = {
            'total_cases': int(total_cases),
            'total_amount': float(total_amount),
            'avg_amount': float(avg_amount),
            'max_amount': float(df['Value'].max()) if 'Value' in df.columns else 0,
            'min_amount': float(df['Value'].min()) if 'Value' in df.columns else 0
        }
        
        # 2. Analysis by Loss Type
        if 'Loss Type' in df.columns:
            loss_type_counts = df['Loss Type'].value_counts().to_dict()
            loss_type_amounts = df.groupby('Loss Type')['Value'].sum().to_dict()
            analysis['by_loss_type'] = {
                'counts': {str(k): int(v) for k, v in loss_type_counts.items()},
                'amounts': {str(k): float(v) for k, v in loss_type_amounts.items()}
            }
        
        # 3. Analysis by Reject Reason
        if 'Reject Reason' in df.columns:
            reject_reason_counts = df['Reject Reason'].value_counts().head(10).to_dict()
            reject_reason_amounts = df.groupby('Reject Reason')['Value'].sum().sort_values(ascending=False).head(10).to_dict()
            analysis['by_reject_reason'] = {
                'counts': {str(k): int(v) for k, v in reject_reason_counts.items()},
                'amounts': {str(k): float(v) for k, v in reject_reason_amounts.items()}
            }
        
        # 4. South Zone Hub-wise Analysis
        if 'Hub Name' in df.columns:
            # Normalize hub names for matching
            def normalize_hub_name(hub_name):
                """Normalize hub name for case-insensitive matching"""
                if pd.isna(hub_name):
                    return None
                return str(hub_name).strip().upper()
            
            # Create mapping of normalized hub names to original South zone hub names
            hub_name_mapping = {}
            for south_hub in SOUTH_ZONE_HUBS:
                normalized = south_hub.upper()
                hub_name_mapping[normalized] = south_hub
            
            # Filter and normalize
            df['normalized_hub'] = df['Hub Name'].apply(normalize_hub_name)
            df_south_hubs = df[df['normalized_hub'].isin(hub_name_mapping.keys())]
            
            if len(df_south_hubs) > 0:
                hub_counts = df_south_hubs['normalized_hub'].value_counts().to_dict()
                hub_amounts = df_south_hubs.groupby('normalized_hub')['Value'].sum().to_dict()
                
                # Add CLM and State info for each hub, plus missing documentation metrics
                hub_details = {}
                for normalized_hub, count in hub_counts.items():
                    # Get original hub name from mapping
                    original_hub = hub_name_mapping.get(normalized_hub, normalized_hub)
                    # Try to get hub info
                    hub_info = HUB_INFO.get(original_hub)
                    if not hub_info:
                        # Try case-insensitive lookup
                        for key, value in HUB_INFO.items():
                            if key.upper() == normalized_hub:
                                hub_info = value
                                original_hub = key
                                break
                    
                    if hub_info:
                        clm, state, bbd = hub_info
                    else:
                        clm, state, bbd = ("Unknown", "Unknown", "N/A")
                    
                    # Calculate missing documentation metrics for this hub
                    hub_data = df_south_hubs[df_south_hubs['normalized_hub'] == normalized_hub]
                    
                    # Missing Ops Remarks
                    if 'Ops Remarks' in hub_data.columns:
                        missing_ops = hub_data[
                            (hub_data['Ops Remarks'].isna()) | 
                            (hub_data['Ops Remarks'].astype(str).str.strip() == '') |
                            (hub_data['Ops Remarks'].astype(str).str.strip().str.lower() == 'nan')
                        ]
                        missing_ops_count = len(missing_ops)
                    else:
                        missing_ops_count = 0
                    missing_ops_pct = round((missing_ops_count / count * 100) if count > 0 else 0)
                    
                    # Missing Image Proof
                    if 'Image Proof' in hub_data.columns:
                        missing_image = hub_data[
                            (hub_data['Image Proof'].isna()) | 
                            (hub_data['Image Proof'].astype(str).str.strip() == '') |
                            (hub_data['Image Proof'].astype(str).str.strip().str.lower() == 'nan')
                        ]
                        missing_image_count = len(missing_image)
                    else:
                        missing_image_count = 0
                    missing_image_pct = round((missing_image_count / count * 100) if count > 0 else 0)
                    
                    # Missing BOTH
                    if 'Ops Remarks' in hub_data.columns and 'Image Proof' in hub_data.columns:
                        missing_both = hub_data[
                            ((hub_data['Ops Remarks'].isna()) | 
                             (hub_data['Ops Remarks'].astype(str).str.strip() == '') |
                             (hub_data['Ops Remarks'].astype(str).str.strip().str.lower() == 'nan')) &
                            ((hub_data['Image Proof'].isna()) | 
                             (hub_data['Image Proof'].astype(str).str.strip() == '') |
                             (hub_data['Image Proof'].astype(str).str.strip().str.lower() == 'nan'))
                        ]
                        missing_both_count = len(missing_both)
                    elif 'Ops Remarks' in hub_data.columns:
                        # If only Ops Remarks exists, missing_both = missing_ops
                        missing_both_count = missing_ops_count
                    else:
                        missing_both_count = 0
                    missing_both_pct = round((missing_both_count / count * 100) if count > 0 else 0)
                    
                    # Calculate total value of records missing both Ops Remarks and Image Proof
                    # This is the Potential Debit amount (value that could be debited due to missing documentation)
                    if 'Ops Remarks' in hub_data.columns and 'Image Proof' in hub_data.columns:
                        missing_both_amount = missing_both['Value'].sum() if 'Value' in missing_both.columns and len(missing_both) > 0 else 0
                    elif 'Ops Remarks' in hub_data.columns:
                        # If only Ops Remarks exists, use missing_ops amount
                        missing_both_amount = missing_ops['Value'].sum() if 'Value' in missing_ops.columns and len(missing_ops) > 0 else 0
                    else:
                        missing_both_amount = 0
                    potential_debit = round(float(missing_both_amount))
                    
                    total_amount = round(float(hub_amounts.get(normalized_hub, 0)))
                    
                    hub_details[normalized_hub] = {
                        'count': int(count),
                        'amount': int(total_amount),
                        'clm': clm,
                        'state': state,
                        'bbd_aop': bbd,
                        'original_hub_name': original_hub,
                        'missing_ops_remarks_count': int(missing_ops_count),
                        'missing_ops_remarks_pct': int(missing_ops_pct),
                        'missing_image_proof_count': int(missing_image_count),
                        'missing_image_proof_pct': int(missing_image_pct),
                        'missing_both_count': int(missing_both_count),
                        'missing_both_pct': int(missing_both_pct),
                        'missing_both_amount': int(missing_both_amount),
                        'potential_debit': int(potential_debit)
                    }
                
                analysis['by_south_zone_hub'] = hub_details
                
                # Clean up temporary column
                if 'normalized_hub' in df.columns:
                    df.drop('normalized_hub', axis=1, inplace=True)
        
        logging.info("Q2 DN data analysis completed")
        return analysis
    
    # ============================================================================
    # OUTPUT FUNCTIONS
    # ============================================================================
    
    def push_results_to_sheet(self, sheet_id: str, worksheet_name: str, all_analyses: Dict, all_dataframes: Dict):
        """
        Push analysis results to Google Sheet - creates separate tables for each worksheet
        """
        try:
            logging.info(f"Pushing results to Google Sheet: {sheet_id}")
            
            spreadsheet = self.sheets_client.open_by_key(sheet_id)
            
            # Get or create worksheet with fixed name
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                worksheet.clear()  # Clear existing data
                logging.info(f"Found existing worksheet: {worksheet_name} - cleared for new data")
            except gspread.exceptions.WorksheetNotFound:
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                logging.info(f"Created new worksheet: {worksheet_name}")
            
            # Get all worksheet names from the dictionaries (sorted to match processing order)
            all_worksheet_names = list(all_analyses.keys()) if all_analyses else list(all_dataframes.keys())
            
            # Prepare summary data
            summary_data = []
            
            # Calculate metrics for each worksheet
            worksheet_summary = {}
            total_cases_all = 0
            total_missing_ops_all = 0
            total_potential_debit_all = 0
            total_fk_fraud_rvp_all = 0
            total_fk_fraud_rto_all = 0
            total_myntra_q2_rto_all = 0
            total_myntra_q2_rvp_all = 0
            
            for ws_name in all_worksheet_names:
                worksheet_cases = 0
                worksheet_missing_ops = 0
                worksheet_debit = 0
                worksheet_fk_fraud_rvp = 0
                worksheet_fk_fraud_rto = 0
                worksheet_myntra_q2_rto = 0
                worksheet_myntra_q2_rvp = 0
                
                # Calculate loss type counts and total cases from dataframe (all from same source)
                if ws_name in all_dataframes:
                    df = all_dataframes[ws_name]
                    
                    # Calculate total cases from dataframe (South Zone filtered)
                    worksheet_cases = len(df)
                    
                    # Calculate missing ops and debit from hub-wise analysis
                    if ws_name in all_analyses and 'by_south_zone_hub' in all_analyses[ws_name]:
                        for hub, details in all_analyses[ws_name]['by_south_zone_hub'].items():
                            worksheet_missing_ops += details.get('missing_ops_remarks_count', 0)
                            worksheet_debit += details.get('potential_debit', 0)
                    
                    # Calculate loss type counts from dataframe
                    if 'Loss Type' in df.columns:
                        # Count FK FRAUD RVP
                        fk_fraud_rvp_df = df[df['Loss Type'].astype(str).str.strip().str.upper() == 'FK FRAUD RVP']
                        worksheet_fk_fraud_rvp = len(fk_fraud_rvp_df)
                        
                        # Count FK FRAUD RTO
                        fk_fraud_rto_df = df[df['Loss Type'].astype(str).str.strip().str.upper() == 'FK FRAUD RTO']
                        worksheet_fk_fraud_rto = len(fk_fraud_rto_df)
                        
                        # Count Myntra Q2 RTO
                        myntra_q2_rto_df = df[df['Loss Type'].astype(str).str.strip().str.upper() == 'MYNTRA Q2 RTO']
                        worksheet_myntra_q2_rto = len(myntra_q2_rto_df)
                        
                        # Count Myntra Q2 RVP
                        myntra_q2_rvp_df = df[df['Loss Type'].astype(str).str.strip().str.upper() == 'MYNTRA Q2 RVP']
                        worksheet_myntra_q2_rvp = len(myntra_q2_rvp_df)
                else:
                    # Fallback to hub-wise calculation if dataframe not available
                    if ws_name in all_analyses and 'by_south_zone_hub' in all_analyses[ws_name]:
                        for hub, details in all_analyses[ws_name]['by_south_zone_hub'].items():
                            worksheet_cases += details.get('count', 0)
                            worksheet_missing_ops += details.get('missing_ops_remarks_count', 0)
                            worksheet_debit += details.get('potential_debit', 0)
                
                worksheet_summary[ws_name] = {
                    'cases': worksheet_cases,
                    'missing_ops': worksheet_missing_ops,
                    'potential_debit': worksheet_debit,
                    'fk_fraud_rvp': worksheet_fk_fraud_rvp,
                    'fk_fraud_rto': worksheet_fk_fraud_rto,
                    'myntra_q2_rto': worksheet_myntra_q2_rto,
                    'myntra_q2_rvp': worksheet_myntra_q2_rvp
                }
                total_cases_all += worksheet_cases
                total_missing_ops_all += worksheet_missing_ops
                total_potential_debit_all += worksheet_debit
                total_fk_fraud_rvp_all += worksheet_fk_fraud_rvp
                total_fk_fraud_rto_all += worksheet_fk_fraud_rto
                total_myntra_q2_rto_all += worksheet_myntra_q2_rto
                total_myntra_q2_rvp_all += worksheet_myntra_q2_rvp
            
            # Add summary at the top
            summary_data.append(["POTENTIAL DEBIT SUMMARY"])
            summary_data.append(["Worksheet", "Total Cases", "Missing Ops Remarks", "Potential Debit (₹)"])
            for ws_name in all_worksheet_names:
                if ws_name in worksheet_summary:
                    summary_data.append([
                        ws_name,
                        str(int(worksheet_summary[ws_name]['cases'])),
                        str(int(worksheet_summary[ws_name]['missing_ops'])),
                        str(int(round(worksheet_summary[ws_name]['potential_debit'])))
                    ])
            summary_data.append([
                "TOTAL",
                str(int(total_cases_all)),
                str(int(total_missing_ops_all)),
                str(int(round(total_potential_debit_all)))
            ])
            summary_data.append([])
            summary_data.append([])
            
            # Process each worksheet and create separate tables
            for ws_name in all_worksheet_names:
                if ws_name not in all_analyses:
                    continue
                
                analysis = all_analyses[ws_name]
                
                # South Zone Hub-wise Analysis for this worksheet
                if 'by_south_zone_hub' in analysis:
                    summary_data.append([f"SOUTH ZONE HUB-WISE Q2 DN ANALYSIS - {ws_name}"])
                    summary_data.append([
                        "Hub Name", "CLM", "State", "Cases", "Total Amount (₹)",
                        "Missing Ops Remarks", "Missing Ops Remarks %", 
                        "Missing Image Proof", "Missing Image Proof %",
                        "Missing BOTH", "Missing BOTH %", "Potential Debit (₹)"
                    ])
                    # Sort by amount descending
                    sorted_hubs = sorted(
                        analysis['by_south_zone_hub'].items(),
                        key=lambda x: x[1]['amount'],
                        reverse=True
                    )
                    total_count = 0
                    total_amount = 0
                    total_missing_ops = 0
                    total_missing_image = 0
                    total_missing_both = 0
                    total_missing_both_amount = 0
                    total_potential_debit = 0
                    for hub, details in sorted_hubs:
                        # Use original hub name if available, otherwise use normalized
                        hub_display = details.get('original_hub_name', hub)
                        summary_data.append([
                            hub_display,
                            details['clm'],
                            details['state'],
                            str(int(details['count'])),
                            str(int(details['amount'])),
                            str(int(details.get('missing_ops_remarks_count', 0))),
                            int(details.get('missing_ops_remarks_pct', 0)) / 100,  # Store as decimal for percentage format
                            str(int(details.get('missing_image_proof_count', 0))),
                            int(details.get('missing_image_proof_pct', 0)) / 100,  # Store as decimal for percentage format
                            str(int(details.get('missing_both_count', 0))),
                            int(details.get('missing_both_pct', 0)) / 100,  # Store as decimal for percentage format
                            str(int(details.get('potential_debit', 0)))
                        ])
                        total_count += details['count']
                        total_amount += details['amount']
                        total_missing_ops += details.get('missing_ops_remarks_count', 0)
                        total_missing_image += details.get('missing_image_proof_count', 0)
                        total_missing_both += details.get('missing_both_count', 0)
                        total_missing_both_amount += details.get('missing_both_amount', 0)
                        total_potential_debit += details.get('potential_debit', 0)
                    # Add total row
                    total_missing_ops_pct = round((total_missing_ops / total_count * 100) if total_count > 0 else 0)
                    total_missing_image_pct = round((total_missing_image / total_count * 100) if total_count > 0 else 0)
                    total_missing_both_pct = round((total_missing_both / total_count * 100) if total_count > 0 else 0)
                    summary_data.append([
                        "TOTAL",
                        "",
                        "",
                        str(int(total_count)),
                        str(int(round(total_amount))),
                        str(int(total_missing_ops)),
                        total_missing_ops_pct / 100,  # Store as decimal for percentage format
                        str(int(total_missing_image)),
                        total_missing_image_pct / 100,  # Store as decimal for percentage format
                        str(int(total_missing_both)),
                        total_missing_both_pct / 100,  # Store as decimal for percentage format
                        str(int(round(total_potential_debit)))
                    ])
                    summary_data.append([])
                    summary_data.append([])  # Extra blank line between worksheets
                    summary_data.append([])  # Extra blank line between worksheets
            
            # Collect top 100 high value shipments with missing Ops Remarks
            top_missing_ops = []
            for ws_name in all_worksheet_names:
                if ws_name in all_dataframes:
                    df = all_dataframes[ws_name]
                    
                    # Filter for records with missing Ops Remarks
                    # Check if Ops Remarks column exists
                    if 'Ops Remarks' in df.columns:
                        missing_ops_df = df[
                            (df['Ops Remarks'].isna()) | 
                            (df['Ops Remarks'].astype(str).str.strip() == '') |
                            (df['Ops Remarks'].astype(str).str.strip().str.lower() == 'nan')
                        ].copy()
                        
                        # Add worksheet name to each record
                        if len(missing_ops_df) > 0:
                            missing_ops_df['Worksheet'] = ws_name
                            missing_ops_df['Ops_Remarks_Status'] = False  # False = not updated
                            top_missing_ops.append(missing_ops_df)
            
            # Combine all missing Ops Remarks records
            if top_missing_ops:
                combined_missing_ops = pd.concat(top_missing_ops, ignore_index=True)
                
                # Sort by Value descending and take top 100
                if 'Value' in combined_missing_ops.columns:
                    combined_missing_ops = combined_missing_ops.sort_values('Value', ascending=False).head(100)
                    
                    # Prepare table data
                    summary_data.append([])
                    summary_data.append(["TOP 100 HIGH VALUE SHIPMENTS - MISSING OPS REMARKS"])
                    summary_data.append([
                        "S.No", "Tracking ID", "Hub Name", "Value (₹)", "Worksheet", "Ops Remarks Status", "Missing Image Proof"
                    ])
                    
                    # Add rows
                    for sno, (idx, row) in enumerate(combined_missing_ops.iterrows(), start=1):
                        tracking_id = str(row.get('TrackingID', '')) if 'TrackingID' in row else ''
                        # Handle NaN values - replace "nan" with empty string
                        if pd.isna(row.get('TrackingID', '')) or str(tracking_id).strip().lower() == 'nan':
                            tracking_id = ''
                        else:
                            tracking_id = str(tracking_id).strip()
                        hub_name = str(row.get('Hub Name', '')).lower() if 'Hub Name' in row and pd.notna(row.get('Hub Name', '')) else ''
                        value = round(float(row.get('Value', 0))) if pd.notna(row.get('Value')) else 0
                        worksheet_name = str(row.get('Worksheet', ''))
                        ops_status = "False" if row.get('Ops_Remarks_Status', False) == False else "True"
                        
                        # Check Image Proof status
                        image_proof = row.get('Image Proof', '')
                        if pd.isna(image_proof) or str(image_proof).strip() == '' or str(image_proof).strip().lower() == 'nan':
                            image_proof_status = "False"  # False = missing/blank
                        else:
                            image_proof_status = "True"  # True = updated/has value
                        
                        summary_data.append([
                            str(sno),  # S.No
                            tracking_id,
                            hub_name,
                            str(value),
                            worksheet_name,
                            ops_status,
                            image_proof_status
                        ])
            
            # Add timestamp
            summary_data.append([])
            summary_data.append(["Report Generated", datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
            summary_data.append(["Analysis Scope", "South Zone Hubs Only"])
            
            # Update worksheet
            time.sleep(1)  # Rate limiting
            worksheet.update(values=summary_data, range_name='A1', value_input_option='USER_ENTERED')
            
            # Format numeric columns to display as whole numbers (no decimals)
            try:
                num_rows = len(summary_data)
                num_columns = max(len(row) for row in summary_data) if summary_data else 0
                
                if num_rows > 0 and num_columns > 0:
                    format_requests = []
                    
                    # Find where summary table ends
                    # Summary table: row 0 (title), row 1 (header), rows 2-N (worksheets + TOTAL), rows N+1-N+2 (blank)
                    summary_table_end_row = 2 + len(all_worksheet_names) + 1 + 2  # header + worksheets + TOTAL + 2 blanks
                    
                    # Format summary table numeric columns (columns 1-3) as whole numbers
                    # Summary table columns: 0=Worksheet, 1=Total Cases, 2=Missing Ops Remarks, 3=Potential Debit
                    summary_numeric_columns = [1, 2, 3]  # 0-indexed
                    for col_idx in summary_numeric_columns:
                        if col_idx < num_columns:
                            format_requests.append({
                                "repeatCell": {
                                    "range": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": 1,  # Start from header row
                                        "endRowIndex": summary_table_end_row,
                                        "startColumnIndex": col_idx,
                                        "endColumnIndex": col_idx + 1
                                    },
                                    "cell": {
                                        "userEnteredFormat": {
                                            "numberFormat": {
                                                "type": "NUMBER",
                                                "pattern": "0"
                                            }
                                        }
                                    },
                                    "fields": "userEnteredFormat.numberFormat"
                                }
                            })
                    
                    # Format hub-wise table numeric columns as whole numbers
                    # Hub-wise tables: columns 3, 4, 5, 7, 9, 11 (Cases, Total Amount, Missing Ops Remarks, 
                    # Missing Image Proof, Missing BOTH, Potential Debit)
                    hub_numeric_columns = [3, 4, 5, 7, 9, 11]  # 0-indexed
                    for col_idx in hub_numeric_columns:
                        if col_idx < num_columns:
                            format_requests.append({
                                "repeatCell": {
                                    "range": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": summary_table_end_row,
                                        "endRowIndex": num_rows,
                                        "startColumnIndex": col_idx,
                                        "endColumnIndex": col_idx + 1
                                    },
                                    "cell": {
                                        "userEnteredFormat": {
                                            "numberFormat": {
                                                "type": "NUMBER",
                                                "pattern": "0"
                                            }
                                        }
                                    },
                                    "fields": "userEnteredFormat.numberFormat"
                                }
                            })
                    
                    # Format percentage columns as percentage with 0 decimal places (only in hub-wise tables)
                    # Columns 6, 8, 10 in hub-wise tables are percentage columns
                    percentage_columns = [6, 8, 10]  # 0-indexed
                    for col_idx in percentage_columns:
                        if col_idx < num_columns:
                            format_requests.append({
                                "repeatCell": {
                                    "range": {
                                        "sheetId": worksheet.id,
                                        "startRowIndex": summary_table_end_row,
                                        "endRowIndex": num_rows,
                                        "startColumnIndex": col_idx,
                                        "endColumnIndex": col_idx + 1
                                    },
                                    "cell": {
                                        "userEnteredFormat": {
                                            "numberFormat": {
                                                "type": "PERCENT",
                                                "pattern": "0%"
                                            }
                                        }
                                    },
                                    "fields": "userEnteredFormat.numberFormat"
                                }
                            })
                    
                    # Format S.No column in TOP 100 table as text to prevent auto-linking
                    # Find the row where TOP 100 table starts
                    top100_table_start_row = None
                    for i, row in enumerate(summary_data):
                        if row and len(row) > 0 and isinstance(row[0], str) and "TOP 100 HIGH VALUE" in row[0]:
                            top100_table_start_row = i
                            break
                    
                    if top100_table_start_row is not None:
                        # Format column 0 (S.No) as text for the TOP 100 table
                        # The table starts at top100_table_start_row, header at top100_table_start_row+1, data starts at top100_table_start_row+2
                        format_requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "startRowIndex": top100_table_start_row + 1,  # Start from header
                                    "endRowIndex": num_rows,
                                    "startColumnIndex": 0,
                                    "endColumnIndex": 1
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "numberFormat": {
                                            "type": "TEXT"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.numberFormat"
                            }
                        })
                    
                    if format_requests:
                        spreadsheet.batch_update({"requests": format_requests})
                        logging.info("Formatted numeric and percentage columns")
            except Exception as e:
                logging.warning(f"Could not format numeric columns: {e}")
            
            # Merge header cells for all tables and adjust column widths
            try:
                num_columns = max(len(row) for row in summary_data) if summary_data else 0
                if num_columns > 0:
                    merge_requests = []
                    
                    # Find and merge all table header rows
                    for i, row in enumerate(summary_data):
                        if row and len(row) > 0 and isinstance(row[0], str):
                            # Check if this is a table header
                            if ("POTENTIAL DEBIT SUMMARY" in row[0] or 
                                "SOUTH ZONE HUB-WISE Q2 DN ANALYSIS" in row[0] or
                                "TOP 100 HIGH VALUE SHIPMENTS" in row[0]):
                                merge_requests.append({
                                    "mergeCells": {
                                        "range": {
                                            "sheetId": worksheet.id,
                                            "startRowIndex": i,
                                            "endRowIndex": i + 1,
                                            "startColumnIndex": 0,
                                            "endColumnIndex": num_columns
                                        },
                                        "mergeType": "MERGE_ALL"
                                    }
                                })
                    
                    # Set column widths
                    # Summary table: wider columns for readability
                    # Hub-wise tables: adjust based on content
                    column_width_requests = []
                    for col_idx in range(num_columns):
                        # Set default width (in pixels, approximately 100 pixels per character)
                        width = 120  # Default width
                        
                        # Adjust specific columns
                        if col_idx == 0:  # First column (Worksheet/Hub Name)
                            width = 200
                        elif col_idx == 1:  # CLM/Second column
                            width = 150
                        elif col_idx == 2:  # State/Third column
                            width = 120
                        elif col_idx == 3:  # Cases/Fourth column
                            width = 80
                        elif col_idx == 4:  # Amount/Fifth column
                            width = 120
                        elif col_idx == 5:  # Missing Ops Remarks/Sixth column
                            width = 120
                        elif col_idx == 6:  # Percentage/Seventh column
                            width = 100
                        elif col_idx == 7:  # Missing Image Proof/Eighth column
                            width = 120
                        elif col_idx == 8:  # Percentage/Ninth column
                            width = 100
                        elif col_idx == 9:  # Missing BOTH/Tenth column
                            width = 100
                        elif col_idx == 10:  # Percentage/Eleventh column
                            width = 100
                        elif col_idx == 11:  # Potential Debit/Twelfth column
                            width = 120
                        
                        column_width_requests.append({
                            "updateDimensionProperties": {
                                "range": {
                                    "sheetId": worksheet.id,
                                    "dimension": "COLUMNS",
                                    "startIndex": col_idx,
                                    "endIndex": col_idx + 1
                                },
                                "properties": {
                                    "pixelSize": width
                                },
                                "fields": "pixelSize"
                            }
                        })
                    
                    # Execute merge and column width requests
                    all_requests = merge_requests + column_width_requests
                    if all_requests:
                        spreadsheet.batch_update({"requests": all_requests})
                        logging.info("Merged table headers and adjusted column widths")
            except Exception as e:
                logging.warning(f"Could not merge headers or adjust column widths: {e}")
            
            logging.info("Successfully pushed analysis results to Google Sheet")
            
        except Exception as e:
            logging.error(f"Error pushing results to sheet: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
    
    # ============================================================================
    # MAIN EXECUTION
    # ============================================================================
    
    def run(self):
        """
        Main execution function - processes multiple worksheets
        """
        try:
            logging.info("="*80)
            logging.info("FLIPKART & MYNTRA Q2 DN DATA ANALYSIS - SOUTH ZONE")
            logging.info("="*80)
            
            all_analyses = {}
            all_dataframes = {}
            
            # Process each sheet and its worksheets
            for sheet_config in SHEET_CONFIGS:
                sheet_id = sheet_config['sheet_id']
                worksheets = sheet_config['worksheets']
                
                for worksheet_name in worksheets:
                    logging.info(f"\n{'='*80}")
                    logging.info(f"Processing sheet: {sheet_id}")
                    logging.info(f"Processing worksheet: {worksheet_name}")
                    logging.info(f"{'='*80}")
                    
                    # Pull data from sheet
                    df = self.pull_data_from_sheet(sheet_id, worksheet_name)
                    
                    if df.empty:
                        logging.warning(f"No data found in worksheet: {worksheet_name}. Skipping.")
                        continue
                    
                    # Clean data
                    df_clean = self.clean_data(df)
                    
                    # Filter for South zone hubs only
                    df_south = self.filter_south_zone_hubs(df_clean)
                    
                    if df_south.empty:
                        logging.warning(f"No South zone hub data found in worksheet: {worksheet_name}. Skipping.")
                        continue
                    
                    # Analyze data (using South zone filtered data)
                    analysis = self.analyze_data(df_south)
                    
                    # Store analysis and dataframe for this worksheet (use worksheet name as key)
                    all_analyses[worksheet_name] = analysis
                    all_dataframes[worksheet_name] = df_south
            
            if not all_analyses:
                logging.error("No data found in any worksheet. Exiting.")
                return
            
            # Prepare TOP 100 data for email (before pushing to sheet)
            combined_missing_ops = None
            top_missing_ops = []
            # Get all worksheet names from all sheet configs
            all_worksheet_names = []
            for sheet_config in SHEET_CONFIGS:
                all_worksheet_names.extend(sheet_config['worksheets'])
            
            for ws_name in all_worksheet_names:
                if ws_name in all_dataframes:
                    df = all_dataframes[ws_name]
                    if 'Ops Remarks' in df.columns:
                        missing_ops_df = df[
                            (df['Ops Remarks'].isna()) | 
                            (df['Ops Remarks'].astype(str).str.strip() == '') |
                            (df['Ops Remarks'].astype(str).str.strip().str.lower() == 'nan')
                        ].copy()
                        if len(missing_ops_df) > 0:
                            missing_ops_df['Worksheet'] = ws_name
                            missing_ops_df['Ops_Remarks_Status'] = False
                            top_missing_ops.append(missing_ops_df)
            
            if top_missing_ops:
                combined_missing_ops = pd.concat(top_missing_ops, ignore_index=True)
                if 'Value' in combined_missing_ops.columns:
                    combined_missing_ops = combined_missing_ops.sort_values('Value', ascending=False).head(25)
            
            # Push results to output sheet (all worksheets combined)
            logging.info(f"\nPushing results to output sheet: {OUTPUT_SHEET_ID}")
            self.push_results_to_sheet(OUTPUT_SHEET_ID, OUTPUT_WORKSHEET_NAME, all_analyses, all_dataframes)
            
            # Send email with TOP 25 high value shipments
            logging.info(f"\nSending email with TOP 25 high value shipments...")
            try:
                if combined_missing_ops is not None and not combined_missing_ops.empty:
                    # Add CLM Name to the dataframe for grouping
                    combined_missing_ops_with_clm = combined_missing_ops.copy()
                    combined_missing_ops_with_clm['CLM Name'] = combined_missing_ops_with_clm['Hub Name'].apply(
                        lambda hub: self.get_clm_for_hub(hub)
                    )
                    
                    # Send CLM-level emails
                    self.send_clm_level_emails(combined_missing_ops_with_clm)
                    
                    # Send consolidated email
                    self.send_top50_email(combined_missing_ops, all_analyses, all_dataframes)
                else:
                    logging.warning("No TOP 25 data found - skipping email")
            except Exception as e:
                logging.warning(f"Error sending email: {e}")
            
            # Send dashboard email with consolidated data
            logging.info(f"\nSending dashboard email with consolidated data...")
            try:
                if all_analyses and all_dataframes:
                    self.send_dashboard_email(all_analyses, all_dataframes)
                else:
                    logging.warning("No analysis data found - skipping dashboard email")
            except Exception as e:
                logging.warning(f"Error sending dashboard email: {e}")
            except Exception as e:
                logging.warning(f"Error sending email: {e}")
            
            logging.info("Analysis completed successfully!")
            
        except Exception as e:
            logging.error(f"Error in main execution: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
            raise
    
    def get_clm_for_hub(self, hub_name: str) -> str:
        """Get CLM name for a given hub name"""
        if pd.isna(hub_name):
            return "Unknown"
        
        hub_name_str = str(hub_name).strip()
        
        # Try exact match first
        hub_info = HUB_INFO.get(hub_name_str)
        if hub_info:
            return hub_info[0]  # CLM is first element
        
        # Try case-insensitive match
        for key, value in HUB_INFO.items():
            if key.upper() == hub_name_str.upper():
                return value[0]
        
        return "Unknown"
    
    def send_clm_level_emails(self, top100_df: pd.DataFrame):
        """Send individual emails to CLMs for their respective hub shipments"""
        try:
            if top100_df.empty:
                logging.warning("No TOP 100 data found - skipping CLM-level email notifications")
                return True
            
            # Check if CLM Name column exists
            if 'CLM Name' not in top100_df.columns:
                logging.warning("CLM Name column not found - skipping CLM-level emails")
                return False
            
            # Group data by CLM
            clm_groups = top100_df.groupby('CLM Name')
            
            # CC recipients
            cc_recipients = ['lokeshh@loadshare.net', 'bharath.s@loadshare.net', 'arunraj@loadshare.net', 'maligai.rasmeen@loadshare.net']
            
            # Send email to each CLM
            for clm_name, clm_data in clm_groups:
                try:
                    # Handle comma-separated CLM names (e.g., "Asif, Haseem")
                    clm_names_list = [name.strip() for name in str(clm_name).split(',')]
                    
                    # Get CLM emails from mapping for each CLM name
                    clm_emails = []
                    for individual_clm_name in clm_names_list:
                        clm_email = CLM_EMAIL.get(individual_clm_name, '')
                        if clm_email:
                            clm_emails.append(clm_email)
                        else:
                            logging.warning(f"No email found for CLM: {individual_clm_name} - skipping")
                    
                    if not clm_emails:
                        logging.warning(f"No valid email found for CLM(s): {clm_name} - skipping")
                        continue
                    
                    # Get unique hubs for this CLM
                    clm_hubs = clm_data['Hub Name'].unique().tolist()
                    hub_list = ', '.join([str(h) for h in clm_hubs if pd.notna(h)])
                    
                    # Get hub emails for this CLM's hubs
                    hub_emails = []
                    for hub_name in clm_hubs:
                        if pd.isna(hub_name):
                            continue
                        hub_name_str = str(hub_name).strip()
                        # Try exact match first
                        hub_email = HUB_EMAIL.get(hub_name_str, '')
                        # Try case-insensitive match if exact match fails
                        if not hub_email:
                            for key, email in HUB_EMAIL.items():
                                if key.upper() == hub_name_str.upper():
                                    hub_email = email
                                    break
                        if hub_email:
                            hub_emails.append(hub_email)
                        else:
                            logging.warning(f"No email found for hub: {hub_name} - skipping")
                    
                    # Combine CLM emails and hub emails for TO recipients
                    to_recipients = clm_emails + hub_emails
                    
                    # Calculate totals for this CLM
                    clm_records = len(clm_data)
                    clm_total_value = clm_data['Value'].sum() if 'Value' in clm_data.columns else 0
                    clm_missing_ops = len(clm_data[clm_data.get('Ops_Remarks_Status', pd.Series([False]*len(clm_data))) == False])
                    clm_missing_image = len(clm_data[
                        (clm_data.get('Image Proof', pd.Series(['']*len(clm_data))).isna()) |
                        (clm_data.get('Image Proof', pd.Series(['']*len(clm_data))).astype(str).str.strip() == '')
                    ])
                    
                    # Create message for this CLM
                    msg = MIMEMultipart()
                    msg['From'] = EMAIL_CONFIG['sender_email']
                    msg['To'] = ', '.join(to_recipients)
                    msg['Cc'] = ', '.join(cc_recipients)
                    # Format date for subject (e.g., "01 Dec 2025")
                    current_date = datetime.now().strftime('%d %b %Y')
                    msg['Subject'] = f"DEC - Debit Note Alert - Missing Proof/Remarks - {current_date}"
                    
                    # Create HTML email body
                    html_body = f"""
                    <html>
                    <head>
                        <style>
                            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; font-size: 11px; }}
                            .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                            .header h2 {{ font-size: 16px; font-weight: bold; margin: 0 0 10px 0; }}
                            .header p {{ font-size: 11px; margin: 5px 0; }}
                            .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                            .summary h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                            .summary p {{ font-size: 11px; margin: 5px 0; }}
                            .warning {{ background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                            .warning h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                            .warning p {{ font-size: 11px; margin: 5px 0; }}
                            .clm-info {{ background-color: #e3f2fd; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                            .clm-info h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                            .clm-info p {{ font-size: 11px; margin: 5px 0; }}
                            .clm-info ul {{ font-size: 11px; margin: 5px 0; }}
                            table {{ border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 11px; }}
                            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                            th {{ background-color: #4CAF50; color: white; font-size: 10px; font-weight: 600; }}
                            td {{ font-size: 11px; }}
                            .status-false {{ color: red; font-weight: bold; }}
                            .status-true {{ color: green; font-weight: bold; }}
                        </style>
                    </head>
                    <body>
                        <div class="header">
                            <h2>Debit Note Alert - Missing Proof - High Value - {clm_name}</h2>
                            <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                            <p><strong>CLM:</strong> {clm_name}</p>
                            <p><strong>Hubs:</strong> {hub_list}</p>
                        </div>
                        
                        <div class="clm-info">
                            <h3>Your Hub Summary</h3>
                            <p><strong>Total Records:</strong> {clm_records}</p>
                            <p><strong>Total Value:</strong> ₹{clm_total_value:,.0f}</p>
                            <p><strong>Missing Ops Remarks:</strong> {clm_missing_ops}</p>
                            <p><strong>Missing Image Proof:</strong> {clm_missing_image}</p>
                            <p style="margin-top: 10px;"><strong>View Full Reports:</strong></p>
                            <ul style="margin-top: 5px;">
                                <li><a href="https://docs.google.com/spreadsheets/d/1vEXO1TGn2S9gJ8kzSkCO9M-eiZoyCYFDwjMmVskkERo/edit" style="color: #1a73e8; text-decoration: none;">IMD Myntra Master Tracker</a></li>
                                <li><a href="https://docs.google.com/spreadsheets/d/1FFa2Vp5QB8Hx7klp6vGD-hcwj9a3OnbQ0JBKBK2Fa4c/edit" style="color: #1a73e8; text-decoration: none;">BRSNR Data</a></li>
                            </ul>
                        </div>
                        
                        <div class="warning">
                            <h3>High Value Shipments - Missing Ops Remarks - Action Required</h3>
                            <table>
                                <tr>
                                    <th>S.No</th>
                                    <th>Tracking ID</th>
                                    <th>Hub Name</th>
                                    <th>Value (₹)</th>
                                    <th>Worksheet</th>
                                    <th>Ops Remarks Status</th>
                                    <th>Missing Image Proof</th>
                                </tr>
                    """
                    
                    # Add rows for this CLM
                    for sno, (idx, row) in enumerate(clm_data.iterrows(), start=1):
                        tracking_id = str(row.get('TrackingID', '')) if 'TrackingID' in row else ''
                        # Handle NaN values - replace "nan" with empty string
                        if pd.isna(row.get('TrackingID', '')) or str(tracking_id).strip().lower() == 'nan':
                            tracking_id = ''
                        else:
                            tracking_id = str(tracking_id).strip()
                        hub_name = str(row.get('Hub Name', '')).lower() if 'Hub Name' in row and pd.notna(row.get('Hub Name', '')) else ''
                        value = round(float(row.get('Value', 0))) if pd.notna(row.get('Value')) else 0
                        worksheet_name = str(row.get('Worksheet', ''))
                        ops_status = "False" if row.get('Ops_Remarks_Status', False) == False else "True"
                        
                        # Check Image Proof status
                        image_proof = row.get('Image Proof', '')
                        if pd.isna(image_proof) or str(image_proof).strip() == '' or str(image_proof).strip().lower() == 'nan':
                            image_proof_status = "False"
                        else:
                            image_proof_status = "True"
                        
                        ops_class = "status-false" if ops_status == "False" else "status-true"
                        image_class = "status-false" if image_proof_status == "False" else "status-true"
                        
                        html_body += f"""
                                <tr>
                                    <td>{sno}</td>
                                    <td><strong>{tracking_id}</strong></td>
                                    <td>{hub_name}</td>
                                    <td>₹{value:,.0f}</td>
                                    <td>{worksheet_name}</td>
                                    <td class="{ops_class}">{ops_status}</td>
                                    <td class="{image_class}">{image_proof_status}</td>
                                </tr>
                        """
                    
                    html_body += f"""
                            </table>
                        </div>
                        
                        <div class="warning">
                            <p><strong>Action Required:</strong> These are high-value shipments with missing Ops Remarks in your hubs that require immediate attention.</p>
                            <p><strong>Priority:</strong> Please prioritize shipments with higher values.</p>
                            <p><strong>Note:</strong> This report is automatically generated from the Q2 DN Analysis system.</p>
                        </div>
                    </body>
                    </html>
                    """
                    
                    # Attach HTML body to email
                    msg.attach(MIMEText(html_body, 'html'))
                    
                    # Send email
                    server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
                    server.starttls()
                    server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
                    
                    # Send to CLM, hub emails, and CC recipients
                    all_recipients = to_recipients + cc_recipients
                    text = msg.as_string()
                    server.sendmail(EMAIL_CONFIG['sender_email'], all_recipients, text)
                    server.quit()
                    
                    logging.info(f"CLM-level email sent to {clm_name} ({', '.join(clm_emails)}) and hubs for {clm_records} records")
                    
                except Exception as e:
                    logging.error(f"Failed to send email to {clm_name}: {e}")
                    import traceback
                    logging.error(f"Full error traceback:\n{traceback.format_exc()}")
                    continue
            
            logging.info(f"CLM-level email notifications completed for {len(clm_groups)} CLMs")
            return True
            
        except Exception as e:
            logging.error(f"Failed to send CLM-level emails: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
            return False
    
    def send_top50_email(self, top50_df: pd.DataFrame, all_analyses: Dict = None, all_dataframes: Dict = None):
        """Send email with TOP 25 high value shipments missing Ops Remarks"""
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = EMAIL_CONFIG['recipient_email']
            msg['Cc'] = 'arunraj@loadshare.net, rakib@loadshare.net, maligai.rasmeen@loadshare.net'
            # Format date for subject (e.g., "01 Dec 2025")
            current_date = datetime.now().strftime('%d %b %Y')
            msg['Subject'] = f"South - Debit Note Alert - Dashboard - {current_date}"
            
            # Calculate summary
            total_records = len(top50_df)
            total_value = top50_df['Value'].sum() if 'Value' in top50_df.columns else 0
            total_missing_ops = len(top50_df[top50_df.get('Ops_Remarks_Status', pd.Series([False]*len(top50_df))) == False])
            total_missing_image = len(top50_df[
                (top50_df.get('Image Proof', pd.Series(['']*len(top50_df))).isna()) |
                (top50_df.get('Image Proof', pd.Series(['']*len(top50_df))).astype(str).str.strip() == '')
            ])
            
            # Calculate POTENTIAL DEBIT SUMMARY
            worksheet_summary_html = ""
            if all_analyses and all_dataframes:
                # Get all worksheet names
                all_worksheet_names = list(all_analyses.keys()) if all_analyses else list(all_dataframes.keys())
                
                total_cases_all = 0
                total_missing_ops_all = 0
                total_potential_debit_all = 0
                
                worksheet_summary_html = '<div class="summary"><h3>POTENTIAL DEBIT SUMMARY</h3><table><tr><th>Worksheet</th><th>Total Cases</th><th>Missing Ops Remarks</th><th>Potential Debit (₹)</th></tr>'
                
                for ws_name in all_worksheet_names:
                    worksheet_cases = 0
                    worksheet_missing_ops = 0
                    worksheet_debit = 0
                    
                    if ws_name in all_dataframes:
                        df = all_dataframes[ws_name]
                        worksheet_cases = len(df)
                        
                        if ws_name in all_analyses and 'by_south_zone_hub' in all_analyses[ws_name]:
                            for hub, details in all_analyses[ws_name]['by_south_zone_hub'].items():
                                worksheet_missing_ops += details.get('missing_ops_remarks_count', 0)
                                worksheet_debit += details.get('potential_debit', 0)
                    
                    total_cases_all += worksheet_cases
                    total_missing_ops_all += worksheet_missing_ops
                    total_potential_debit_all += worksheet_debit
                    
                    worksheet_summary_html += f'<tr><td>{ws_name}</td><td>{int(worksheet_cases)}</td><td>{int(worksheet_missing_ops)}</td><td>₹{int(round(worksheet_debit)):,}</td></tr>'
                
                worksheet_summary_html += f'<tr style="font-weight: bold;"><td>TOTAL</td><td>{int(total_cases_all)}</td><td>{int(total_missing_ops_all)}</td><td>₹{int(round(total_potential_debit_all)):,}</td></tr>'
                worksheet_summary_html += '</table></div>'
            
            # Create HTML email body
            html_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; font-size: 11px; }}
                    .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                    .header h2 {{ font-size: 16px; font-weight: bold; margin: 0 0 10px 0; }}
                    .header p {{ font-size: 11px; margin: 5px 0; }}
                    .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                    .summary h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                    .summary p {{ font-size: 11px; margin: 5px 0; }}
                    .warning {{ background-color: #fff3cd; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                    .warning h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                    .warning p {{ font-size: 11px; margin: 5px 0; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 11px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #4CAF50; color: white; font-size: 10px; font-weight: 600; }}
                    td {{ font-size: 11px; }}
                    .high-value {{ background-color: #ffebee; }}
                    .medium-value {{ background-color: #fff3e0; }}
                    .status-false {{ color: red; font-weight: bold; }}
                    .status-true {{ color: green; font-weight: bold; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>Q2 DN - TOP 25 High Value Shipments Missing Ops Remarks</h2>
                    <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                </div>
                
                {worksheet_summary_html}
                
                <div class="summary">
                    <h3>Summary</h3>
                    <p><strong>Total Records:</strong> {total_records}</p>
                    <p><strong>Total Value:</strong> ₹{total_value:,.0f}</p>
                    <p><strong>Missing Ops Remarks:</strong> {total_missing_ops}</p>
                    <p><strong>Missing Image Proof:</strong> {total_missing_image}</p>
                </div>
                
                <div class="warning">
                    <h3>TOP 25 High Value Shipments - Missing Ops Remarks</h3>
                    <table>
                        <tr>
                            <th>S.No</th>
                            <th>Tracking ID</th>
                            <th>Hub Name</th>
                            <th>Value (₹)</th>
                            <th>Worksheet</th>
                            <th>Ops Remarks Status</th>
                            <th>Missing Image Proof</th>
                        </tr>
            """
            
            # Add rows
            for sno, (idx, row) in enumerate(top50_df.iterrows(), start=1):
                tracking_id = str(row.get('TrackingID', '')) if 'TrackingID' in row else ''
                # Handle NaN values - replace "nan" with empty string
                if pd.isna(row.get('TrackingID', '')) or str(tracking_id).strip().lower() == 'nan':
                    tracking_id = ''
                else:
                    tracking_id = str(tracking_id).strip()
                hub_name = str(row.get('Hub Name', '')).lower() if 'Hub Name' in row and pd.notna(row.get('Hub Name', '')) else ''
                value = round(float(row.get('Value', 0))) if pd.notna(row.get('Value')) else 0
                worksheet_name = str(row.get('Worksheet', ''))
                ops_status = "False" if row.get('Ops_Remarks_Status', False) == False else "True"
                
                # Check Image Proof status
                image_proof = row.get('Image Proof', '')
                if pd.isna(image_proof) or str(image_proof).strip() == '' or str(image_proof).strip().lower() == 'nan':
                    image_proof_status = "False"
                else:
                    image_proof_status = "True"
                
                ops_class = "status-false" if ops_status == "False" else "status-true"
                image_class = "status-false" if image_proof_status == "False" else "status-true"
                
                html_body += f"""
                        <tr>
                            <td>{sno}</td>
                            <td><strong>{tracking_id}</strong></td>
                            <td>{hub_name}</td>
                            <td>₹{value:,.0f}</td>
                            <td>{worksheet_name}</td>
                            <td class="{ops_class}">{ops_status}</td>
                            <td class="{image_class}">{image_proof_status}</td>
                        </tr>
                """
            
            html_body += f"""
                    </table>
                </div>
                
                <div class="warning">
                    <p><strong>Action Required:</strong> These are high-value shipments with missing Ops Remarks that require immediate attention.</p>
                    <p><strong>Priority:</strong> Please prioritize shipments with higher values.</p>
                    <p><strong>Note:</strong> This report is automatically generated from the Q2 DN Analysis system.</p>
                </div>
                
                <div style="margin-top: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px;">
                    <p><strong>View Full Reports:</strong></p>
                    <ul style="margin-top: 5px;">
                        <li><a href="https://docs.google.com/spreadsheets/d/1vEXO1TGn2S9gJ8kzSkCO9M-eiZoyCYFDwjMmVskkERo/edit" style="color: #1a73e8; text-decoration: none;">IMD Myntra Master Tracker</a></li>
                        <li><a href="https://docs.google.com/spreadsheets/d/1FFa2Vp5QB8Hx7klp6vGD-hcwj9a3OnbQ0JBKBK2Fa4c/edit" style="color: #1a73e8; text-decoration: none;">BRSNR Data</a></li>
                    </ul>
                </div>
            </body>
            </html>
            """
            
            # Attach HTML body to email
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            recipients = [EMAIL_CONFIG['recipient_email'], 'arunraj@loadshare.net', 'rakib@loadshare.net', 'maligai.rasmeen@loadshare.net']
            text = msg.as_string()
            server.sendmail(EMAIL_CONFIG['sender_email'], recipients, text)
            server.quit()
            
            logging.info(f"Email sent successfully to {EMAIL_CONFIG['recipient_email']}")
            logging.info(f"Email CC'd to: arunraj@loadshare.net, rakib@loadshare.net, maligai.rasmeen@loadshare.net")
            
        except Exception as e:
            logging.error(f"Error sending email: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
            raise
    
    def send_dashboard_email(self, all_analyses: Dict, all_dataframes: Dict):
        """Send consolidated dashboard email with POTENTIAL DEBIT SUMMARY and Hub-wise analysis"""
        try:
            # Create message
            msg = MIMEMultipart()
            msg['From'] = EMAIL_CONFIG['sender_email']
            msg['To'] = EMAIL_CONFIG['recipient_email']
            msg['Cc'] = 'arunraj@loadshare.net, rakib@loadshare.net, maligai.rasmeen@loadshare.net'
            
            # Format date for subject (e.g., "01 Dec 2025")
            current_date = datetime.now().strftime('%d %b %Y')
            msg['Subject'] = f"South - Debit Note Alert - Dashboard - {current_date}"
            
            # Get all worksheet names
            all_worksheet_names = list(all_analyses.keys()) if all_analyses else list(all_dataframes.keys())
            
            # Calculate POTENTIAL DEBIT SUMMARY
            total_cases_all = 0
            total_missing_ops_all = 0
            total_potential_debit_all = 0
            
            worksheet_summary_html = '<div class="summary"><h3>POTENTIAL DEBIT SUMMARY</h3><table><tr><th>Worksheet</th><th>Total Cases</th><th>Missing Ops Remarks</th><th>Potential Debit (₹)</th></tr>'
            
            for ws_name in all_worksheet_names:
                worksheet_cases = 0
                worksheet_missing_ops = 0
                worksheet_debit = 0
                
                if ws_name in all_dataframes:
                    df = all_dataframes[ws_name]
                    worksheet_cases = len(df)
                    
                    if ws_name in all_analyses and 'by_south_zone_hub' in all_analyses[ws_name]:
                        for hub, details in all_analyses[ws_name]['by_south_zone_hub'].items():
                            worksheet_missing_ops += details.get('missing_ops_remarks_count', 0)
                            worksheet_debit += details.get('potential_debit', 0)
                
                total_cases_all += worksheet_cases
                total_missing_ops_all += worksheet_missing_ops
                total_potential_debit_all += worksheet_debit
                
                worksheet_summary_html += f'<tr><td>{ws_name}</td><td>{int(worksheet_cases)}</td><td>{int(worksheet_missing_ops)}</td><td>₹{int(round(worksheet_debit)):,}</td></tr>'
            
            worksheet_summary_html += f'<tr style="font-weight: bold;"><td>TOTAL</td><td>{int(total_cases_all)}</td><td>{int(total_missing_ops_all)}</td><td>₹{int(round(total_potential_debit_all)):,}</td></tr>'
            worksheet_summary_html += '</table></div>'
            
            # Calculate Hub-wise consolidated summary (across all worksheets)
            hub_consolidated = {}
            for ws_name in all_worksheet_names:
                if ws_name in all_analyses and 'by_south_zone_hub' in all_analyses[ws_name]:
                    for hub, details in all_analyses[ws_name]['by_south_zone_hub'].items():
                        original_hub = details.get('original_hub_name', hub)
                        if original_hub not in hub_consolidated:
                            hub_consolidated[original_hub] = {
                                'count': 0,
                                'amount': 0,
                                'missing_ops': 0,
                                'missing_image': 0,
                                'missing_both': 0,
                                'potential_debit': 0,
                                'clm': details.get('clm', 'Unknown'),
                                'state': details.get('state', 'Unknown')
                            }
                        hub_consolidated[original_hub]['count'] += details.get('count', 0)
                        hub_consolidated[original_hub]['amount'] += details.get('amount', 0)
                        hub_consolidated[original_hub]['missing_ops'] += details.get('missing_ops_remarks_count', 0)
                        hub_consolidated[original_hub]['missing_image'] += details.get('missing_image_proof_count', 0)
                        hub_consolidated[original_hub]['missing_both'] += details.get('missing_both_count', 0)
                        hub_consolidated[original_hub]['potential_debit'] += details.get('potential_debit', 0)
            
            # Create Hub-wise HTML table
            hub_wise_html = ""
            if hub_consolidated:
                hub_wise_html = '<div class="summary"><h3>SOUTH ZONE HUB-WISE CONSOLIDATED ANALYSIS</h3><table><tr><th>Hub Name</th><th>CLM</th><th>State</th><th>Cases</th><th>Total Amount (₹)</th><th>Missing Ops Remarks</th><th>Missing Image Proof</th><th>Missing BOTH</th><th>Potential Debit (₹)</th></tr>'
                
                # Sort by amount descending
                sorted_hubs = sorted(hub_consolidated.items(), key=lambda x: x[1]['amount'], reverse=True)
                
                for hub_name, details in sorted_hubs:
                    missing_ops_pct = round((details['missing_ops'] / details['count'] * 100) if details['count'] > 0 else 0, 1)
                    missing_image_pct = round((details['missing_image'] / details['count'] * 100) if details['count'] > 0 else 0, 1)
                    missing_both_pct = round((details['missing_both'] / details['count'] * 100) if details['count'] > 0 else 0, 1)
                    
                    hub_wise_html += f'<tr><td>{hub_name}</td><td>{details["clm"]}</td><td>{details["state"]}</td><td>{int(details["count"])}</td><td>₹{int(details["amount"]):,}</td><td>{int(details["missing_ops"])} ({missing_ops_pct}%)</td><td>{int(details["missing_image"])} ({missing_image_pct}%)</td><td>{int(details["missing_both"])} ({missing_both_pct}%)</td><td>₹{int(details["potential_debit"]):,}</td></tr>'
                
                # Calculate totals
                total_hub_count = sum(d['count'] for d in hub_consolidated.values())
                total_hub_amount = sum(d['amount'] for d in hub_consolidated.values())
                total_hub_missing_ops = sum(d['missing_ops'] for d in hub_consolidated.values())
                total_hub_missing_image = sum(d['missing_image'] for d in hub_consolidated.values())
                total_hub_missing_both = sum(d['missing_both'] for d in hub_consolidated.values())
                total_hub_potential_debit = sum(d['potential_debit'] for d in hub_consolidated.values())
                
                total_missing_ops_pct = round((total_hub_missing_ops / total_hub_count * 100) if total_hub_count > 0 else 0, 1)
                total_missing_image_pct = round((total_hub_missing_image / total_hub_count * 100) if total_hub_count > 0 else 0, 1)
                total_missing_both_pct = round((total_hub_missing_both / total_hub_count * 100) if total_hub_count > 0 else 0, 1)
                
                hub_wise_html += f'<tr style="font-weight: bold;"><td>TOTAL</td><td></td><td></td><td>{int(total_hub_count)}</td><td>₹{int(total_hub_amount):,}</td><td>{int(total_hub_missing_ops)} ({total_missing_ops_pct}%)</td><td>{int(total_hub_missing_image)} ({total_missing_image_pct}%)</td><td>{int(total_hub_missing_both)} ({total_missing_both_pct}%)</td><td>₹{int(total_hub_potential_debit):,}</td></tr>'
                hub_wise_html += '</table></div>'
            
            # Create HTML email body
            html_body = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; font-size: 11px; }}
                    .header {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; }}
                    .header h2 {{ font-size: 16px; font-weight: bold; margin: 0 0 10px 0; }}
                    .header p {{ font-size: 11px; margin: 5px 0; }}
                    .summary {{ background-color: #e8f5e8; padding: 15px; border-radius: 5px; margin: 10px 0; }}
                    .summary h3 {{ font-size: 11px; font-weight: bold; margin: 0 0 10px 0; }}
                    .summary p {{ font-size: 11px; margin: 5px 0; }}
                    table {{ border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 11px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                    th {{ background-color: #4CAF50; color: white; font-size: 10px; font-weight: 600; }}
                    td {{ font-size: 11px; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h2>South Zone - Debit Note Dashboard</h2>
                    <p><strong>Generated:</strong> {datetime.now().strftime('%d %B %Y at %H:%M:%S')}</p>
                </div>
                
                {worksheet_summary_html}
                
                {hub_wise_html}
                
                <div style="margin-top: 20px; padding: 10px; background-color: #f0f0f0; border-radius: 5px;">
                    <p><strong>View Full Reports:</strong></p>
                    <ul style="margin-top: 5px;">
                        <li><a href="https://docs.google.com/spreadsheets/d/1vEXO1TGn2S9gJ8kzSkCO9M-eiZoyCYFDwjMmVskkERo/edit" style="color: #1a73e8; text-decoration: none;">IMD Myntra Master Tracker</a></li>
                        <li><a href="https://docs.google.com/spreadsheets/d/1FFa2Vp5QB8Hx7klp6vGD-hcwj9a3OnbQ0JBKBK2Fa4c/edit" style="color: #1a73e8; text-decoration: none;">BRSNR Data</a></li>
                    </ul>
                </div>
            </body>
            </html>
            """
            
            # Attach HTML body to email
            msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            recipients = [EMAIL_CONFIG['recipient_email'], 'arunraj@loadshare.net', 'rakib@loadshare.net', 'maligai.rasmeen@loadshare.net']
            text = msg.as_string()
            server.sendmail(EMAIL_CONFIG['sender_email'], recipients, text)
            server.quit()
            
            logging.info(f"Dashboard email sent successfully to {EMAIL_CONFIG['recipient_email']}")
            logging.info(f"Dashboard email CC'd to: arunraj@loadshare.net, rakib@loadshare.net, maligai.rasmeen@loadshare.net")
            
        except Exception as e:
            logging.error(f"Error sending dashboard email: {e}")
            import traceback
            logging.error(f"Full error traceback:\n{traceback.format_exc()}")
            raise

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    analyzer = Q2DNAnalyzer()
    analyzer.run()

