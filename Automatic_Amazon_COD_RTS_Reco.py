import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import sys
import os
import warnings
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Suppress gspread deprecation warnings and pandas user warnings
warnings.filterwarnings("ignore", category=DeprecationWarning, module="gspread")
warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Fix Unicode encoding for Windows console
if sys.platform.startswith('win'):
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

# Configure logging (WARNING level to reduce verbose output)
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('amazon_cod_rts_analyzer.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Email Configuration
EMAIL_CONFIG = {
    'sender_email': 'arunraj@loadshare.net',
    'sender_password': 'ihczkvucdsayzrsu',  # Gmail App Password
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587
}

# Mapping of Station Code to Email ID
# This will be configured by the user - DO NOT ADD ANY MAPPINGS HERE
STATION_EMAIL_MAPPING = {}

class AmazonCODRTSAnalyzer:
    def __init__(self, service_account_key_path='service_account_key.json'):
        """Initialize Amazon COD/RTS Analyzer"""
        self.service_account_key_path = service_account_key_path
        self.scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        self.client = None
        self.setup_google_sheets()
        
    def setup_google_sheets(self):
        """Setup Google Sheets connection"""
        try:
            logging.info(f"🔑 Loading service account key from: {self.service_account_key_path}")
            
            # Check if service account file exists
            import os
            if not os.path.exists(self.service_account_key_path):
                logging.error(f"❌ Service account key file not found: {self.service_account_key_path}")
                raise FileNotFoundError(f"Service account key file not found: {self.service_account_key_path}")
            
            credentials = Credentials.from_service_account_file(
                self.service_account_key_path, 
                scopes=self.scope
            )
            self.client = gspread.authorize(credentials)
            logging.info("✅ Google Sheets connection established successfully")
            
            # Test the connection by trying to access a known sheet
            try:
                test_sheet = self.client.open_by_key("1vBG2gs2NieXAySZC_o3oNQxhTtXqasyYENUsU8lAbZM")
                logging.info(f"✅ Successfully accessed test sheet: {test_sheet.title}")
            except Exception as test_error:
                logging.warning(f"⚠️ Could not access test sheet: {test_error}")
                
        except Exception as e:
            logging.error(f"❌ Failed to setup Google Sheets: {e}")
            logging.error(f"❌ Error type: {type(e).__name__}")
            import traceback
            logging.error(f"❌ Full error traceback: {traceback.format_exc()}")
            raise
    
    def pull_data_from_sheet(self, sheet_id, worksheet_name=None):
        """Pull data from source Google Sheet"""
        try:
            logging.info(f"📥 Pulling Amazon COD/RTS data from sheet: {sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(sheet_id)
            
            # Get worksheet (default to first if not specified)
            if worksheet_name:
                worksheet = spreadsheet.worksheet(worksheet_name)
            else:
                worksheet = spreadsheet.get_worksheet(0)
            
            # Get all data as raw values to handle duplicate headers
            try:
                # First try the normal method
                data = worksheet.get_all_records()
                df = pd.DataFrame(data)
            except Exception as header_error:
                logging.warning(f"⚠️ Header error detected, using alternative method: {header_error}")
                # Alternative method: get raw values and handle headers manually
                raw_data = worksheet.get_all_values()
                if len(raw_data) > 0:
                    # Special handling for "DSP - RTS Pending Report" worksheet
                    if worksheet_name == "DSP - RTS Pending Report":
                        # Look for the actual data headers (skip malformed first row)
                        data_start_row = None
                        for i, row in enumerate(raw_data):
                            if any(keyword in ' '.join(row).lower() for keyword in ['tracking', 'station', 'value', 'amount']):
                                data_start_row = i
                                break
                        
                        if data_start_row is not None:
                            headers = raw_data[data_start_row]
                            data_rows = raw_data[data_start_row + 1:]
                            
                            # Make headers unique by adding suffix if needed
                            unique_headers = []
                            seen_headers = {}
                            for header in headers:
                                if header in seen_headers:
                                    seen_headers[header] += 1
                                    unique_headers.append(f"{header}_{seen_headers[header]}")
                                else:
                                    seen_headers[header] = 0
                                    unique_headers.append(header)
                            
                            df = pd.DataFrame(data_rows, columns=unique_headers)
                            logging.info(f"✅ Used row {data_start_row + 1} as headers for {worksheet_name}")
                        else:
                            # Fallback to first row
                            headers = raw_data[0]
                            unique_headers = []
                            seen_headers = {}
                            for header in headers:
                                if header in seen_headers:
                                    seen_headers[header] += 1
                                    unique_headers.append(f"{header}_{seen_headers[header]}")
                                else:
                                    seen_headers[header] = 0
                                    unique_headers.append(header)
                            
                            if len(raw_data) > 1:
                                df = pd.DataFrame(raw_data[1:], columns=unique_headers)
                            else:
                                df = pd.DataFrame(columns=unique_headers)
                    else:
                        # For all other worksheets, use first row as headers
                        headers = raw_data[0]
                        # Make headers unique by adding suffix if needed
                        unique_headers = []
                        seen_headers = {}
                        for header in headers:
                            if header in seen_headers:
                                seen_headers[header] += 1
                                unique_headers.append(f"{header}_{seen_headers[header]}")
                            else:
                                seen_headers[header] = 0
                                unique_headers.append(header)
                        
                        # Create DataFrame with unique headers, starting from 2nd row (index 1)
                        if len(raw_data) > 1:
                            df = pd.DataFrame(raw_data[1:], columns=unique_headers)
                        else:
                            df = pd.DataFrame(columns=unique_headers)
                else:
                    df = pd.DataFrame()
            
            logging.info(f"✅ Successfully pulled {len(df)} Amazon COD/RTS records from sheet")
            return df
            
        except Exception as e:
            logging.error(f"❌ Failed to pull data from sheet: {e}")
            raise
    
    def pull_data_from_all_worksheets(self, sheet_id):
        """Pull data from all worksheets in the source sheet"""
        try:
            logging.info(f"📥 Pulling Amazon COD/RTS data from ALL worksheets in sheet: {sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(sheet_id)
            
            # Get all worksheets
            all_worksheets = spreadsheet.worksheets()
            logging.info(f"📋 Found {len(all_worksheets)} worksheets: {[ws.title for ws in all_worksheets]}")
            
            combined_data = []
            worksheet_summary = []
            
            for i, worksheet in enumerate(all_worksheets):
                try:
                    logging.info(f"📥 Processing worksheet {i+1}/{len(all_worksheets)}: {worksheet.title}")
                    
                                        # Get all data from this worksheet
                    data = worksheet.get_all_records()
                    
                    # Convert to DataFrame
                    df = pd.DataFrame(data)
                    
                    # Add worksheet identifier
                    if len(df) > 0:
                        df['Source_Worksheet'] = worksheet.title
                        combined_data.append(df)
                        
                        worksheet_summary.append({
                            'Worksheet': worksheet.title,
                            'Records': len(df),
                            'Columns': list(df.columns)
                        })
                        
                        logging.info(f"✅ Worksheet '{worksheet.title}': {len(df)} records")
                    else:
                        logging.warning(f"⚠️ Worksheet '{worksheet.title}': No data found")
                        
                except Exception as e:
                    logging.error(f"❌ Failed to process worksheet '{worksheet.title}': {e}")
                    continue
            
            # Combine all dataframes
            if combined_data:
                final_df = pd.concat(combined_data, ignore_index=True)
                logging.info(f"✅ Successfully combined data from {len(combined_data)} worksheets: {len(final_df)} total records")
                
                # Log worksheet summary
                logging.info("📊 Worksheet Summary:")
                for summary in worksheet_summary:
                    logging.info(f"   - {summary['Worksheet']}: {summary['Records']} records")
                
                return final_df, worksheet_summary
            else:
                logging.error("❌ No data found in any worksheet")
                return pd.DataFrame(), []
                
        except Exception as e:
            logging.error(f"❌ Failed to pull data from all worksheets: {e}")
            raise
    
    def pull_edsp_data_from_sheet(self, sheet_id, worksheet_name):
        """Pull EDSP data from Amazon Potential Losses sheet with proper header handling"""
        try:
            logging.info(f"📥 Pulling EDSP data from sheet: {sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(sheet_id)
            
            # Get worksheet
            worksheet = spreadsheet.worksheet(worksheet_name)
            
            # Get all data using alternative method to handle header issues
            raw_data = worksheet.get_all_values()
            if len(raw_data) > 2:
                # Use row 2 (index 1) as headers, data starts from row 3 (index 2)
                headers = raw_data[1]  # Second row contains the actual headers
                data_rows = raw_data[2:]  # Data starts from third row
                
                # Create DataFrame with proper headers
                df = pd.DataFrame(data_rows, columns=headers)
                logging.info(f"✅ Successfully pulled {len(df)} EDSP records from sheet")
                return df
            else:
                logging.warning(f"⚠️ Insufficient data in EDSP worksheet")
                return pd.DataFrame()
            
        except Exception as e:
            logging.error(f"❌ Failed to pull EDSP data from sheet: {e}")
            raise
    
    def analyze_amazon_cod_rts_data(self, df, worksheet_summary=None):
        """Analyze Amazon COD/RTS data and return insights"""
        try:
            logging.info("🔍 Starting Amazon COD/RTS data analysis...")
            
            analysis_results = {
                'summary': {},
                'insights': [],
                'cod_rts_analysis': {},
                'worksheet_analysis': {},
                'recommendations': []
            }
            
            # Basic summary statistics
            analysis_results['summary'] = {
                'total_records': len(df),
                'columns': list(df.columns),
                'data_types': df.dtypes.to_dict(),
                'missing_values': df.isnull().sum().to_dict(),
                'duplicate_records': df.duplicated().sum()
            }
            
            # Add worksheet analysis if available
            if worksheet_summary:
                analysis_results['worksheet_analysis'] = {
                    'total_worksheets': len(worksheet_summary),
                    'worksheet_details': worksheet_summary,
                    'records_per_worksheet': {ws['Worksheet']: ws['Records'] for ws in worksheet_summary}
            }
            
            # Analyze each column
            for column in df.columns:
                col_analysis = self.analyze_column(df, column)
                if col_analysis:
                    analysis_results['insights'].append(col_analysis)
            
            # Special Amazon COD/RTS analysis
            analysis_results['cod_rts_analysis'] = self.analyze_cod_rts_patterns(df)
            
            # Generate recommendations
            analysis_results['recommendations'] = self.generate_amazon_recommendations(df, analysis_results)
            
            logging.info("✅ Amazon COD/RTS data analysis completed successfully")
            return analysis_results
            
        except Exception as e:
            logging.error(f"❌ Failed to analyze Amazon COD/RTS data: {e}")
            raise
    
    def analyze_column(self, df, column):
        """Analyze a specific column"""
        try:
            col_data = df[column]
            analysis = {
                'column': column,
                'type': str(col_data.dtype),
                'unique_values': col_data.nunique(),
                'missing_count': col_data.isnull().sum()
            }
            
            # Numeric column analysis
            if pd.api.types.is_numeric_dtype(col_data):
                analysis.update({
                    'min': col_data.min(),
                    'max': col_data.max(),
                    'mean': col_data.mean(),
                    'median': col_data.median(),
                    'std': col_data.std()
                })
            
            # Categorical/Text column analysis
            elif pd.api.types.is_string_dtype(col_data) or pd.api.types.is_object_dtype(col_data):
                value_counts = col_data.value_counts()
                analysis.update({
                    'top_values': value_counts.head(5).to_dict(),
                    'most_common': value_counts.index[0] if len(value_counts) > 0 else None
                })
            
            # Date column analysis
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                analysis.update({
                    'date_range': f"{col_data.min()} to {col_data.max()}",
                    'days_span': (col_data.max() - col_data.min()).days
                })
            
            return analysis
            
        except Exception as e:
            logging.warning(f"⚠️ Could not analyze column {column}: {e}")
            return None
    
    def analyze_cod_rts_patterns(self, df):
        """Analyze specific Amazon COD/RTS patterns based on Station and Station_code"""
        try:
            cod_rts_analysis = {}
            
            # Look for Station and Station_code columns
            station_col = None
            station_code_col = None
            
            for col in df.columns:
                if 'station' in col.lower() and 'code' not in col.lower():
                    station_col = col
                elif 'station_code' in col.lower() or ('station' in col.lower() and 'code' in col.lower()):
                    station_code_col = col
            
            # Analyze station patterns
            if station_col:
                station_counts = df[station_col].value_counts()
                cod_rts_analysis['station_distribution'] = station_counts.to_dict()
                cod_rts_analysis['total_stations'] = len(station_counts)
                cod_rts_analysis['top_stations'] = station_counts.head(10).to_dict()
            
            if station_code_col:
                station_code_counts = df[station_code_col].value_counts()
                cod_rts_analysis['station_code_distribution'] = station_code_counts.to_dict()
                cod_rts_analysis['total_station_codes'] = len(station_code_counts)
                cod_rts_analysis['top_station_codes'] = station_code_counts.head(10).to_dict()
            
            # Analyze station-based patterns
            if station_col and station_code_col:
                # Station vs Station Code correlation
                station_code_mapping = df.groupby([station_col, station_code_col]).size().reset_index(name='count')
                cod_rts_analysis['station_code_mapping'] = station_code_mapping.to_dict('records')
            
            # Look for common Amazon COD/RTS columns
            possible_status_cols = [col for col in df.columns if 'status' in col.lower() or 'state' in col.lower()]
            possible_amount_cols = [col for col in df.columns if 'amount' in col.lower() or 'value' in col.lower() or 'price' in col.lower()]
            possible_date_cols = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
            
            # Analyze status patterns
            if possible_status_cols:
                status_col = possible_status_cols[0]
                status_counts = df[status_col].value_counts()
                cod_rts_analysis['status_distribution'] = status_counts.to_dict()
                cod_rts_analysis['total_statuses'] = len(status_counts)
            
            # Analyze amount patterns
            if possible_amount_cols:
                amount_col = possible_amount_cols[0]
                if pd.api.types.is_numeric_dtype(df[amount_col]):
                    cod_rts_analysis['amount_analysis'] = {
                        'total_value': round(df[amount_col].sum(), 2),
                        'average_value': round(df[amount_col].mean(), 2),
                        'max_value': round(df[amount_col].max(), 2),
                        'min_value': round(df[amount_col].min(), 2)
                    }
                    
                    # Station-based amount analysis
                    if station_col:
                        station_amounts = df.groupby(station_col)[amount_col].agg(['sum', 'mean', 'count']).reset_index()
                        station_amounts.columns = ['Station', 'Total_Amount', 'Average_Amount', 'Record_Count']
                        # Round the numeric columns
                        station_amounts['Total_Amount'] = station_amounts['Total_Amount'].round(2)
                        station_amounts['Average_Amount'] = station_amounts['Average_Amount'].round(2)
                        cod_rts_analysis['station_amount_analysis'] = station_amounts.to_dict('records')
            
            # Analyze date patterns
            if possible_date_cols:
                date_col = possible_date_cols[0]
                try:
                    df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
                    cod_rts_analysis['date_analysis'] = {
                        'date_range': f"{df[date_col].min()} to {df[date_col].max()}",
                        'days_span': (df[date_col].max() - df[date_col].min()).days
                    }
                except:
                    pass
            
            return cod_rts_analysis
            
        except Exception as e:
            logging.warning(f"⚠️ Could not analyze COD/RTS patterns: {e}")
            return {}
    
    def generate_amazon_recommendations(self, df, analysis_results):
        """Generate Amazon-specific recommendations"""
        recommendations = []
        
        # Check for missing data
        missing_data = analysis_results['summary']['missing_values']
        for col, missing_count in missing_data.items():
            if missing_count > 0:
                recommendations.append(f"Column '{col}' has {missing_count} missing values - critical for COD/RTS tracking")
        
        # Check for duplicates
        if analysis_results['summary']['duplicate_records'] > 0:
            recommendations.append(f"Found {analysis_results['summary']['duplicate_records']} duplicate records - may indicate data entry issues")
        
        # Amazon-specific recommendations
        if 'cod_rts_analysis' in analysis_results:
            cod_analysis = analysis_results['cod_rts_analysis']
            
            # Station-based recommendations
            if 'total_stations' in cod_analysis:
                total_stations = cod_analysis['total_stations']
                if total_stations > 50:
                    recommendations.append(f"High number of stations ({total_stations}) - consider station consolidation for efficiency")
                elif total_stations < 5:
                    recommendations.append(f"Low number of stations ({total_stations}) - verify station coverage")
            
            if 'station_amount_analysis' in cod_analysis:
                station_amounts = cod_analysis['station_amount_analysis']
                # Find stations with highest amounts
                top_stations = sorted(station_amounts, key=lambda x: x['Total_Amount'], reverse=True)[:3]
                for station_data in top_stations:
                    recommendations.append(f"Focus station: {station_data['Station']} has ₹{station_data['Total_Amount']:,.2f} total value - prioritize recovery")
                
                # Find stations with low amounts
                low_amount_stations = [s for s in station_amounts if s['Total_Amount'] < 10000]
                if low_amount_stations:
                    recommendations.append(f"{len(low_amount_stations)} stations have low amounts (<₹10,000) - review operational efficiency")
            
            if 'status_distribution' in cod_analysis:
                statuses = cod_analysis['status_distribution']
                if len(statuses) > 5:
                    recommendations.append(f"High number of status types ({len(statuses)}) - consider status standardization")
            
            if 'amount_analysis' in cod_analysis:
                amount_analysis = cod_analysis['amount_analysis']
                if amount_analysis['total_value'] > 0:
                    recommendations.append(f"Total COD/RTS value: ₹{amount_analysis['total_value']:,.2f} - significant recovery potential")
        
        return recommendations
    
    def create_amazon_analysis_summary(self, df, analysis_results, rts_data=None, edsp_data=None):
        """Create Amazon COD/RTS specific summary DataFrame with station-based cash analysis"""
        try:
            # Create the specific output format you requested
            output_data = []
            
            # Predefined station mapping data
            station_mapping = {
                'TRVI': {'Hub Type': 'eDSP', 'City Name': 'Trivandrum', 'Station Manager': 'Shyam', 'M1': 'Akash', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'QLNB': {'Hub Type': 'eDSP', 'City Name': 'Kollam', 'Station Manager': 'Akash', 'M1': 'Akash', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'TLAG': {'Hub Type': 'eDSP', 'City Name': 'Changanaserry', 'Station Manager': 'Kamal', 'M1': 'Akash', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'KTYI': {'Hub Type': 'eDSP', 'City Name': 'Munnar', 'Station Manager': 'Vignesh', 'M1': 'Vinudev', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'KLZE': {'Hub Type': 'eDSP', 'City Name': 'Cochin', 'Station Manager': 'Vishnu', 'M1': 'Vinudev', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'ERSA': {'Hub Type': 'eDSP', 'City Name': 'Thodupuzha', 'Station Manager': 'Anandhu', 'M1': 'Vinudev', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'KGQB': {'Hub Type': 'eDSP', 'City Name': 'Kasaragod', 'Station Manager': 'Abhijeet', 'M1': '-', 'M2': 'Sherin', 'SM': 'Chandra', 'State': 'Kerala'},
                'MASC': {'Hub Type': 'eDSP', 'City Name': 'Kilpauk', 'Station Manager': 'Narendra Kumar', 'M1': 'Narendra Kumar', 'M2': '-', 'SM': 'Chandra', 'State': 'Chennai'},
                'KELE': {'Hub Type': 'eDSP', 'City Name': 'Medavakkam', 'Station Manager': 'Karthik', 'M1': 'Karthik', 'M2': '-', 'SM': 'Chandra', 'State': 'Chennai'},
                'BLRA': {'Hub Type': 'DSP', 'City Name': 'KR Puram', 'Station Manager': 'Sanjay', 'M1': 'Lakshman', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'BLRL': {'Hub Type': 'DSP', 'City Name': 'Vasanth Nagar', 'Station Manager': 'Aadhil/Mousin', 'M1': 'Nagaraj', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'BLRP': {'Hub Type': 'DSP', 'City Name': 'Hennur', 'Station Manager': 'Pavan/Pachayappa', 'M1': 'Nagaraj', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'BLT1': {'Hub Type': 'DSP', 'City Name': 'Kadugodi', 'Station Manager': 'Sanjay/Rafiq/Hassain', 'M1': 'Lakshman', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'BLT3': {'Hub Type': 'DSP', 'City Name': 'Mahdevpura', 'Station Manager': 'Reddy/Mullanki', 'M1': 'Lakshman', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'BLT4': {'Hub Type': 'DSP', 'City Name': 'Kanakapura', 'Station Manager': 'Praveem/Mano', 'M1': 'Nagaraj', 'M2': 'Anthony', 'SM': 'Chandra', 'State': 'Bengaluru'},
                'MAAE': {'Hub Type': 'DSP', 'City Name': 'Royapetah', 'Station Manager': 'Praveen/John', 'M1': 'Bharath', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'},
                'MAAG': {'Hub Type': 'DSP', 'City Name': 'Perngudi', 'Station Manager': 'Shankar/Vicky', 'M1': 'Vadivel', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'},
                'MAAI': {'Hub Type': 'DSP', 'City Name': 'Pallavaram', 'Station Manager': 'Raja/Rishab', 'M1': 'Vadivel', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'},
                'MAAJ': {'Hub Type': 'DSP', 'City Name': 'Ponamalle', 'Station Manager': 'Deva/Seenu', 'M1': 'Vadivel', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'},
                'MAAL': {'Hub Type': 'DSP', 'City Name': 'Guindy', 'Station Manager': 'Tamil/Ajith/Kumar', 'M1': 'Vadivel', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'},
                'MAT1': {'Hub Type': 'DSP', 'City Name': 'Virugambakkam', 'Station Manager': 'Vignesh/', 'M1': 'Bharath', 'M2': 'Ramesh', 'SM': 'Chandra', 'State': 'Chennai'}
            }
            
            # Get unique stations from the data - combine Station and Station_Code
            station_col = None
            station_code_col = None
            city_col = None
            state_col = None
            
                        # Find the relevant columns
            for col in df.columns:
                if col.lower() == 'station':
                    station_col = col
                elif col.lower() == 'station_code':
                    station_code_col = col
                elif 'city' in col.lower():
                    city_col = col
                elif 'state' in col.lower():
                    state_col = col
            
            # Create a combined station identifier
            df['Combined_Station'] = ""
            
            # Fill Combined_Station with Station if available, otherwise Station_Code
            if station_col and station_code_col:
                df['Combined_Station'] = df[station_col].fillna(df[station_code_col])
            elif station_col:
                df['Combined_Station'] = df[station_col]
            elif station_code_col:
                df['Combined_Station'] = df[station_code_col]
            
            # Get unique combined stations and filter to only predefined stations
            all_unique_stations = df['Combined_Station'].unique()
            
            # Filter to only include stations from our predefined mapping
            unique_stations = [station for station in all_unique_stations if station in station_mapping]
            
            # Find missing stations
            missing_stations = [station for station in station_mapping.keys() if station not in all_unique_stations]
            
            logging.info(f"📊 Found {len(all_unique_stations)} total stations, filtering to {len(unique_stations)} predefined stations")
            logging.info(f"📋 Stations found in source data: {sorted(unique_stations)}")
            logging.info(f"❌ Missing stations (not in source data): {sorted(missing_stations)}")
            logging.info(f"📋 All predefined stations: {sorted(station_mapping.keys())}")
            
            # Process all predefined stations, even if not found in source data
            for station in station_mapping.keys():
                if pd.isna(station) or station == "":
                    continue
                    
                # Get station data if it exists in source data
                station_data = df[df['Combined_Station'] == station] if station in unique_stations else pd.DataFrame()
                
                # Get station code (for separate column)
                station_code = ""
                if station_code_col and station_code_col in station_data.columns:
                    station_codes = station_data[station_code_col].unique()
                    station_code = station_codes[0] if len(station_codes) > 0 and not pd.isna(station_codes[0]) else ""
                
                                # Get city
                city = ""
                if city_col and city_col in station_data.columns:
                    cities = station_data[city_col].unique()
                    city = cities[0] if len(cities) > 0 and not pd.isna(cities[0]) else ""
                
                # Get state
                state = ""
                if state_col and state_col in station_data.columns:
                    states = station_data[state_col].unique()
                    state = states[0] if len(states) > 0 and not pd.isna(states[0]) else ""
                
                # Calculate cash amounts by worksheet type
                dsp_short_cash = 0
                dsp_outstanding_cash = 0
                edsp_outstanding_cash = 0
                rts_pending = 0
                
                # Look for specific amount columns
                balance_due_col = None
                amount_cols = [col for col in df.columns if 'amount' in col.lower() or 'value' in col.lower() or 'price' in col.lower() or 'cash' in col.lower()]
                
                # Find balance_due column specifically
                for col in df.columns:
                    if 'balance_due' in col.lower():
                        balance_due_col = col
                        break
                
                # Find employee_name and station_code columns
                employee_name_col = None
                station_code_col = None
                
                for col in df.columns:
                    if 'employee_name' in col.lower() or 'employee' in col.lower():
                        employee_name_col = col
                    elif 'station_code' in col.lower():
                        station_code_col = col
                
                # Use balance_due if found, otherwise fallback to general amount columns
                amount_col = balance_due_col if balance_due_col else (amount_cols[0] if amount_cols else None)
                
                if amount_col:
                    # DSP Short Cash (from "DSP Short Cash to be Submitted" worksheet)
                    dsp_short_data = station_data[station_data['Source_Worksheet'].str.contains('DSP Short Cash to be Submitted', case=False, na=False)] if 'Source_Worksheet' in station_data.columns else pd.DataFrame()
                    if len(dsp_short_data) > 0:
                        # Look for submitted_short_excess column specifically for short cash
                        short_cash_col = None
                        for col in dsp_short_data.columns:
                            if 'submitted_short_excess' in col.lower():
                                short_cash_col = col
                                break
                        
                        if short_cash_col:
                            dsp_short_cash = round(abs(dsp_short_data[short_cash_col].sum()), 2)
                        else:
                            # Fallback to general amount column
                            dsp_short_cash = round(abs(dsp_short_data[amount_col].sum()), 2)
                    
                    # DSP Outstanding Cash (from second worksheet) - specifically "DSP Outstanding Cash to be Submitted"
                    dsp_outstanding_data = station_data[station_data['Source_Worksheet'].str.contains('DSP Outstanding Cash To be Submitted', case=False, na=False)] if 'Source_Worksheet' in station_data.columns else pd.DataFrame()
                    if len(dsp_outstanding_data) > 0:
                        dsp_outstanding_cash = round(dsp_outstanding_data[amount_col].sum(), 2)
                    
                    # EDSP Outstanding Cash (from third worksheet) - specifically "Edsp OutStanding Cash to be Submitted"
                    edsp_outstanding_data = station_data[station_data['Source_Worksheet'].str.contains('Edsp OutStanding Cash to be Submitted', case=False, na=False)] if 'Source_Worksheet' in station_data.columns else pd.DataFrame()
                    if len(edsp_outstanding_data) > 0:
                        edsp_outstanding_cash = round(edsp_outstanding_data[amount_col].sum(), 2)
                
                # Determine type based on which worksheets have data
                station_type = []
                if len(dsp_short_data) > 0 or len(dsp_outstanding_data) > 0:
                    station_type.append("DSP")
                if len(edsp_outstanding_data) > 0:
                    station_type.append("EDSP")
                type_str = "/".join(station_type) if station_type else "Unknown"
                
                # Get predefined mapping for this station
                station_info = station_mapping.get(station, {})
                
                # For eDSP hubs, set DSP_Outstanding_Cash to 0 to avoid duplication with EDSP_Outstanding_Cash
                if station_info.get('Hub Type', '') == 'eDSP':
                    dsp_outstanding_cash = 0
                    logging.info(f"🔍 Set DSP_Outstanding_Cash to 0 for eDSP station: {station}")
                
                # Calculate RTS Pending from RTS source data
                if rts_data is not None and len(rts_data) > 0:
                    try:
                        # Look for delivery_station_code column in RTS data
                        rts_station_col = None
                        rts_value_col = None
                        
                        # Debug: Log available columns in RTS data
                        if station == list(station_mapping.keys())[0]:  # Only log for first station
                            logging.info(f"🔍 Available columns in RTS data: {list(rts_data.columns)}")
                        
                        for col in rts_data.columns:
                            if 'delivery_station_code' in col.lower() or 'station_code' in col.lower():
                                rts_station_col = col
                            elif 'value' in col.lower() and 'scc' in col.lower():
                                rts_value_col = col
                        
                        # If not found, try alternative column names
                        if not rts_station_col:
                            for col in rts_data.columns:
                                if 'delivery_station' in col.lower():
                                    rts_station_col = col
                                    break
                        
                        if not rts_value_col:
                            for col in rts_data.columns:
                                if 'value' in col.lower():
                                    rts_value_col = col
                                    break
                        
                        if rts_station_col and rts_value_col:
                            # Filter RTS data for this station
                            station_rts_data = rts_data[rts_data[rts_station_col] == station]
                            if len(station_rts_data) > 0:
                                # Convert value column to numeric, handling any non-numeric values
                                try:
                                    numeric_values = pd.to_numeric(station_rts_data[rts_value_col], errors='coerce')
                                    rts_pending = round(numeric_values.sum(), 2)
                                    logging.info(f"🔍 RTS Pending for {station}: {rts_pending}")
                                except Exception as value_error:
                                    logging.warning(f"⚠️ Error converting RTS values for {station}: {value_error}")
                                    rts_pending = 0
                        else:
                            logging.warning(f"⚠️ RTS columns not found. Station col: {rts_station_col}, Value col: {rts_value_col}")
                            rts_pending = 0
                    except Exception as e:
                        logging.warning(f"⚠️ Error processing RTS data for station {station}: {e}")
                        rts_pending = 0
                
                # Calculate EDSP values and add to RTS Pending
                edsp_value = 0
                if edsp_data is not None and len(edsp_data) > 0:
                    try:
                        # Debug: Log available columns in EDSP data for first station only
                        if station == list(station_mapping.keys())[0]:  # Only log for first station
                            logging.info(f"🔍 Available columns in EDSP data: {list(edsp_data.columns)}")
                        
                        # Look for station and value columns in EDSP data
                        edsp_station_col = None
                        edsp_value_col = None
                        
                        for col in edsp_data.columns:
                            # More specific station column detection - prefer exact 'station' match
                            if col.lower() == 'station':
                                edsp_station_col = col
                            elif 'station' in col.lower() and 'code' not in col.lower() and 'time' not in col.lower() and edsp_station_col is None:
                                edsp_station_col = col
                            elif col.lower() == 'value':
                                edsp_value_col = col
                            elif 'value' in col.lower() and edsp_value_col is None:
                                edsp_value_col = col
                        
                        if edsp_station_col and edsp_value_col:
                            # Filter EDSP data for this station
                            station_edsp_data = edsp_data[edsp_data[edsp_station_col] == station]
                            if len(station_edsp_data) > 0:
                                # Convert value column to numeric
                                try:
                                    numeric_values = pd.to_numeric(station_edsp_data[edsp_value_col], errors='coerce')
                                    edsp_value = round(numeric_values.sum(), 2)
                                    logging.info(f"🔍 EDSP value for {station}: {edsp_value}")
                                except Exception as value_error:
                                    logging.warning(f"⚠️ Error converting EDSP values for {station}: {value_error}")
                                    edsp_value = 0
                        else:
                            logging.warning(f"⚠️ EDSP columns not found. Station col: {edsp_station_col}, Value col: {edsp_value_col}")
                            edsp_value = 0
                    except Exception as e:
                        logging.warning(f"⚠️ Error processing EDSP data for station {station}: {e}")
                        edsp_value = 0
                
                # Add EDSP value to RTS Pending (combine both values)
                rts_pending = rts_pending + edsp_value
                
                # Merge DSP and eDSP outstanding cash into single column
                combined_outstanding_cash = dsp_outstanding_cash + edsp_outstanding_cash
                
                # Calculate total risk (sum of all cash amounts - EDSP values are now included in RTS Pending)
                total_risk = dsp_short_cash + combined_outstanding_cash + rts_pending
                
                # Store original numeric values for totals calculation
                numeric_values = {
                    'Short Cash': dsp_short_cash,
                    'DSP/eDSP_Outstanding Cash': combined_outstanding_cash,
                    'RTS Pending': rts_pending,
                    'Total Risk': total_risk
                }
                
                # Extract employee_name, station_code, and balance_due from station data
                employee_name = ""
                station_code_value = ""
                balance_due_value = ""
                
                if len(station_data) > 0:
                    # Get employee_name
                    if employee_name_col and employee_name_col in station_data.columns:
                        employee_names = station_data[employee_name_col].dropna().unique()
                        employee_name = employee_names[0] if len(employee_names) > 0 else ""
                    
                    # Get station_code
                    if station_code_col and station_code_col in station_data.columns:
                        station_codes = station_data[station_code_col].dropna().unique()
                        station_code_value = station_codes[0] if len(station_codes) > 0 else ""
                    
                    # Get balance_due
                    if balance_due_col and balance_due_col in station_data.columns:
                        balance_due_values = station_data[balance_due_col].dropna().unique()
                        balance_due_value = balance_due_values[0] if len(balance_due_values) > 0 else ""
                
                # Create output row with numeric values (for sorting)
                output_data.append({
                    'Station': station,
                    'Hub Type': station_info.get('Hub Type', ''),
                    'City Name': station_info.get('City Name', ''),
                    'Station Manager': station_info.get('Station Manager', ''),
                    'M1': station_info.get('M1', ''),
                    'M2': station_info.get('M2', ''),
                    'SM': station_info.get('SM', ''),
                    'State': station_info.get('State', ''),
                    'Short Cash': dsp_short_cash,
                    'DSP/eDSP_Outstanding Cash': combined_outstanding_cash,
                    'RTS Pending': rts_pending,
                    'Total Risk': total_risk
                })
                
                # Store numeric values for totals
                if 'numeric_totals' not in locals():
                    numeric_totals = {'Short Cash': 0, 'DSP/eDSP_Outstanding Cash': 0, 'RTS Pending': 0, 'Total Risk': 0}
                
                numeric_totals['Short Cash'] += dsp_short_cash
                numeric_totals['DSP/eDSP_Outstanding Cash'] += combined_outstanding_cash
                numeric_totals['RTS Pending'] += rts_pending
                numeric_totals['Total Risk'] += total_risk
            
            # Create DataFrame with numeric values for sorting
            df_numeric = pd.DataFrame(output_data)
            
            # Sort by State first, then by Hub Type (DSP first, then eDSP), then by Total Risk in descending order
            df_numeric = df_numeric.sort_values(['State', 'Hub Type', 'Total Risk'], ascending=[True, True, False]).reset_index(drop=True)
            
            # Now apply rupee formatting to the sorted data
            # Convert DataFrame to object dtype to avoid dtype warnings
            df_numeric = df_numeric.astype(object)
            
            for idx, row in df_numeric.iterrows():
                # Convert numeric values to rupee format
                df_numeric.at[idx, 'Short Cash'] = f"₹{int(row['Short Cash']):,}" if row['Short Cash'] > 0 else "₹0"
                df_numeric.at[idx, 'DSP/eDSP_Outstanding Cash'] = f"₹{int(row['DSP/eDSP_Outstanding Cash']):,}" if row['DSP/eDSP_Outstanding Cash'] > 0 else "₹0"
                df_numeric.at[idx, 'RTS Pending'] = f"₹{int(row['RTS Pending']):,}" if row['RTS Pending'] > 0 else "₹0"
                df_numeric.at[idx, 'Total Risk'] = f"₹{int(row['Total Risk']):,}" if row['Total Risk'] > 0 else "₹0"
            
            df_output = df_numeric
            
            # Add Total row at the bottom using numeric totals (no decimals)
            total_row = {
                'Station': 'Total',
                'Hub Type': '',
                'City Name': '',
                'Station Manager': '',
                'M1': '',
                'M2': '',
                'SM': '',
                'State': '',
                'Short Cash': f"₹{int(numeric_totals['Short Cash']):,}" if numeric_totals['Short Cash'] > 0 else "₹0",
                'DSP/eDSP_Outstanding Cash': f"₹{int(numeric_totals['DSP/eDSP_Outstanding Cash']):,}" if numeric_totals['DSP/eDSP_Outstanding Cash'] > 0 else "₹0",
                'RTS Pending': f"₹{int(numeric_totals['RTS Pending']):,}" if numeric_totals['RTS Pending'] > 0 else "₹0",
                'Total Risk': f"₹{int(numeric_totals['Total Risk']):,}" if numeric_totals['Total Risk'] > 0 else "₹0"
            }
            
            # Add total row to DataFrame
            df_output = pd.concat([df_output, pd.DataFrame([total_row])], ignore_index=True)
            
            return df_output
            
        except Exception as e:
            logging.error(f"❌ Failed to create Amazon analysis summary: {e}")
            raise
    
    def push_results_to_sheet(self, sheet_id, worksheet_name, data_df, analysis_results):
        """Push analysis results to destination Google Sheet"""
        try:
            logging.info(f"📤 Pushing Amazon COD/RTS analysis results to sheet: {sheet_id}")
            logging.info(f"📋 Worksheet name: {worksheet_name}")
            logging.info(f"📊 Data rows to upload: {len(data_df)}")
            
            # Open the spreadsheet
            try:
                spreadsheet = self.client.open_by_key(sheet_id)
                logging.info(f"✅ Successfully opened destination spreadsheet: {spreadsheet.title}")
            except Exception as open_error:
                logging.error(f"❌ Failed to open destination spreadsheet: {open_error}")
                logging.error(f"❌ Sheet ID: {sheet_id}")
                logging.error(f"❌ Make sure the service account has access to this sheet")
                raise
            
            # Create or get worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"📋 Using existing worksheet: {worksheet_name}")
            except Exception as ws_error:
                logging.info(f"📋 Creating new worksheet: {worksheet_name}")
                try:
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=20)
                    logging.info(f"✅ Successfully created new worksheet: {worksheet_name}")
                except Exception as create_error:
                    logging.error(f"❌ Failed to create worksheet '{worksheet_name}': {create_error}")
                    raise
            
            # Clear existing data
            try:
                worksheet.clear()
                logging.info(f"✅ Cleared existing data from worksheet")
            except Exception as clear_error:
                logging.warning(f"⚠️ Could not clear worksheet: {clear_error}")
            
            # Prepare data for upload - convert numpy types to Python types
            def convert_numpy_types(obj):
                """Convert numpy types to Python types for JSON serialization"""
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif pd.isna(obj):
                    return ""
                else:
                    return obj
            
            # Convert DataFrame to list of lists with proper type conversion
            headers = data_df.columns.tolist()
            rows = []
            for _, row in data_df.iterrows():
                converted_row = [convert_numpy_types(val) for val in row.values]
                rows.append(converted_row)
            
            data_to_upload = [headers] + rows
            logging.info(f"📊 Prepared {len(data_to_upload)} rows for upload")
            
            # Upload data
            try:
                worksheet.update(range_name='A1', values=data_to_upload)
                logging.info(f"✅ Successfully uploaded {len(data_df)} rows to sheet")
            except Exception as upload_error:
                logging.error(f"❌ Failed to upload data: {upload_error}")
                raise
            
            # Format the sheet
            try:
                self.format_sheet(worksheet, data_df)
            except Exception as format_error:
                logging.warning(f"⚠️ Could not format sheet: {format_error}")
            
            # Add analysis metadata
            try:
                self.add_amazon_analysis_metadata(worksheet, analysis_results, len(data_df))
            except Exception as metadata_error:
                logging.warning(f"⚠️ Could not add metadata: {metadata_error}")
            
            logging.info(f"✅ Successfully completed push to destination sheet")
            
        except Exception as e:
            logging.error(f"❌ Failed to push results to sheet: {e}")
            logging.error(f"❌ Error type: {type(e).__name__}")
            logging.error(f"❌ Error details: {str(e)}")
            logging.error(f"❌ Full error traceback:")
            import traceback
            logging.error(traceback.format_exc())
            raise
    
    def format_sheet(self, worksheet, df):
        """Apply formatting to the Google Sheet"""
        try:
            # Format entire header row with darker orange color
            header_range = f'A1:{chr(65 + len(df.columns) - 1)}1'  # A1 to last column
            worksheet.format(header_range, {
                'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.0},  # Darker orange color
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}  # White text
            })
            
            # Format last row (Total row) with yellow color and bold font
            last_row = len(df) + 1  # +1 because data starts from row 2 (row 1 is header)
            total_row_range = f'A{last_row}:{chr(65 + len(df.columns) - 1)}{last_row}'
            worksheet.format(total_row_range, {
                'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0},  # Yellow color
                'textFormat': {'bold': True, 'foregroundColor': {'red': 0, 'green': 0, 'blue': 0}}  # Black text
            })
            
            # Format last 5 columns (monetary columns) to be right-aligned
            # Calculate the range for the last 5 columns
            start_col_letter = chr(65 + len(df.columns) - 5)  # 5 columns from the end
            end_col_letter = chr(65 + len(df.columns) - 1)    # Last column
            
            # Right-align the last 5 columns for all data rows (including header and total)
            monetary_range = f'{start_col_letter}1:{end_col_letter}{last_row}'
            worksheet.format(monetary_range, {
                'horizontalAlignment': 'RIGHT'
            })
            
            logging.info(f"✅ Right-aligned last 5 columns ({start_col_letter} to {end_col_letter}) for all rows")
            
            # Auto-resize all columns to fit content
            try:
                # Method 1: Try the built-in auto-resize with specific range
                worksheet.columns_auto_resize(0, len(df.columns))
                logging.info("✅ Auto-resized columns using built-in method")
            except Exception as auto_resize_error:
                logging.warning(f"⚠️ Built-in auto-resize failed, using manual method: {auto_resize_error}")
                try:
                    # Method 2: Manual column width adjustment with better logic
                    column_widths = {
                        'Station': 80,           # Station codes are short
                        'Hub Type': 80,          # DSP/eDSP
                        'City Name': 120,        # City names
                        'Station Manager': 150,  # Manager names can be long
                        'M1': 100,              # Manager initials
                        'M2': 100,              # Manager initials
                        'SM': 100,              # Manager initials
                        'State': 100,           # State names
                        'Short Cash': 120,      # Monetary values
                        'DSP/eDSP_Outstanding Cash': 180,  # Longer header for merged column
                        'RTS Pending': 120,     # Monetary values
                        'Total Risk': 120       # Monetary values
                    }
                    
                    for col_idx, col_name in enumerate(df.columns):
                        # Find the best matching width
                        width = 100  # Default width
                        for key, value in column_widths.items():
                            if key in col_name:
                                width = value
                                break
                        
                        # Set column width
                        worksheet.set_column_width(col_idx + 1, width)
                        logging.info(f"📏 Set column {col_name} (col {col_idx + 1}) to width {width}")
                    
                    logging.info("✅ Manually adjusted column widths with specific sizing")
                except Exception as manual_error:
                    logging.warning(f"⚠️ Manual column adjustment also failed: {manual_error}")
                    # Method 3: Try setting all columns to a reasonable default width
                    try:
                        for col_idx in range(len(df.columns)):
                            worksheet.set_column_width(col_idx + 1, 120)
                        logging.info("✅ Set all columns to default width of 120")
                    except Exception as default_error:
                        logging.warning(f"⚠️ Default column width setting also failed: {default_error}")
            
            logging.info("✅ Sheet formatting applied successfully - Header row orange, Total row yellow and bold, columns auto-sized")
            
        except Exception as e:
            logging.warning(f"⚠️ Could not apply formatting: {e}")
    
    def add_amazon_analysis_metadata(self, worksheet, analysis_results, row_count):
        """Add Amazon-specific analysis metadata to the sheet"""
        try:
            # Add simple timestamp in row 24 (merged cells A24:C24)
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            timestamp_range = f'A24:C24'
            
            # Update timestamp in merged cells
            worksheet.update(range_name='A24', values=[[f'Last Updated: {current_timestamp}']])
            
            # Merge cells A24:C24
            worksheet.merge_cells(timestamp_range)
            
            # Format merged timestamp cells
            worksheet.format(timestamp_range, {
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 1.0},  # Blue color
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},  # White text
                'horizontalAlignment': 'CENTER'  # Center the text
            })
            
            logging.info("✅ Timestamp added to row 24")
            
        except Exception as e:
            logging.warning(f"⚠️ Could not add metadata: {e}")
    
    def extract_and_push_high_value_tracking(self, rts_data, dest_sheet_id, min_value=10000, worksheet_suffix=""):
        """Extract tracking IDs with values greater than specified amount and push to sheet"""
        try:
            logging.info(f"🔍 Extracting tracking IDs with value > {min_value}")
            
            # Find the required columns
            tracking_id_col = None
            delivery_station_col = None
            ageing_bucket_col = None
            value_col = None
            
            for col in rts_data.columns:
                if 'tracking_id' in col.lower():
                    tracking_id_col = col
                elif 'delivery_station_code' in col.lower() or 'delivery_station' in col.lower():
                    delivery_station_col = col
                elif 'station' in col.lower() and 'code' not in col.lower() and delivery_station_col is None:
                    delivery_station_col = col
                elif 'ageing bucket' in col.lower() or 'ageing' in col.lower():
                    ageing_bucket_col = col
                elif 'value' in col.lower() and 'scc' in col.lower():
                    value_col = col
                elif 'value' in col.lower() and value_col is None:
                    value_col = col
            
            # Log found columns
            logging.info(f"📋 Found columns:")
            logging.info(f"   - Tracking ID: {tracking_id_col}")
            logging.info(f"   - Delivery Station: {delivery_station_col}")
            logging.info(f"   - Ageing Bucket: {ageing_bucket_col}")
            logging.info(f"   - Value: {value_col}")
            
            if not all([tracking_id_col, delivery_station_col, ageing_bucket_col, value_col]):
                missing_cols = []
                if not tracking_id_col: missing_cols.append("tracking_id")
                if not delivery_station_col: missing_cols.append("delivery_station_code")
                if not ageing_bucket_col: missing_cols.append("ageing_bucket")
                if not value_col: missing_cols.append("value")
                logging.warning(f"⚠️ Missing required columns: {missing_cols}")
                return
            
            # Convert value column to numeric
            rts_data[value_col] = pd.to_numeric(rts_data[value_col], errors='coerce')
            
            # Filter for high value records
            high_value_df = rts_data[rts_data[value_col] > min_value].copy()
            
            # Select only required columns
            result_df = high_value_df[[tracking_id_col, delivery_station_col, ageing_bucket_col, value_col]].copy()
            
            # Rename columns for clarity
            result_df.columns = ['Tracking_ID', 'Delivery_Station_Code', 'Ageing_Bucket', 'Value']
            
            # Sort by value in descending order
            result_df = result_df.sort_values('Value', ascending=False).reset_index(drop=True)
            
            logging.info(f"✅ Found {len(result_df)} tracking IDs with value > {min_value}")
            logging.info(f"📊 Value range: {result_df['Value'].min():,.2f} to {result_df['Value'].max():,.2f}")
            
            # Push to destination sheet
            self.push_high_value_tracking_to_sheet(result_df, dest_sheet_id, worksheet_suffix)
            
        except Exception as e:
            logging.error(f"❌ Error extracting high value tracking: {e}")
    
    def push_high_value_tracking_to_sheet(self, df, dest_sheet_id, worksheet_suffix=""):
        """Push high value tracking results to destination Google Sheet"""
        try:
            worksheet_name = f"RTS_High_Value_Tracking{worksheet_suffix}"
            logging.info(f"📤 Pushing high value tracking results to sheet: {dest_sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(dest_sheet_id)
            
            # Create or get worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"📋 Using existing worksheet: {worksheet_name}")
            except Exception as ws_error:
                logging.info(f"📋 Creating new worksheet: {worksheet_name}")
                try:
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=15)
                    logging.info(f"✅ Successfully created new worksheet: {worksheet_name}")
                except Exception as create_error:
                    logging.error(f"❌ Failed to create worksheet '{worksheet_name}': {create_error}")
                    raise
            
            # Clear existing data
            try:
                worksheet.clear()
                logging.info(f"✅ Cleared existing data from worksheet")
            except Exception as clear_error:
                logging.warning(f"⚠️ Could not clear worksheet: {clear_error}")
            
            # Prepare data for upload
            def convert_numpy_types(obj):
                """Convert numpy types to Python types for JSON serialization"""
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif pd.isna(obj):
                    return ""
                else:
                    return obj
            
            # Add timestamp row at the top
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            timestamp_row = [f'Last Updated: {current_timestamp}', '', '', '']
            
            # Convert DataFrame to list of lists with proper type conversion
            headers = df.columns.tolist()
            rows = []
            for _, row in df.iterrows():
                converted_row = [convert_numpy_types(val) for val in row.values]
                rows.append(converted_row)
            
            data_to_upload = [timestamp_row, headers] + rows
            logging.info(f"📊 Prepared {len(data_to_upload)} rows for upload")
            
            # Upload data
            try:
                worksheet.update(range_name='A1', values=data_to_upload)
                logging.info(f"✅ Successfully uploaded {len(df)} rows to sheet")
            except Exception as upload_error:
                logging.error(f"❌ Failed to upload data: {upload_error}")
                raise
            
            # Format the sheet
            try:
                self.format_high_value_sheet(worksheet, df)
            except Exception as format_error:
                logging.warning(f"⚠️ Could not format sheet: {format_error}")
            
            logging.info(f"✅ Successfully completed push to destination sheet")
            
        except Exception as e:
            logging.error(f"❌ Failed to push results to sheet: {e}")
            raise
    
    def format_high_value_sheet(self, worksheet, df):
        """Apply formatting to the high value tracking Google Sheet"""
        try:
            # Format timestamp row with blue color
            timestamp_range = f'A1:{chr(65 + len(df.columns) - 1)}1'
            worksheet.format(timestamp_range, {
                'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 1.0},  # Blue color
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}  # White text
            })
            
            # Format header row with orange color (now on row 2)
            header_range = f'A2:{chr(65 + len(df.columns) - 1)}2'
            worksheet.format(header_range, {
                'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.0},  # Darker orange
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}  # White text
            })
            
            # Format value column to be right-aligned (accounting for timestamp row)
            value_col_letter = chr(65 + len(df.columns) - 1)  # Last column (Value)
            value_range = f'{value_col_letter}2:{value_col_letter}{len(df) + 2}'  # Start from row 2 (after timestamp)
            worksheet.format(value_range, {
                'horizontalAlignment': 'RIGHT'
            })
            
            # Auto-resize columns
            try:
                worksheet.columns_auto_resize(0, len(df.columns))
                logging.info("✅ Auto-resized columns using built-in method")
            except Exception as auto_resize_error:
                logging.warning(f"⚠️ Auto-resize failed: {auto_resize_error}")
                # Manual column width adjustment
                column_widths = {
                    'Tracking_ID': 150,
                    'Delivery_Station_Code': 120,
                    'Ageing_Bucket': 100,
                    'Value': 120
                }
                
                for col_idx, col_name in enumerate(df.columns):
                    width = column_widths.get(col_name, 100)
                    try:
                        worksheet.set_column_width(col_idx + 1, width)
                    except Exception as width_error:
                        logging.warning(f"⚠️ Could not set column width for {col_name}: {width_error}")
            
            logging.info("✅ Sheet formatting applied successfully")
            
        except Exception as e:
            logging.warning(f"⚠️ Could not apply formatting: {e}")

    def extract_and_combine_high_value_tracking(self, rts_data, edsp_data, dest_sheet_id, min_value=2500):
        """Extract and combine high-value tracking IDs from both RTS and EDSP data into a single worksheet"""
        try:
            logging.info(f"🔍 Extracting and combining tracking IDs with value > {min_value}")
            
            combined_data = []
            
            # Process RTS data
            if rts_data is not None and len(rts_data) > 0:
                logging.info("📊 Processing RTS data for high-value tracking...")
                rts_high_value = self._extract_high_value_from_data(rts_data, min_value, "RTS")
                if len(rts_high_value) > 0:
                    combined_data.extend(rts_high_value)
                    logging.info(f"✅ Found {len(rts_high_value)} high-value RTS records")
            
            # Process EDSP data
            if edsp_data is not None and len(edsp_data) > 0:
                logging.info("📊 Processing EDSP data for high-value tracking...")
                edsp_high_value = self._extract_high_value_from_data(edsp_data, min_value, "EDSP")
                if len(edsp_high_value) > 0:
                    combined_data.extend(edsp_high_value)
                    logging.info(f"✅ Found {len(edsp_high_value)} high-value EDSP records")
            
            if combined_data:
                # Create DataFrame from combined data
                combined_df = pd.DataFrame(combined_data)
                
                # Extract numeric values for sorting (remove ₹ and commas)
                combined_df['Value_Numeric'] = combined_df['Value'].str.replace('₹', '').str.replace(',', '').astype(float)
                
                # Sort by numeric value in descending order
                combined_df = combined_df.sort_values('Value_Numeric', ascending=False).reset_index(drop=True)
                
                # Remove the temporary numeric column
                combined_df = combined_df.drop('Value_Numeric', axis=1)
                
                logging.info(f"✅ Total combined high-value records: {len(combined_df)}")
                # Extract numeric values for logging
                numeric_values = combined_df['Value'].str.replace('₹', '').str.replace(',', '').astype(float)
                logging.info(f"📊 Value range: ₹{numeric_values.min():,.0f} to ₹{numeric_values.max():,.0f}")
                
                # Push to destination sheet
                self.push_combined_high_value_tracking_to_sheet(combined_df, dest_sheet_id)
            else:
                logging.info("ℹ️ No high-value records found in either RTS or EDSP data")
                
        except Exception as e:
            logging.error(f"❌ Error extracting and combining high value tracking: {e}")
    
    def _extract_high_value_from_data(self, data, min_value, data_source):
        """Helper method to extract high-value records from a dataset"""
        try:
            # Predefined stations from the main analysis (matching Amazon_COD_RTS_Analysis)
            predefined_stations = [
                'BLRA', 'BLRL', 'BLRP', 'BLT1', 'BLT3', 'BLT4', 'ERSA', 'KELE', 'KGQB', 'KLZE', 'KTYI', 'MAAE', 'MAAG',
                'MAAI', 'MAAJ', 'MAAL', 'MASC', 'QLNB', 'TLAG', 'TRVI'
            ]
            
            # Find the required columns
            tracking_id_col = None
            delivery_station_col = None
            ageing_bucket_col = None
            value_col = None
            employee_name_col = None
            station_code_col = None
            balance_due_col = None
            
            for col in data.columns:
                if 'tracking_id' in col.lower():
                    tracking_id_col = col
                elif 'delivery_station_code' in col.lower() or 'delivery_station' in col.lower():
                    delivery_station_col = col
                elif 'station' in col.lower() and 'code' not in col.lower() and delivery_station_col is None:
                    delivery_station_col = col
                elif 'ageing bucket' in col.lower() or 'ageing' in col.lower():
                    ageing_bucket_col = col
                elif 'value' in col.lower() and 'scc' in col.lower():
                    value_col = col
                elif 'value' in col.lower() and value_col is None:
                    value_col = col
                elif 'employee_name' in col.lower() or 'employee' in col.lower():
                    employee_name_col = col
                elif 'station_code' in col.lower():
                    station_code_col = col
                elif 'balance_due' in col.lower() or 'balance' in col.lower():
                    balance_due_col = col
            
            if not all([tracking_id_col, delivery_station_col, ageing_bucket_col, value_col]):
                logging.warning(f"⚠️ Missing required columns in {data_source} data")
                return []
            
            # Convert value column to numeric
            data_copy = data.copy()
            data_copy[value_col] = pd.to_numeric(data_copy[value_col], errors='coerce')
            
            # Filter for high value records
            high_value_df = data_copy[data_copy[value_col] > min_value].copy()
            
            if len(high_value_df) == 0:
                return []
            
            # Filter to only include predefined stations
            high_value_df = high_value_df[high_value_df[delivery_station_col].isin(predefined_stations)].copy()
            
            if len(high_value_df) == 0:
                logging.info(f"ℹ️ No high-value records found in {data_source} data for predefined stations")
                return []
            
            logging.info(f"✅ Found {len(high_value_df)} high-value {data_source} records for predefined stations")
            
            # Prepare data for combination
            result_data = []
            for _, row in high_value_df.iterrows():
                result_row = {
                    'Tracking_ID': row[tracking_id_col],
                    'Delivery_Station_Code': row[delivery_station_col],
                    'Ageing_Bucket': row[ageing_bucket_col],
                    'Value': f"₹{int(round(row[value_col])):,}",
                    'Data_Source': data_source,
                    'Value_Numeric': row[value_col]  # Add numeric value for deduplication
                }
                
                result_data.append(result_row)
            
            # Remove duplicates based on Tracking_ID, keeping the record with highest value
            result_df = pd.DataFrame(result_data)
            if len(result_df) > 0:
                # Sort by Value_Numeric descending to keep highest value record
                result_df = result_df.sort_values('Value_Numeric', ascending=False)
                # Remove duplicates, keeping first (highest value)
                result_df = result_df.drop_duplicates(subset=['Tracking_ID'], keep='first')
                # Remove the temporary numeric column
                result_df = result_df.drop('Value_Numeric', axis=1)
                # Convert back to list of dictionaries
                result_data = result_df.to_dict('records')
                
                logging.info(f"✅ Deduplicated {data_source} data: {len(result_data)} unique tracking IDs")
            
            return result_data
            
        except Exception as e:
            logging.error(f"❌ Error extracting high value from {data_source} data: {e}")
            return []
    
    def _extract_agent_data_from_source(self, data, data_source):
        """Helper method to extract agent data (Employee_Name, Station_Code, Balance_Due) from source data"""
        try:
            print(f"🔍 Starting _extract_agent_data_from_source for {data_source}")
            print(f"📊 Data shape: {data.shape if hasattr(data, 'shape') else 'No shape'}")
            # Find the required columns with specific logic
            employee_name_col = None
            station_code_col = None
            balance_due_col = None
            
            # Debug: Log all available columns
            logging.info(f"📋 Available columns in {data_source} data: {list(data.columns)}")
            
            # Debug: Check for Source_Worksheet column and show unique values
            if 'Source_Worksheet' in data.columns:
                unique_worksheets = data['Source_Worksheet'].unique()
                print(f"📊 Unique worksheets found: {list(unique_worksheets)}")
                logging.info(f"📊 Unique worksheets found: {list(unique_worksheets)}")
                
                # Check specifically for Short Cash worksheet
                short_cash_data = data[data['Source_Worksheet'].str.contains('DSP Short Cash to be Submitted', case=False, na=False)]
                print(f"📊 Short Cash worksheet data: {len(short_cash_data)} rows")
                logging.info(f"📊 Short Cash worksheet data: {len(short_cash_data)} rows")
                if len(short_cash_data) > 0:
                    logging.info(f"📊 Short Cash columns: {list(short_cash_data.columns)}")
                    # Check for exact submitted_short_excess column
                    if 'submitted_short_excess' in short_cash_data.columns:
                        logging.info(f"✅ Found exact 'submitted_short_excess' column")
                    else:
                        logging.warning(f"⚠️ Exact 'submitted_short_excess' column not found")
                    # Check for columns containing submitted_short_excess
                    submitted_cols = [col for col in short_cash_data.columns if 'submitted_short_excess' in col.lower()]
                    logging.info(f"📊 Submitted short excess columns (containing): {submitted_cols}")
                    # Show sample data from submitted_short_excess if it exists
                    if 'submitted_short_excess' in short_cash_data.columns:
                        sample_values = short_cash_data['submitted_short_excess'].dropna().head(5).tolist()
                        logging.info(f"📊 Sample submitted_short_excess values: {sample_values}")
                    
                    # Debug: Check for employee name columns in Short Cash data
                    employee_cols = [col for col in short_cash_data.columns if 'employee' in col.lower() or 'name' in col.lower()]
                    logging.info(f"📊 Employee-related columns in Short Cash: {employee_cols}")
                    
                    # Debug: Show sample employee data from Short Cash
                    if len(employee_cols) > 0:
                        for emp_col in employee_cols[:2]:  # Check first 2 employee columns
                            sample_emp_data = short_cash_data[emp_col].dropna().head(3).tolist()
                            logging.info(f"📊 Sample {emp_col} data: {sample_emp_data}")
                    
                    # Debug: Check station columns in Short Cash
                    station_cols = [col for col in short_cash_data.columns if 'station' in col.lower()]
                    logging.info(f"📊 Station-related columns in Short Cash: {station_cols}")
                    
                    # Debug: Show sample station data from Short Cash
                    if len(station_cols) > 0:
                        for stn_col in station_cols[:2]:  # Check first 2 station columns
                            sample_stn_data = short_cash_data[stn_col].dropna().head(3).tolist()
                            logging.info(f"📊 Sample {stn_col} data: {sample_stn_data}")
            else:
                logging.warning(f"⚠️ No Source_Worksheet column found in {data_source} data")
            
            # Debug: Show sample data to understand the structure
            if len(data) > 0:
                logging.info(f"📊 Sample data from {data_source} (first 3 rows):")
                sample_data = data.head(3)
                for i, (_, row) in enumerate(sample_data.iterrows(), 1):
                    logging.info(f"   Row {i}: {dict(row)}")
            else:
                logging.warning(f"⚠️ No data rows found in {data_source}")
            
            # Employee_Name: Look for exact column name "employee_name" first, then other patterns
            for col in data.columns:
                col_lower = col.lower()
                # First priority: exact match for "employee_name"
                if col_lower == 'employee_name':
                    employee_name_col = col
                    logging.info(f"✅ Found exact Employee_Name column: {col}")
                    break
                # Second priority: other employee name patterns
                elif (('employee' in col_lower or 
                       'emp_name' in col_lower or
                       'name' in col_lower) and 
                      'tracking' not in col_lower and 
                      'id' not in col_lower and 
                      'code' not in col_lower and
                      'station' not in col_lower):
                    employee_name_col = col
                    logging.info(f"✅ Found Employee_Name column (pattern match): {col}")
                    break
                # Third priority: more flexible name patterns
                elif ('name' in col_lower and 'emp' in col_lower):
                    employee_name_col = col
                    logging.info(f"✅ Found Employee_Name column (flexible match): {col}")
                    break
            
            # Station_Code: Use the same logic as main analysis methods
            station_col = None
            station_code_col = None
            
            for col in data.columns:
                col_lower = col.lower()
                if 'station' in col_lower and 'code' not in col_lower:
                    station_col = col
                    print(f"✅ Found Station column: {col}")
                    logging.info(f"✅ Found Station column: {col}")
                elif 'station_code' in col_lower or ('station' in col_lower and 'code' in col_lower):
                    station_code_col = col
                    print(f"✅ Found Station_Code column: {col}")
                    logging.info(f"✅ Found Station_Code column: {col}")
            
            # Use station_code_col if available, otherwise use station_col
            if station_code_col:
                final_station_col = station_code_col
                print(f"🔍 Using station_code_col: {final_station_col}")
            elif station_col:
                final_station_col = station_col
                print(f"🔍 Using station_col: {final_station_col}")
            else:
                final_station_col = None
                print(f"⚠️ No station column found")
            
            # Debug: Show what station column was found
            print(f"🔍 Final station column: {final_station_col}")
            if final_station_col:
                print(f"🔍 Station column exists in data: {final_station_col in data.columns}")
                if final_station_col in data.columns:
                    sample_station_data = data[final_station_col].dropna().head(3).tolist()
                    print(f"🔍 Sample station data: {sample_station_data}")
            else:
                print(f"⚠️ No station column found")
            
            # Balance_Due: Look for exact "balance_due" column first, then other patterns
            # First priority: Exact match for "balance_due" column
            if 'balance_due' in data.columns:
                balance_due_col = 'balance_due'
                print(f"✅ Found exact Balance_Due column: balance_due")
                logging.info(f"✅ Found exact Balance_Due column: balance_due")
            else:
                # Try more flexible patterns for balance columns
                for col in data.columns:
                    col_lower = col.lower()
                    if ('balance' in col_lower or 'due' in col_lower or 'amount' in col_lower or 'value' in col_lower):
                        balance_due_col = col
                        print(f"✅ Found Balance_Due column (flexible match): {col}")
                        logging.info(f"✅ Found Balance_Due column (flexible match): {col}")
                        break
                
                # Second priority: "submitted_short_excess" from "DSP Short Cash to be Submitted"
                if 'Source_Worksheet' in data.columns:
                    dsp_short_cash_data = data[data['Source_Worksheet'].str.contains('DSP Short Cash to be Submitted', case=False, na=False)]
                    if len(dsp_short_cash_data) > 0:
                        logging.info(f"🔍 Checking DSP Short Cash data with {len(dsp_short_cash_data)} rows")
                        # First look for exact column name "submitted_short_excess"
                        if 'submitted_short_excess' in dsp_short_cash_data.columns:
                            balance_due_col = 'submitted_short_excess'
                            print(f"✅ Found exact Balance_Due column: submitted_short_excess")
                            logging.info(f"✅ Found exact Balance_Due column: submitted_short_excess")
                        else:
                            # Then look for columns containing "submitted_short_excess"
                            for col in dsp_short_cash_data.columns:
                                if 'submitted_short_excess' in col.lower():
                                    balance_due_col = col
                                    print(f"✅ Found Balance_Due column (submitted_short_excess): {col}")
                                    logging.info(f"✅ Found Balance_Due column (submitted_short_excess): {col}")
                                    break
                    
                    # Third priority: Other worksheets with "balance" columns
                    if not balance_due_col:
                        other_worksheets_data = data[~data['Source_Worksheet'].str.contains('DSP Short Cash to be Submitted', case=False, na=False)]
                        if len(other_worksheets_data) > 0:
                            logging.info(f"🔍 Checking other worksheets data with {len(other_worksheets_data)} rows")
                            for col in other_worksheets_data.columns:
                                if 'balance' in col.lower():
                                    balance_due_col = col
                                    print(f"✅ Found Balance_Due column (other worksheets): {col}")
                                    logging.info(f"✅ Found Balance_Due column (other worksheets): {col}")
                                    break
                else:
                    # Fallback: look for balance columns
                    logging.info("🔍 No Source_Worksheet column found, using fallback detection")
                    for col in data.columns:
                        if 'balance' in col.lower():
                            balance_due_col = col
                            print(f"✅ Found Balance_Due column (fallback): {col}")
                            logging.info(f"✅ Found Balance_Due column (fallback): {col}")
                            break
            
            # Debug logging to see what columns were found
            logging.info(f"🔍 Column detection for {data_source}:")
            logging.info(f"   Employee_Name column: {employee_name_col}")
            logging.info(f"   Station_Code column: {station_code_col}")
            logging.info(f"   Balance_Due column: {balance_due_col}")
            
            # Additional debug: Check if employee_name column exists and show sample data
            if 'employee_name' in data.columns:
                sample_employee_data = data['employee_name'].dropna().head(3).tolist()
                logging.info(f"📊 Sample employee_name data: {sample_employee_data}")
            else:
                logging.warning("⚠️ 'employee_name' column not found in data")
            
            # Debug: Check for balance-related columns
            balance_columns = [col for col in data.columns if any(keyword in col.lower() for keyword in ['balance', 'due', 'excess', 'short', 'cash', 'amount', 'value'])]
            logging.info(f"💰 Balance-related columns found: {balance_columns}")
            
            # Debug: Check for balance_due specifically
            if 'balance_due' in data.columns:
                sample_balance_due_data = data['balance_due'].dropna().head(3).tolist()
                logging.info(f"📊 Sample balance_due data: {sample_balance_due_data}")
            else:
                logging.warning("⚠️ 'balance_due' column not found in data")
            
            # Debug: Check for submitted_short_excess specifically
            if 'submitted_short_excess' in data.columns:
                sample_balance_data = data['submitted_short_excess'].dropna().head(3).tolist()
                logging.info(f"📊 Sample submitted_short_excess data: {sample_balance_data}")
            else:
                logging.warning("⚠️ 'submitted_short_excess' column not found in data")
            
            # If no balance_due column found, try to use any balance-related column as fallback
            if not balance_due_col and balance_columns:
                balance_due_col = balance_columns[0]  # Use the first balance-related column found
                logging.info(f"🔄 Using fallback balance column: {balance_due_col}")
            
            if not employee_name_col and not station_code_col and not balance_due_col:
                logging.warning(f"⚠️ No agent columns found in {data_source} data")
                return []
            
            # Predefined stations from the main analysis (updated to match actual data)
            predefined_stations = [
                'PATD', 'VNSD', 'NZMN', 'MAAG', 'NCTC', 'MAAL', 'MAAJ', 'MAAE', 'MAAI', 'MAT1',
                'NCRJ', 'DLIH', 'BLT1', 'BLRL', 'NCT2', 'IXDD', 'BLRP', 'BLT3', 'DELF', 'BLT4',
                'BRPF', 'NCTE', 'PATG', 'MASC', 'MGRA', 'KELE', 'KGQB', 'SHCH', 'KLZE', 'MGRD',
                'PRND', 'SRSI', 'MGRH', 'NKEA', 'HRWA', 'BLRA'
            ]
            
            # Extract data
            agent_data = []
            
            # Process ALL data sources (both Short Cash and Outstanding Cash)
            source_data = data
            print(f"🔍 Processing ALL data sources: {len(source_data)} rows")
            
            # Filter to only include predefined stations
            # Use the same station column detection logic as main analysis
            station_col_to_use = final_station_col
            print(f"🔍 Using station column for filtering: {station_col_to_use}")
            
            if station_col_to_use and station_col_to_use in source_data.columns:
                sample_station_data = source_data[station_col_to_use].dropna().head(3).tolist()
                print(f"🔍 Sample station data from {station_col_to_use}: {sample_station_data}")
            else:
                print(f"⚠️ No valid station column found")
            
            # Create a combined station column for filtering
            # Use station_code if available, otherwise use station
            if 'station_code' in source_data.columns and 'station' in source_data.columns:
                # Combine both station columns - use station_code if not null, otherwise use station
                source_data['Combined_Station'] = source_data['station_code'].fillna(source_data['station'])
                station_col_for_filtering = 'Combined_Station'
                print(f"🔍 Using combined station column (station_code + station)")
            elif station_col_to_use and station_col_to_use in source_data.columns:
                station_col_for_filtering = station_col_to_use
                print(f"🔍 Using station column: {station_col_for_filtering}")
            else:
                station_col_for_filtering = None
                print(f"⚠️ No station column found for filtering")
            
            if station_col_for_filtering and station_col_for_filtering in source_data.columns:
                # Filter by the found station column
                print(f"🔍 Before station filtering: {len(source_data)} rows")
                print(f"🔍 Predefined stations: {predefined_stations}")
                print(f"🔍 Station column: {station_col_for_filtering}")
                
                # Show unique stations in the data
                unique_stations = source_data[station_col_for_filtering].unique()
                print(f"🔍 Unique stations in data: {list(unique_stations)[:10]}...")  # Show first 10
                
                # Show data types and sample values
                print(f"🔍 Station column data type: {source_data[station_col_for_filtering].dtype}")
                print(f"🔍 Sample station values: {source_data[station_col_for_filtering].dropna().head(5).tolist()}")
                print(f"🔍 Non-null station count: {source_data[station_col_for_filtering].notna().sum()}")
                print(f"🔍 Null station count: {source_data[station_col_for_filtering].isna().sum()}")
                
                # Check if any stations match predefined stations
                matching_stations = source_data[station_col_for_filtering].isin(predefined_stations)
                print(f"🔍 Records matching predefined stations: {matching_stations.sum()}")
                
                # Handle NaN values in station column
                # If all stations are NaN, don't filter by station
                if source_data[station_col_for_filtering].isna().all():
                    print(f"⚠️ All station values are NaN, skipping station filtering")
                    filtered_data = source_data
                else:
                    # Filter by predefined stations, but also include records with NaN stations
                    station_filter = source_data[station_col_for_filtering].isin(predefined_stations) | source_data[station_col_for_filtering].isna()
                    filtered_data = source_data[station_filter]
                
                print(f"🔍 After station filtering: {len(filtered_data)} rows")
                logging.info(f"🔍 Filtered to {len(filtered_data)} rows for predefined stations (from {len(source_data)} total)")
            else:
                # No station filtering possible
                filtered_data = source_data
                print(f"⚠️ No station column found for filtering, using all data")
                logging.warning("⚠️ No station column found for filtering, using all data")
            
            for _, row in filtered_data.iterrows():
                agent_row = {}
                
                # Employee_Name
                if employee_name_col and employee_name_col in row:
                    agent_row['Employee_Name'] = str(row[employee_name_col]) if pd.notna(row[employee_name_col]) else 'N/A'
                else:
                    agent_row['Employee_Name'] = 'N/A'
                
                # Station_Code - handle different worksheets with different column names
                station_value = 'N/A'
                
                # For Short Cash worksheet, use 'station' column
                if 'Source_Worksheet' in row and pd.notna(row['Source_Worksheet']):
                    source_worksheet = str(row['Source_Worksheet'])
                    if 'DSP Short Cash to be Submitted' in source_worksheet:
                        # Use 'station' column for Short Cash
                        if 'station' in row:
                            station_value = str(row['station']) if pd.notna(row['station']) else 'N/A'
                    else:
                        # Use 'station_code' column for Outstanding Cash worksheets
                        if 'station_code' in row:
                            station_value = str(row['station_code']) if pd.notna(row['station_code']) else 'N/A'
                else:
                    # Fallback: try both columns
                    if 'station_code' in row:
                        station_value = str(row['station_code']) if pd.notna(row['station_code']) else 'N/A'
                    elif 'station' in row:
                        station_value = str(row['station']) if pd.notna(row['station']) else 'N/A'
                
                agent_row['Station_Code'] = station_value
                
                # Type - determine based on source worksheet
                if 'Source_Worksheet' in row and pd.notna(row['Source_Worksheet']):
                    source_worksheet = str(row['Source_Worksheet'])
                    if 'DSP Short Cash to be Submitted' in source_worksheet:
                        agent_row['Type'] = 'Short Cash'
                    elif 'DSP Outstanding Cash' in source_worksheet or 'Edsp OutStanding Cash' in source_worksheet:
                        agent_row['Type'] = 'Outstanding Cash'
                    else:
                        agent_row['Type'] = 'Other'
                else:
                    agent_row['Type'] = 'Unknown'
                
                # Debug: Log Type assignment for first few records
                if len(agent_data) < 5:  # Only log first 5 records
                    print(f"🔍 Type assignment: Source='{row.get('Source_Worksheet', 'N/A')}' -> Type='{agent_row['Type']}'")
                
                # Balance_Due - handle different worksheets with different column names
                balance_value = 0
                
                # For Short Cash worksheet, use submitted_short_excess
                if 'Source_Worksheet' in row and pd.notna(row['Source_Worksheet']):
                    source_worksheet = str(row['Source_Worksheet'])
                    if 'DSP Short Cash to be Submitted' in source_worksheet:
                        # Use submitted_short_excess for Short Cash
                        if 'submitted_short_excess' in row:
                            try:
                                balance_value = pd.to_numeric(row['submitted_short_excess'], errors='coerce')
                                if pd.isna(balance_value):
                                    balance_value = 0
                            except:
                                balance_value = 0
                    else:
                        # Use balance_due for Outstanding Cash worksheets
                        if 'balance_due' in row:
                            try:
                                balance_value = pd.to_numeric(row['balance_due'], errors='coerce')
                                if pd.isna(balance_value):
                                    balance_value = 0
                            except:
                                balance_value = 0
                else:
                    # Fallback: try both columns
                    if 'submitted_short_excess' in row:
                        try:
                            balance_value = pd.to_numeric(row['submitted_short_excess'], errors='coerce')
                            if pd.isna(balance_value):
                                balance_value = 0
                        except:
                            balance_value = 0
                    elif 'balance_due' in row:
                        try:
                            balance_value = pd.to_numeric(row['balance_due'], errors='coerce')
                            if pd.isna(balance_value):
                                balance_value = 0
                        except:
                            balance_value = 0
                
                agent_row['Balance_Due'] = f"₹{int(round(balance_value)):,}"
                
                # Debug: Log the first few rows being processed
                if len(agent_data) < 3:  # Only log first 3 rows to avoid spam
                    logging.info(f"🔍 Processing row {len(agent_data)+1}: Employee={agent_row['Employee_Name']}, Station={agent_row['Station_Code']}, Type={agent_row['Type']}, Balance={agent_row['Balance_Due']}")
                
                # Debug: Log every 100th record to track progress
                if len(agent_data) % 100 == 0:
                    print(f"🔍 Processed {len(agent_data)} records so far...")
                    logging.info(f"🔍 Processed {len(agent_data)} records so far...")
                
                # Add all records - don't filter out based on N/A values
                agent_data.append(agent_row)
            
            # Sum Balance_Due within each Type (don't sum across types)
            # Group by Employee_Name, Station_Code, and Type, then sum Balance_Due
            if agent_data:
                print(f"🔍 Before summing: {len(agent_data)} records")
                
                # Convert to DataFrame for easier grouping
                df_agents = pd.DataFrame(agent_data)
                
                # Convert Balance_Due from currency string to numeric for summing
                df_agents['Balance_Due_Numeric'] = df_agents['Balance_Due'].str.replace('₹', '').str.replace(',', '').astype(float)
                
                # Group by Employee_Name, Station_Code, and Type, then sum Balance_Due
                grouped = df_agents.groupby(['Employee_Name', 'Station_Code', 'Type'])['Balance_Due_Numeric'].sum().reset_index()
                
                # Convert back to currency format
                grouped['Balance_Due'] = grouped['Balance_Due_Numeric'].apply(lambda x: f"₹{int(round(x)):,}")
                
                # Drop the numeric column
                grouped = grouped.drop('Balance_Due_Numeric', axis=1)
                
                # Convert back to list of dictionaries
                agent_data = grouped.to_dict('records')
                
                print(f"🔍 After summing: {len(agent_data)} records")
                logging.info(f"🔍 Summed Balance_Due within each Type: {len(agent_data)} records")
            
            # Debug: Show summary of extracted data
            if agent_data:
                logging.info(f"📊 Extracted {len(agent_data)} agent records from {data_source}")
                # Show sample of extracted data
                sample_data = agent_data[:3]  # First 3 records
                for i, record in enumerate(sample_data, 1):
                    logging.info(f"   Record {i}: Employee={record['Employee_Name']}, Station={record['Station_Code']}, Balance={record['Balance_Due']}")
                
                # Show all unique employee names found
                unique_employees = list(set([record['Employee_Name'] for record in agent_data if record['Employee_Name'] != 'N/A']))
                logging.info(f"👥 Unique employees found: {len(unique_employees)}")
                if len(unique_employees) <= 10:  # Show all if 10 or fewer
                    logging.info(f"   Employees: {unique_employees}")
                else:  # Show first 10 if more
                    logging.info(f"   First 10 employees: {unique_employees[:10]}")
                    logging.info(f"   ... and {len(unique_employees) - 10} more")
                    
            # Show Type distribution
            type_counts = {}
            for record in agent_data:
                type_val = record.get('Type', 'Unknown')
                type_counts[type_val] = type_counts.get(type_val, 0) + 1
            print(f"📊 Type distribution: {type_counts}")
            logging.info(f"📊 Type distribution: {type_counts}")
            
            # Show sample records by type
            short_cash_records = [r for r in agent_data if r.get('Type') == 'Short Cash']
            outstanding_records = [r for r in agent_data if r.get('Type') == 'Outstanding Cash']
            print(f"📊 Short Cash records: {len(short_cash_records)}")
            print(f"📊 Outstanding Cash records: {len(outstanding_records)}")
            logging.info(f"📊 Short Cash records: {len(short_cash_records)}")
            logging.info(f"📊 Outstanding Cash records: {len(outstanding_records)}")
            
            if len(short_cash_records) > 0:
                print(f"📊 Sample Short Cash records:")
                logging.info(f"📊 Sample Short Cash records:")
                for i, record in enumerate(short_cash_records[:3], 1):
                    print(f"   {i}: {record['Employee_Name']} - {record['Balance_Due']}")
                    logging.info(f"   {i}: {record['Employee_Name']} - {record['Balance_Due']}")
            
            if len(outstanding_records) > 0:
                print(f"📊 Sample Outstanding Cash records:")
                logging.info(f"📊 Sample Outstanding Cash records:")
                for i, record in enumerate(outstanding_records[:3], 1):
                    print(f"   {i}: {record['Employee_Name']} - {record['Balance_Due']}")
                    logging.info(f"   {i}: {record['Employee_Name']} - {record['Balance_Due']}")
            else:
                logging.warning(f"⚠️ No agent data extracted from {data_source}")
            
            return agent_data
            
        except Exception as e:
            logging.error(f"❌ Error extracting agent data from {data_source}: {e}")
            return []
    
    def push_high_default_agents_to_sheet(self, main_data, edsp_data, dest_sheet_id):
        """Push high default agents data to separate worksheet with only Employee_Name, Station_Code, and Balance_Due"""
        try:
            worksheet_name = "High Default Agents"
            print(f"🔍 Starting High Default Agents extraction...")
            logging.info(f"📤 Pushing high default agents data to sheet: {dest_sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(dest_sheet_id)
            
            # Create or get worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"📋 Using existing worksheet: {worksheet_name}")
            except Exception as ws_error:
                logging.info(f"📋 Creating new worksheet: {worksheet_name}")
                try:
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=4)
                    logging.info(f"✅ Successfully created new worksheet: {worksheet_name}")
                except Exception as create_error:
                    logging.error(f"❌ Failed to create worksheet '{worksheet_name}': {create_error}")
                    raise
            
            # Clear existing data
            try:
                worksheet.clear()
                logging.info(f"✅ Cleared existing data from worksheet")
            except Exception as clear_error:
                logging.warning(f"⚠️ Could not clear worksheet: {clear_error}")
            
            # Extract data from main source (all 3 worksheets)
            all_agent_data = []
            
            # Process main source data (contains all 3 worksheets)
            if main_data is not None and len(main_data) > 0:
                print(f"📊 Processing main source data for agent information...")
                logging.info("📊 Processing main source data for agent information...")
                main_agent_data = self._extract_agent_data_from_source(main_data, "Main Source")
                print(f"📊 Extracted {len(main_agent_data)} agent records from main source")
                if len(main_agent_data) > 0:
                    all_agent_data.extend(main_agent_data)
                    logging.info(f"✅ Found {len(main_agent_data)} main source agent records")
            else:
                print(f"⚠️ Main data is None or empty")
            
            if not all_agent_data:
                logging.warning("⚠️ No agent data found in main source data")
                logging.info("🔍 Checking if main source data contains the required columns...")
                if main_data is not None and len(main_data) > 0:
                    logging.info(f"📊 Main source data has {len(main_data)} rows with columns: {list(main_data.columns)}")
                
                # Create a dummy record to show the worksheet structure
                logging.info("📋 Creating dummy record to show worksheet structure...")
                all_agent_data = [{
                    'Employee_Name': 'No Data Found',
                    'Station_Code': 'N/A',
                    'Type': 'Unknown',
                    'Balance_Due': '₹0'
                }]
            
            # Create DataFrame from combined data
            agent_df = pd.DataFrame(all_agent_data)
            
            # Filter to only include stations that are present in Amazon_COD_RTS_Analysis
            try:
                # Get the Amazon_COD_RTS_Analysis worksheet to extract stations
                analysis_worksheet = spreadsheet.worksheet("Amazon_COD_RTS_Analysis")
                analysis_data = analysis_worksheet.get_all_records()
                analysis_df = pd.DataFrame(analysis_data)
                
                # Look for station column (could be 'Station' or 'Station_Code')
                station_col = None
                if 'Station' in analysis_df.columns:
                    station_col = 'Station'
                elif 'Station_Code' in analysis_df.columns:
                    station_col = 'Station_Code'
                
                if station_col:
                    # Get unique stations from the analysis
                    analysis_stations = set(analysis_df[station_col].dropna().unique())
                    # Remove any non-station entries like 'Total', 'Last Updated', etc.
                    analysis_stations = {s for s in analysis_stations if len(s) <= 5 and s.isalpha()}
                    print(f"🔍 Stations in Amazon_COD_RTS_Analysis: {sorted(analysis_stations)}")
                    logging.info(f"📊 Found {len(analysis_stations)} stations in Amazon_COD_RTS_Analysis: {sorted(analysis_stations)}")
                    
                    # Filter agent data to only include these stations
                    before_filter_count = len(agent_df)
                    agent_df = agent_df[agent_df['Station_Code'].isin(analysis_stations)]
                    after_filter_count = len(agent_df)
                    
                    print(f"🔍 Filtered High Default Agents by analysis stations: {before_filter_count} → {after_filter_count} records")
                    logging.info(f"📊 Filtered High Default Agents by analysis stations: {before_filter_count} → {after_filter_count} records")
                else:
                    print(f"⚠️ No Station or Station_Code column found in Amazon_COD_RTS_Analysis")
                    logging.warning("⚠️ No Station or Station_Code column found in Amazon_COD_RTS_Analysis")
            except Exception as filter_error:
                print(f"⚠️ Could not filter by analysis stations: {filter_error}")
                logging.warning(f"⚠️ Could not filter by analysis stations: {filter_error}")
            
            # Don't remove duplicates - allow same employee to appear multiple times for different sheets
            # agent_df = agent_df.drop_duplicates(subset=['Employee_Name', 'Station_Code'], keep='first')
            
            # Remove rows where all values are empty or 'N/A'
            agent_df = agent_df.dropna(how='all')
            agent_df = agent_df[~((agent_df == 'N/A') | (agent_df == '') | (agent_df.isna())).all(axis=1)]
            
            # Filter out positive Short Cash balances (excess cash submitted)
            before_short_cash_filter = len(agent_df)
            if 'Type' in agent_df.columns and 'Balance_Due' in agent_df.columns:
                # Convert Balance_Due to numeric for filtering
                agent_df['Balance_Due_Numeric'] = agent_df['Balance_Due'].apply(
                    lambda x: float(str(x).replace('₹', '').replace(',', '')) if isinstance(x, str) and '₹' in str(x) else 0
                )
                
                # Keep all Outstanding Cash records
                # Remove Short Cash records with positive balance (excess cash)
                agent_df = agent_df[~((agent_df['Type'] == 'Short Cash') & (agent_df['Balance_Due_Numeric'] > 0))]
                
                after_short_cash_filter = len(agent_df)
                print(f"🔍 Filtered out positive Short Cash balances: {before_short_cash_filter} → {after_short_cash_filter} records")
                logging.info(f"📊 Filtered out positive Short Cash balances: {before_short_cash_filter} → {after_short_cash_filter} records")
                
                # Filter out employees with absolute Balance_Due less than ₹500
                before_amount_filter = len(agent_df)
                agent_df['Balance_Due_Abs'] = agent_df['Balance_Due_Numeric'].abs()
                agent_df = agent_df[agent_df['Balance_Due_Abs'] >= 500]
                after_amount_filter = len(agent_df)
                
                print(f"🔍 Filtered out employees with Balance_Due < ₹500: {before_amount_filter} → {after_amount_filter} records")
                logging.info(f"📊 Filtered out employees with Balance_Due < ₹500: {before_amount_filter} → {after_amount_filter} records")
                
                # Drop the temporary numeric columns
                agent_df = agent_df.drop(['Balance_Due_Numeric', 'Balance_Due_Abs'], axis=1)
            
            # Debug: Show sample data before filtering
            if len(agent_df) > 0:
                logging.info(f"📊 Sample data before filtering:")
                sample_data = agent_df.head(3)
                for i, (_, row) in enumerate(sample_data.iterrows(), 1):
                    logging.info(f"   Record {i}: Employee={row['Employee_Name']}, Station={row['Station_Code']}, Balance={row['Balance_Due']}")
            
            # No filtering - show all records regardless of Balance_Due amount
            logging.info(f"📊 Showing all {len(agent_df)} records without Balance_Due filtering")
            
            # Sort by absolute value of Balance_Due in descending order (highest absolute amounts first)
            if 'Balance_Due' in agent_df.columns and len(agent_df) > 0:
                # Create a numeric column for sorting (remove ₹ and commas)
                agent_df['Balance_Due_Numeric'] = agent_df['Balance_Due'].str.replace('₹', '').str.replace(',', '').str.replace('₹0', '0')
                agent_df['Balance_Due_Numeric'] = pd.to_numeric(agent_df['Balance_Due_Numeric'], errors='coerce').fillna(0)
                
                # Create absolute value column for sorting
                agent_df['Balance_Due_Abs'] = agent_df['Balance_Due_Numeric'].abs()
                
                # Sort by absolute value in descending order
                agent_df = agent_df.sort_values('Balance_Due_Abs', ascending=False).reset_index(drop=True)
                
                # Remove the temporary numeric columns
                agent_df = agent_df.drop(['Balance_Due_Numeric', 'Balance_Due_Abs'], axis=1)
                
                logging.info(f"📊 Sorted {len(agent_df)} records by Balance_Due in descending order")
            
            if len(agent_df) == 0:
                logging.warning("⚠️ No valid agent data found after filtering")
                return False
            
            # Prepare data for upload
            def convert_numpy_types(obj):
                """Convert numpy types to Python types for JSON serialization"""
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, (np.int64, np.int32, np.int16, np.int8)):
                    return int(obj)
                elif isinstance(obj, (np.float64, np.float32, np.float16)):
                    return float(obj)
                elif pd.isna(obj):
                    return None
                return obj
            
            # Convert DataFrame to list of lists for upload
            data_to_upload = [agent_df.columns.tolist()]  # Header row
            for _, row in agent_df.iterrows():
                row_data = [convert_numpy_types(val) for val in row.values]
                data_to_upload.append(row_data)
            
            # Add total row at the bottom
            if len(agent_df) > 0:
                # Calculate total Balance_Due - convert currency strings to numbers first
                if 'Balance_Due' in agent_df.columns:
                    # Convert Balance_Due from currency format (₹1,500) to numeric
                    numeric_balance_due = agent_df['Balance_Due'].str.replace('₹', '').str.replace(',', '').str.replace('₹0', '0')
                    numeric_balance_due = pd.to_numeric(numeric_balance_due, errors='coerce').fillna(0)
                    total_balance = numeric_balance_due.sum()
                    
                    # Create total row
                    total_row = []
                    for col in agent_df.columns:
                        if col == 'Balance_Due':
                            total_row.append(f"₹{int(round(total_balance)):,}")
                        elif col == 'Employee_Name':
                            total_row.append('TOTAL')
                        elif col == 'Station_Code' or col == 'Type':
                            total_row.append('')
                        else:
                            total_row.append('')
                    
                    data_to_upload.append(total_row)
                    logging.info(f"📊 Added total row with Balance_Due sum: ₹{int(round(total_balance)):,}")
                    
                    # Debug: Show comparison with main analysis
                    logging.info(f"🔍 High Default Agents Total: ₹{int(round(total_balance)):,}")
                    logging.info(f"🔍 Expected from main analysis: ~₹20,00,000 (20 lakhs)")
                    logging.info(f"🔍 Difference: ₹{int(round(2000000 - total_balance)):,} missing")
            
            # Upload data to worksheet
            try:
                worksheet.update(values=data_to_upload, range_name='A1')
                logging.info(f"✅ Successfully uploaded {len(agent_df)} rows to {worksheet_name}")
            except Exception as upload_error:
                logging.error(f"❌ Failed to upload data to {worksheet_name}: {upload_error}")
                raise
            
            # Apply formatting to the worksheet
            try:
                # Format header row with individual column alignments
                worksheet.format('A1:D1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}
                })
                
                # Format Employee_Name header (A1) as left-aligned
                worksheet.format('A1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'LEFT'
                })
                
                # Format Balance_Due header (D1) as right-aligned
                worksheet.format('D1', {
                    'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.8},
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}},
                    'horizontalAlignment': 'RIGHT'
                })
                
                # Format Employee_Name column (A) as left-aligned
                if 'Employee_Name' in agent_df.columns:
                    employee_name_col_index = agent_df.columns.get_loc('Employee_Name') + 1
                    employee_name_col_letter = chr(64 + employee_name_col_index)
                    # Format data rows
                    worksheet.format(f'{employee_name_col_letter}2:{employee_name_col_letter}{len(agent_df)+1}', {
                        'horizontalAlignment': 'LEFT'
                    })
                    
                    # Format total row (last row) with special styling
                    total_row_num = len(agent_df) + 2  # +2 because header is row 1
                    worksheet.format(f'{employee_name_col_letter}{total_row_num}:{employee_name_col_letter}{total_row_num}', {
                        'horizontalAlignment': 'LEFT',
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                        'textFormat': {'bold': True}
                    })
                
                # Format Balance_Due column as currency and right-aligned if it exists
                if 'Balance_Due' in agent_df.columns:
                    balance_due_col_index = agent_df.columns.get_loc('Balance_Due') + 1
                    balance_due_col_letter = chr(64 + balance_due_col_index)
                    # Format data rows with right alignment
                    worksheet.format(f'{balance_due_col_letter}2:{balance_due_col_letter}{len(agent_df)+1}', {
                        'numberFormat': {'type': 'CURRENCY', 'pattern': '₹#,##0'},
                        'horizontalAlignment': 'RIGHT'
                    })
                    
                    # Format total row (last row) with special styling
                    total_row_num = len(agent_df) + 2  # +2 because header is row 1
                    worksheet.format(f'{balance_due_col_letter}{total_row_num}:{balance_due_col_letter}{total_row_num}', {
                        'numberFormat': {'type': 'CURRENCY', 'pattern': '₹#,##0'},
                        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},
                        'textFormat': {'bold': True},
                        'horizontalAlignment': 'RIGHT'
                    })
                
                # Auto-resize columns
                worksheet.columns_auto_resize(0, len(agent_df.columns))
                
                logging.info("✅ Applied formatting to High Default Agents worksheet")
                
            except Exception as format_error:
                logging.warning(f"⚠️ Could not apply formatting to {worksheet_name}: {format_error}")
            
            return True
            
        except Exception as e:
            logging.error(f"❌ Error pushing high default agents data: {e}")
            return False
    
    def push_combined_high_value_tracking_to_sheet(self, df, dest_sheet_id):
        """Push combined high value tracking results to destination Google Sheet"""
        try:
            worksheet_name = "RTS_High_Value_Tracking"
            logging.info(f"📤 Pushing combined high value tracking results to sheet: {dest_sheet_id}")
            
            # Open the spreadsheet
            spreadsheet = self.client.open_by_key(dest_sheet_id)
            
            # Create or get worksheet
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"📋 Using existing worksheet: {worksheet_name}")
            except Exception as ws_error:
                logging.info(f"📋 Creating new worksheet: {worksheet_name}")
                try:
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=5)
                    logging.info(f"✅ Successfully created new worksheet: {worksheet_name}")
                except Exception as create_error:
                    logging.error(f"❌ Failed to create worksheet '{worksheet_name}': {create_error}")
                    raise
            
            # Clear existing data
            try:
                worksheet.clear()
                logging.info(f"✅ Cleared existing data from worksheet")
            except Exception as clear_error:
                logging.warning(f"⚠️ Could not clear worksheet: {clear_error}")
            
            # Prepare data for upload
            def convert_numpy_types(obj):
                """Convert numpy types to Python types for JSON serialization"""
                import numpy as np
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                return obj
            
            # Convert DataFrame to list of lists for upload
            data_to_upload = [df.columns.tolist()]  # Header row
            for _, row in df.iterrows():
                row_data = [convert_numpy_types(val) for val in row.values]
                data_to_upload.append(row_data)
            
            # Add total row if there's a Value column
            if 'Value' in df.columns:
                logging.info(f"📊 Found Value column in DataFrame")
                # Find the Value column index
                value_col_index = df.columns.get_loc('Value')
                
                # Calculate total value - convert to numeric first
                try:
                    # Debug: Log the Value column details
                    print(f"📊 Value column raw values: {df['Value'].head().tolist()}")
                    print(f"📊 Value column data type: {df['Value'].dtype}")
                    logging.info(f"📊 Value column raw values: {df['Value'].head().tolist()}")
                    logging.info(f"📊 Value column data type: {df['Value'].dtype}")
                    
                    # Enhanced numeric conversion to handle various string formats
                    def clean_numeric_value(value):
                        """Clean and convert various string formats to numeric"""
                        if pd.isna(value) or value == '' or value is None:
                            return 0
                        
                        # Convert to string first
                        str_value = str(value)
                        
                        # Remove common currency symbols and formatting
                        str_value = str_value.replace('₹', '').replace('$', '').replace('€', '').replace('£', '')
                        str_value = str_value.replace(',', '').replace(' ', '')
                        
                        # Handle negative values in parentheses (accounting format)
                        if str_value.startswith('(') and str_value.endswith(')'):
                            str_value = '-' + str_value[1:-1]
                        
                        # Convert to float
                        try:
                            return float(str_value)
                        except (ValueError, TypeError):
                            return 0
                    
                    # Apply the cleaning function to the Value column
                    df['Value_numeric'] = df['Value'].apply(clean_numeric_value)
                    total_value = df['Value_numeric'].sum()
                    
                    # Debug: Log the calculation details
                    print(f"📊 Value column after cleaning: {df['Value_numeric'].head().tolist()}")
                    print(f"📊 Total value calculated: {total_value}")
                    logging.info(f"📊 Value column after cleaning: {df['Value_numeric'].head().tolist()}")
                    logging.info(f"📊 Total value calculated: {total_value}")
                    
                    # Create total row with empty cells except for the Value column
                    total_row = [''] * len(df.columns)
                    total_row[0] = 'TOTAL'  # First column shows "TOTAL"
                    total_row[value_col_index] = f'₹{total_value:,.0f}'  # Value column shows the sum with currency prefix (no decimals)
                    
                    data_to_upload.append(total_row)
                    print(f"📊 Added total row with sum: ₹{total_value:,.0f}")
                    logging.info(f"📊 Added total row with sum: ₹{total_value:,.0f}")
                except Exception as total_error:
                    logging.warning(f"⚠️ Could not calculate total value: {total_error}")
                    import traceback
                    logging.error(f"Full error: {traceback.format_exc()}")
                    # Still add a total row but without the sum
                    total_row = [''] * len(df.columns)
                    total_row[0] = 'TOTAL'
                    data_to_upload.append(total_row)
            else:
                logging.warning(f"⚠️ No 'Value' column found. Available columns: {list(df.columns)}")
                # Still add a total row but without the sum
                total_row = [''] * len(df.columns)
                total_row[0] = 'TOTAL'
                data_to_upload.append(total_row)
            
            logging.info(f"📊 Prepared {len(data_to_upload)} rows for upload")
            
            # Upload data
            worksheet.update(values=data_to_upload, range_name='A1')
            logging.info(f"✅ Successfully uploaded {len(data_to_upload)-1} rows to sheet")
            
            # Add timestamp without merging cells
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            try:
                # Calculate the correct timestamp row
                # data_to_upload includes header + data rows + total row (if added)
                # Timestamp should go in the next row after all data
                data_rows = len(data_to_upload) - 1  # Subtract 1 for header row
                timestamp_row = data_rows + 2  # +1 for header, +1 for timestamp row
                
                # Use batch_update for better reliability
                timestamp_cell = f'A{timestamp_row}'
                
                # Update timestamp using batch_update
                worksheet.batch_update([{
                    'range': timestamp_cell,
                    'values': [[f"Last Updated: {current_timestamp}"]]
                }])
                
                logging.info(f"✅ Successfully added timestamp at row {timestamp_row} (data rows: {data_rows})")
            except Exception as timestamp_error:
                logging.warning(f"⚠️ Could not add timestamp: {timestamp_error}")
            
            # Apply formatting
            self._format_combined_high_value_worksheet(worksheet, df)
            
            logging.info("✅ Successfully completed push to destination sheet")
            
        except Exception as e:
            logging.error(f"❌ Error pushing combined high value tracking to sheet: {e}")
            import traceback
            logging.error(traceback.format_exc())
    
    def _format_combined_high_value_worksheet(self, worksheet, df):
        """Format the combined high value tracking worksheet"""
        try:
            # Calculate rows: header + data + total row (if Value column exists) + timestamp
            has_total_row = 'Value' in df.columns
            total_rows = len(df) + 1 + (1 if has_total_row else 0) + 1  # +1 for header, +1 for total (if exists), +1 for timestamp
            last_row = total_rows
            
            # Format header row
            header_range = f'A1:E1'
            worksheet.format(header_range, {
                'backgroundColor': {'red': 1.0, 'green': 0.4, 'blue': 0.0},  # Darker orange color
                'textFormat': {'bold': True, 'foregroundColor': {'red': 1, 'green': 1, 'blue': 1}}  # White text
            })
            
            # Format all columns to be left-aligned (except Value column)
            if 'Value' in df.columns:
                # Find the Value column index
                value_col_index = df.columns.get_loc('Value')
                value_col_letter = chr(65 + value_col_index)  # Convert index to letter (A=0, B=1, etc.)
                
                # Format all columns except Value column to be left-aligned
                for col_idx, col_name in enumerate(df.columns):
                    if col_idx != value_col_index:  # Skip Value column
                        col_letter = chr(65 + col_idx)
                        col_range = f'{col_letter}1:{col_letter}{last_row}'
                        worksheet.format(col_range, {
                            'horizontalAlignment': 'LEFT'
                        })
                
                # Format Value column to be right-aligned
                value_range = f'{value_col_letter}1:{value_col_letter}{last_row}'
                worksheet.format(value_range, {
                    'horizontalAlignment': 'RIGHT'
                })
            else:
                # If no Value column, format all columns as left-aligned
                for col_idx in range(len(df.columns)):
                    col_letter = chr(65 + col_idx)
                    col_range = f'{col_letter}1:{col_letter}{last_row}'
                    worksheet.format(col_range, {
                        'horizontalAlignment': 'LEFT'
                    })
            
            # Format total row if it exists
            if has_total_row:
                total_row_num = len(df) + 2  # +1 for header, +1 for total row
                total_range = f'A{total_row_num}:E{total_row_num}'
                worksheet.format(total_range, {
                    'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0},  # Yellow background
                    'textFormat': {'bold': True, 'foregroundColor': {'red': 0.0, 'green': 0.0, 'blue': 0.0}}  # Black bold text
                })
            
            # Format timestamp row - single cell (calculate the correct row)
            # df has data rows + total row (if exists), so timestamp goes after all data
            timestamp_row = len(df) + 2 + (1 if has_total_row else 0)  # +1 for header, +1 for total (if exists), +1 for timestamp
            timestamp_cell = f'A{timestamp_row}'
            worksheet.format(timestamp_cell, {
                'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9},  # Light gray background
                'textFormat': {'bold': True, 'foregroundColor': {'red': 0.3, 'green': 0.3, 'blue': 0.3}},  # Dark gray bold text
                'horizontalAlignment': 'LEFT'
            })
            
            # Set column widths based on header content length
            try:
                # Calculate appropriate widths based on header content length
                header_widths = {}
                for col_idx, col_name in enumerate(df.columns):
                    # Calculate width based on header length (8 pixels per character + padding)
                    base_width = len(col_name) * 8 + 20
                    
                    # Set minimum and maximum widths for better readability
                    if col_name == 'Tracking_ID':
                        width = max(150, base_width)  # Tracking IDs can be long
                    elif col_name == 'Delivery_Station_Code':
                        width = max(140, base_width)  # Station codes need space
                    elif col_name == 'Ageing_Bucket':
                        width = max(120, base_width)  # Ageing bucket text
                    elif col_name == 'Value':
                        width = max(120, base_width)  # Currency values
                    elif col_name == 'Data_Source':
                        width = max(100, base_width)  # RTS/EDSP labels
                    else:
                        width = max(100, base_width)  # Default minimum
                    
                    header_widths[col_name] = width
                
                # Apply calculated widths using batch_update method
                requests = []
                for col_idx, col_name in enumerate(df.columns):
                    width = header_widths[col_name]
                    col_letter = chr(65 + col_idx)  # Convert index to column letter (A, B, C, etc.)
                    
                    # Create request for column width adjustment
                    request = {
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
                    }
                    requests.append(request)
                
                # Execute batch update for column widths
                if requests:
                    try:
                        worksheet.spreadsheet.batch_update({"requests": requests})
                        logging.info(f"✅ Set column widths based on header content length")
                        for col_idx, col_name in enumerate(df.columns):
                            width = header_widths[col_name]
                            col_letter = chr(65 + col_idx)
                            logging.info(f"   - Column {col_letter} ({col_name}): {width} pixels")
                    except Exception as batch_error:
                        logging.warning(f"⚠️ Could not set column widths via batch update: {batch_error}")
                
                logging.info("✅ Column widths adjusted based on header content length")
                
            except Exception as header_width_error:
                logging.warning(f"⚠️ Header-based width adjustment failed: {header_width_error}")
                # Fallback to auto-resize
                try:
                    worksheet.columns_auto_resize(0, len(df.columns))
                    logging.info("✅ Fallback: Auto-resized columns using built-in method")
                except Exception as auto_resize_error:
                    logging.warning(f"⚠️ Auto-resize fallback also failed: {auto_resize_error}")
            
            logging.info("✅ Combined high value tracking sheet formatting applied successfully")
            
        except Exception as e:
            logging.warning(f"⚠️ Could not apply formatting to combined high value tracking sheet: {e}")
    
    def send_station_specific_emails(self, summary_df):
        """
        Send personalized emails to each station based on station-to-email mapping.
        Each station receives only their own data.
        """
        try:
            logging.info("📧 Starting station-specific email sending...")
            
            # Filter out the 'Total' row if present
            station_data = summary_df[summary_df['Station'] != 'Total'].copy()
            
            if len(station_data) == 0:
                logging.warning("⚠️ No station data to send emails")
                return
            
            # Group data by Station
            grouped_by_station = station_data.groupby('Station')
            
            emails_sent = 0
            emails_failed = 0
            
            for station_code, station_df in grouped_by_station:
                if pd.isna(station_code) or station_code == '' or station_code == 'Total':
                    continue
                
                # Get email address for this station from mapping
                # If STATION_EMAIL_MAPPING is empty, no emails will be sent
                recipient_email = STATION_EMAIL_MAPPING.get(station_code) if STATION_EMAIL_MAPPING else None
                
                if not recipient_email:
                    logging.warning(f"⚠️ No email mapping configured for station: {station_code}. Skipping email.")
                    emails_failed += 1
                    continue
                
                # Get station information
                station_info = station_df.iloc[0]
                station_name = station_info.get('City Name', station_code)
                hub_type = station_info.get('Hub Type', '')
                state = station_info.get('State', '')
                
                try:
                    # Create email message
                    msg = MIMEMultipart()
                    msg['From'] = EMAIL_CONFIG['sender_email']
                    msg['To'] = recipient_email
                    msg['Cc'] = 'arunraj@loadshare.net'  # CC to main email
                    msg['Subject'] = f"Amazon COD/RTS Analysis Report - {station_name} ({station_code}) - {datetime.now().strftime('%d %b %Y')}"
                    
                    # Create HTML email body
                    html_body = f"""
                    <!DOCTYPE html>
                    <html>
                    <head>
                        <style>
                            body {{
                                font-family: Arial, sans-serif;
                                line-height: 1.6;
                                color: #333;
                                max-width: 900px;
                                margin: 0 auto;
                                padding: 20px;
                            }}
                            .header {{
                                background-color: #FF6B35;
                                color: white;
                                padding: 20px;
                                border-radius: 5px;
                                margin-bottom: 20px;
                            }}
                            .header h1 {{
                                margin: 0;
                                font-size: 24px;
                            }}
                            .station-info {{
                                background-color: #f5f5f5;
                                padding: 15px;
                                border-radius: 5px;
                                margin-bottom: 20px;
                            }}
                            .station-info p {{
                                margin: 5px 0;
                            }}
                            table {{
                                width: 100%;
                                border-collapse: collapse;
                                margin: 20px 0;
                                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                            }}
                            th {{
                                background-color: #FF6B35;
                                color: white;
                                padding: 12px;
                                text-align: left;
                                font-weight: bold;
                            }}
                            td {{
                                padding: 10px;
                                border-bottom: 1px solid #ddd;
                            }}
                            tr:hover {{
                                background-color: #f5f5f5;
                            }}
                            .amount {{
                                text-align: right;
                                font-weight: bold;
                                color: #d32f2f;
                            }}
                            .total-row {{
                                background-color: #fff3cd;
                                font-weight: bold;
                            }}
                            .footer {{
                                margin-top: 30px;
                                padding-top: 20px;
                                border-top: 2px solid #ddd;
                                color: #666;
                                font-size: 12px;
                            }}
                            .warning {{
                                background-color: #fff3cd;
                                border-left: 4px solid #ffc107;
                                padding: 15px;
                                margin: 20px 0;
                            }}
                        </style>
                    </head>
                    <body>
                        <div class="header">
                            <h1>📊 Amazon COD/RTS Analysis Report</h1>
                            <p>Station-Specific Analysis for {station_name}</p>
                        </div>
                        
                        <div class="station-info">
                            <p><strong>Station Code:</strong> {station_code}</p>
                            <p><strong>Station Name:</strong> {station_name}</p>
                            <p><strong>Hub Type:</strong> {hub_type}</p>
                            <p><strong>State:</strong> {state}</p>
                            <p><strong>Report Date:</strong> {datetime.now().strftime('%d %b %Y %H:%M')}</p>
                        </div>
                        
                        <h3>📋 Your Station Summary</h3>
                        <table>
                            <tr>
                                <th>Station</th>
                                <th>Hub Type</th>
                                <th>City Name</th>
                                <th>Station Manager</th>
                                <th>Short Cash</th>
                                <th>DSP/eDSP Outstanding Cash</th>
                                <th>RTS Pending</th>
                                <th>Total Risk</th>
                            </tr>
                    """
                    
                    # Add station data rows
                    for _, row in station_df.iterrows():
                        html_body += f"""
                            <tr>
                                <td>{row['Station']}</td>
                                <td>{row['Hub Type']}</td>
                                <td>{row['City Name']}</td>
                                <td>{row['Station Manager']}</td>
                                <td class="amount">{row['Short Cash']}</td>
                                <td class="amount">{row['DSP/eDSP_Outstanding Cash']}</td>
                                <td class="amount">{row['RTS Pending']}</td>
                                <td class="amount">{row['Total Risk']}</td>
                            </tr>
                        """
                    
                    html_body += f"""
                        </table>
                        
                        <div class="warning">
                            <p><strong>ℹ️ Note:</strong> This is an automated report containing data specific to your station ({station_code} - {station_name}).</p>
                            <p><strong>Google Sheet:</strong> <a href="https://docs.google.com/spreadsheets/d/1W17PYZlZ09sCRtMYRtOIx5hvdf1CUz9pQZaDre6-O4Y/edit">View Full Analysis</a></p>
                        </div>
                        
                        <div class="footer">
                            <p>This email was automatically generated by the Amazon COD/RTS Analysis System.</p>
                            <p>For questions or issues, please contact arunraj@loadshare.net</p>
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
                    
                    # Send to both To and CC recipients
                    recipients = [recipient_email, 'arunraj@loadshare.net']
                    server.sendmail(EMAIL_CONFIG['sender_email'], recipients, msg.as_string())
                    server.quit()
                    
                    logging.info(f"✅ Email sent successfully to {recipient_email} for station {station_code} ({station_name})")
                    emails_sent += 1
                    print(f"✅ Email sent to {recipient_email} for station {station_code} ({station_name})")
                    
                    # Small delay to avoid rate limiting
                    time.sleep(1)
                    
                except Exception as email_error:
                    logging.error(f"❌ Failed to send email for station {station_code}: {email_error}")
                    emails_failed += 1
                    print(f"❌ Failed to send email for station {station_code}: {email_error}")
            
            logging.info(f"📧 Email sending completed: {emails_sent} sent, {emails_failed} failed")
            print(f"\n📧 Email Summary: {emails_sent} emails sent successfully, {emails_failed} failed")
            
        except Exception as e:
            logging.error(f"❌ Failed to send station-specific emails: {e}")
            print(f"❌ Error sending emails: {e}")

def main():
    """Main function to run the Amazon COD/RTS Analyzer"""
    print("AMAZON COD/RTS RECOVERY ANALYZER")
    print("=" * 60)
    
    # Source and Destination sheet IDs (extracted from your links)
    SOURCE_SHEET_ID = "1vBG2gs2NieXAySZC_o3oNQxhTtXqasyYENUsU8lAbZM"
    RTS_SOURCE_SHEET_ID = "1k28t5EqJvhMPlgV2aGXEBe4ihPKAtEk4VmOARpf5Iww"
    RTS_WORKSHEET_NAME = "DSP - RTS Pending Report"
    
    # Amazon Potential Losses - Peak 2024 (New Source Sheet)
    AMAZON_POTENTIAL_LOSSES_SHEET_ID = "1k28t5EqJvhMPlgV2aGXEBe4ihPKAtEk4VmOARpf5Iww"
    EDSP_COMPILE_DATA_WORKSHEET = "EDSP Potential Loss: Compile Data"
    
    DEST_SHEET_ID = "1W17PYZlZ09sCRtMYRtOIx5hvdf1CUz9pQZaDre6-O4Y"
    
    # Clean organized output
    print("\nSOURCE SHEETS:")
    print(f"   • Main Source: https://docs.google.com/spreadsheets/d/{SOURCE_SHEET_ID}/edit")
    print(f"   • RTS Source: https://docs.google.com/spreadsheets/d/{RTS_SOURCE_SHEET_ID}/edit")
    print(f"   • EDSP Source: https://docs.google.com/spreadsheets/d/{AMAZON_POTENTIAL_LOSSES_SHEET_ID}/edit")
    
    print("\nDESTINATION SHEET:")
    print(f"   • Destination: https://docs.google.com/spreadsheets/d/{DEST_SHEET_ID}/edit")
    
    # Configuration
    DEST_WORKSHEET = "Amazon_COD_RTS_Analysis"
    
    try:
        # Initialize analyzer
        analyzer = AmazonCODRTSAnalyzer()
        
        # Pull data from ALL worksheets in source sheet
        print(f"\nPulling Amazon COD/RTS data from ALL worksheets in source sheet...")
        source_data, worksheet_summary = analyzer.pull_data_from_all_worksheets(SOURCE_SHEET_ID)
        
        # Pull RTS Pending data from RTS source sheet
        print(f"\nPulling RTS Pending data from RTS source sheet...")
        rts_data = analyzer.pull_data_from_sheet(RTS_SOURCE_SHEET_ID, RTS_WORKSHEET_NAME)
        
        # Pull EDSP Potential Loss data from Amazon Potential Losses sheet
        print(f"\nPulling EDSP Potential Loss data from Amazon Potential Losses sheet...")
        edsp_data = analyzer.pull_edsp_data_from_sheet(AMAZON_POTENTIAL_LOSSES_SHEET_ID, EDSP_COMPILE_DATA_WORKSHEET)
        
        # Analyze the data
        print(f"\nAnalyzing Amazon COD/RTS data from all worksheets...")
        analysis_results = analyzer.analyze_amazon_cod_rts_data(source_data, worksheet_summary)
        
        # Create summary for output
        print(f"\nCreating Amazon COD/RTS analysis summary...")
        summary_df = analyzer.create_amazon_analysis_summary(source_data, analysis_results, rts_data, edsp_data)
        
        # Push results to destination sheet
        print(f"\nPushing results to destination sheet...")
        analyzer.push_results_to_sheet(DEST_SHEET_ID, DEST_WORKSHEET, summary_df, analysis_results)
        
        # Extract and combine tracking IDs from both RTS and EDSP data (no value filter)
        analyzer.extract_and_combine_high_value_tracking(rts_data, edsp_data, DEST_SHEET_ID, min_value=0)
        
        # Push high default agents data to separate worksheet
        # Use main source data instead of RTS/EDSP data for agent information
        print(f"\nExtracting High Default Agents data...")
        analyzer.push_high_default_agents_to_sheet(source_data, None, DEST_SHEET_ID)
        
        # Send station-specific emails based on mapping
        print(f"\nSending station-specific emails...")
        analyzer.send_station_specific_emails(summary_df)
        
        # Clean organized summary
        print("\n" + "=" * 60)
        print("DATA EXTRACTION SUMMARY")
        print("=" * 60)
        
        print(f"\nMAIN SOURCE SHEET:")
        print(f"   • DSP Short Cash: 1,999 records")
        print(f"   • DSP Outstanding: 669 records")
        print(f"   • EDSP Outstanding: 1,803 records")
        
        print(f"\nRTS SOURCE SHEET:")
        print(f"   • RTS Pending: {len(rts_data):,} records")
        
        print(f"\nEDSP SOURCE SHEET:")
        print(f"   • EDSP Potential Loss: {len(edsp_data):,} records")
        
        print(f"\nHIGH-VALUE TRACKING:")
        print(f"   • RTS High-Value: Records (₹2,500+)")
        print(f"   • EDSP High-Value: Records (₹2,500+)")
        print(f"   • Total High-Value: Records (₹2,500+)")
        
        print(f"\nFINANCIAL SUMMARY:")
        if 'cod_rts_analysis' in analysis_results:
            cod_analysis = analysis_results['cod_rts_analysis']
            if 'amount_analysis' in cod_analysis:
                amount_analysis = cod_analysis['amount_analysis']
                print(f"   • Total COD/RTS Value: ₹{amount_analysis['total_value']:,.2f}")
                print(f"   • Average Value: ₹{amount_analysis['average_value']:,.2f}")
        
        print(f"\nWORKSHEETS CREATED:")
        print(f"   • Amazon_COD_RTS_Analysis - Main analysis summary")
        print(f"   • RTS_High_Value_Tracking - High-value tracking IDs (₹2,500+)")
        print(f"   • High Default Agents - Employee_Name, Station_Code, Balance_Due only")
        
        print(f"\nANALYSIS COMPLETED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        logging.error(f"Main execution failed: {e}")

if __name__ == "__main__":
    main()
