# South COD Monitor

Automated monitoring system for South COD (Cash on Delivery) data extraction, analysis, and reporting.

## Overview

This script extracts COD data from Google Sheets, calculates Actual Gap metrics, and sends automated email alerts when increases are detected. It runs daily at 1:00 PM UTC (6:30 PM IST) via GitHub Actions.

## Features

- 📊 Extracts data from Google Sheets Dashboard
- 🔍 Filters data for 21 specific hubs
- 📈 Calculates Actual Gap metrics
- 📧 Sends email alerts when Actual Gap increases
- 📝 Generates Excel reports
- 🔄 Updates Google Sheets with formatted data
- 🎨 Preserves manually editable columns (Van Adhoc, Legal Issue, Old Balance)

## Repository Setup

### 1. Create GitHub Repository

1. Go to GitHub and create a new repository named `South_COD_Monitor`
2. Make it private (recommended for sensitive data)
3. Clone the repository locally

### 2. Copy Required Files

Copy these files to the repository:
- `South_COD_Monitor.py` - Main script
- `requirements_south_cod_monitor.txt` - Python dependencies
- `.github/workflows/daily_schedule.yml` - GitHub Actions workflow
- `README.md` - This file
- `.gitignore` - Git ignore file

### 3. Configure GitHub Secrets

Go to your repository → Settings → Secrets and variables → Actions → New repository secret

Add the following secrets:

#### Required Secrets:

1. **SERVICE_ACCOUNT_KEY**
   - Value: Complete JSON content of your `service_account_key.json` file
   - How to get: Copy the entire contents of the JSON file
   - Example format:
     ```json
     {
       "type": "service_account",
       "project_id": "...",
       "private_key_id": "...",
       ...
     }
     ```

2. **GMAIL_SENDER_EMAIL**
   - Value: Your Gmail address (e.g., `arunraj@loadshare.net`)
   - Used for sending email notifications

3. **GMAIL_APP_PASSWORD**
   - Value: Gmail App Password (16-character password)
   - How to get:
     1. Go to Google Account → Security
     2. Enable 2-Step Verification
     3. Go to App Passwords
     4. Generate a new app password for "Mail"
     5. Copy the 16-character password

### 4. Verify Google Sheets Access

Ensure your service account email has access to:
- **Source Sheet**: `1t04OxK-GdiDDUq85HNKtyDO2GqYDBoX2eG0M34aR3jA` (Dashboard worksheet)
- **Output Sheet**: `1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM` (COD Monitor worksheet)

Share both sheets with the service account email (Editor access).

## Workflow Schedule

The script runs automatically:
- **Schedule**: Daily at 1:00 PM IST (7:30 AM UTC)
- **Manual Trigger**: Available via GitHub Actions → Run workflow

## Email Configuration

### Recipients:
- **TO**: All hub emails, CLM emails, Lokesh, Bharath, and Maligai Rasmeen
- **BCC**: Rakib only

### Email Content:
- Full COD Monitor data table
- Alert section when Actual Gap increases are detected
- Highlighted rows for hubs with increases

## Local Testing

To test the script locally:

1. Install dependencies:
   ```bash
   pip install -r requirements_south_cod_monitor.txt
   ```

2. Ensure `service_account_key.json` is in the same directory

3. Set environment variables (optional, defaults are in script):
   ```bash
   export GMAIL_SENDER_EMAIL="your-email@loadshare.net"
   export GMAIL_APP_PASSWORD="your-app-password"
   ```

4. Run the script:
   ```bash
   python South_COD_Monitor.py
   ```

## Output Files

- `South_COD_Monitor_Report.xlsx` - Excel report (generated locally, not uploaded to GitHub)

## Monitoring

- Check workflow runs: GitHub → Actions tab
- View logs: Click on any workflow run to see detailed logs
- Error notifications: Failed runs will upload logs as artifacts

## Troubleshooting

### Workflow Fails

1. Check Actions tab for error messages
2. Verify all secrets are set correctly
3. Ensure service account has access to Google Sheets
4. Check Gmail App Password is valid

### Email Not Sending

1. Verify `GMAIL_SENDER_EMAIL` and `GMAIL_APP_PASSWORD` secrets
2. Check Gmail App Password hasn't expired
3. Review script logs in GitHub Actions

### Google Sheets Access Issues

1. Verify service account email has Editor access
2. Check spreadsheet IDs are correct
3. Ensure worksheet names match exactly

## Configuration

Key configuration variables in `South_COD_Monitor.py`:

- `SPREADSHEET_ID`: Source Google Sheet ID
- `OUTPUT_SPREADSHEET_ID`: Destination Google Sheet ID
- `TEST_MODE`: Set to `True` to send test emails only
- `EMAIL_ENABLED`: Set to `False` to disable email sending

## Support

For issues or questions, check the workflow logs or contact the development team.

