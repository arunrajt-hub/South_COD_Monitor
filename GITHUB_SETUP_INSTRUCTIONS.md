# GitHub Repository Setup Instructions

## Step-by-Step Guide

### 1. Create GitHub Repository

1. Go to [GitHub](https://github.com) and sign in
2. Click the **+** icon in the top right → **New repository**
3. Repository name: `South_COD_Monitor`
4. Description: "Automated South COD Monitor - Daily scheduled reports"
5. Visibility: **Private** (recommended for sensitive data)
6. **DO NOT** initialize with README, .gitignore, or license
7. Click **Create repository**

### 2. Initialize Local Repository

Open terminal/PowerShell in your project directory and run:

```bash
# Initialize git repository
git init

# Add all files
git add South_COD_Monitor.py
git add requirements_south_cod_monitor.txt
git add .github/workflows/daily_schedule.yml
git add README.md
git add .gitignore

# Commit files
git commit -m "Initial commit: South COD Monitor with GitHub Actions workflow"

# Add remote repository (replace YOUR_USERNAME with your GitHub username)
git remote add origin https://github.com/YOUR_USERNAME/South_COD_Monitor.git

# Push to GitHub
git branch -M main
git push -u origin main
```

### 3. Configure GitHub Secrets

1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** for each secret:

#### Secret 1: SERVICE_ACCOUNT_KEY
- **Name**: `SERVICE_ACCOUNT_KEY`
- **Value**: Open your `service_account_key.json` file and copy the **entire JSON content**
- Click **Add secret**

#### Secret 2: GMAIL_SENDER_EMAIL
- **Name**: `GMAIL_SENDER_EMAIL`
- **Value**: `arunraj@loadshare.net` (or your email)
- Click **Add secret**

#### Secret 3: GMAIL_APP_PASSWORD
- **Name**: `GMAIL_APP_PASSWORD`
- **Value**: Your 16-character Gmail App Password
- Click **Add secret**

### 4. Get Gmail App Password

1. Go to [Google Account](https://myaccount.google.com/)
2. Click **Security** in the left sidebar
3. Under "Signing in to Google", click **2-Step Verification** (enable if not already)
4. Scroll down and click **App passwords**
5. Select app: **Mail**
6. Select device: **Other (Custom name)**
7. Enter name: "GitHub Actions"
8. Click **Generate**
9. Copy the 16-character password (no spaces)
10. Use this as your `GMAIL_APP_PASSWORD` secret

### 5. Verify Service Account Access

1. Find your service account email in `service_account_key.json`:
   - Look for `"client_email"` field
   - Example: `south-cod-monitor@project-id.iam.gserviceaccount.com`

2. Share Google Sheets with service account:
   - Open source sheet: `https://docs.google.com/spreadsheets/d/1t04OxK-GdiDDUq85HNKtyDO2GqYDBoX2eG0M34aR3jA`
   - Click **Share** → Enter service account email → **Editor** → **Send**
   
   - Open output sheet: `https://docs.google.com/spreadsheets/d/1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM`
   - Click **Share** → Enter service account email → **Editor** → **Send**

### 6. Test the Workflow

1. Go to your repository → **Actions** tab
2. You should see "South COD Monitor - Daily Schedule" workflow
3. Click on it → **Run workflow** → **Run workflow** (manual trigger)
4. Wait for it to complete (usually 2-5 minutes)
5. Check the logs for any errors

### 7. Verify Schedule

The workflow is scheduled to run daily at:
- **1:00 PM IST** (7:30 AM UTC)

To verify:
1. Go to **Actions** tab
2. You should see scheduled runs appearing daily
3. Check the time matches your timezone

## Troubleshooting

### Workflow Not Running

- Check if workflow file is in `.github/workflows/` directory
- Verify cron syntax: `'0 13 * * *'` (1 PM UTC)
- Check repository settings → Actions → ensure Actions are enabled

### Authentication Errors

- Verify `SERVICE_ACCOUNT_KEY` secret contains complete JSON
- Ensure service account email has access to Google Sheets
- Check service account key hasn't expired

### Email Not Sending

- Verify `GMAIL_APP_PASSWORD` is correct (16 characters, no spaces)
- Check `GMAIL_SENDER_EMAIL` matches the account with App Password
- Ensure 2-Step Verification is enabled on Gmail account

### Import Errors

- Check `requirements_south_cod_monitor.txt` has all dependencies
- Verify Python version in workflow (currently 3.9)

## Next Steps

1. Monitor first few runs to ensure everything works
2. Check email recipients receive reports
3. Adjust schedule if needed (modify cron in workflow file)
4. Set up notifications for workflow failures (GitHub Settings → Notifications)

## Schedule Time Reference

Current schedule: `30 7 * * *` = 1:00 PM IST (7:30 AM UTC)

To change the schedule, edit `.github/workflows/daily_schedule.yml`:

- **IST (UTC+5:30)**: For 1 PM IST, use `30 7 * * *` (7:30 AM UTC)
- **Other times**: Use [crontab.guru](https://crontab.guru/) to generate cron expressions

## Security Notes

⚠️ **IMPORTANT**:
- Never commit `service_account_key.json` to GitHub
- Never commit Gmail passwords
- Keep repository private
- Regularly rotate App Passwords
- Review workflow logs for sensitive data exposure

