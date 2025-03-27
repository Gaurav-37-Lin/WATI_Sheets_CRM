import os
import time
import sys
import logging
from flask import Flask, request, jsonify, redirect
from apscheduler.schedulers.background import BackgroundScheduler
from rentmax_analysis import process_all_files, post_journey_to_apps_script
import requests
import json
import pandas as pd

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Set up environment variables and directories.
# LOG_FOLDER: Folder for log files (should be persistent on Render).
# WATI_WEBHOOK_TOKEN: Token to verify incoming webhook requests.
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")
os.makedirs(LOG_FOLDER, exist_ok=True)
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

# Zoho OAuth and API credentials (for the India data center).
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.environ.get("ZOHO_REDIRECT_URI")  # e.g., "https://wati-sheets-crm.onrender.com/oauth/callback"
ZOHO_REFRESH_TOKEN = os.environ.get("ZOHO_REFRESH_TOKEN")

############################################
# Basic Endpoints
############################################

@app.route("/")
def index():
    """
    Root endpoint: returns a status message and shows where log files are stored.
    """
    abs_log_path = os.path.abspath(LOG_FOLDER)
    return f"Webhook is running. Log files are stored in: {abs_log_path}"

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    """
    Webhook endpoint to receive data from WATI.
    Validates the token, parses the JSON payload, and appends the message to a log file.
    """
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # Try to parse the JSON payload.
    try:
        data = request.get_json(force=True)
    except Exception as e:
        app.logger.error("Failed to parse JSON payload: %s", e)
        return jsonify({"status": "invalid json"}), 400

    if not data:
        return jsonify({"status": "no data"}), 400

    # Extract WhatsApp ID and timestamp from the payload.
    wa_id = data.get("waId", "unknown")
    raw_ts = data.get("timestamp", "")
    try:
        # Try to interpret raw_ts as an epoch value.
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except Exception:
        # Fallback: use the raw timestamp string.
        time_str = str(raw_ts)

    # Determine the sender name: if 'owner' is True, use operatorName; otherwise use senderName.
    sender_name = data.get("operatorName", "Bot") if data.get("owner") else data.get("senderName", "User")
    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

    # Determine the log file path based on the WhatsApp ID.
    log_file = os.path.join(LOG_FOLDER, f"{wa_id}.txt")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
        app.logger.info("Appended log line to: %s", os.path.abspath(log_file))
    except Exception as e:
        app.logger.error("Error writing to %s: %s", log_file, e)

    app.logger.info("WATI Webhook data received: %s", data)
    return jsonify({"status": "received"}), 200

############################################
# OAuth Callback Endpoint for Zoho
############################################

@app.route("/oauth/callback")
def oauth_callback():
    """
    OAuth callback endpoint that handles the redirect from Zoho.
    It expects a 'code' parameter in the query string and exchanges it for an access token.
    """
    code = request.args.get("code")
    state = request.args.get("state")
    if not code:
        return "Error: No authorization code provided.", 400

    # Use Zoho India endpoint for token exchange.
    token_url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {
        "code": code,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "redirect_uri": ZOHO_REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    try:
        response = requests.post(token_url, data=payload)
    except Exception as e:
        app.logger.error("Exception during token exchange: %s", e)
        return f"Exception during token exchange: {e}", 500

    if response.status_code == 200:
        try:
            token_data = response.json()
        except Exception as e:
            app.logger.error("Failed to parse token exchange response: %s", e)
            return f"Failed to parse token exchange response: {e}", 500

        # In production, store token_data (access token, refresh token) securely.
        return jsonify({
            "message": "OAuth callback successful. Tokens received.",
            "token_data": token_data,
            "state": state
        })
    else:
        app.logger.error("Token exchange failed: %s", response.text)
        return f"Token exchange failed: {response.text}", response.status_code

############################################
# Zoho Token Refresh and CRM Integration
############################################

def get_zoho_access_token():
    """
    Refreshes the Zoho access token using the refresh token.
    Returns a new access token or None if the refresh fails.
    """
    data = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    token_url = "https://accounts.zoho.in/oauth/v2/token"
    try:
        response = requests.post(token_url, data=data)
    except Exception as e:
        app.logger.error("Exception during token refresh: %s", e)
        return None

    if response.status_code == 200:
        try:
            token_data = response.json()
            access_token = token_data.get("access_token")
            if access_token:
                app.logger.info("Obtained new Zoho access token.")
                return access_token
            else:
                app.logger.error("Access token not found in response: %s", token_data)
                return None
        except Exception as e:
            app.logger.error("Failed to parse token refresh response: %s", e)
            return None
    else:
        app.logger.error("Failed to refresh token: %s", response.text)
        return None

def push_to_zoho_crm(journey):
    """
    Pushes a journey record to Zoho CRM as a new Lead.
    This function first refreshes the access token, then maps the journey data to Zoho fields,
    and finally posts it to the Zoho CRM Leads endpoint.
    """
    access_token = get_zoho_access_token()
    if not access_token:
        app.logger.error("Cannot push to Zoho CRM without a valid access token.")
        return

    headers = {
        "Authorization": "Zoho-oauthtoken " + access_token,
        "Content-Type": "application/json"
    }
    # Map journey record to Zoho CRM Lead fields. Adjust as needed.
    lead_data = {
        "data": [{
            "Last_Name": journey.get("username", "Unknown"),
            "Phone": journey.get("mobile_number", ""),
            "Lead_Source": "WATI Chatbot",
            "Description": json.dumps(journey)  # Store full journey details; customize as needed.
        }]
    }
    create_url = "https://www.zohoapis.in/crm/v2/Leads"
    try:
        response = requests.post(create_url, headers=headers, json=lead_data, timeout=10)
        app.logger.info("Zoho CRM response: %s %s", response.status_code, response.text)
    except Exception as e:
        app.logger.error("Exception while pushing to Zoho CRM: %s", e)

############################################
# Scheduled Log Processing
############################################

def process_logs():
    """
    Scheduled job that processes log files, extracts journey records,
    and pushes them to Google Sheets and Zoho CRM.
    """
    app.logger.info("Starting scheduled log processing...")
    abs_log_path = os.path.abspath(LOG_FOLDER)
    app.logger.info("DEBUG: Searching for .txt files in: %s", abs_log_path)
    
    try:
        records = process_all_files()
    except Exception as e:
        app.logger.error("Error processing files: %s", e)
        return

    app.logger.info("DEBUG: Total records extracted: %s", len(records))
    if not records:
        app.logger.warning("No journeys extracted. Check if the expected bot prompt is present in the logs.")
    
    for journey in records:
        try:
            # Post journey to Google Sheets first (existing integration)
            post_journey_to_apps_script(journey)
        except Exception as e:
            app.logger.error("Error posting journey to Google Sheets: %s", e)
        try:
            # Then push the journey to Zoho CRM.
            push_to_zoho_crm(journey)
        except Exception as e:
            app.logger.error("Error pushing journey to Zoho CRM: %s", e)
    
    app.logger.info("Finished processing logs.")

############################################
# Application Startup
############################################

if __name__ == "__main__":
    # Set up a background scheduler to process logs every minute.
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=process_logs, trigger="interval", minutes=1)
    scheduler.start()
    
    try:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
