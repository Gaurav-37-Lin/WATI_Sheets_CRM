import os
import time
import sys
import logging
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from rentmax_analysis import process_all_files, post_journey_to_apps_script
import requests
import json
import pandas as pd

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

############################################
# Environment Variables for Zoho Credentials
############################################
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")
os.makedirs(LOG_FOLDER, exist_ok=True)
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

# Zoho OAuth & API credentials (for India data center)
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.environ.get("ZOHO_REDIRECT_URI")  # e.g. "https://wati-sheets-crm.onrender.com/oauth/callback"
ZOHO_REFRESH_TOKEN = os.environ.get("ZOHO_REFRESH_TOKEN")

############################################
# Hardcoded Zoho Field API Keys
############################################
# Make sure these match exactly the API names in your Zoho CRM Leads module.
JOURNEY_ATTEMPTS_FIELD = "Journey_Attempts"
RENT_TENANT_CITY_FIELD = "Rent_Tenant_City"
RENT_TENANT_CONFIG_FIELD = "Rent_Tenant_Configuration"
RENT_TENANT_CONFIG_MORE_FIELD = "Rent_Tenant_Configuration_More"
RENT_TENANT_LOCALITY_FIELD = "Rent_Tenant_Locality"
RENT_TENANT_BUDGET_WRONG_FIELD = "Rent_Tenant_Budget_Wrong"
RENT_TENANT_BUDGET_CORRECT_FIELD = "Rent_Tenant_Budget_Correct"
RENT_TENANT_EMAIL_FIELD = "Rent_Tenant_Email"
RENT_TENANT_EST_MOVE_IN_FIELD = "Rent_Tenant_Est_Move_In"
# If you have additional fields like "Intro_Selection" or "Main_Selection", you can define them here:
# INTRO_SELECTION_FIELD = "Intro_Selection"
# MAIN_SELECTION_FIELD = "Main_Selection"

############################################
# Flask Endpoints
############################################

@app.route("/")
def index():
    """Returns a status message and shows the log folder location."""
    abs_log_path = os.path.abspath(LOG_FOLDER)
    return f"Webhook is running. Log files are stored in: {abs_log_path}"

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    """
    Receives webhook data from WATI, verifies the token, and appends the log line
    to a file named after the mobile number (waId).
    """
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    try:
        data = request.get_json(force=True)
    except Exception as e:
        app.logger.error("Failed to parse JSON payload: %s", e)
        return jsonify({"status": "invalid json"}), 400

    if not data:
        return jsonify({"status": "no data"}), 400

    wa_id = data.get("waId", "unknown")
    raw_ts = data.get("timestamp", "")
    try:
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except Exception:
        time_str = str(raw_ts)

    sender_name = data.get("operatorName", "Bot") if data.get("owner") else data.get("senderName", "User")
    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

    log_file = os.path.join(LOG_FOLDER, f"{wa_id}.txt")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
        app.logger.info("Appended log line to: %s", os.path.abspath(log_file))
    except Exception as e:
        app.logger.error("Error writing to %s: %s", log_file, e)

    app.logger.info("WATI Webhook data received: %s", data)
    return jsonify({"status": "received"}), 200

@app.route("/oauth/callback")
def oauth_callback():
    """
    Handles the OAuth callback from Zoho.
    Expects a 'code' parameter and exchanges it for an access token.
    """
    code = request.args.get("code")
    state = request.args.get("state")
    if not code:
        return "Error: No authorization code provided.", 400

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
        # In production, store token_data securely (access token, refresh token, etc.).
        return jsonify({
            "message": "OAuth callback successful. Tokens received.",
            "token_data": token_data,
            "state": state
        })
    else:
        app.logger.error("Token exchange failed: %s", response.text)
        return f"Token exchange failed: {response.text}", response.status_code

############################################
# Zoho Token Refresh and CRM Update
############################################

def get_zoho_access_token():
    """
    Uses the refresh token to obtain a new access token from Zoho.
    Returns the new access token or None if the refresh fails.
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

def update_zoho_crm(journey):
    """
    Searches for an existing lead in Zoho CRM using two criteria:
      - Mobile == journey's mobile_number
      - Lead_Source == "WATI"
    If found, updates only the specified fields (hardcoded field API names below).
    Does NOT update Mobile or Lead_Source.
    """
    mobile = journey.get("mobile_number")
    if not mobile:
        app.logger.error("No mobile number found in journey; cannot update Zoho CRM.")
        return

    access_token = get_zoho_access_token()
    if not access_token:
        app.logger.error("Cannot update Zoho CRM without a valid access token.")
        return

    headers = {
        "Authorization": "Zoho-oauthtoken " + access_token,
        "Content-Type": "application/json"
    }
    # Search for a lead where Mobile == journey.mobile_number AND Lead_Source == "WATI"
    criteria = f"((Mobile:equals:{mobile}) and (Lead_Source:equals:WATI))"
    search_url = f"https://www.zohoapis.in/crm/v2/Leads/search?criteria={criteria}"
    try:
        search_response = requests.get(search_url, headers=headers, timeout=10)
    except Exception as e:
        app.logger.error("Exception during Zoho CRM search: %s", e)
        return

    if search_response.status_code == 200:
        search_data = search_response.json()
        if "data" in search_data and len(search_data["data"]) > 0:
            record_id = search_data["data"][0].get("id")
            # Hardcode the mapping from journey dict -> Zoho CRM fields:
            update_payload = {
                "data": [{
                    JOURNEY_ATTEMPTS_FIELD: journey.get("journey_attempts"),
                    RENT_TENANT_CITY_FIELD: journey.get("rent_tenant_btn_city"),
                    RENT_TENANT_CONFIG_FIELD: journey.get("rent_tenant_btn_configuration"),
                    RENT_TENANT_CONFIG_MORE_FIELD: journey.get("rent_tenant_btn_configuration_more"),
                    RENT_TENANT_LOCALITY_FIELD: journey.get("rent_tenant_txt_locality"),
                    RENT_TENANT_BUDGET_WRONG_FIELD: journey.get("rent_tenant_txt_budget_wrong"),
                    RENT_TENANT_BUDGET_CORRECT_FIELD: journey.get("rent_tenant_txt_budget_correct"),
                    RENT_TENANT_EMAIL_FIELD: journey.get("rent_tenant_txt_email"),
                    RENT_TENANT_EST_MOVE_IN_FIELD: journey.get("rent_tenant_btn_est_move_in")
                    # Add more fields if needed, e.g.:
                    # "Intro_Selection": journey.get("intro_selection"),
                    # "Main_Selection": journey.get("main_selection"),
                }]
            }
            update_url = f"https://www.zohoapis.in/crm/v2/Leads/{record_id}"
            try:
                update_response = requests.put(update_url, headers=headers, json=update_payload, timeout=10)
                if update_response.status_code in [200, 201]:
                    app.logger.info("Successfully updated lead for mobile %s", mobile)
                else:
                    app.logger.error("Failed to update lead for mobile %s: %s", mobile, update_response.text)
            except Exception as e:
                app.logger.error("Exception during update call for mobile %s: %s", mobile, e)
        else:
            app.logger.info("No existing lead found for mobile %s with Lead_Source=WATI. Not creating a new entry.", mobile)
    else:
        app.logger.error("Error searching for lead with mobile %s: %s", mobile, search_response.text)

############################################
# Scheduled Log Processing
############################################

def process_logs():
    """
    Scheduled job that processes log files, extracts journey records,
    posts them to Google Sheets, and updates existing leads in Zoho CRM.
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
        # 1) Post journey to Google Sheets
        try:
            post_journey_to_apps_script(journey)
        except Exception as e:
            app.logger.error("Error posting journey to Google Sheets: %s", e)
        # 2) Update the existing lead in Zoho CRM
        try:
            update_zoho_crm(journey)
        except Exception as e:
            app.logger.error("Error updating journey in Zoho CRM: %s", e)
    
    app.logger.info("Finished processing logs.")

############################################
# Application Startup
############################################

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=process_logs, trigger="interval", minutes=1)
    scheduler.start()
    
    try:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
