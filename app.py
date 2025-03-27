import os
import requests
import json

def get_zoho_access_token():
    """
    Uses the refresh token to obtain a new access token from Zoho.
    Make sure the following environment variables are set on Render:
      - ZOHO_CLIENT_ID
      - ZOHO_CLIENT_SECRET
      - ZOHO_REFRESH_TOKEN
    """
    data = {
        "refresh_token": os.environ.get("ZOHO_REFRESH_TOKEN"),
        "client_id": os.environ.get("ZOHO_CLIENT_ID"),
        "client_secret": os.environ.get("ZOHO_CLIENT_SECRET"),
        "grant_type": "refresh_token"
    }
    # Use the proper endpoint based on your region. Here, we use the India data center.
    token_url = "https://accounts.zoho.in/oauth/v2/token"
    response = requests.post(token_url, data=data)
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data.get("access_token")
        if access_token:
            print("Obtained new Zoho access token.", flush=True)
            return access_token
        else:
            print("Access token not found in response:", token_data, flush=True)
            return None
    else:
        print("Failed to refresh token:", response.status_code, response.text, flush=True)
        return None

def push_to_zoho_crm(journey):
    """
    Pushes a journey record to Zoho CRM as a new Lead.
    The function first obtains a fresh access token using the refresh token.
    Adjust the field mapping as needed.
    """
    access_token = get_zoho_access_token()
    if not access_token:
        print("Cannot push to Zoho CRM without a valid access token.", flush=True)
        return

    headers = {
        "Authorization": "Zoho-oauthtoken " + access_token,
        "Content-Type": "application/json"
    }
    
    # Map your journey data to Zoho CRM Lead fields.
    # Adjust these field names to match your Zoho CRM configuration.
    lead_data = {
        "data": [{
            "Last_Name": journey.get("username", "Unknown"),
            "Phone": journey.get("mobile_number", ""),
            "Lead_Source": "WATI Chatbot",
            # You can store a summary or the full JSON in a description field.
            "Description": json.dumps(journey)
        }]
    }
    
    # For Zoho CRM in India, use the .in endpoint.
    create_url = "https://www.zohoapis.in/crm/v2/Leads"
    
    try:
        response = requests.post(create_url, headers=headers, json=lead_data, timeout=10)
        print("Zoho CRM response:", response.status_code, response.text, flush=True)
    except Exception as e:
        print("Exception while pushing to Zoho CRM:", e, flush=True)

# Example of integrating this into your existing main process:
def main():
    # Assume you have a function process_all_files() that returns journey records.
    from rentmax_analysis import process_all_files  # if not already imported
    records = process_all_files()
    print("DEBUG: Total records extracted:", len(records), flush=True)
    for journey in records:
        # Push to Google Sheets if you already have that function:
        # post_journey_to_apps_script(journey)
        # Now, push the record to Zoho CRM:
        push_to_zoho_crm(journey)

if __name__ == "__main__":
    main()
import os
import time
import sys
import logging
from flask import Flask, request, jsonify, redirect
from apscheduler.schedulers.background import BackgroundScheduler
from rentmax_analysis import process_all_files, post_journey_to_apps_script
import requests

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Use the environment variable for the log folder (e.g. a persistent disk folder)
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")
os.makedirs(LOG_FOLDER, exist_ok=True)

WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

# Zoho OAuth credentials (India data center)
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.environ.get("ZOHO_REDIRECT_URI")  
# e.g. "https://wati-sheets-crm.onrender.com/oauth/callback"

@app.route("/")
def index():
    abs_log_path = os.path.abspath(LOG_FOLDER)
    return f"Webhook is running. Log files are stored in: {abs_log_path}"

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    wa_id = data.get("waId", "unknown")
    raw_ts = data.get("timestamp", "")
    try:
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except Exception:
        time_str = str(raw_ts)

    # Use operatorName if owner is true; otherwise senderName
    sender_name = data.get("operatorName", "Bot") if data.get("owner") else data.get("senderName", "User")
    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

    # Append the log to a single file per mobile number
    log_file = os.path.join(LOG_FOLDER, f"{wa_id}.txt")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
        app.logger.info(f"Appended log line to: {os.path.abspath(log_file)}")
    except Exception as e:
        app.logger.error(f"Error writing to {log_file}: {e}")

    app.logger.info("WATI Webhook data received: %s", data)
    return jsonify({"status": "received"}), 200

@app.route("/oauth/callback")
def oauth_callback():
    """
    This endpoint handles the OAuth callback from Zoho (India data center).
    Zoho will redirect to this URL with a 'code' query parameter.
    The code is exchanged for an access token.
    """
    code = request.args.get("code")
    state = request.args.get("state")
    if not code:
        return "Error: No authorization code provided.", 400

    # Because you're on .in data center, use the .in domain:
    token_url = "https://accounts.zoho.in/oauth/v2/token"
    payload = {
        "code": code,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "redirect_uri": ZOHO_REDIRECT_URI,
        "grant_type": "authorization_code"
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        token_data = response.json()
        # For production, store token_data securely (e.g., refresh_token).
        return jsonify({
            "message": "OAuth callback successful. Tokens received.",
            "token_data": token_data,
            "state": state
        })
    else:
        return f"Token exchange failed: {response.text}", response.status_code

def process_logs():
    app.logger.info("Starting scheduled log processing...")
    app.logger.info("DEBUG: Searching for .txt files in: %s", os.path.abspath(LOG_FOLDER))
    records = process_all_files()
    app.logger.info("DEBUG: Total records extracted: %s", len(records))
    if not records:
        app.logger.warning("No journeys extracted. Check if the expected bot prompt is present in the logs.")
    for journey in records:
        post_journey_to_apps_script(journey)
    app.logger.info("Finished processing logs.")

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=process_logs, trigger="interval", minutes=1)
    scheduler.start()
    try:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
