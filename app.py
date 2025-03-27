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

# Zoho OAuth credentials (if you decide to set up OAuth with Zoho)
ZOHO_CLIENT_ID = os.environ.get("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.environ.get("ZOHO_CLIENT_SECRET")
ZOHO_REDIRECT_URI = os.environ.get("ZOHO_REDIRECT_URI")  # e.g., "https://your-app.onrender.com/oauth/callback"
# (You might also use a refresh token flow later; this example shows the basic authorization code exchange.)

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
    This endpoint handles the OAuth callback from Zoho.
    Zoho will redirect to this URL with a 'code' query parameter.
    The code is exchanged for an access token.
    """
    code = request.args.get("code")
    state = request.args.get("state")
    if not code:
        return "Error: No authorization code provided.", 400

    token_url = "https://accounts.zoho.com/oauth/v2/token"
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
        # For production, store token_data securely.
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
