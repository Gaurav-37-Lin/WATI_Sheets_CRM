import os
import time
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import logging

# Import log processing functions from rentmax_analysis.py
from rentmax_analysis import process_all_files, post_journey_to_apps_script

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

# Environment variables and defaults
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")
os.makedirs(LOG_FOLDER, exist_ok=True)

WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

@app.route("/")
def index():
    abs_log_path = os.path.abspath(LOG_FOLDER)
    return f"Webhook is running. Log files are stored in: {abs_log_path}"

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    """Endpoint to receive incoming WATI messages and log them to a file."""
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # Extract phone ID (waId) and timestamp
    wa_id = data.get("waId", "unknown")
    raw_ts = data.get("timestamp", "")
    try:
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except Exception:
        time_str = str(raw_ts)

    # Determine if message is from user or bot
    if data.get("owner"):
        sender_name = data.get("operatorName", "Bot")
    else:
        sender_name = data.get("senderName", "User")

    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

    # Append the log line to a file named after the user's WhatsApp number
    log_file = os.path.join(LOG_FOLDER, f"{wa_id}.txt")
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
        app.logger.info(f"Appended log line to: {os.path.abspath(log_file)}")
    except Exception as e:
        app.logger.error(f"Error writing to {log_file}: {e}")

    app.logger.info("WATI Webhook data received: %s", data)
    return jsonify({"status": "received"}), 200

def process_logs():
    """
    Background job function that:
    1. Reads all .txt log files in LOG_FOLDER
    2. Extracts journeys via RentMAX logic
    3. Posts each journey to the Apps Script endpoint
    """
    app.logger.info("Starting scheduled log processing...")
    records = process_all_files()  # from rentmax_analysis.py
    app.logger.info("DEBUG: Total records extracted: %s", len(records))
    for journey in records:
        post_journey_to_apps_script(journey)
    app.logger.info("Finished processing logs.")

if __name__ == "__main__":
    # Start APScheduler background job: runs every 30 minutes
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=process_logs, trigger="interval", minutes=30)
    scheduler.start()
    try:
        # Run the Flask app
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
