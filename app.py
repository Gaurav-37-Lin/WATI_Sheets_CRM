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

    # Use operatorName if owner is True, else senderName
    sender_name = data.get("operatorName", "Bot") if data.get("owner") else data.get("senderName", "User")
    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

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
    app.logger.info("Starting scheduled log processing...")
    # Debug print: show the folder being searched
    app.logger.info("DEBUG: Searching for .txt files in: %s", os.path.abspath(LOG_FOLDER))
    records = process_all_files()
    app.logger.info("DEBUG: Total records extracted: %s", len(records))
    if not records:
        app.logger.warning("No journeys extracted. Check if the expected Bot prompt is present in the logs.")
    for journey in records:
        post_journey_to_apps_script(journey)
    app.logger.info("Finished processing logs.")

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    # For testing, set to run every minute. Change to minutes=30 for production.
    scheduler.add_job(func=process_logs, trigger="interval", minutes=1)
    scheduler.start()
    try:
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
