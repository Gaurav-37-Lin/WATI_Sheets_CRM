import os
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# Use environment variable for the logs folder, defaulting to "logs"
LOG_FOLDER = os.environ.get("LOG_FOLDER", "logs")

# Ensure the log folder exists
os.makedirs(LOG_FOLDER, exist_ok=True)

# Get the webhook token from an environment variable
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

@app.route("/")
def index():
    # Return a message with the absolute path of the log directory
    abs_log_path = os.path.abspath(LOG_FOLDER)
    return f"Webhook is running. Log files are stored in: {abs_log_path}"

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    # Validate the token from the query parameter
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # Parse the JSON payload
    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # Get the WhatsApp ID and set a default if not provided
    wa_id = data.get("waId", "unknown")

    # Parse timestamp to a readable format
    raw_ts = data.get("timestamp", "")
    try:
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except Exception:
        time_str = str(raw_ts)

    # Determine the sender's label based on "owner" field
    owner = data.get("owner")
    if owner is True:
        sender_name = data.get("operatorName", "Bot")
    else:
        sender_name = data.get("senderName", "User")

    text = data.get("text", "")
    log_line = f"[{time_str}] {sender_name}: {text}"

    # Build the log file path (e.g., logs/918779501765.txt)
    log_file = os.path.join(LOG_FOLDER, f"{wa_id}.txt")
    
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
        # Print the full path for debugging
        print(f"Appended log line to: {os.path.abspath(log_file)}")
    except Exception as e:
        print(f"Error writing to {log_file}: {e}")

    print("WATI Webhook data received:", data)
    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    app.run(debug=True, port=5000)
