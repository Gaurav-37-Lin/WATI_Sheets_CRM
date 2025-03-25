import os
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# Read the secret token from an environment variable for security
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

# Ensure a directory for log files exists (Render's filesystem is ephemeral)
os.makedirs("logs", exist_ok=True)

@app.route("/")
def index():
    return "Webhook is running."

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    """Endpoint to receive WhatsApp webhook data from WATI."""
    # 1. Validate the request using a token (security check)
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # 2. Parse JSON payload
    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # 3. Extract relevant fields from WATI data
    wa_id = data.get("waId", "unknown")  # WhatsApp phone ID of sender
    raw_ts = data.get("timestamp", "")   # Unix epoch timestamp (in seconds)
    try:
        # Convert epoch timestamp to human-readable format (local time)
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(raw_ts)))
    except:
        ts = str(raw_ts)  # Fallback to raw timestamp if conversion fails

    # Determine who the sender is (user or our bot/operator)
    owner = data.get("owner")  # WATI 'owner': True if message is from the business (our side)
    if owner is True:
        sender_name = data.get("operatorName", "Bot")   # Bot/operator message
    else:
        sender_name = data.get("senderName", "User")    # Inbound user message

    text = data.get("text", "")  # The message text content

    # 4. Format the log line in "[timestamp] sender: message" format
    log_line = f"[{ts}] {sender_name}: {text}"

    # 5. Append the log line to a file named after the user’s WhatsApp ID (waId)
    log_file = f"logs/{wa_id}.txt"
    try:
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Error writing to {log_file}: {e}")

    # 6. (Optional) print to console for debugging
    print("Received webhook message:", log_line)

    # 7. Respond to WATI to acknowledge receipt (HTTP 200 OK)
    return jsonify({"status": "received"}), 200
