import os
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

# Get your secret token from Render environment variables
# (Set "WATI_WEBHOOK_TOKEN" in Render's Environment tab)
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "default_token")

# Ensure there's a logs directory (Render’s free tier is ephemeral, so this may reset on redeploy)
os.makedirs("logs", exist_ok=True)

@app.route("/")
def index():
    return "Hello from Flask! Your app is running."

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    """
    Receives JSON data from WATI.
    Expects a query parameter like ?token=YOUR_TOKEN matching WEBHOOK_TOKEN.
    """
    # Check the token
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # Parse JSON payload
    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # Extract details for logging
    # 'waId' is the user's WhatsApp number (e.g., '918779501765')
    wa_id = data.get("waId", "unknown")

    # For a readable timestamp, convert WATI's 'timestamp' if it exists
    # WATI often gives epoch as string in 'timestamp', but let's just store raw if conversion fails
    raw_ts = data.get("timestamp", "")
    try:
        # Convert to integer and then to localtime
        epoch_ts = int(raw_ts)
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(epoch_ts))
    except:
        # Fallback to the raw value if parsing fails
        time_str = str(raw_ts)

    # Determine the sender label
    # If 'owner' == False, it's an inbound user message. If True, it's from your side (bot/template).
    # We can also use 'senderName' for user or 'operatorName' for bot messages.
    owner = data.get("owner")
    if owner is True:
        sender_name = data.get("operatorName", "Bot")
    else:
        sender_name = data.get("senderName", "User")

    # The actual text
    text = data.get("text", "")

    # Format a chat-style log line
    # Example: [2025-03-25 06:00:10] g: Hi
    log_line = f"[{time_str}] {sender_name}: {text}"

    # Write to a file named after the user's waId
    # This is how RentMAX typically expects chat logs
    log_path = f"logs/{wa_id}.txt"
    try:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception as e:
        print(f"Error writing log to {log_path}: {e}")

    # Print to console for debugging (optional)
    print("WATI Webhook data received:", data)

    # Acknowledge success so WATI doesn't retry
    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    # Local test run (not used in production on Render)
    app.run(debug=True, port=5000)
