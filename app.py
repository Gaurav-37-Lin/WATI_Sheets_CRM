import os
from flask import Flask, request, jsonify

app = Flask(__name__)

# Read a token from the environment (Render dashboard or a .env file for local dev)
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "my_default_token")

@app.route("/")
def index():
    return "Hello from Flask! Your app is running."

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    # Simple token check (e.g., ?token=xxx in the URL)
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # Get JSON payload from WATI
    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # Here’s where you’d process the inbound WhatsApp data
    # For example, print to logs or store in a file:
    print("WATI Webhook data received:", data)

    # Return a success response so WATI knows we handled it
    return jsonify({"status": "received"}), 200

# If you run locally, you can do: python app.py
# But on Render, you'll use a Start Command like:
# gunicorn app:app --bind 0.0.0.0:$PORT
if __name__ == "__main__":
    # For local testing only (not used in production with gunicorn)
    app.run(debug=True, port=5000)
