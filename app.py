import os
from flask import Flask, request, jsonify

app = Flask(__name__)

# Retrieve the webhook token from environment variables (set this in Render)
WEBHOOK_TOKEN = os.environ.get("WATI_WEBHOOK_TOKEN", "my_default_token")

@app.route("/")
def index():
    return "Hello from Flask! Your app is running."

@app.route("/wati-webhook", methods=["POST"])
def wati_webhook():
    # Check for token in the query string for basic security
    token = request.args.get("token")
    if token != WEBHOOK_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    # Get JSON payload from the incoming webhook
    data = request.get_json(force=True)
    if not data:
        return jsonify({"status": "no data"}), 400

    # Process the inbound data (here we simply log it)
    print("WATI Webhook data received:", data)
    
    return jsonify({"status": "received"}), 200

if __name__ == "__main__":
    # For local testing. On Render, Gunicorn will be used.
    app.run(debug=True, port=5000)