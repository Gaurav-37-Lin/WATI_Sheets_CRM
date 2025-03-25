import os
import glob
import re
import pandas as pd
import numpy as np
import requests

# Environment variables for folder and Apps Script URL
CHAT_FOLDER = os.environ.get("LOG_FOLDER", "logs")
APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "")

##########################################################
# FLOW-SPECIFIC COLUMN FILTERING
##########################################################
COMMON_COLS = [
    "file", "username", "flow", "journey_start", "journey_end",
    "total_messages", "main_selection", "intro_selection", "extra_responses"
]

FLOW_COLUMNS = {
    "RentTenant": COMMON_COLS + [
        "rent_tenant_btn_city",
        "rent_tenant_btn_configuration",
        "rent_tenant_btn_configuration_more",
        "rent_tenant_txt_locality",
        "rent_tenant_txt_budget_correct",
        "rent_tenant_txt_budget_wrong",
        "rent_tenant_txt_email",
        "rent_tenant_btn_est_move_in"
    ],
    "RentOwner": COMMON_COLS + [
        "rent_owner_btn_city",
        "rent_owner_btn_configuration",
        "rent_owner_btn_configuration_more",
        "rent_owner_txt_locality",
        "rent_owner_txt_rent_expectation_correct",
        "rent_owner_txt_rent_expectation_wrong"
    ],
    "BuyBuyer": COMMON_COLS + [
        "buy_buyer_btn_configuration",
        "buy_buyer_btn_configuration_more",
        "buy_buyer_txt_locality",
        "buy_buyer_txt_budget_correct",
        "buy_buyer_txt_budget_wrong",
        "buy_buyer_txt_email"
    ],
    "BuySeller": COMMON_COLS + [
        "buy_seller_btn_configuration",
        "buy_seller_btn_configuration_more",
        "buy_seller_txt_locality",
        "buy_seller_txt_sale_expectation_correct",
        "buy_seller_txt_sale_expectation_wrong",
        "buy_seller_txt_email"
    ],
    "ChannelPartner": COMMON_COLS + [
        "cp_mode_of_operation",
        "cp_name",
        "cp_area_expertise",
        "cp_office_location",
        "cp_rera_registered",
        "cp_rera_info"
    ],
    "TalkToExpert": COMMON_COLS + [
        "message"
    ]
}

##########################################################
# HELPER FUNCTIONS
##########################################################
def remove_emoji(text):
    if not isinstance(text, str):
        return text
    pattern = re.compile("[" 
                         u"\U0001F600-\U0001F64F"
                         u"\U0001F300-\U0001F5FF"
                         u"\U0001F680-\U0001F6FF"
                         u"\U0001F1E0-\U0001F1FF"
                         "]+", flags=re.UNICODE)
    return pattern.sub(r'', text)

def is_greeting(text):
    greetings = {"hi", "hello", "hey", "greetings"}
    normalized = re.sub(r'[^\w\s]', '', text.lower()).strip()
    return normalized in greetings

def filter_greetings(msgs):
    return [msg for msg in msgs if not is_greeting(msg)]

def parse_chat_file(file_path):
    """
    Parses a chat log file in the format:
      [timestamp] Sender: Message
    """
    pattern = r"\[(.*?)\]\s(.*?):\s(.*)"
    messages = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = re.match(pattern, line)
            if m:
                raw_ts, sender, text = m.groups()
                try:
                    ts = pd.to_datetime(raw_ts, errors='coerce')
                except Exception:
                    ts = None
                messages.append({
                    "timestamp": ts,
                    "sender": sender.strip(),
                    "message": text.strip()
                })
    return messages

def split_sessions(messages, gap_threshold=600):
    sessions = []
    current = []
    for i, msg in enumerate(messages):
        if i == 0:
            current.append(msg)
        else:
            gap = (msg["timestamp"] - messages[i-1]["timestamp"]).total_seconds()
            if gap > gap_threshold:
                sessions.append(current)
                current = [msg]
            else:
                current.append(msg)
    if current:
        sessions.append(current)
    return sessions

def detect_flow(main_sel, intro_sel):
    main_sel = main_sel.lower()
    intro_sel = intro_sel.lower()
    flow = None
    if "rent" in main_sel:
        if "tenant" in intro_sel:
            flow = "RentTenant"
        elif "owner" in intro_sel:
            flow = "RentOwner"
        elif "channel" in intro_sel:
            flow = "ChannelPartner"
    elif "buy" in main_sel or "sell" in main_sel:
        if "buyer" in intro_sel:
            flow = "BuyBuyer"
        elif "seller" in intro_sel:
            flow = "BuySeller"
        elif "channel" in intro_sel:
            flow = "ChannelPartner"
    elif "talk" in main_sel:
        flow = "TalkToExpert"
    return flow

def validate_numeric(value):
    return bool(re.fullmatch(r'\d+', value))

def extract_valid_response(texts, start_index, validate_func):
    wrong = []
    i = start_index
    while i < len(texts):
        value = texts[i]
        if validate_func(value):
            return value, i + 1, wrong
        else:
            wrong.append(value)
            i += 1
    return None, i, wrong

##########################################################
# JOURNEY EXTRACTION
##########################################################
def extract_journeys_from_session(session, file_name):
    journeys = []
    # Identify indices where the Bot sends "How can we assist you today?"
    journey_start_indices = []
    for idx, msg in enumerate(session):
        if msg["sender"].lower() == "bot" and "how can we assist you today" in msg["message"].lower():
            journey_start_indices.append(idx)
    if not journey_start_indices:
        return journeys

    for k, start_idx in enumerate(journey_start_indices):
        end_idx = journey_start_indices[k+1] if k+1 < len(journey_start_indices) else len(session)
        segment_msgs = session[start_idx:end_idx]
        non_bot = [msg for msg in segment_msgs if msg["sender"].lower() != "bot"]
        if not non_bot:
            continue
        texts = [remove_emoji(msg["message"]).strip() for msg in non_bot]
        texts = filter_greetings(texts)
        if len(texts) < 2:
            continue
        main_sel = texts[0]
        intro_sel = texts[1]
        flow = detect_flow(main_sel, intro_sel)
        if not flow:
            flow = "Unknown"
        username = non_bot[0]["sender"]
        journey_record = {
            "file": file_name,
            "username": username,
            "flow": flow,
            "journey_start": session[start_idx]["timestamp"],
            "journey_end": session[end_idx-1]["timestamp"],
            "total_messages": end_idx - start_idx,
            "main_selection": main_sel,
            "intro_selection": intro_sel,
            "extra_responses": ""
        }
        pointer = 2  # We've used texts[0] and texts[1]

        # Flow-specific logic
        if flow == "TalkToExpert":
            journey_record["message"] = "Talk to Expert selected"
        elif flow == "RentTenant":
            if pointer < len(texts):
                journey_record["rent_tenant_btn_city"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                apt_type = texts[pointer]
                journey_record["rent_tenant_btn_configuration"] = apt_type
                pointer += 1
                if apt_type.lower() == "more" and pointer < len(texts):
                    journey_record["rent_tenant_btn_configuration_more"] = texts[pointer]
                    pointer += 1
            if pointer < len(texts):
                journey_record["rent_tenant_txt_locality"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                valid_budget, new_ptr, wrongs = extract_valid_response(texts, pointer, validate_numeric)
                journey_record["rent_tenant_txt_budget_correct"] = valid_budget
                journey_record["rent_tenant_txt_budget_wrong"] = "; ".join(wrongs) if wrongs else None
                pointer = new_ptr
            if pointer < len(texts):
                journey_record["rent_tenant_txt_email"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                journey_record["rent_tenant_btn_est_move_in"] = texts[pointer]
                pointer += 1
        elif flow == "RentOwner":
            if pointer < len(texts):
                journey_record["rent_owner_btn_city"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                apt_size = texts[pointer]
                journey_record["rent_owner_btn_configuration"] = apt_size
                pointer += 1
                if apt_size.lower() == "more" and pointer < len(texts):
                    journey_record["rent_owner_btn_configuration_more"] = texts[pointer]
                    pointer += 1
            if pointer < len(texts):
                journey_record["rent_owner_txt_locality"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                valid_expect, new_ptr, wrongs = extract_valid_response(texts, pointer, validate_numeric)
                journey_record["rent_owner_txt_rent_expectation_correct"] = valid_expect
                journey_record["rent_owner_txt_rent_expectation_wrong"] = "; ".join(wrongs) if wrongs else None
                pointer = new_ptr
        elif flow == "BuyBuyer":
            if pointer < len(texts):
                apt_size = texts[pointer]
                journey_record["buy_buyer_btn_configuration"] = apt_size
                pointer += 1
                if apt_size.lower() == "more" and pointer < len(texts):
                    journey_record["buy_buyer_btn_configuration_more"] = texts[pointer]
                    pointer += 1
            if pointer < len(texts):
                journey_record["buy_buyer_txt_locality"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                valid_budget, new_ptr, wrongs = extract_valid_response(texts, pointer, validate_numeric)
                journey_record["buy_buyer_txt_budget_correct"] = valid_budget
                journey_record["buy_buyer_txt_budget_wrong"] = "; ".join(wrongs) if wrongs else None
                pointer = new_ptr
            if pointer < len(texts):
                journey_record["buy_buyer_txt_email"] = texts[pointer]
                pointer += 1
        elif flow == "BuySeller":
            if pointer < len(texts):
                apt_size = texts[pointer]
                journey_record["buy_seller_btn_configuration"] = apt_size
                pointer += 1
                if apt_size.lower() == "more" and pointer < len(texts):
                    journey_record["buy_seller_btn_configuration_more"] = texts[pointer]
                    pointer += 1
            if pointer < len(texts):
                journey_record["buy_seller_txt_locality"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                valid_sale, new_ptr, wrongs = extract_valid_response(texts, pointer, validate_numeric)
                journey_record["buy_seller_txt_sale_expectation_correct"] = valid_sale
                journey_record["buy_seller_txt_sale_expectation_wrong"] = "; ".join(wrongs) if wrongs else None
                pointer = new_ptr
            if pointer < len(texts):
                journey_record["buy_seller_txt_email"] = texts[pointer]
                pointer += 1
        elif flow == "ChannelPartner":
            if pointer < len(texts):
                journey_record["cp_mode_of_operation"] = texts[pointer]
                pointer += 1
            mode = journey_record.get("cp_mode_of_operation", "").lower()
            if "firm" in mode or "company" in mode:
                if pointer < len(texts):
                    journey_record["cp_name"] = texts[pointer]
                    pointer += 1
            if pointer < len(texts):
                journey_record["cp_area_expertise"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                journey_record["cp_office_location"] = texts[pointer]
                pointer += 1
            if pointer < len(texts):
                journey_record["cp_rera_registered"] = texts[pointer]
                pointer += 1
            if "yes" in journey_record.get("cp_rera_registered", "").lower():
                if pointer < len(texts):
                    journey_record["cp_rera_info"] = texts[pointer]
                    pointer += 1

        if pointer < len(texts):
            journey_record["extra_responses"] = "; ".join(texts[pointer:])
        journeys.append(journey_record)
    return journeys

def process_file(file_path):
    messages = parse_chat_file(file_path)
    if not messages:
        return []
    sessions = split_sessions(messages)
    file_records = []
    for session in sessions:
        recs = extract_journeys_from_session(session, os.path.basename(file_path))
        if recs:
            file_records.extend(recs)
    return file_records

def process_all_files():
    """Collects all journeys from every .txt file in CHAT_FOLDER."""
    all_records = []
    file_paths = glob.glob(os.path.join(CHAT_FOLDER, "*.txt"))
    print("DEBUG: Found files:", file_paths)
    for file_path in file_paths:
        recs = process_file(file_path)
        all_records.extend(recs)
    return all_records

##########################################################
# APPS SCRIPT WEB APP INTEGRATION
##########################################################
def post_journey_to_apps_script(journey):
    """
    Sends one journey as JSON to the Apps Script Web App endpoint.
    The doPost(e) in Apps Script should parse this and append a row.
    """
    try:
        response = requests.post(APPS_SCRIPT_URL, json=journey, timeout=10)
        print("Response status code:", response.status_code)
        print("Response text:", response.text)
        if response.status_code == 200:
            try:
                resp_data = response.json()
            except Exception as json_err:
                print("Error decoding JSON:", json_err)
                resp_data = {}
            if resp_data.get("result") == "success":
                print(f"Successfully posted journey for {journey.get('username')} to Apps Script.")
            else:
                print(f"Apps Script returned an error: {resp_data}")
        else:
            print(f"HTTP {response.status_code} error when posting to Apps Script: {response.text}")
    except Exception as e:
        print(f"Exception posting to Apps Script: {e}")

def main():
    # For local testing or manual invocation
    records = process_all_files()
    print("DEBUG: Total records extracted:", len(records))
    for journey in records:
        post_journey_to_apps_script(journey)

if __name__ == "__main__":
    main()
