import os
import glob
import re
import json
import pandas as pd
import numpy as np
import requests

# Directory where log files are stored (should match LOG_FOLDER in app.py)
CHAT_FOLDER = os.environ.get("LOG_FOLDER", "logs")
APPS_SCRIPT_URL = os.environ.get("APPS_SCRIPT_URL", "")

##########################################################
# (Flow column definitions for reference – unchanged)
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

def parse_chat_file_from_offset(file_path, offset):
    pattern = r"\[(.*?)\]\s(.*?):\s(.*)"
    messages = []
    current_line = 0
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if current_line < offset:
                current_line += 1
                continue
            line = line.strip()
            if not line:
                current_line += 1
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
            current_line += 1
    return messages, current_line

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

def extract_journeys_from_session(session, file_name):
    journeys = []
    journey_start_indices = []
    for idx, msg in enumerate(session):
        if msg["sender"].lower() == "bot" and "how can we assist you today" in msg["message"].lower():
            journey_start_indices.append(idx)
    if not journey_start_indices:
        print(f"DEBUG: No journey start prompt found in file: {file_name}", flush=True)
        return journeys

    for k, start_idx in enumerate(journey_start_indices):
        end_idx = journey_start_indices[k+1] if (k+1 < len(journey_start_indices)) else len(session)
        segment_msgs = session[start_idx:end_idx]
        non_bot = [msg for msg in segment_msgs if msg["sender"].lower() != "bot"]
        if not non_bot:
            continue
        texts = [remove_emoji(msg["message"]).strip() for msg in non_bot]
        texts = filter_greetings(texts)
        if len(texts) < 1:
            continue

        main_sel = texts[0]
        intro_sel = texts[1] if len(texts) > 1 else ""
        flow = detect_flow(main_sel, intro_sel) or "Unknown"
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

        pointer = 2
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
    offset_file = file_path + ".offset"
    start_line = 0
    journey_count = 0
    if os.path.exists(offset_file):
        try:
            with open(offset_file, "r") as f:
                offset_data = json.load(f)
                start_line = offset_data.get("line_offset", 0)
                journey_count = offset_data.get("journey_count", 0)
        except Exception as e:
            print(f"Error reading offset file {offset_file}: {e}", flush=True)
    messages, new_offset = parse_chat_file_from_offset(file_path, start_line)
    if not messages:
        return []
    sessions = split_sessions(messages)
    # Determine current time and a threshold of 7 minutes ago
    now = pd.Timestamp.now()
    threshold = now - pd.Timedelta(minutes=7)
    complete_sessions = []
    incomplete_session = None
    if sessions:
        # Process all sessions except possibly the last one
        for s in sessions[:-1]:
            complete_sessions.append(s)
        # Check the last session: if its last message timestamp is older than threshold, include it; otherwise, skip it
        last_session = sessions[-1]
        if last_session and last_session[-1]["timestamp"] <= threshold:
            complete_sessions.append(last_session)
        else:
            incomplete_session = last_session
    file_records = []
    lines_processed = 0
    new_journeys = 0
    for session in complete_sessions:
        recs = extract_journeys_from_session(session, os.path.basename(file_path))
        if recs:
            new_journeys += len(recs)
            file_records.extend(recs)
        lines_processed += len(session)
    # Update offset only up to the end of the last complete session
    final_offset = start_line + lines_processed
    journey_count += new_journeys
    offset_info = {"line_offset": final_offset, "journey_count": journey_count}
    try:
        with open(offset_file, "w") as f:
            json.dump(offset_info, f)
    except Exception as e:
        print(f"Error writing offset file {offset_file}: {e}", flush=True)
    # Extract mobile number from the file name (remove .txt and any .done suffix)
    base_name = os.path.basename(file_path).replace(".done", "")
    mobile = base_name.replace(".txt", "")
    for rec in file_records:
        rec["mobile_number"] = mobile
        rec["no_of_attempts"] = journey_count
    return file_records

def process_all_files():
    all_records = []
    file_paths = glob.glob(os.path.join(CHAT_FOLDER, "*.txt"))
    print("DEBUG: Searching for .txt files in:", os.path.abspath(CHAT_FOLDER), flush=True)
    print("DEBUG: Found files:", file_paths, flush=True)
    for file_path in file_paths:
        recs = process_file(file_path)
        all_records.extend(recs)
    return all_records

def post_journey_to_apps_script(journey):
    for key, value in journey.items():
        if hasattr(value, "isoformat"):
            journey[key] = value.isoformat()
    try:
        response = requests.post(APPS_SCRIPT_URL, json=journey, timeout=10)
        print("Response status code:", response.status_code, flush=True)
        print("Response text:", response.text, flush=True)
        if response.status_code == 200:
            try:
                resp_data = response.json()
            except Exception as json_err:
                print("Error decoding JSON:", json_err, flush=True)
                resp_data = {}
            if resp_data.get("result") == "success":
                print(f"Successfully posted journey for {journey.get('username')} to Apps Script.", flush=True)
            else:
                print(f"Apps Script returned an error: {resp_data}", flush=True)
        else:
            print(f"HTTP {response.status_code} error when posting to Apps Script: {response.text}", flush=True)
    except Exception as e:
        print(f"Exception posting to Apps Script: {e}", flush=True)

def main():
    records = process_all_files()
    print("DEBUG: Total records extracted:", len(records), flush=True)
    for journey in records:
        post_journey_to_apps_script(journey)

if __name__ == "__main__":
    main()
