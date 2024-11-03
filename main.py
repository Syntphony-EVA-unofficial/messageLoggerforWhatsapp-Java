from datetime import datetime, timedelta, timezone
import os
import concurrent
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS, cross_origin
from flask_socketio import SocketIO, emit
from pymongo import MongoClient
import logging
import pytz
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone


# Load environment variables from .env file
load_dotenv()

# Get the MongoDB URI from environment variables
MONGO_URI = os.getenv('MONGO_URI')

# Conectar a la base de datos MongoDB Atlas
client = MongoClient(MONGO_URI)
db = client['SyntphonyWhatsappLogs']  # Asegúrate de que este nombre coincide con el nombre de tus base de datos en Atlas

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")


app.config['CORS_HEADERS'] = 'Content-Type'

logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)



@app.route('/messageLogger', methods=['POST'])
def messageLogger():
    data = request.get_json()
    logger.info(f"Received a POST request to /messageLogger {data}")
    
    if not isinstance(data, dict) or "messages" not in data or not isinstance(data["messages"], list):
        return jsonify({"error": "Invalid data format, expected a dictionary with a 'messages' array"}), 400

    messages = data["messages"]
    metadata_batch = [get_metadata(message) for message in messages]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.submit(store_data_in_db, metadata_batch)

    return jsonify({"message": "Data logged successfully"}), 200



if __name__ == "__main__":
    port = int(os.getenv('PORT', 8080))
    logger.info(f"Starting Flask-SocketIO app on port {port}")
    socketio.run(app, host='0.0.0.0', port=port)

def store_data_in_db(metadata_batch):
    if not metadata_batch:
        logger.info("No metadata to process.")
        return

    metadata_batch = [metadata for metadata in metadata_batch if metadata['record']]
    if not metadata_batch:
        logger.info("No valid metadata to process after filtering.")
        return

    first_metadata = metadata_batch[0]
    phone_id = first_metadata.get('companyphone')
    client_phone = first_metadata.get('clientphone')
    current_time = datetime.now(timezone.utc).isoformat()

    if phone_id and client_phone:
        collection = db[phone_id]
        existing_doc = collection.find_one({"client_phone": client_phone})

        bulk_messages = [
            {
                "sender": metadata["data"]["sender"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "payload": metadata["data"]["data"]
            }
            for metadata in metadata_batch
        ]

        if existing_doc:
            last_interaction_time = datetime.fromisoformat(existing_doc["end_time"])
            if datetime.now(timezone.utc) - last_interaction_time < timedelta(hours=24):
                new_end_time = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()
                collection.update_one(
                    {"_id": existing_doc["_id"]},
                    {
                        "$set": {"end_time": new_end_time, "last_updated": current_time},
                        "$push": {"messages": {"$each": bulk_messages}}
                    }
                )
                logger.info(f"Updated existing document for client {client_phone} with new bulk messages")
                
                # Emit update to clients via SocketIO
                socketio.emit('update', {'phone_id': phone_id, 'client_phone': client_phone, 'update_type': 'update'}, broadcast=True)
            else:
                new_doc = {
                    "client_phone": client_phone,
                    "phoneid": phone_id,
                    "start_time": current_time,
                    "end_time": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
                    "created_at": current_time,
                    "last_updated": current_time,
                    "messages": bulk_messages
                }
                collection.insert_one(new_doc)
                logger.info(f"Created new document for client {client_phone} with bulk messages (new session)")
                
                # Emit new session to clients via SocketIO
                socketio.emit('update', {'phone_id': phone_id, 'client_phone': client_phone, 'update_type': 'new'}, broadcast=True)
        else:
            new_doc = {
                "client_phone": client_phone,
                "phoneid": phone_id,
                "start_time": current_time,
                "end_time": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
                "created_at": current_time,
                "last_updated": current_time,
                "messages": bulk_messages
            }
            collection.insert_one(new_doc)
            logger.info(f"Created new document for client {client_phone} with bulk messages")
            
            # Emit new session to clients via SocketIO
            socketio.emit('update', {'phone_id': phone_id, 'client_phone': client_phone, 'update_type': 'new'}, broadcast=True)
    else:
        logger.error("phone_id or client_phone not found in data")

def extract_status(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "statuses" in value:
                    return value["statuses"][0]["status"]   # Return the first status
        return None
    except KeyError:
        return None
    
def extract_phone_number_id_SM(data):
    try:
        phone_id = data.get("phoneId")
        return phone_id
    except KeyError:
        logger.error("phoneId not found in data")
        return None

def has_messages(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "messages" in value:
                    return True
        return False
    except KeyError:
        return False

def has_status(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "statuses" in value:
                    return True
        return False
    except KeyError:
        return False
    
def extract_phone_number_id_CM(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "metadata" in value and "phone_number_id" in value["metadata"]:
                    return value["metadata"]["phone_number_id"]
        return None
    except KeyError:
        logger.error("phone_number_id not found in data")
        return None
        
def extract_clientPhone_SM(data):
    try:
        return data["data"]["to"]
    except KeyError:
        logger.error("to field not found in data")
        return None

def extract_clientPhone_CM(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "messages" in value:
                    return value["messages"][0]["from"]  # Return the "from" field of the first message
        return None
    except KeyError:
        logger.error("from field not found in data")
        return None

def get_metadata(data):
    metadata = {}
    metadata['record'] = True  # Default to recording the data

    # Check if the message contains statuses and set `record` to False if so


    if data['sender'] == 'Client':
        if has_messages(data):
            logger.info(f"Data received from client {extract_clientPhone_CM(data)} has messages")
            metadata['clientphone'] = extract_clientPhone_CM(data)
            metadata['companyphone'] = extract_phone_number_id_CM(data)
        elif has_status(data):
            logger.info(f"Data received from client has status: {extract_status(data)}")
            metadata['clientphone'] = extract_recipient_id(data)  # Extract recipient_id
            metadata['companyphone'] = extract_phone_number_id_CM(data)
            metadata['record'] = False  # Do not record this data
        else:
            logger.error(f"Data received from client does not have messages or status: {data}")
            metadata['record'] = False
    elif data['sender'] == 'Syntphony':
        logger.info(f"Data received from Syntphony using: {extract_phone_number_id_SM(data)}")
        metadata['clientphone'] = extract_clientPhone_SM(data)
        metadata['companyphone'] = extract_phone_number_id_SM(data)
    else:
        logger.error(f"Data sender not recognized: {data}")
        metadata['record'] = False  # Do not process if sender is unrecognized

    metadata['data'] = data
    return metadata

def extract_recipient_id(data):
    try:
        for entry in data["data"]["entry"]:
            for change in entry["changes"]:
                value = change["value"]
                if "statuses" in value:
                    return value["statuses"][0]["recipient_id"]  # Return the recipient_id of the first status
        return None
    except KeyError:
        logger.error("recipient_id not found in data")
        return None
