from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import pickle
import pika
import logging
import datetime
import os
import json
import threading
import time
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker

# -----------------------------------------------------------------------------
# Flask Initialization
# -----------------------------------------------------------------------------
app = Flask(__name__)
CORS(app)

# -----------------------------------------------------------------------------
# Logging Configuration
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -----------------------------------------------------------------------------
# Global RabbitMQ Connection Manager
# -----------------------------------------------------------------------------
class RabbitMQConnectionManager:
    """A thread-safe, persistent RabbitMQ connection manager."""
    
    def __init__(self, host='localhost'):
        self.host = host
        self.connection = None
        self.channel = None
        self.lock = threading.Lock()

    def connect(self):
        """Establish or reuse an existing RabbitMQ connection."""
        with self.lock:
            try:
                if self.connection and self.connection.is_open:
                    return  # already connected

                logging.info("[RabbitMQ] Connecting...")
                self.connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=self.host, heartbeat=600)
                )
                self.channel = self.connection.channel()
                self.channel.exchange_declare(exchange='rtspurl_for_framer', exchange_type='fanout', durable=True)
                self.channel.queue_declare(queue='logs', durable=True)
                logging.info("[RabbitMQ] Connection established ✅")

            except Exception as e:
                logging.error(f"[RabbitMQ] Connection failed ❌: {e}")
                self.connection = None
                self.channel = None

    def publish(self, exchange: str, routing_key: str, body: bytes):
        """Publish a message using the shared connection."""
        try:
            if not self.channel or self.channel.is_closed:
                self.connect()
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        except (AMQPConnectionError, ChannelClosedByBroker) as e:
            logging.error(f"[RabbitMQ] Publish error, reconnecting: {e}")
            self.connection = None
            self.channel = None
            time.sleep(1)
            self.connect()
            self.channel.basic_publish(exchange=exchange, routing_key=routing_key, body=body)
        except Exception as e:
            logging.error(f"[RabbitMQ] Failed to publish: {e}")

    def close(self):
        """Close RabbitMQ connection gracefully."""
        with self.lock:
            if self.connection and self.connection.is_open:
                self.connection.close()
                logging.info("[RabbitMQ] Connection closed ✅")

# Create a global RabbitMQ manager instance
rabbitmq_manager = RabbitMQConnectionManager()

# -----------------------------------------------------------------------------
# Helper Logging Functions
# -----------------------------------------------------------------------------
def send_log_to_rabbitmq(log_message: dict):
    """Send log data to RabbitMQ 'logs' queue."""
    try:
        rabbitmq_manager.publish(exchange='', routing_key='logs', body=pickle.dumps(log_message))
    except Exception as e:
        logging.error(f"Failed to send log to RabbitMQ: {e}")

def log_to_rabbit(level: str, event_type: str, message: str):
    """Log messages locally and push to RabbitMQ."""
    log_method = logging.info if level == "INFO" else logging.error
    log_method(message)

    log_data = {
        "log_level": level,
        "Event_Type": event_type,
        "Message": message,
        "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    send_log_to_rabbitmq(log_data)

def log_info(message: str):
    log_to_rabbit("INFO", "Push RTSPURL into Queue by API", message)

def log_exception(message: str):
    log_to_rabbit("EXCEPTION", "Push RTSPURL into Queue by API", message)

# -----------------------------------------------------------------------------
# API Endpoint: Start / Update Camera Details
# -----------------------------------------------------------------------------
@app.route('/CommanApiStart', methods=['POST'])
def update_camera_details():
    """Receive camera details and push them to RabbitMQ."""
    try:
        data = request.get_json(force=True)
        cameras = data.get("cameras", [])
        logging.info(f"Received camera data: {json.dumps(data, indent=2)}")

        if not cameras:
            log_exception("No cameras provided in the request.")
            return jsonify({"error": "No cameras provided!"}), 400

        rabbitmq_manager.connect()  # Ensure connection before loop

        for camera in cameras:
            required_fields = ["camera_id", "url", "user_id"]
            missing_fields = [f for f in required_fields if f not in camera]
            if missing_fields:
                msg = f"Missing required fields: {missing_fields} for camera {camera.get('camera_id', 'Unknown')}"
                log_exception(msg)
                return jsonify({"error": msg}), 400

            # Prepare frame payload
            frame_data = {
                "CameraId": camera["camera_id"],
                "CameraUrl": camera["url"],
                "Running": camera.get("running", False),
                "UserId": camera["user_id"],
                "ModelFile": camera.get("model_file", "[]")
            }

            try:
                rabbitmq_manager.publish(
                    exchange='rtspurl_for_framer',
                    routing_key='',
                    body=pickle.dumps(frame_data)
                )
                log_info(f"Frame from camera {frame_data['CameraId']} pushed to RabbitMQ successfully.")
            except Exception as e:
                log_exception(f"Failed to publish message for camera {camera['camera_id']}: {e}")
                return jsonify({"error": f"RabbitMQ publish failed for {camera['camera_id']}!"}), 500

        log_info("All cameras processed successfully.")
        return jsonify({"message": "Cameras added/updated successfully!"}), 201

    except Exception as e:
        log_exception(f"Unexpected error: {e}")
        return jsonify({"error": str(e)}), 500

# -----------------------------------------------------------------------------
# API Endpoint: Serve Image Files
# -----------------------------------------------------------------------------
@app.route('/app/<folder>/<camera_id>/<filename>', methods=['GET'])
def get_image(folder, camera_id, filename):
    """Serve image files stored under given folder/camera."""
    try:
        camera_folder = os.path.join(os.getcwd(), folder, camera_id)
        return send_from_directory(camera_folder, filename)
    except Exception as e:
        log_exception(f"Failed to serve image {filename} for camera {camera_id}: {e}")
        return jsonify({"error": f"File not found: {filename}"}), 404

# -----------------------------------------------------------------------------
# Application Entry Point
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    rabbitmq_manager.connect()  # Connect once at startup
    try:
        app.run(host='0.0.0.0', port=6565, debug=True)
    finally:
        rabbitmq_manager.close()

















# from flask import Flask, request, jsonify, send_from_directory
# from flask_cors import CORS
# import pickle
# import pika
# import logging
# import datetime
# import os
# import json

# # -----------------------------------------------------------------------------
# # Flask App Initialization
# # -----------------------------------------------------------------------------
# app = Flask(__name__)
# CORS(app)

# # -----------------------------------------------------------------------------
# # Logging Configuration
# # -----------------------------------------------------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s [%(levelname)s] %(message)s",
#     handlers=[logging.StreamHandler()]
# )

# # -----------------------------------------------------------------------------
# # RabbitMQ Connection Utility
# # -----------------------------------------------------------------------------
# def get_rabbitmq_connection():
#     """Establish and return a RabbitMQ connection."""
#     try:
#         connection = pika.BlockingConnection(
#             pika.ConnectionParameters(host='localhost', heartbeat=600)
#         )
#         return connection
#     except Exception as e:
#         raise ConnectionError(f"RabbitMQ connection failed: {e}")

# # -----------------------------------------------------------------------------
# # Function to Send Logs to RabbitMQ
# # -----------------------------------------------------------------------------
# def send_log_to_rabbitmq(log_message: dict):
#     """Send log data to RabbitMQ 'logs' queue."""
#     try:
#         connection = get_rabbitmq_connection()
#         channel = connection.channel()
#         channel.queue_declare(queue='logs')
#         channel.basic_publish(exchange='', routing_key='logs', body=pickle.dumps(log_message))
#         connection.close()
#     except Exception as e:
#         logging.error(f"Failed to send log to RabbitMQ: {e}")

# # -----------------------------------------------------------------------------
# # Logging Wrappers
# # -----------------------------------------------------------------------------
# def log_to_rabbit(level: str, event_type: str, message: str):
#     """Helper function to log messages both locally and to RabbitMQ."""
#     log_method = logging.info if level == "INFO" else logging.error
#     log_method(message)

#     log_data = {
#         "log_level": level,
#         "Event_Type": event_type,
#         "Message": message,
#         "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     }
#     send_log_to_rabbitmq(log_data)

# def log_info(message: str):
#     log_to_rabbit("INFO", "Push RTSPURL into Queue by API", message)

# def log_exception(message: str):
#     log_to_rabbit("EXCEPTION", "Push RTSPURL into Queue by API", message)

# # -----------------------------------------------------------------------------
# # API Endpoint: Start / Update Camera Details
# # -----------------------------------------------------------------------------
# @app.route('/CommanApiStart', methods=['POST'])
# def update_camera_details():
#     """Receive camera details and push them to RabbitMQ."""
#     try:
#         data = request.get_json(force=True)
#         cameras = data.get("cameras", [])
#         logging.info(f"Received camera data: {json.dumps(data, indent=2)}")

#         if not cameras:
#             log_exception("No cameras provided in the request.")
#             return jsonify({"error": "No cameras provided!"}), 400

#         for camera in cameras:
#             required_fields = ["camera_id", "url", "user_id"]
#             missing_fields = [f for f in required_fields if f not in camera]
#             if missing_fields:
#                 msg = f"Missing required fields: {missing_fields} for camera {camera.get('camera_id', 'Unknown')}"
#                 log_exception(msg)
#                 return jsonify({"error": msg}), 400

#             frame_data = {
#                 "CameraId": camera["camera_id"],
#                 "CameraUrl": camera["url"],
#                 "Running": camera.get("running", False),
#                 "UserId": camera["user_id"],
#                 "ModelFile": camera.get("model_file", "[]")
#             }

#             try:
#                 connection = get_rabbitmq_connection()
#                 channel = connection.channel()
#                 channel.exchange_declare(exchange='rtspurl_for_framer', exchange_type='fanout')
#                 channel.basic_publish(
#                     exchange='rtspurl_for_framer',
#                     routing_key='',
#                     body=pickle.dumps(frame_data)
#                 )
#                 connection.close()
#                 log_info(f"Frame from camera {frame_data['CameraId']} pushed to RabbitMQ successfully.")
#             except Exception as e:
#                 log_exception(f"Failed to publish message for camera {camera['camera_id']}: {e}")
#                 return jsonify({"error": f"RabbitMQ publish failed for {camera['camera_id']}!"}), 500

#         log_info("All cameras processed successfully.")
#         return jsonify({"message": "Cameras added/updated successfully!"}), 201

#     except Exception as e:
#         log_exception(f"Unexpected error: {e}")
#         return jsonify({"error": str(e)}), 500

# # -----------------------------------------------------------------------------
# # API Endpoint: Serve Image Files
# # -----------------------------------------------------------------------------
# @app.route('/app/<folder>/<camera_id>/<filename>', methods=['GET'])
# def get_image(folder, camera_id, filename):
#     """Serve image files stored under given folder/camera."""
#     try:
#         camera_folder = os.path.join(os.getcwd(), folder, camera_id)
#         return send_from_directory(camera_folder, filename)
#     except Exception as e:
#         log_exception(f"Failed to serve image {filename} for camera {camera_id}: {e}")
#         return jsonify({"error": f"File not found: {filename}"}), 404

# # -----------------------------------------------------------------------------
# # Main Entry Point
# # -----------------------------------------------------------------------------
# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=6565, debug=True)


























# # from flask import Flask, request, jsonify,send_from_directory
# # from flask_cors import CORS
# # import pickle
# # import pika
# # import logging
# # import datetime
# # import os


# # app = Flask(__name__)
# # CORS(app)

# # # Function to send logs to RabbitMQ
# # def send_log_to_rabbitmq(log_message):
# #     try:
# #         connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=600))
# #         channel = connection.channel()
# #         channel.queue_declare(queue='logs')
        
# #         # Serialize the log message as JSON and send it to RabbitMQ
# #         channel.basic_publish(
# #             exchange='',
# #             routing_key='logs',
# #             body=pickle.dumps(log_message)
# #         )
# #         connection.close()
# #     except Exception as e:
# #         print(f"Failed to send log to RabbitMQ: {e}")

# # # Wrapper functions for different log levels
# # def log_info(message):
# #     logging.info(message)
# #     current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# #     message_data = {
# #         "log_level" : "INFO",
# #         "Event_Type":"Push RTSPULR into Queue by API",
# #         "Message":message,
# #         "datetime" : current_time,

# #     }
# #     send_log_to_rabbitmq(message_data)

# # def log_exception(message):
# #     logging.error(message)
# #     current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
# #     message_data = {
# #         "log_level" : "EXCEPTION",
# #         "Event_Type":"Push RTSPULR into Queue by API",
# #         "Message":message,
# #         "datetime" :current_time,

# #     }
# #     send_log_to_rabbitmq(message_data)

# # @app.route('/CommanApiStart', methods=['POST'])
# # def update_camera_details():
# #     data = request.get_json()
# #     print("This is data:",data)

# #     cameras = data.get("cameras", [])
# #     if not cameras:
# #         log_exception("No cameras provided in the request.")
# #         return jsonify({"error": "No cameras provided!"}),

# #     for camera in cameras:
# #         required_fields = ["camera_id", "url"]

# #         if not all(field in camera for field in required_fields):
# #             missing_fields = [field for field in required_fields if field not in camera]
# #             log_exception(f"Missing required fields: {missing_fields} for camera {camera.get('camera_id', 'Unknown')}!")
# #             return jsonify({"error": f"Missing required fields in camera details for camera {camera.get('camera_id')}!"}), 400

# #         camera_id = camera["camera_id"]
# #         camera_url = camera["url"]
# #         running = camera.get("running", False)
# #         user_id = camera["user_id"]
# #         model_file = camera.get("model_file", "[]")
# #         print("This is object list :", model_file, type(model_file))

# #         # Connect to RabbitMQ
# #         try:
# #             connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=600))
# #             channel = connection.channel()
# #             log_info(f"Connected to RabbitMQ for camera {camera_id}.")
# #             try:
                
# #                 # Declare an exchange
# #                 channel.exchange_declare(exchange='rtspurl_for_framer', exchange_type='fanout')
# #             except pika.exceptions.ChannelClosedByBroker:
# #                 channel = connection.channel()
# #                 channel.exchange_declare(exchange='rtspurl_for_framer', exchange_type='fanout')
# #                 log_info(f"Queue 'rtspurl_from_api_db' declared successfully.")
# #         except Exception as e:
# #             log_exception(f"Failed to connect to RabbitMQ for camera {camera_id}: {e}")
# #             return jsonify({"error": "Failed to connect to RabbitMQ!"}), 500

# #         frame_data = {
# #             "CameraId": camera_id,
# #             "CameraUrl": camera_url,
# #             "Running": running,
# #             "UserId": user_id,
# #             "ModelFile": model_file
            
# #         }
# #         serialized_frame = pickle.dumps(frame_data)
# #         print("This is frame_data :", frame_data)

# #         # Send the frame to the queue
# #         try:
# #             channel.basic_publish(
# #                 exchange="rtspurl_for_framer",
# #                 routing_key="",
# #                 body=serialized_frame
# #             )
# #             log_info(f"Sent a frame from camera {camera_id} to RabbitMQ queue.")
# #         except Exception as e:
# #             log_exception(f"Failed to publish message for camera {camera_id}: {e}")

# #     log_info("Cameras added/updated successfully.")
# #     return jsonify({"message": "Cameras added/updated successfully!"}), 201

# # @app.route('/app/<folder>/<camera_id>/<filename>')
# # def get_image(folder,camera_id, filename):
# #     print(camera_id,filename)
# #     camera_folder = os.path.join(os.path.join(os.getcwd(), folder),camera_id)
# #     print(camera_folder)
# #     return send_from_directory(camera_folder, filename)

# # if __name__ == '__main__':
# #     app.run(host='0.0.0.0', port=6565, debug=True)
