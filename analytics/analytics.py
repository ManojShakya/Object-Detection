
import pika
import os
import pickle  # To deserialize and serialize frames
import struct  # To handle frame size unpacking
from ultralytics import YOLO
import datetime
import cv2
import logging
import time
import requests
import math
import importlib
import base64
import numpy as np

# List of YOLO object class names
Object_list = ['Person', 'Bicycle', 'Car', 'Motorcycle', 'Airplane', 'Bus', 'Train', 'Truck', 'Boat', 'Traffic Light', 'Fire Hydrant', 
               'Stop Sign', 'Parking Meter', 'Bench', 'Bird', 'Cat', 'Dog', 'Horse', 'Sheep', 'Cow', 'Elephant', 'Bear', 'Zebra', 
               'Giraffe', 'Backpack', 'Umbrella', 'Handbag', 'Tie', 'Suitcase', 'Frisbee', 'Skis', 'Snowboard', 'Sports Ball', 'Kite', 
               'Baseball Bat', 'Baseball Glove', 'Skateboard', 'Surfboard', 'Tennis Racket', 'Bottle', 'Wine Glass', 'Cup', 'Fork', 
               'Knife', 'Spoon', 'Bowl', 'Banana', 'Apple', 'Sandwich', 'Orange', 'Broccoli', 'Carrot', 'Hot Dog', 'Pizza', 'Donut', 
               'Cake', 'Chair', 'Couch', 'Potted Plant', 'Bed', 'Dining Table', 'Toilet', 'TV', 'Laptop', 'Mouse', 'Remote', 
               'Keyboard', 'Cell Phone', 'Microwave', 'Oven', 'Toaster', 'Sink', 'Refrigerator', 'Book', 'Clock', 'Vase', 
               'Scissors', 'Teddy Bear', 'Hair Drier', 'Toothbrush']

# Cattle for detection
CATTLE_CLASSES = ["cat", "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe"]

'''
person, cattle, fall detection, fire and smoke detection, helemt detection, seat belt detection, site helmet detection, triple riding
'''

# Load the YOLO model
# model = YOLO("yolov8m.pt")
# seat_belt_model=YOLO("belt_mobile_65v8s_best.pt")
# helmet_model = YOLO("hemletYoloV8_100epochs.pt")
# fall_model = YOLO("Fall_detectorv8.pt")
# fire_smoke_model = YOLO("fire_smoke_detectorv8.pt")


# Function to send logs to RabbitMQ
def send_log_to_rabbitmq(log_message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', heartbeat=600))
        channel = connection.channel()
        channel.queue_declare(queue='anpr_logs_1')  # Declare the queue for logs
        
        # Serialize the log message as JSON and send it to RabbitMQ
        channel.basic_publish(
            exchange='',
            routing_key='anpr_logs_1',
            body=pickle.dumps(log_message)
        )
        connection.close()
    except Exception as e:
        print(f"Failed to send log to RabbitMQ: {e}")

# Wrapper functions for logging and sending logs to RabbitMQ
def log_info(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "INFO",
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

def log_error(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "ERROR",
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)    

def log_exception(message):
    logging.error(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "EXCEPTION",
        "Event_Type":"Start threads for send frames",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)


# Directory to save frames 
vehicle_frame = "vehicle_frame_1"
os.makedirs(vehicle_frame, exist_ok=True)


def setup_rabbitmq_connection(rabbitmq_host, retries=15, retry_delay=5):
    """
    Set up a RabbitMQ connection and declare the queue.
    """
    for attempt in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host, heartbeat=600))
            channel = connection.channel()
            log_info(f"Connected to RabbitMQ at {rabbitmq_host}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            log_error(f"RabbitMQ connection failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(retry_delay)
    raise log_exception(f"Could not connect to RabbitMQ after {retries} attempts")


def publish_to_queue(camera_id, frame, processed_channel,processed_queue_name):
    """Publish processed data to RabbitMQ."""
    print("publish")
    processed_frame_data = {
        "camera_id": camera_id,
        "frame": frame
    }
    serialized_frame = pickle.dumps(processed_frame_data)
    processed_channel.basic_publish(exchange="", routing_key=processed_queue_name, body=serialized_frame)


def process_cattle(frame, results):
    """Process detections for 'Cattle on Road'."""
    cattle_detected = 0
    for result in results.boxes.data.tolist():
        x1, y1, x2, y2, cattle_score, cattle_id = result
        label = Object_list[cattle_id].lower()
        if label in CATTLE_CLASSES and cattle_score > 0.52:             
            # Draw bounding box
            cv2.rectangle(frame, (int(x1), int(y1)), (int(x2), int(y2)), (0, 255, 0), 2)
            cattle_detected += 1

    return frame, cattle_detected


module_cache = {}

def get_run_model(model_name, module_name, selected_classes, frame, detected_object):
    """Load detection module only once and reuse it"""
    if module_name not in module_cache:
        print(f"[INFO] Importing detection module: {module_name}")
        module = importlib.import_module(module_name)  # e.g., "model"
        module_cache[module_name] = module
    else:
        module = module_cache[module_name]

    # Call run_detection inside that module
    frame, detected_object = module.run_detection(model_name, frame, selected_classes, detected_object)
    return frame, detected_object

def process_frame(ch, method, properties, body,rabbitmq_host):
    """
    Callback function to process the received frames from RabbitMQ.

    Args:
        ch, method, properties: RabbitMQ parameters.
        body: The serialized frame data received from the queue.
        processed_channel: RabbitMQ channel for sending processed frames.
    """
    # if not processed_channel.is_open:
    #     log_error("Receiver channel is closed. Attempting to reconnect.")
    #     processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)
    try:
        # Deserialize the frame and metadata
        frame_data = pickle.loads(body)

        camera_id = frame_data["camera_id"]
        date_time = frame_data["date_time"]
        #frame = frame_data["frame"]
        jpg_original = base64.b64decode(frame_data["frame"])
        np_arr = np.frombuffer(jpg_original, np.uint8)
        frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
        user_id = frame_data["user_id"]
        model_file =  frame_data["model_file"]
        

        # Detect and classify objects in the frame
        detected_object = {}
        flag = 0
        #object_for_detection = object_list.lower()

        # if not object_for_detection:
        #     return frame, detected_object, flag
        
        results = []
        for m in model_file:
            model_name = m["model_name"]
            module_name = m["module_name"]
            selected_classes = m.get("classeslist", [])
            if not selected_classes:
                return frame, detected_object, flag
            frame,detected_object = get_run_model(model_name,module_name,selected_classes,frame,detected_object)    
            print(detected_object)        

                                
        # Serialize the processed license plate frame
        #print("Detected object :", detected_object)
        if detected_object:
            image_info = {
                "Event_Type":"Analytics",
                "CameraId": camera_id,
                'Datetime': datetime,
                'Image': frame,
                'Object': detected_object,
                "UserId": user_id,
            }
            serialized_frame = pickle.dumps(image_info)
            # Send the processed frame to the 'processed_frames' queue
            processed_channel.basic_publish(
                exchange="",
                routing_key="video_analytics_1",
                body=serialized_frame
            )
            log_info("Object detected successfully")
    except Exception as e:
        log_exception(f"Error processing frame: {e}")
        processed_connection, processed_channel = setup_rabbitmq_connection( rabbitmq_host)


def main(queue_name="all_frame", exchange_name="analytics_data", rabbitmq_host="localhost"):
    """
    Main function to set up RabbitMQ connections for receiving and sending frames.

    Args:
        queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'video_frames'.
        processed_queue_name (str): The RabbitMQ queue to send processed frames to. Defaults to 'processed_frames'.
    """
    # Set up RabbitMQ connection and channel for receiving frames
    receiver_connection, receiver_channel = setup_rabbitmq_connection(rabbitmq_host)
    
    # ---------------------------------------------------
    while True:
        try:
            if not receiver_channel.is_open:
                log_error("Receiver channel is closed. Attempting to reconnect.",rabbitmq_host)
                time.sleep(25)
                receiver_connection, receiver_channel = setup_rabbitmq_connection(rabbitmq_host)
            receiver_channel.exchange_declare(exchange=queue_name, exchange_type="fanout")
            receiver_channel.queue_declare(queue="all_frames")
            receiver_channel.queue_bind(exchange=queue_name, queue="all_frames")
            receiver_channel.basic_consume(
                queue="all_frames", 
                on_message_callback=lambda ch, method, properties, body: process_frame(
                    ch, method, properties, body, rabbitmq_host
                ),
                auto_ack=True
            )
            log_info("Waiting for video frames...",rabbitmq_host)
            receiver_channel.start_consuming()
        
        except pika.exceptions.ConnectionClosedByBroker as e:
            log_error("Connection closed by broker, reconnecting...",rabbitmq_host)
            time.sleep(25)
            receiver_connection, receiver_channel = setup_rabbitmq_connection(rabbitmq_host)
        except Exception as e:
            log_exception(f"Unexpected error: {e}", rabbitmq_host)
            time.sleep(25)
            continue 


if __name__ == "__main__":
    # Start the receiver and sender
    main()





















# def main(queue_name="all_frames_1", processed_queue_name="video_analytics_1", rabbitmq_host="localhost"):
#     """
#     Main function to set up RabbitMQ connections for receiving and sending frames.

#     Args:
#         queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'video_frames'.
#         processed_queue_name (str): The RabbitMQ queue to send processed frames to. Defaults to 'processed_frames'.
#     """
#     # Set up RabbitMQ connection and channel for receiving frames
#     receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name,rabbitmq_host)

#     # # Set up RabbitMQ connection and channel for sending processed frames
#     # processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)

#     # # ---------------------------------------------------
#     # if not receiver_channel.is_open:
#     #     log_error("Receiver channel is closed. Attempting to reconnect.")
#     #     receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
#     # if not processed_channel.is_open:
#     #     log_error("Receiver channel is closed. Attempting to reconnect.")
#     #     processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)    

#     while True:
#         try:
#             if not receiver_channel.is_open:
#                 log_error("Receiver channel is closed. Attempting to reconnect.")
#                 time.sleep(25)
#                 receiver_connection, receiver_channel = setup_rabbitmq_connection(rabbitmq_host)
#             # if not processed_channel.is_open:
#             #     log_error("Receiver channel is closed. Attempting to reconnect.")
#             #     time.sleep(25)
#             #     processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)
            
#             receiver_channel.basic_consume(
#                 queue=queue_name, 
#                 on_message_callback=lambda ch, method, properties, body: process_frame(
#                     ch, method, properties, body, processed_channel,processed_queue_name, rabbitmq_host
#                 ),
#                 auto_ack=True
#             )
#             log_info("Waiting for video frames...")
#             receiver_channel.start_consuming()
        
#         except pika.exceptions.ConnectionClosedByBroker as e:
#             log_error("Connection closed by broker, reconnecting...")
#             time.sleep(25)
#             receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name, rabbitmq_host)
#             processed_connection, processed_channel = setup_rabbitmq_connection(processed_queue_name, rabbitmq_host)
#         except Exception as e:
#             log_exception(f"Unexpected error: {e}")
#             time.sleep(25)
#             continue 
