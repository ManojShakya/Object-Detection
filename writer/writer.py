import pika
import os
import pickle  # To deserialize and serialize frames
import time
import cv2
import requests
import logging
import datetime
import threading

# MEDIA_FOLDER = os.path.join(os.getcwd(), "media")
# if not os.path.exists(MEDIA_FOLDER):
#     os.makedirs(MEDIA_FOLDER)

save_frame = 'media'
os.makedirs(save_frame, exist_ok=True)

# BaseUrl = 'https://vmspyapi.ajeevi.in'


def send_log_to_rabbitmq(log_message):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',heartbeat=600))
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
        "Event_Type":"Send Camera Details in Queue",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

def log_error(message):
    logging.info(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "ERROR",
        "Event_Type":"Send Camera Details in Queue",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)    

def log_exception(message):
    logging.error(message)
    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message_data = {
        "log_level" : "EXCEPTION",
        "Event_Type":"Send Camera Details in Queue",
        "Message":message,
        "datetime" : current_time,

    }
    send_log_to_rabbitmq(message_data)

# Global check_list and a timer dictionary
check_list = set()
clear_timer = {}

def clear_check_list_entry(object_count):
    time.sleep(300)  # Wait for 5 minutes
    check_list.discard(object_count)
    clear_timer.pop(object_count, None)

# def push_detection_data_to_base_url(camera_ip, camera_id, object_count, object_detect, framePath, alert_type, user_id):
#     api_url = f'https://vmspyapi.ajeevi.in/api/CameraAlert/'
#     object_detect_str = " ".join(object_detect) if isinstance(object_detect, list) else str(object_detect)
#     check_list = []
#     check_list = check_list.append(object_count)
#     if object_count not in check_list:
#         payload = {
#             "cameraId": int(camera_id),
#             "framePath": framePath,
#             "objectName": object_detect_str,
#             "objectCount": object_count,
#             "alertStatus": alert_type,
#             "userid": user_id

#         }

#         headers = {"accept": "*/*", "Content-Type": "application/json",}
#         print("Last data received :", payload)
#         try:
#             response = requests.post(api_url, json=payload, headers=headers)
#             response.raise_for_status()
#             print("This is respose :", response.raise_for_status())
#         except requests.RequestException as e:
#             print(f"Error pushing data to API: {e}")
#             #Logs(camera_ip, "Error", f"Error pushing data to API: {e}")
#             if e.response:
#                 print(f"Response Content: {e.response.text}")
def push_detection_data_to_base_url(camera_ip, camera_id, object_count, object_detect, framePath, alert_type, user_id):
    # api_url = 'https://vmspyapi.ajeevi.in/api/CameraAlert/'
    api_url = 'http://14.195.152.244:7006/api/CameraAlert/'

    object_detect_str = " ".join(object_detect) if isinstance(object_detect, list) else str(object_detect)

    if object_count not in check_list:
        # Add to check list
        check_list.add(object_count)

        # Start timer to clear after 5 minutes if not already running
        if object_count not in clear_timer:
            t = threading.Thread(target=clear_check_list_entry, args=(object_count,))
            t.daemon = True
            t.start()
            clear_timer[object_count] = t

        payload = {
            "cameraId": int(camera_id),
            "framePath": framePath,
            "objectName": object_detect_str,
            "objectCount": object_count,
            "alertStatus": alert_type,
            "userid": user_id
        }

        headers = {"accept": "*/*", "Content-Type": "application/json"}
        print("Last data received:", payload)

        try:
            response = requests.post(api_url, json=payload, headers=headers)
            response.raise_for_status()
            print("This is response:", response.text)
        except requests.RequestException as e:
            print(f"Error pushing data to API: {e}")
            if e.response:
                print(f"Response Content: {e.response.text}")

api_url ="http://14.195.152.244:7007/transaction_update"
 #api_url = os.getenv("CREDIT_URL")



def post_data(api_url, credit_id, camera_id, event_id=4):
    
    payload = {
        "event_credit_id": credit_id, 
        "device_id": camera_id, 
        "event_type_id":event_id
    }
    print(payload)
    try:
        response = requests.post(api_url, json=payload)
        response.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
        print("Data posted successfully:", response.json())
        log_info("Data posted successfully")
        return response
    except requests.exceptions.RequestException as e:
        print("An error occurred:", e)
        log_error(f"An error occurred {e}")
        return None

def setup_rabbitmq_connection(queue_name, retries=5, retry_delay=5):
    """
    Set up a RabbitMQ connection and declare the queue.
    """
    rabbitmq_host = "localhost"
    for attempt in range(retries):
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
            channel = connection.channel()
            channel.queue_declare(queue=queue_name)
            #print(f"Connected to RabbitMQ at {rabbitmq_host}")
            log_info(f"Connected to RabbitMQ at {rabbitmq_host}")
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            #print(f"RabbitMQ connection failed (attempt {attempt+1}/{retries}): {e}")
            log_error(f"RabbitMQ connection failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(retry_delay)
    raise Exception(log_exception(f"Could not connect to RabbitMQ after {retries} attempts"))

predic = {}
last_object_detected = None
def write_analytics(ch, method, properties, body):
    """
    Callback function to process the received frames from RabbitMQ.

    Args:
        ch, method, properties: RabbitMQ parameters.
        body: The serialized frame data received from the queue.
        sheet: Excel sheet object to write logs data.
        file_name: Name of the Excel file.
    """
    global last_object_detected
    try:
        # Deserialize the frame and metadata
        analytics_data = pickle.loads(body)
        event_type = analytics_data["Event_Type"]
        camera_id = analytics_data["CameraId"]
        camera_ip = analytics_data["CameraIp"]
        datetime = analytics_data["Datetime"]
        frame = analytics_data["Image"]
        object_detected = analytics_data["Object"]
        user_id = analytics_data["UserId"]
        credit_id = analytics_data["CreditId"]
        # Print the data to the console
        print("Detected Object :", object_detected)

        # Save the logs data to the Excel file  
        # Save frame if MEDIA_FOLDER is set
        # camera_folder = os.path.join(MEDIA_FOLDER, str(camera_ip))
        # os.makedirs(camera_folder, exist_ok=True)
        # filename = f"{datetime}.jpg"
        # file_path = os.path.join(camera_folder, filename)
        datetime = datetime.replace(":", "_").replace(" ", "_")

        frame_dir = os.path.join(save_frame, str(camera_ip))
        os.makedirs(frame_dir, exist_ok=True)
        full_frame_path = os.path.join(os.getcwd(), frame_dir, f'{datetime}.jpg')
        #cv2.imwrite(full_frame_path, frame)

        # Calculate object count
        object_count = sum(object_detected.values())
        if object_detected != last_object_detected:
            last_object_detected = object_detected.copy()
            try:
                cv2.imwrite(full_frame_path, frame)
                post_data(api_url,credit_id, camera_id)
                print("this full path :", full_frame_path)
                push_detection_data_to_base_url(camera_ip, camera_id, object_count, object_detected, full_frame_path, 'B', user_id)
                # print("Image saved")
                log_info("Image saved")
                #Logs(camera_ip, "Success", "Image saved")
            except Exception as e:
                #print(f"Error saving image: {e}")
                log_exception(f"Error saving image: {e}")
                



        
    except Exception as e:
        #print(f"Error processing frame: {e}")
        log_exception(f"Error processing frame: {e}")

def main(queue_name="video_analytics_1"):
    """
    Main function to set up RabbitMQ connections for receiving and sending frames.

    Args:
        queue_name (str): The RabbitMQ queue to consume frames from. Defaults to 'anpr_logs'.
        excel_file_name (str): The name of the Excel file to save logs.
    """
    # Set up the Excel file
    #workbook, sheet = setup_excel(excel_file_name)

    # Set up RabbitMQ connection and channel for receiving frames
    receiver_connection, receiver_channel = setup_rabbitmq_connection(queue_name)
    if not receiver_connection.is_open:
        log_error("Receiver channel is closed. Attempting to reconnect.")
        connection, channel = setup_rabbitmq_connection(queue_name)


    try:
        # Start consuming frames from the 'anpr_logs' queue
        receiver_channel.basic_consume(
            queue=queue_name, 
            on_message_callback=lambda ch, method, properties, body: write_analytics(
                ch, method, properties, body
            ),
            auto_ack=True
        )
        print("Waiting for logs message...")
        receiver_channel.start_consuming()
    except Exception as e:
        #print(f"An error occurred: {e}")
        log_error(f"An error occurred: {e}")
    finally:
        # Close the connections when done
        receiver_connection.close()
        print("Receiver stopped. RabbitMQ connections closed.")
        log_info("Receiver stopped. RabbitMQ connections closed.")

if __name__ == "__main__":
    # Start the receiver
    main()
