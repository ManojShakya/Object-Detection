#!/usr/bin/env python3
"""
Real-time camera framer + RabbitMQ publisher
- Main process: consumes camera start/stop messages from 'rtspurl_for_framer' (fanout exchange)
  and manages camera worker processes.
- Worker process: connects to camera, captures frames at interval, encodes to JPEG, publishes
  serialized payload to 'all_frame' exchange (fanout).
"""

import os
import time
import json
import pickle
import signal
import logging
import datetime
import threading
from multiprocessing import Process, current_process
from typing import Any, Dict, List, Optional

import cv2
import pika
from pika.exceptions import AMQPConnectionError, ChannelClosedByBroker, ProbableAuthenticationError

# -------------------------------
# CONFIG
# -------------------------------
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
CAMERA_EXCHANGE = "rtspurl_for_framer"   # exchange from API -> our consumer; type=fanout
CAMERA_QUEUE = "camera_url_detail"       # queue we consume from (bound to CAMERA_EXCHANGE)
FRAME_EXCHANGE = "all_frame"             # exchange where workers publish frames (fanout)
LOG_QUEUE = "framer_logs"                # queue for logs
FRAME_INTERVAL = int(os.getenv("FRAME_INTERVAL", 20))  # send every N frames (default 20)
FRAME_MAX_RETRIES = 50
RECONNECT_DELAY = 2  # seconds for RabbitMQ reconnect attempts
SHOW_DEBUG_WINDOW = os.getenv("SHOW_DEBUG_WINDOW", "0") == "1"  # set=1 for local debug display

# -------------------------------
# LOGGING
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("framer_service")

# -------------------------------
# Utilities: RabbitMQ helpers
# -------------------------------


def create_blocking_connection(host: str, heartbeat: int = 600, retry: int = 5, retry_delay: float = 2.0):
    """
    Create and return a BlockingConnection. Retries on failure.
    Each process should call this to get its own connection.
    """
    attempt = 0
    while attempt < retry:
        try:
            params = pika.ConnectionParameters(host=host, heartbeat=heartbeat)
            conn = pika.BlockingConnection(params)
            return conn
        except (AMQPConnectionError, ProbableAuthenticationError) as e:
            attempt += 1
            logger.warning("RabbitMQ connection attempt %d/%d failed: %s", attempt, retry, str(e))
            time.sleep(retry_delay)
    raise ConnectionError(f"Could not connect to RabbitMQ at {host} after {retry} attempts")


def declare_fanout_exchange(channel: pika.channel.Channel, exchange_name: str, durable: bool = False):
    channel.exchange_declare(exchange=exchange_name, exchange_type="fanout", durable=durable)


# -------------------------------
# Logger that publishes logs to RabbitMQ (in-process)
# -------------------------------
class RabbitMQLogger:
    def __init__(self, host: str = RABBITMQ_HOST, queue: str = LOG_QUEUE):
        self.host = host
        self.queue = queue
        self._conn: Optional[pika.BlockingConnection] = None
        self._channel: Optional[pika.channel.Channel] = None
        self._ensure_connection()

    def _ensure_connection(self):
        if self._conn and self._conn.is_open:
            return
        try:
            self._conn = create_blocking_connection(self.host, retry=3, retry_delay=1.0)
            self._channel = self._conn.channel()
            self._channel.queue_declare(queue=self.queue, durable=True)
            logger.info("Logger connected to RabbitMQ queue=%s", self.queue)
        except Exception as e:
            logger.warning("Logger failed to connect to RabbitMQ: %s", str(e))
            self._conn = None
            self._channel = None

    def send(self, level: str, event: str, message: str):
        payload = {
            "log_level": level,
            "Event_Type": event,
            "Message": message,
            "datetime": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "process": current_process().pid
        }
        # local log
        if level == "ERROR" or level == "EXCEPTION":
            logger.error(message)
        else:
            logger.info(message)

        # try to publish to rabbitmq
        try:
            self._ensure_connection()
            if self._channel:
                self._channel.basic_publish(
                    exchange='',
                    routing_key=self.queue,
                    body=pickle.dumps(payload),
                    properties=pika.BasicProperties(delivery_mode=2)  # persistent
                )
        except Exception as e:
            logger.warning("Failed to publish log: %s", str(e))
            # reset connection; next call will try to reconnect
            self._conn = None
            self._channel = None


# Global logger instance (main process)
mq_logger = RabbitMQLogger()


# -------------------------------
# Worker: capture frames and publish
# -------------------------------
class CameraWorker:
    """
    A single camera worker (runs inside a separate process).
    It opens its own RabbitMQ connection and publishes encoded frames.
    """

    def __init__(
        self,
        camera_url: str,
        camera_id: int,
        user_id: int,
        object_list: Optional[List[str]],
        rabbitmq_host: str = RABBITMQ_HOST,
        frame_interval: int = FRAME_INTERVAL,
        max_retries: int = FRAME_MAX_RETRIES
    ):
        self.camera_url = camera_url
        self.camera_id = camera_id
        self.user_id = user_id
        self.object_list = object_list or []
        self.rabbitmq_host = rabbitmq_host
        self.frame_interval = frame_interval
        self.max_retries = max_retries
        self._stop_requested = False

        # Each worker process will create its own logger instance to publish worker-specific logs
        self.logger = RabbitMQLogger(host=self.rabbitmq_host, queue=LOG_QUEUE)

        # Child process connections must be created after fork, so set to None now
        self.conn: Optional[pika.BlockingConnection] = None
        self.chan: Optional[pika.channel.Channel] = None

    def _connect_rabbit(self):
        """Open a fresh RabbitMQ connection and declare frame exchange."""
        if self.conn and self.conn.is_open and self.chan and not self.chan.is_closed:
            return
        self.conn = create_blocking_connection(self.rabbitmq_host, retry=5, retry_delay=1.0)
        self.chan = self.conn.channel()
        declare_fanout_exchange(self.chan, FRAME_EXCHANGE, durable=True)
        # we publish to an exchange (fanout); consumers will bind queues to this exchange
        self.logger.send("INFO", "Worker", f"Worker connected to RabbitMQ for camera {self.camera_id}")

    def stop(self):
        self._stop_requested = True

    def run(self):
        """Main capture loop. This function should be invoked inside a child Process target."""
        # NOTE: This method is executed in a child process.
        # Create/process-local RabbitMQ logger (already created in __init__)
        try:
            self._connect_rabbit()
        except Exception as e:
            self.logger.send("ERROR", "Worker", f"Could not connect to RabbitMQ: {e}")
            return

        retry_count = 0
        frame_counter = 0

        # Accept numeric indexes for webcam (e.g., 0) or RTSP urls
        cap_target = None
        try:
            # allow numeric camera id strings or actual strings
            try:
                cap_target = int(self.camera_url)
            except Exception:
                cap_target = self.camera_url
        except Exception:
            cap_target = self.camera_url

        while not self._stop_requested and retry_count < self.max_retries:
            cap = cv2.VideoCapture(cap_target)
            if not cap.isOpened():
                retry_count += 1
                self.logger.send("ERROR", "Worker", f"Camera {self.camera_id}: cannot open stream (attempt {retry_count})")
                time.sleep(2 + retry_count * 0.5)
                continue

            self.logger.send("INFO", "Worker", f"Camera {self.camera_id}: capture started")
            last_frame_time = time.time()

            try:
                while not self._stop_requested:
                    ret, frame = cap.read()
                    if not ret:
                        # no frame, maybe short network blip — break to reconnect if extended
                        if time.time() - last_frame_time > 5:
                            self.logger.send("ERROR", "Worker", f"Camera {self.camera_id}: no frames for 5s, reconnecting")
                            break
                        continue

                    last_frame_time = time.time()
                    frame_counter += 1

                    if frame_counter % self.frame_interval != 0:
                        continue  # skip frames to reduce load

                    frame_counter = 0
                    # compress frame to JPEG to reduce size
                    success, jpg = cv2.imencode('.jpg', frame, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
                    if not success:
                        self.logger.send("ERROR", "Worker", f"Camera {self.camera_id}: JPEG encoding failed")
                        continue
                    jpg_bytes = jpg.tobytes()

                    payload = {
                        "camera_id": self.camera_id,
                        "user_id": self.user_id,
                        "date_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "object_list": self.object_list,
                        # store encoded bytes; consumer should decode jpeg
                        "frame_jpeg": jpg_bytes
                    }

                    # ensure rabbit connection is ready
                    try:
                        if not (self.conn and self.conn.is_open and self.chan and not self.chan.is_closed):
                            self._connect_rabbit()
                        # publish encoded frame as pickled payload (persistent)
                        self.chan.basic_publish(
                            exchange=FRAME_EXCHANGE,
                            routing_key="",
                            body=pickle.dumps(payload),
                            properties=pika.BasicProperties(delivery_mode=1)  # non-persistent to reduce disk pressure; change to 2 if persistence required
                        )
                        self.logger.send("INFO", "Worker", f"Camera {self.camera_id}: frame published (pid:{current_process().pid})")
                    except (AMQPConnectionError, ChannelClosedByBroker) as e:
                        # on publish failure, reconnect and retry once
                        self.logger.send("ERROR", "Worker", f"Camera {self.camera_id}: publish error {e}, reconnecting")
                        self.conn = None
                        self.chan = None
                        time.sleep(1)
                        continue
                    except Exception as e:
                        self.logger.send("EXCEPTION", "Worker", f"Camera {self.camera_id}: unexpected publish error {e}")
                        # keep running but log; do not crash
                        continue

                    # optional debug window on local machine
                    if SHOW_DEBUG_WINDOW:
                        cv2.imshow(f"Camera-{self.camera_id}", frame)
                        if cv2.waitKey(1) & 0xFF == ord('q'):
                            self.logger.send("INFO", "Worker", f"Camera {self.camera_id}: debug stop requested")
                            self._stop_requested = True
                            break

                # end inner loop
            except Exception as e:
                self.logger.send("EXCEPTION", "Worker", f"Camera {self.camera_id}: main loop error: {e}")
            finally:
                try:
                    cap.release()
                except Exception:
                    pass
                if SHOW_DEBUG_WINDOW:
                    try:
                        cv2.destroyAllWindows()
                    except Exception:
                        pass

                # increase retry counter and back off
                retry_count += 1
                time.sleep(min(5 + retry_count, 30))

        self.logger.send("ERROR" if retry_count >= self.max_retries else "INFO",
                         "Worker",
                         f"Camera {self.camera_id}: worker exiting after {retry_count} retries")
        # close rabbit connection if open
        try:
            if self.conn and self.conn.is_open:
                self.conn.close()
        except Exception:
            pass


# -------------------------------
# Process management (main process)
# -------------------------------
camera_processes: Dict[int, Process] = {}
camera_meta: Dict[int, Dict[str, Any]] = {}  # store url/userid/object_list

# Graceful shutdown
_shutdown_requested = False


def start_camera_process(camera_url: str, camera_id: int, user_id: int, object_list: Optional[List[str]], frame_interval: int = FRAME_INTERVAL):
    """
    Spawn a separate process for the camera worker.
    """
    if camera_id in camera_processes and camera_processes[camera_id].is_alive():
        logger.info("Process already running for camera %s", camera_id)
        return

    worker = CameraWorker(camera_url=camera_url, camera_id=camera_id, user_id=user_id, object_list=object_list, frame_interval=frame_interval)
    p = Process(target=worker.run, args=(), daemon=True)
    p.start()
    camera_processes[camera_id] = p
    camera_meta[camera_id] = {"camera_url": camera_url, "user_id": user_id, "object_list": object_list}
    logger.info("Started process %d for camera %s", p.pid, camera_id)


def stop_camera_process(camera_id: int):
    """
    Terminate the camera process if it exists.
    """
    p = camera_processes.get(camera_id)
    if not p:
        logger.warning("No process found for camera %s", camera_id)
        return
    if p.is_alive():
        logger.info("Terminating process %d for camera %s", p.pid, camera_id)
        p.terminate()
        p.join(timeout=10)
    camera_processes.pop(camera_id, None)
    camera_meta.pop(camera_id, None)
    logger.info("Stopped camera %s", camera_id)


def monitor_camera_processes(interval: int = 25):
    """
    Monitor child processes and restart if needed (for camera_status == True).
    """
    while not _shutdown_requested:
        for cam_id, proc in list(camera_processes.items()):
            if not proc.is_alive():
                logger.warning("Camera process for %s died unexpectedly. Restarting...", cam_id)
                meta = camera_meta.get(cam_id)
                if meta:
                    start_camera_process(meta["camera_url"], cam_id, meta["user_id"], meta.get("object_list", []))
                else:
                    logger.error("No metadata to restart camera %s", cam_id)
        time.sleep(interval)


# -------------------------------
# Main consumer: fetch camera data messages and control processes
# -------------------------------


def on_camera_message(ch, method, properties, body):
    """
    Callback: receive camera control messages (expected pickled dict like in your API).
    Expected fields: CameraId, CameraUrl, Running, UserId, ObjectList (optional)
    """
    try:
        camera_data = pickle.loads(body)
        cam_id = camera_data.get("CameraId")
        cam_url = camera_data.get("CameraUrl")
        running = camera_data.get("Running", False)
        # normalize boolean-like strings
        if isinstance(running, str):
            running = running.strip().lower() in ("1", "true", "yes", "on")
        user_id = camera_data.get("UserId")
        object_list = camera_data.get("ObjectList") or camera_data.get("ModelFile") or []

        logger.info("Received control message for camera %s running=%s", cam_id, running)

        if running:
            start_camera_process(cam_url, cam_id, user_id, object_list)
        else:
            # stop
            stop_camera_process(cam_id)

    except Exception as e:
        logger.exception("Failed to process camera control message: %s", e)


def consume_camera_control_loop(rabbit_host: str = RABBITMQ_HOST):
    """
    Connect to CAMERA_EXCHANGE (fanout) and consume camera control messages.
    We'll create a durable, exclusive queue for this consumer and bind it.
    """
    conn = create_blocking_connection(rabbit_host, retry=5, retry_delay=2.0)
    ch = conn.channel()
    declare_fanout_exchange(ch, CAMERA_EXCHANGE, durable=True)

    # create a durable named queue and bind it to exchange; name matches CAMERA_QUEUE
    ch.queue_declare(queue=CAMERA_QUEUE, durable=True)
    ch.queue_bind(exchange=CAMERA_EXCHANGE, queue=CAMERA_QUEUE)
    logger.info("Waiting for camera control messages on queue=%s bound to exchange=%s", CAMERA_QUEUE, CAMERA_EXCHANGE)

    ch.basic_consume(queue=CAMERA_QUEUE, on_message_callback=on_camera_message, auto_ack=True)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        ch.stop_consuming()
    except Exception as e:
        logger.exception("Camera consumer loop crashed: %s", e)
    finally:
        try:
            conn.close()
        except Exception:
            pass


# -------------------------------
# Signal handlers
# -------------------------------
def _signal_handler(sig, frame):
    global _shutdown_requested
    logger.info("Signal %s received, shutting down...", sig)
    _shutdown_requested = True
    # stop all child processes
    for cam_id in list(camera_processes.keys()):
        stop_camera_process(cam_id)
    # exit after giving children time
    time.sleep(1)
    raise SystemExit(0)


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# -------------------------------
# Entrypoint
# -------------------------------
if __name__ == "__main__":
    # start monitor thread
    monitor_thread = threading.Thread(target=monitor_camera_processes, kwargs={"interval": 25}, daemon=True)
    monitor_thread.start()

    # start consuming control messages (blocking)
    try:
        consume_camera_control_loop(RABBITMQ_HOST)
    except SystemExit:
        logger.info("Shutdown complete")
    except Exception as e:
        logger.exception("Fatal error in main: %s", e)
