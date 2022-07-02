import zmq
import time
import logging
import json
import yaml
import os
import argparse

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
from zmq_helper import MessageStore, ThreadedSubscriber

parser = argparse.ArgumentParser(description="ZMQ Message Queue")
parser.add_argument(
    "--config", type=str, default="configuration.yaml", help="config file"
)
args = parser.parse_args()
# parse yaml configuration file
with open(args.config, "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        exit(1)

sub_config = config.get("subscriber")
srv_config = config.get("server")
SUB_HOST = sub_config.get("host", "localhost")
SUB_PORT = sub_config.get("port", 5556)
SRV_HOST = srv_config.get("host", "*")
SRV_PORT = srv_config.get("port", 5557)
PERSISTENT_DIRECTORY = config.get("persisitent_directory", "queue")
if not os.path.exists(PERSISTENT_DIRECTORY):
    os.makedirs(PERSISTENT_DIRECTORY)
    logging.info("Created persistent directory: {}".format(PERSISTENT_DIRECTORY))

# Subscriber
context = zmq.Context()
subscriber = context.socket(zmq.SUB)
subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

subscriber.connect("tcp://{}:{}".format(SUB_HOST, SUB_PORT))
logging.info("Subscriber connected to {}:{}".format(SUB_HOST, SUB_PORT))

server = context.socket(zmq.REP)
server.bind("tcp://{}:{}".format(SRV_HOST, SRV_PORT))
logging.info("Server bound to {}:{}".format(SRV_HOST, SRV_PORT))

storage = MessageStore(PERSISTENT_DIRECTORY)


def on_exception(exception: Exception) -> None:
    logging.error("Exception: {}".format(exception))


with ThreadedSubscriber(
    callback=storage.add_message, subscriber=subscriber, on_exception=on_exception
):
    while True:
        rec_msg = server.recv_json()
        """
        rec_msg:
            0: get first message
            1: get first message and remove it from queue
            2: remove first message from queue
        response codes:
            0: no data available
            1: first message removed from queue

        """
        logging.info("Received request code {}".format(rec_msg))

        if rec_msg == 0 or rec_msg == 1:
            msg_raw = storage.front()
            if msg_raw is not None:
                msg_json = json.loads(msg_raw)
                server.send_json(msg_json)
                if rec_msg == 1:
                    storage.remove_first()
            else:
                server.send_json(0)
        elif rec_msg == 2:
            storage.remove_first()
            server.send_json(1)

        time.sleep(0.01)
