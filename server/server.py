from flask import Flask, request, jsonify, make_response
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from PIL import Image
import json
import datetime
import zmq
import time
import asyncio
import zmq.asyncio
import threading
import yaml
import argparse
from utils import get_path_filename, save_message

parser = argparse.ArgumentParser(description="ZMQ Message Queue Server")
parser.add_argument(
    "--config", type=str, default="config.yaml", help="config file"
)
args = parser.parse_args()


# load yaml file
with open(args.config, "r") as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)
        exit(1)
server_config = config.get("server")
SRV_HOST = server_config.get("host")
SRV_PORT = server_config.get("port")
SRV_USER = server_config.get("basic_auth_user")
SRV_PASSWORD = server_config.get("basic_auth_password")

zmq_config = config.get("zmq")
ZMQ_HOST = zmq_config.get("host")
ZMQ_PORT = zmq_config.get("port")

RESULT_SAVE_PATH = config.get("result_save_path")

app = Flask(__name__)

auth = HTTPBasicAuth()

users = {SRV_USER: generate_password_hash(SRV_PASSWORD)}


class zmqclient:
    def __init__(self):
        self.context = zmq.asyncio.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.port = ZMQ_PORT
        self.host = ZMQ_HOST

    async def send_async(self, msg=None):
        pub = self.context.socket(zmq.PUB)
        print("Connecting to {}:{}".format(self.host, self.port))
        pub.bind("tcp://{}:{}".format(self.host, self.port))
        # while True:
        await asyncio.sleep(1)
        await pub.send_string(msg)
        await asyncio.sleep(0.3)

    def close(self):
        self.socket.close()
        self.context.term()


zmclient = zmqclient()
loop = asyncio.get_event_loop()
mutex = threading.Lock()


def loop_in_thread(loop, coro):
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coro)


def transmit_results_zmq(results):
    t = threading.Thread(
        target=loop_in_thread, args=(loop, zmclient.send_async(results))
    )
    try:
        while loop.is_running():
            time.sleep(0.05)
        time.sleep(0.05)
        t.start()

    except Exception as e:
        print("Error: unable to start thread")




@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username


@app.route("/results/flower", methods=["POST"])
@auth.login_required
def flower_results():
    # check if the post request has json data
    if not request.json:
        return make_response(jsonify(result="error"), 400)
    res = request.get_json()
    #print(json.dumps(res, indent=4))
    mutex.acquire()
    transmit_results_zmq(json.dumps(res))
    mutex.release()
    return make_response(jsonify(result="ok"), 200)

@app.route("/results/pollinator", methods=["POST"])
@auth.login_required
def pollinator_results():
    # check if the post request has json data
    if not request.json:
        return make_response(jsonify(result="error"), 400)
    res = request.get_json()
    """
    print(json.dumps(res, indent=4))
    mutex.acquire()
    transmit_results_zmq(json.dumps(res))
    mutex.release()
    """
    if save_message(res, RESULT_SAVE_PATH):
        return make_response(jsonify(result="ok"), 200)
    else:
        return make_response(jsonify(result="error", description="Failed to store message."), 500)

app.run(host=SRV_HOST, port=SRV_PORT, debug=False)
