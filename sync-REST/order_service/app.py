import structlog
from utils import c_logging as logger

from flask import Flask, jsonify, request
import requests
from requests.exceptions import Timeout, RequestException

import os

import time

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

NOTIFICATION_SERVICE = os.getenv('NOTIFICATION_SERVICE_URL','http://localhost:8081')
INVENTORY_SERVICE = os.getenv('INVENTORY_SERVICE_URL','http://localhost:8082')

@app.get("/health")
def health():
    log.info("Service  = order_service, endpoint = health, status = running")
    return jsonify(status ="ok")


@app.post("/order")
def place_order():
    """endpoint = /order, params : name?
    return json{order_status:}"""
    #==================================Start the clock=============================
    start = time.time()
    #====================================fetch the params===============================
    delay = request.args.get(key="delay",default="0")
    fault = request.args.get(key="fault",default="false")
    try:
        delay = int(delay)
    except ValueError:
        delay = 0 
    params = {
        "delay" :delay,
        "fault" : fault 
    }
    #=========================================send request to inventory=========================
    try : 
        inventory_responce = requests.post(f'{INVENTORY_SERVICE}/reserve', params= params, timeout=(0.5,4))
    except Timeout:
        log.exception(f'service = ORDER_SERVICE, endpoint  = /reserve,  inventory request Timedout, Order service =runnig, status = 503, latency = {int((time.time()-start)*1000)}')
        return jsonify(
            inventory_service = "unavailable",
            notification_service = "okay",
            order_service = "okay",
            error = "inventory service Timedout"
        ), 503  
    except RequestException:
        log.exception(f'service = ORDER_SERVICE, endpoint  = /reserve,  inventory service failure 400 bad request, Order service =runnig, status = 503, latency = {int((time.time()-start)*1000)}',)
        return jsonify(
            inventory_service = "unavailable",
            notification_service = "okay",
            order_service = "okay",
            error = "400 Bad request (induced failure)"
        ), 503
    #=====================================Send request to notification=======================================================
    try :
        notification_responce = requests.post(f'{INVENTORY_SERVICE}/reserve', params= params, timeout=(0.5,4))
    except Exception:
        log.exception(f'service = ORDER_SERVICE, endpoint  = /reserve,  notification service failure 400, Order service =runnig,  latency = {int((time.time()-start)*1000)}')
        return jsonify(
            inventory_service = "okay",
            notification_service = "unavailable",
            order_service = "okay"
        )
    
    #=========================================End===============================================================================
    log.info(f'service = ORDER_SERVICE, endpoint  = /reserve,  all services running, Order service =runnig,  latency = {int((time.time()-start)*1000)}')
    return jsonify(
            i_message = inventory_responce
            n_message = notification_responce
            inventory_service = "okay",
            notification_service = "okay",
            order_service = "okay"
        )



if __name__ == "main":
    app.run(host="0.0.0.0", port=8080)


