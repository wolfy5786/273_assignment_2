import structlog
from logging_utils import c_logging as logger

from flask import Flask, jsonify, request, abort
import random

import time

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

@app.get("/health")
def health():
    log.info("Service  = inventory_service, endpoint = health, status = running")
    return jsonify(status ="ok")

@app.post("/reserve")
def reserve_inventory():
    """endpoint = /reserve, ?params = , delay="""
    #======================================Start the Clock===============================
    start = time.time()
    #====================================fetch the params===============================
    delay = request.args.get(key="delay",default="0")
    fault = request.args.get(key="fault",default="false")
    try:
        delay = int(delay)
    except ValueError:
        delay = 0 
    #====================================ADD Delay===================================================================
    time.sleep(delay)
    
    # #====================================ADD Random Delay (20% of the time)==========================================
    # if random.random() < 0.2:  # 20% probability
    #     random_delay = 2
    #     log.info(f"Service = inventory_service, endpoint = /reserve, adding random delay of {random_delay}s")
    #     time.sleep(random_delay)
    #====================================Induce Failure==============================================
    if fault == "true":
        log.info("service = inventory, endpoint  = /reserve, inducing error")
        abort(400,"Bad request (intentional)")
    #=======================================Respond===========================================================
    resp = {"status":"ok"}
    log.info(f'Service = inventory_service, endpoint = /reserve, status = running, latency_ms ={int((time.time()-start)*1000)} ')
    return jsonify(resp),   200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8082)


