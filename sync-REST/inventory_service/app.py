import structlog
from utils import c_logging as logger

from flask import Flask, jsonify, request, abort


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
    """endpoint = /reserve, params?delay"""
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
    #====================================Induce Failure==============================================
    if fault == "true":
        log.info("service = inventory, endpoint  = /reserve, inducing error")
        abort(400,"Bad request (intentional)")
    #=======================================Respond===========================================================
    resp = {"status":"ok"}
    log.info(f'Service = inventory_service, endpoint = /reserve, status = running, latency_ms ={int((time.time()-start)*1000)} ')
    return jsonify(resp)


if __name__ == "main":
    app.run(host="0.0.0.0", port=8082)


