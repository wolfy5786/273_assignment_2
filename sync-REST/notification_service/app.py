import structlog
from logging_utils import c_logging as logger

from flask import Flask, jsonify


import time

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

@app.get("/health")
def health():
    log.info("Service  = notification_service, endpoint = health, status = running")
    return jsonify(status ="ok")


@app.post("/send")
def notify():
    """endpoint = /send
    return json{notified: }"""
    start = time.time()
    resp = {"notified":True}
    log.info(f'Service = notification_service, endpoint = /send, status = running, latency_ms ={int((time.time()-start)*1000)} ')
    return jsonify(resp), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8081)


