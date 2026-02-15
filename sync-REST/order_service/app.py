import structlog
from utils import c_logging as logger

from flask import Flask, jsonify


import time

logger.configure_logging()
log = structlog.get_logger()

app = Flask(__name__)

@app.get("/health")
def health():
    log.info("Service  = order_service, endpoint = health, status = running")
    return jsonify(status ="ok")


@app.post("/order")
def place_order():
    """endpoint = /order, params : name?
    return json{order_status:}"""


    
    pass



if __name__ == "main":
    app.run(host="0.0.0.0", port=8080)


