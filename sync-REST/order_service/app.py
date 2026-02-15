import structlog
from utils import c_logging as logger

logger.configure_logging()
log = structlog.get_logger()

