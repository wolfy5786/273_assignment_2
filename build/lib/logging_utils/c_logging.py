import logging
import structlog


def configure_logging() -> None:
    #Standard logging setup
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(message)s",
    )

    renderer = structlog.processors.JSONRenderer()

    # Structlog configuration
    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        cache_logger_on_first_use=True,
    )