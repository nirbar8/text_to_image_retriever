"""CLI entry point for Victor scheduler."""

from __future__ import annotations

import logging
import signal
import sys
from typing import NoReturn

from apscheduler.schedulers.background import BackgroundScheduler

from retriever.components.victor.scheduler import VictorScheduler
from retriever.components.victor.settings import VictorSettings

# Logging configuration
_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
_LOG_LEVEL = logging.INFO

# Scheduler job configuration
_JOB_ID = "victor_publish_tiles"
_JOB_NAME = "Publish ready tiles to embedding queues"
_JOB_TRIGGER = "interval"
_MAX_JOB_INSTANCES = 1  # Prevent concurrent execution

# Exit codes
_EXIT_SUCCESS = 0

logging.basicConfig(level=_LOG_LEVEL, format=_LOG_FORMAT)
logger = logging.getLogger(__name__)


def main() -> None:
    """Start Victor scheduler with graceful shutdown handling.
    
    Runs the scheduler as a background job that periodically checks TilesDB
    for tiles ready for indexing and publishes them to RabbitMQ queues.
    
    The scheduler runs until interrupted by SIGINT (Ctrl+C) or SIGTERM.
    """
    logger.info("Starting Victor Scheduler")

    settings = VictorSettings()
    scheduler_instance = VictorScheduler(settings)

    scheduler = _create_scheduler(scheduler_instance, settings)
    _register_signal_handlers(scheduler)

    try:
        scheduler.start()
        logger.info(
            "Victor Scheduler running (interval=%ss, batch_size=%s)",
            settings.schedule_interval_seconds,
            settings.batch_size,
        )
        _keep_alive()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        _shutdown_scheduler(scheduler)


def _create_scheduler(
    scheduler_instance: VictorScheduler,
    settings: VictorSettings,
) -> BackgroundScheduler:
    """Create and configure APScheduler instance.
    
    Args:
        scheduler_instance: VictorScheduler instance to run
        settings: Victor configuration settings
        
    Returns:
        Configured BackgroundScheduler
    """
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        scheduler_instance.run_once,
        trigger=_JOB_TRIGGER,
        seconds=settings.schedule_interval_seconds,
        id=_JOB_ID,
        name=_JOB_NAME,
        max_instances=_MAX_JOB_INSTANCES,
    )
    return scheduler


def _register_signal_handlers(scheduler: BackgroundScheduler) -> None:
    """Register signal handlers for graceful shutdown.
    
    Args:
        scheduler: BackgroundScheduler to shutdown on signal
    """
    def shutdown_handler(signum: int, frame) -> NoReturn:
        """Handle shutdown signals."""
        signal_name = signal.Signals(signum).name
        logger.info("Received signal %s, shutting down", signal_name)
        scheduler.shutdown(wait=False)
        sys.exit(_EXIT_SUCCESS)

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)


def _keep_alive() -> None:
    """Keep the main thread alive while scheduler runs in background."""
    import time
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass


def _shutdown_scheduler(scheduler: BackgroundScheduler) -> None:
    """Gracefully shutdown the scheduler.
    
    Args:
        scheduler: BackgroundScheduler to shutdown
    """
    logger.info("Shutting down scheduler")
    scheduler.shutdown(wait=True)


if __name__ == "__main__":
    main()

