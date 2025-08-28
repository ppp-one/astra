"""Custom logging handler for observatory operations with database storage.

This module provides a specialized logging handler that extends Python's standard
logging.Handler to provide dual-output logging: console display and database storage.
It's designed specifically for observatory automation systems where logging events
need to be both immediately visible and persistently stored for analysis.

Key features:
- Dual logging output (console and database)
- Error state tracking for the parent instance
- Automatic timestamp formatting with microsecond precision
- Exception and stack trace capture
- SQL injection protection through quote escaping
- UTC timezone standardization

The handler is particularly useful for long-running observatory operations where:
- Real-time monitoring of system status is required
- Historical log analysis is needed for debugging
- Error states need to be tracked at the instance level
- Database queries on log data are necessary

Typical usage:
    handler = LoggingHandler(observatory_instance)
    logger = logging.getLogger('observatory')
    logger.addHandler(handler)
    logger.info("Observatory operations started")

Note:
    The handler expects the instance to have 'error_free' attribute and 'cursor' attribute.
"""

import logging
import traceback
from datetime import UTC, datetime
from typing import Any


class LoggingHandler(logging.Handler):
    """Custom logging handler for dual-output to console and database.

    Extends Python's standard logging.Handler to provide specialized logging
    for observatory automation systems. Simultaneously outputs log messages
    to console for real-time monitoring and stores them in database for
    persistent storage and analysis.

    Args:
        instance (Any): Parent instance with 'error_free' (bool) and 'cursor' attributes.

    Attributes:
        instance (Any): Parent instance that owns this logging handler.
    """

    def __init__(self, instance: Any) -> None:
        """Initialize the logging handler with a parent instance.

        Args:
            instance (Any): Parent object with error_free (bool) and cursor attributes.
        """
        logging.Handler.__init__(self)
        self.instance = instance

    def emit(self, record: logging.LogRecord) -> None:
        """Process and emit a log record to console and database.

        This method is called automatically by the logging framework when a log
        message is generated. It formats the record for console output, tracks
        error states in the parent instance, and stores the record in the database.

        Args:
            record: The log record to be processed and emitted.

        Note:
            If the log level is ERROR or higher, sets instance.error_free to False.
            All log records are stored in the 'log' database table with timestamp,
            level, module, function, line number, and message.
        """
        if record.levelno == logging.ERROR:
            self.instance.error_free = False

        print(f"[{record.levelname}] {record.msg} {str(record.exc_info)}")

        dt_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname.lower()
        message = record.msg if isinstance(record.msg, str) else str(record.msg)
        message.replace("\n", " ")

        if record.exc_info:
            message += "\n" + "".join(traceback.format_exception(*record.exc_info))

        if record.stack_info:
            message += "\n" + record.stack_info

        # make message safe for sql
        message = message.replace("'", "''")

        self.instance.cursor.execute(
            f"INSERT INTO log VALUES ('{dt_str}', '{level}', '{message}')"
        )

        # self.instance.cursor.execute(
        #     "INSERT INTO log (datetime, level, message) VALUES (?, ?, ?)",
        #     (dt_str, level, message),
        # )
