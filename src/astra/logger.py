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
    >>> from astra.logger import ObservatoryLogger, DatabaseLoggingHandler
    >>> from astra.database_manager import DatabaseManager
    >>> observatory_name = 'MyObservatory'
    >>> db_manager = DatabaseManager(observatory_name)
    >>> logger = ObservatoryLogger(observatory_name)
    >>> logger.addHandler(DatabaseLoggingHandler(db_manager))


Note:
    The handler expects the instance to have 'error_free' attribute and 'cursor' attribute.
"""

import logging
import sys
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Optional, Protocol


class DatabaseManagerProtocol(Protocol):
    def execute(self, query: str) -> Any: ...


class ObservatoryLogger(logging.Logger):
    """Custom logger for observatory operations with error tracking.

    Attributes:
        error_source (list): List to track sources of errors.
        error_free (bool): Flag indicating if the logger has encountered errors.

    Methods:
        error: Overrides logging.Logger.error to set error_free to False.
        critical: Overrides logging.Logger.critical to set error_free to False.
        report_device_issue: Logs device-specific issues and tracks error sources.
    """

    def __init__(
        self,
        name: str,
        error_source: list | None = None,
        error_free: bool = True,
        level=logging.INFO,
    ) -> None:
        super().__init__(name, level=level)
        self.error_source = [] if error_source is None else error_source
        self.error_free = error_free

    def error(self, msg, *args, **kwargs):
        self.error_free = False
        super().error(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.error_free = False
        super().critical(msg, *args, **kwargs)

    def report_device_issue(
        self,
        device_type: str,
        device_name: str,
        message: str,
        exception: Optional[Exception] = None,
        exc_info: bool = True,
        level: Literal["error", "warning"] = "error",
    ) -> None:
        error = f"{device_type} {device_name}: {message}" + (
            f". Exception: {str(exception)}" if exception is not None else ""
        )
        self.error_source.append(
            {"device_type": device_type, "device_name": device_name, "error": error}
        )
        if level == "warning":
            self.warning(error, exc_info=exc_info)
        else:
            self.error(error, exc_info=exc_info)


class DatabaseLoggingHandler(logging.Handler):
    """Custom logging handler for dual-output to console and database.

    Extends Python's standard logging.Handler to provide specialized logging
    for observatory automation systems. Simultaneously outputs log messages
    to console for real-time monitoring and stores them in database for
    persistent storage and analysis.

    Attributes:
        database_manager (DatabaseManager): Instance managing database operations,
            specifically database_manager.execute.
    """

    def __init__(self, database_manager: DatabaseManagerProtocol) -> None:
        logging.Handler.__init__(self)
        self.database_manager = database_manager

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
        dt_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        level = record.levelname.lower()
        message = record.msg if isinstance(record.msg, str) else str(record.msg)

        if record.exc_info:
            message += "\n" + "".join(traceback.format_exception(*record.exc_info))

        if record.stack_info:
            message += "\n" + record.stack_info

        # make message safe for sql
        message = message.replace("'", "''")
        try:
            self.database_manager.execute(
                f"INSERT INTO log VALUES ('{dt_str}', '{level}', '{message}')"
            )
        except Exception as e:
            print(f"Failed to log to database: {e}")


class ConsoleStreamHandler(logging.StreamHandler):
    def __init__(self, log_traceback: bool = True, **kwargs) -> None:
        super().__init__(**kwargs)
        self.setFormatter(CustomFormatter())
        self.log_traceback = log_traceback

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if self.stream is not None and not self.stream.closed:
                self.stream.write(msg + self.terminator)
                self.flush()
            else:
                print(f"Stream closed. Log: {msg}")

        except Exception:
            self.handleError(record)


class FileHandler(logging.FileHandler):
    FORMAT = "%(levelname)s,%(asctime)s.%(msecs)03d,%(process)d,%(name)s,(%(filename)s:%(lineno)d),%(message)s"
    DATEFMT = "%Y-%m-%d %H:%M:%S"

    def __init__(
        self, filename: str | Path, log_traceback: bool = True, **kwargs
    ) -> None:
        super().__init__(filename, **kwargs)
        self.log_traceback = log_traceback
        self.setFormatter(logging.Formatter(self.FORMAT, self.DATEFMT))
        self.setLevel(logging.ERROR)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            if self.stream is not None and not self.stream.closed:
                self.stream.write(msg + self.terminator)
                self.flush()
            else:
                print(f"Stream closed. Log: {msg}")

        except Exception:
            self.handleError(record)


class CustomFormatter(logging.Formatter):
    """
    A custom logging formatter that allows customizable formatting and color-coded output.

    Uses ANSI escape codes to colorize log messages based on their severity level,
    if the console supports it.

    Parameters
    ----------
    fmt : str, optional
        The log message format. Defaults to '%(asctime)s :: %(levelname)-8s :: %(message)s'.
    datefmt : str, optional
        The date format for log timestamps. Defaults to '%H:%M:%S'.

    Attributes
    ----------
    grey : str
        ANSI escape code for grey text.
    green : str
        ANSI escape code for green text.
    yellow : str
        ANSI escape code for yellow text.
    red : str
        ANSI escape code for red text.
    bold_red : str
        ANSI escape code for bold red text.
    reset : str
        ANSI escape code to reset text formatting.

    Methods
    -------
    format(record)
        Format the log record according to the specified log level's formatting.

    Usage
    -----
    formatter = CustomFormatter(fmt='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    Note
    ----
    The ANSI escape codes are used to colorize the output text in supported terminals.
    """

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def _console_supports_colors(self):
        return sys.stdout.isatty()

    def _all_color(self, color):
        return color + self.custom_format + self.reset

    def _type_color(self, color):
        return color + "%(levelname)-8s " + self.reset + ":: %(asctime)s :: %(message)s"

    def _type_and_message_color(self, color):
        return (
            color
            + "%(levelname)-8s "
            + self.reset
            + ":: %(asctime)s :: "
            + color
            + "%(message)s"
            + self.reset
        )

    def __init__(
        self,
        fmt=None,
        datefmt=None,
    ) -> None:
        if fmt is None:
            fmt = "%(levelname)-8s :: %(asctime)s :: %(message)s"
        if datefmt is None:
            datefmt = "%Y-%m-%d %H:%M:%S"
        self.custom_format = fmt

        if self._console_supports_colors():
            self.FORMATS = {
                logging.DEBUG: self._type_color(self.grey),
                logging.INFO: self._type_color(self.green),
                logging.WARNING: self._type_and_message_color(self.yellow),
                logging.ERROR: self._type_and_message_color(self.red),
                logging.CRITICAL: self._all_color(self.bold_red),
            }
        else:
            self.FORMATS = {
                logging.DEBUG: self.custom_format,
                logging.INFO: self.custom_format,
                logging.WARNING: self.custom_format,
                logging.ERROR: self.custom_format,
                logging.CRITICAL: self.custom_format,
            }
        super().__init__(fmt, datefmt)

    def format(self, record):
        """Format the log record according to the specified log level's formatting."""
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt, datefmt=self.datefmt)

        # Temporarily hide exc_info so base Formatter does not append it automatically
        exc_info = getattr(record, "exc_info", None)
        exc_text = getattr(record, "exc_text", None)
        record.exc_info = None
        record.exc_text = None

        try:
            s = formatter.format(record)
        finally:
            record.exc_info = exc_info
            record.exc_text = exc_text

        # If error or critical, append traceback if present
        if (
            exc_info
            and record.levelno >= logging.ERROR
            and isinstance(exc_info, tuple)
            and any(exc_info)
        ):
            msg_contains_tb = False
            tb_marker = "Traceback (most recent call last):"
            # inspect both the already-formatted string and the original message
            if tb_marker in s or (
                isinstance(record.msg, str) and tb_marker in record.msg
            ):
                msg_contains_tb = True
            # also detect common library prefixes (requests/urllib3)
            if (
                not msg_contains_tb
                and isinstance(record.msg, str)
                and ("requests.exceptions" in record.msg or "urllib3" in record.msg)
            ):
                msg_contains_tb = True

            if not msg_contains_tb:
                traceback_msg = (
                    "\n"
                    + "-" * 35
                    + "TRACEBACK"
                    + "-" * 36
                    + "\n"
                    + "".join(traceback.format_exception(*exc_info)).rstrip("\n")
                    + "\n"
                    + "-" * 80
                )
                if self._console_supports_colors():
                    traceback_msg = self.red + traceback_msg + self.reset
            s += traceback_msg if exc_info and not msg_contains_tb else ""

        return s
