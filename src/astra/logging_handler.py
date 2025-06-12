import logging
import traceback
from datetime import UTC, datetime


class LoggingHandler(logging.Handler):
    def __init__(self, instance):
        logging.Handler.__init__(self)
        self.instance = instance

    def emit(self, record):
        if record.levelno == logging.ERROR:
            self.instance.error_free = False

        # print(f"[{record.levelname}] {record.msg} {str(record.exc_info)}")

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
