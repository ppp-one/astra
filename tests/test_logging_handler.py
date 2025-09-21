import io
import logging
from astra.logger import DatabaseLoggingHandler, ObservatoryLogger, ConsoleStreamHandler


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, *args, **kwargs):
        # simply record the SQL that would be run
        self.executed.append(sql)


class FakeInstance:
    def __init__(self):
        self.cursor = FakeCursor()

    def execute(self, query: str) -> None:
        # Delegate to cursor for compatibility with DatabaseLoggingHandler
        self.cursor.execute(query)


def teardown_logger(name: str) -> None:
    """Remove all handlers from the named logger to avoid test interference."""
    logger = ObservatoryLogger(name)
    for h in list(logger.handlers):
        logger.removeHandler(h)


def test_emit_inserts_info_and_prints():
    inst = FakeInstance()
    logger_name = "test_logging_handler_info"
    logger = ObservatoryLogger(logger_name)
    teardown_logger(logger_name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(DatabaseLoggingHandler(inst))
    stream = io.StringIO()
    logger.addHandler(ConsoleStreamHandler(stream=stream))

    logger.info("Test message")

    output = stream.getvalue()
    assert "INFO" in output, f"INFO not in {output}"
    assert "Test message" in output

    # the fake cursor should have recorded an INSERT
    assert len(inst.cursor.executed) == 1
    sql = inst.cursor.executed[0]
    assert "INSERT INTO log" in sql
    assert "info" in sql.lower()
    assert "Test message" in sql

    # info level should not flip error_free
    assert logger.error_free is True

    teardown_logger(logger_name)


def test_emit_error_sets_error_free_and_stores_exception():
    inst = FakeInstance()
    logger_name = "test_logging_handler_error"
    logger = ObservatoryLogger(logger_name)
    teardown_logger(logger_name)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(DatabaseLoggingHandler(inst))
    stream = io.StringIO()
    logger.addHandler(ConsoleStreamHandler(stream=stream))

    try:
        raise ValueError("boom")
    except ValueError:
        # include exception info in the log record
        logger.error("Something went wrong", exc_info=True)

    output = stream.getvalue()
    # printed output should include level and some exc_info representation
    assert "ERROR" in output, f"ERROR not in {output}"
    assert "Something went wrong" in output
    assert "ValueError" in output

    # the fake cursor should have recorded an INSERT that contains the traceback
    assert len(inst.cursor.executed) == 1
    sql = inst.cursor.executed[0]
    assert "INSERT INTO log" in sql
    assert "error" in sql.lower()
    # exception name should be present in stored message (traceback appended)
    assert "ValueError" in sql

    # error should flip the error_free flag
    assert logger.error_free is False

    teardown_logger(logger_name)
