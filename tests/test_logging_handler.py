import logging
from astra.logging_handler import LoggingHandler


class FakeCursor:
    def __init__(self):
        self.executed = []

    def execute(self, sql, *args, **kwargs):
        # simply record the SQL that would be run
        self.executed.append(sql)


class FakeInstance:
    def __init__(self):
        self.cursor = FakeCursor()
        self.error_free = True


def teardown_logger(name: str) -> None:
    """Remove all handlers from the named logger to avoid test interference."""
    logger = logging.getLogger(name)
    for h in list(logger.handlers):
        logger.removeHandler(h)


def test_emit_inserts_info_and_prints(capsys):
    inst = FakeInstance()
    logger_name = "test_logging_handler_info"
    logger = logging.getLogger(logger_name)
    teardown_logger(logger_name)
    logger.setLevel(logging.DEBUG)

    handler = LoggingHandler(inst)
    logger.addHandler(handler)

    logger.info("Test message")

    # printed output should include the level and message
    captured = capsys.readouterr()
    assert "[INFO]" in captured.out
    assert "Test message" in captured.out

    # the fake cursor should have recorded an INSERT
    assert len(inst.cursor.executed) == 1
    sql = inst.cursor.executed[0]
    assert "INSERT INTO log" in sql
    assert "info" in sql.lower()
    assert "Test message" in sql

    # info level should not flip error_free
    assert inst.error_free is True

    teardown_logger(logger_name)


def test_emit_error_sets_error_free_and_stores_exception(capsys):
    inst = FakeInstance()
    logger_name = "test_logging_handler_error"
    logger = logging.getLogger(logger_name)
    teardown_logger(logger_name)
    logger.setLevel(logging.DEBUG)

    handler = LoggingHandler(inst)
    logger.addHandler(handler)

    try:
        raise ValueError("boom")
    except ValueError:
        # include exception info in the log record
        logger.error("Something went wrong", exc_info=True)

    captured = capsys.readouterr()
    # printed output should include level and some exc_info representation
    assert "[ERROR]" in captured.out
    assert "Something went wrong" in captured.out
    assert "ValueError" in captured.out

    # the fake cursor should have recorded an INSERT that contains the traceback
    assert len(inst.cursor.executed) == 1
    sql = inst.cursor.executed[0]
    assert "INSERT INTO log" in sql
    assert "error" in sql.lower()
    # exception name should be present in stored message (traceback appended)
    assert "ValueError" in sql

    # error should flip the error_free flag
    assert inst.error_free is False

    teardown_logger(logger_name)
