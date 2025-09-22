import pytest
from unittest.mock import MagicMock
from astra.queue_manager import QueueManager


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.execute = MagicMock()
    return db


@pytest.fixture
def mock_thread_manager():
    tm = MagicMock()
    tm.start_thread = MagicMock()
    tm.remove_dead_threads = MagicMock()
    return tm


@pytest.fixture
def queue_manager(mock_observatory_logger, mock_db, mock_thread_manager):
    qm = QueueManager(mock_observatory_logger, mock_db, mock_thread_manager)
    # Replace the real queue with a simple list for testing
    qm.queue = MagicMock()
    return qm


def test_start_queue_thread(queue_manager, mock_thread_manager):
    queue_manager.start_queue_thread()
    assert mock_thread_manager.start_thread.called


def test_queue_get_query(queue_manager, mock_db):
    queue_manager.queue_is_running = True
    queue_manager.queue.get = MagicMock(
        side_effect=[(None, {"type": "query", "data": "SELECT 1"}), EOFError()]
    )
    queue_manager.queue_get()
    assert mock_db.execute.called
    assert mock_db.execute.call_args[0][0] == "SELECT 1"


def test_queue_get_log_info(queue_manager, mock_observatory_logger):
    queue_manager.queue_is_running = True
    queue_manager.queue.get = MagicMock(
        side_effect=[(None, {"type": "log", "data": ["info", "msg"]}), EOFError()]
    )
    queue_manager.queue_get()
    assert mock_observatory_logger.info.called
    assert mock_observatory_logger.info.call_args[0][0] == "msg"


def test_queue_get_log_warning(queue_manager, mock_observatory_logger):
    queue_manager.queue_is_running = True
    queue_manager.queue.get = MagicMock(
        side_effect=[
            (None, {"type": "log", "data": ["warning", "warnmsg"]}),
            EOFError(),
        ]
    )
    queue_manager.queue_get()
    assert mock_observatory_logger.warning.called
    assert mock_observatory_logger.warning.call_args[0][0] == "warnmsg"


def test_queue_get_log_error(queue_manager, mock_observatory_logger):
    queue_manager.queue_is_running = True
    meta = {"device_type": "Camera", "device_name": "cam1"}
    queue_manager.queue.get = MagicMock(
        side_effect=[
            (meta, {"type": "log", "data": ["error", Exception("fail")]}),
            EOFError(),
        ]
    )
    queue_manager.queue_get()
    assert mock_observatory_logger.report_device_issue.called
    assert (
        mock_observatory_logger.report_device_issue.call_args[1]["device_type"]
        == "Camera"
    )
    assert (
        mock_observatory_logger.report_device_issue.call_args[1]["device_name"]
        == "cam1"
    )


def test_queue_get_log_debug(queue_manager, mock_observatory_logger):
    queue_manager.queue_is_running = True
    queue_manager.queue.get = MagicMock(
        side_effect=[(None, {"type": "log", "data": ["debug", "debugmsg"]}), EOFError()]
    )
    queue_manager.queue_get()
    assert mock_observatory_logger.debug.called
    assert mock_observatory_logger.debug.call_args[0][0] == "debugmsg"


def test_queue_get_eoferror(queue_manager):
    queue_manager.queue_is_running = True

    def raise_eof():
        raise EOFError()

    queue_manager.queue.get = raise_eof
    queue_manager.queue_get()
    # Should exit cleanly without error


def test_queue_get_exception(queue_manager, mock_observatory_logger):
    queue_manager.queue_is_running = True

    def raise_exc():
        raise RuntimeError("fail")

    queue_manager.queue.get = raise_exc
    queue_manager.queue_get()
    assert mock_observatory_logger.report_device_issue.called
    assert not queue_manager.queue_is_running
