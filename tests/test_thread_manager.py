import pytest
from threading import Event

from astra.thread_manager import ThreadManager


@pytest.fixture
def tm():
    return ThreadManager()


def dummy_target(event: Event):
    event.wait(0.1)


def test_start_and_get_thread(tm):
    event = Event()
    th = tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="test",
        device_name="dev1",
    )
    assert th in [t["thread"] for t in tm.threads]
    thread_id = tm.get_thread_ids()[0]
    assert tm.get_thread(thread_id) == th


def test_join_thread(tm):
    event = Event()
    th = tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="test",
        device_name="dev2",
    )
    thread_id = tm.get_thread_ids()[0]
    tm.join_thread(thread_id)
    assert not th.is_alive()


def test_remove_dead_threads(tm):
    event = Event()
    _ = tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="test",
        device_name="dev3",
    )
    thread_id = tm.get_thread_ids()[0]
    tm.join_thread(thread_id)
    tm.remove_dead_threads()
    assert len(tm.threads) == 0


def test_stop_thread(tm):
    event = Event()
    th = tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="test",
        device_name="dev4",
    )
    thread_id = tm.get_thread_ids()[0]
    tm.stop_thread(thread_id)
    assert not th.is_alive()
    assert len(tm.threads) == 0


def test_stop_all(tm):
    events = [Event() for _ in range(3)]
    for i, event in enumerate(events):
        tm.start_thread(
            target=dummy_target,
            args=(event,),
            thread_type="test",
            device_name=f"dev{i}",
        )
    tm.stop_all()
    assert len(tm.threads) == 0


def test_is_thread_running(tm):
    event = Event()
    _ = tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="test",
        device_name="dev5",
        thread_id="sched1",
    )
    assert tm.is_thread_running("sched1")
    tm.stop_thread("sched1")
    assert not tm.is_thread_running("sched1")


def test_get_thread_summary(tm):
    event = Event()
    tm.start_thread(
        target=dummy_target,
        args=(event,),
        thread_type="typeA",
        device_name="devA",
        thread_id="idA",
    )
    summary = tm.get_thread_summary()
    assert len(summary) == 1
    assert summary[0]["type"] == "typeA"
    assert summary[0]["device_name"] == "devA"
    assert summary[0]["id"] == "idA"
