"""Thread management for device operations within the Astra framework.

Key capabilities:
    - Start and manage threads for device operations
    - Track thread status and provide summaries
    - Safely stop and clean up threads
"""

from threading import Thread
from typing import Any, Callable, Dict, List


class ThreadManager:
    """Manages threads for device operations within the Astra framework.

    Attributes:
        threads (List[Dict[str, Any]]): List of dictionaries containing thread information.
    """

    def __init__(self):
        self.threads: List[Dict[str, Any]] = []

    def start_thread(
        self,
        target: Callable,
        args: tuple = (),
        thread_type: str = "",
        device_name: str = "",
        thread_id: Any = None,
        daemon: bool = True,
    ) -> Thread:
        """Start a new thread for the specified target function."""
        th = Thread(target=target, args=args, daemon=daemon)
        th.start()
        thread_info = {
            "type": thread_type,
            "device_name": device_name,
            "thread": th,
            "id": thread_id if thread_id is not None else id(th),
        }
        self.threads.append(thread_info)
        return th

    def join_thread(self, thread_id: Any) -> None:
        """Wait for the specified thread to complete."""
        for th_info in self.threads:
            if th_info["id"] == thread_id:
                th_info["thread"].join()
                break

    def remove_dead_threads(self) -> None:
        """Remove threads that have completed from the threads list."""
        self.threads = [th for th in self.threads if th["thread"].is_alive()]

    def get_thread_ids(self) -> List[Any]:
        """Return a list of all thread IDs."""
        return [th_info["id"] for th_info in self.threads]

    def get_thread(self, thread_id: Any) -> Thread | None:
        """Return the thread object for the specified thread ID, or None if not found."""
        for th_info in self.threads:
            if th_info["id"] == thread_id:
                return th_info["thread"]
        return None

    def stop_thread(self, thread_id: Any) -> None:
        """Stop and remove the specified thread from the threads list."""
        for th_info in self.threads:
            if th_info["id"] == thread_id and th_info["thread"].is_alive():
                th_info["thread"].join()
                self.threads.remove(th_info)
                break

    def stop_all(self) -> None:
        """Stop and remove all threads from the threads list."""
        for th_info in self.threads:
            if th_info["thread"].is_alive():
                th_info["thread"].join()
        self.threads.clear()

    def is_thread_running(self, schedule: str) -> bool:
        """Return True if any thread of the given type is currently alive."""
        for th in self.threads:
            if th["id"] == schedule and th["thread"].is_alive():
                return True
        return False

    def get_thread_summary(self) -> list[dict]:
        """
        Return a summary list of all threads with type, device_name, and id.
        """
        return [self._thread_summary(th_info) for th_info in self.threads]

    def _thread_summary(self, thread_info: dict) -> dict:
        return {
            "type": thread_info.get("type"),
            "device_name": thread_info.get("device_name"),
            "id": thread_info.get("id"),
        }
