"""Manages the multiprocessing queue and its running state.

Also starts and manages the thread that processes the queue.

Key capabilities:
    - Create and manage a multiprocessing queue for inter-process communication
    - Maintain the running state of the queue processing
    - Start a dedicated thread to monitor and process queue messages
    - Handle database operations and logging from device processes
    - Ensure proper error handling and logging for queue operations
"""

import multiprocessing

from astra.database_manager import DatabaseManager
from astra.logger import ObservatoryLogger
from astra.thread_manager import ThreadManager


class QueueManager:
    """Manages the multiprocessing queue and its running state.

    Also starts and manages the thread that processes the queue.

    Attributes:
        manager (multiprocessing.Manager): Manager for creating shared objects.
        queue (multiprocessing.Queue): The multiprocessing queue for inter-process communication.
        queue_is_running (bool): Flag indicating if the queue processing is active.
        thread (threading.Thread | None): The thread that processes the queue.

    """

    def __init__(
        self,
        logger: ObservatoryLogger,
        database_manager: DatabaseManager,
        thread_manager: ThreadManager,
    ):
        self.manager = multiprocessing.Manager()
        self.queue = self.manager.Queue()
        self.queue_is_running = True
        self.database_manager = database_manager
        self.thread_manager = thread_manager
        self.logger = logger

    def start_queue_thread(
        self,
    ):
        self.thread_manager.start_thread(
            target=self.queue_get,
            thread_type="queue",
            device_name="queue",
            thread_id="queue",
        )

    def queue_get(self) -> None:
        """
        Process multiprocessing queue messages for database operations and logging.

        Continuously monitors the multiprocessing queue for database queries and
        log messages from device processes. Handles different message types and
        maintains system operation by processing database operations and managing
        thread cleanup.

        Message Types Handled:
            - 'query': Executes SQL database queries from device processes
            - 'log': Processes log messages with different severity levels
                - 'info', 'warning', 'error', 'debug' log levels supported
                - Error messages are added to error_source for monitoring

        Background Operations:
            - Cleans up completed threads from the threads list
            - Maintains database consistency across multiprocessing boundaries
            - Ensures proper error propagation from device processes

        Error Handling:
            - Catches and logs queue processing errors
            - Adds queue errors to error_source for monitoring
            - Stops queue processing on fatal errors

        Note:
            - Runs continuously until queue_running flag is False
            - Essential for multiprocessing communication with devices
            - Handles both synchronous database operations and asynchronous logging
            - Thread cleanup prevents memory leaks in long-running operations
        """

        while self.queue_is_running:
            try:
                metadata, r = self.queue.get()

                if r["type"] == "query":
                    self.database_manager.execute(r["data"])
                elif r["type"] == "log":
                    if r["data"][0] == "info":
                        self.logger.info(r["data"][1])
                    elif r["data"][0] == "warning":
                        self.logger.warning(r["data"][1])
                    elif r["data"][0] == "error":
                        self.logger.report_device_issue(
                            device_type=metadata["device_type"],
                            device_name=metadata["device_name"],
                            message="Error for {metadata['device_name']}",
                            exception=r["data"][1],
                        )
                    elif r["data"][0] == "debug":
                        self.logger.debug(r["data"][1])

                # pick up work of watchdog
                self.thread_manager.remove_dead_threads()

            except EOFError:
                # This exception is raised when the multiprocessing queue's
                # underlying connection is closed, which typically happens
                # when the main process or the multiprocessing.Manager process
                # is shutting down (such as at the end of a test run or program).
                #
                # If we did not catch EOFError here, the thread would log
                # unnecessary errors during normal shutdown, cluttering logs
                # and potentially causing confusion.
                break
            except Exception as e:
                self.logger.report_device_issue(
                    device_type="Queue",
                    device_name="queue_get",
                    message=f"Error running queue_get for Queue, {type(e).__name__}",
                    exception=e,
                )
                self.queue_is_running = False
