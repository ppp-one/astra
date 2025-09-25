import threading
from collections import deque


class CameraRegistry:
    """Registry to manage all camera instances

    Purpose: Central registry for all camera instances.
    - Tracks all cameras (cameras list).
    - Provides thread-safe methods to register cameras, get running cameras, and manage the "master" camera.
    - Ensures only one camera is marked as master at any time.

    The master camera is responsible for coordinating actions that require exclusive
    control, such as pointing the telescope.

    """

    cameras = []
    _lock = threading.Lock()

    @classmethod
    def register(cls, camera):
        with cls._lock:
            cls.cameras.append(camera)

    @classmethod
    def get_running_cameras(cls):
        with cls._lock:
            return [cam for cam in cls.cameras if cam.is_running()]

    @classmethod
    def get_master(cls):
        with cls._lock:
            for cam in cls.cameras:
                if cam.master:
                    return cam
            return None

    @classmethod
    def set_master(cls, camera):
        with cls._lock:
            for cam in cls.cameras:
                cam.master = False
            camera.master = True


class Camera:
    """Class representing a camera device

    This will be useful when making a multi-camera setup.
    """

    def __init__(
        self, name, image_handler, paired_devices, logger, database_manager
    ) -> None:
        name = name
        self.image_handler = image_handler
        self.paired_devices = paired_devices
        self.logger = logger
        self.database_manager = database_manager
        self._running = False
        self._master = False
        self._action_queue = deque()
        self._condition = threading.Condition()

        CameraRegistry.register(self)

    @property
    def master(self) -> bool:
        return self._master

    @master.setter
    def master(self, value: bool) -> None:
        self._master = value

    @property
    def action(self):
        # Get from front of queue
        if self._action_queue:
            return self._action_queue[0]
        return None

    def is_running(self) -> bool:
        return self._running

    def start_action(self, action) -> None:
        with self._condition:
            self._action_queue.append(action)
            while self._running or self._action_queue[0] != action:
                # Wait until no action is running and this action is at the front of
                # the queue.
                self._condition.wait()
            self.set_master_maybe()
            self._action = action
            self.logger.info(f"Camera action started: {action}")
            self._running = True

    def finish_action(self) -> None:
        with self._condition:
            self._running = False
            if self._action_queue:
                self._action_queue.popleft()
            self._condition.notify_all()
            if self.master:
                running_cameras = CameraRegistry.get_running_cameras()
                if running_cameras:
                    CameraRegistry.set_master(running_cameras[0])
                else:
                    self.master = False

    def set_master_maybe(self) -> None:
        master = CameraRegistry.get_master()
        if not master:
            CameraRegistry.set_master(self)
            self.logger.info(f"Camera {self} is now the master camera.")
        else:
            self.logger.info(
                f"Camera {self} is not the master camera. Current master is {master}."
            )
