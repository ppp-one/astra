"""Database management for observatory data within the Astra framework.

Key capabilities:
    - Create and manage SQLite databases for observatories
    - Execute SQL queries and return results as lists or pandas DataFrames
    - Perform periodic backups of the database
"""

import os
import sqlite3
from datetime import UTC, datetime

import pandas as pd
import psutil
from sqlite3worker.sqlite3worker import Sqlite3Worker

from astra.config import Config
from astra.logger import ObservatoryLogger


class DatabaseManager:
    """Manages the SQLite database for an observatory, including creation, querying,
    and periodic backups.

    Attributes:
        observatory_name (str): Name of the observatory.
        run_backup (bool): Flag to indicate if a backup should be run.
        backup_time (datetime): Scheduled time for daily backups.
        db_path (str): File path to the SQLite database.
        logger (ObservatoryLogger): Logger instance for logging messages.

    Examples:
        >>> from astra.config import ObservatoryConfig
        >>> from astra.database_manager import DatabaseManager
        >>> observatory_config = ObservatoryConfig.from_config()
        >>> db_manager = DatabaseManager(observatory_config.observatory_name)
        >>> db_manager.execute_select_to_df("SELECT * FROM polling", table="polling")

    """

    def __init__(
        self,
        observatory_name: str,
        run_backup: bool = True,
        backup_time: datetime = datetime.strptime("12:00", "%H:%M"),
        logger=None,
    ):
        self.observatory_name = observatory_name
        self.run_backup = run_backup
        self.backup_time = backup_time
        self.db_path = Config().paths.logs / f"{self.observatory_name}.db"
        self.logger = (
            logger
            if isinstance(logger, ObservatoryLogger)
            else ObservatoryLogger(observatory_name)
        )

        self._cursor = None

    @property
    def cursor(self) -> Sqlite3Worker:
        if self._cursor is None:
            self._cursor = self.create_database()

        return self._cursor

    def execute(self, query: str):
        return self.cursor.execute(query)

    def execute_select(self, query: str) -> list[tuple]:
        """
        Execute a SELECT query and return the result as a list of tuples.
        Only SELECT queries are allowed.

        For static type checking to ensure the return type is always a list of tuples.
        """
        assert query.strip().lower().startswith("select"), "Only SELECT queries allowed"
        result = self.cursor.execute(query)  # type: ignore
        if not isinstance(result, list):
            raise TypeError("Expected a list of tuples as the result")

        return result

    def execute_select_to_df(
        self, query: str, table: str | None = None
    ) -> pd.DataFrame:
        """
        Execute a SELECT query and return the result as a pandas DataFrame.
        Only SELECT queries are allowed.
        """
        if table is None:
            query_lower = query.strip().lower()
            if " from polling " in query_lower:
                table = "polling"
            elif " from images " in query_lower:
                table = "images"
            elif " from log " in query_lower:
                table = "log"
            else:
                raise ValueError(
                    "Table name could not be inferred from query. "
                    "Please provide the table name."
                )

        if table == "polling":
            columns = [
                "device_type",
                "device_name",
                "device_command",
                "device_value",
                "datetime",
            ]
        elif table == "images":
            columns = ["filepath", "camera_name", "complete_hdr", "date_obs"]
        elif table == "log":
            columns = ["datetime", "level", "message"]
        else:
            raise ValueError(f"Unknown table: {table}")

        poll_records = self.execute_select(query)
        return pd.DataFrame(
            poll_records,
            columns=columns,
        )

    @classmethod
    def from_observatory_config(cls, observatory_config):
        return cls(
            observatory_name=observatory_config.observatory_name,
            run_backup=True,
            backup_time=datetime.strptime(
                observatory_config["Misc"]["backup_time"], "%H:%M"
            ),
        )

    def create_database(self, max_queue_size: int = 2000) -> Sqlite3Worker:
        """
        Create and initialize the observatory database.

        Creates a SQLite database for storing observatory data including device polling
        information, image metadata, and log entries. The database includes three main
        tables: polling (device status data), images (image file information), and
        log (system log messages).

        Returns:
            Sqlite3Worker: The database cursor object for executing queries and managing
            the database connection with a maximum queue size of 200.

        Note:
            The database file is created in the logs directory using the observatory
            name as the filename with a .db extension.
        """
        cursor = Sqlite3Worker(self.db_path, max_queue_size=max_queue_size)

        # Enable WAL mode - critical for concurrent access
        cursor.execute("PRAGMA journal_mode=WAL")

        # Optimize for high write throughput
        cursor.execute("PRAGMA synchronous=NORMAL")  # Faster than FULL, safer than OFF
        cursor.execute("PRAGMA cache_size=10000")  # Increase cache (10MB for large DB)
        cursor.execute("PRAGMA temp_store=memory")  # Use memory for temp operations

        db_command_0 = """CREATE TABLE IF NOT EXISTS polling (
                device_type   TEXT,
                device_name TEXT,
                device_command TEXT,
                device_value TEXT,
                datetime TEXT)"""

        cursor.execute(db_command_0)

        db_command_1 = """CREATE TABLE IF NOT EXISTS images (
                filename   TEXT,
                camera_name TEXT,
                complete_hdr INTEGER,
                date_obs TEXT)"""

        cursor.execute(db_command_1)

        db_command_2 = """CREATE TABLE IF NOT EXISTS log (
                datetime TEXT,
                level TEXT,
                message TEXT)"""

        cursor.execute(db_command_2)

        return cursor

    def backup(self) -> None:
        """
        Back up database tables from the previous 24 hours to CSV files.

        Creates timestamped CSV backups of the main database tables (polling, log,
        autoguider_log, autoguider_info_log) and stores them in an archive directory.
        Also monitors disk usage and logs a warning if disk usage exceeds 90%.

        The backup process:
            1. Checks available disk space and warns if usage > 90%
            2. Creates an archive directory if it doesn't exist
            3. Exports specified database tables to timestamped CSV files
            4. Logs the backup completion or any errors encountered

        Raises:
            Exception: Any errors during the backup process are logged and added
                to the error_source list for monitoring.
        """

        try:
            self.run_backup = False
            self.logger.info("Backing up database")

            # check disk space
            disk_usage = psutil.disk_usage("/")
            if disk_usage.percent > 90:
                self.logger.warning(f"Disk usage {disk_usage.percent}% is high")

            # create backup directory if not exists
            archive_path = Config().paths.logs / "archive"
            archive_path.mkdir(exist_ok=True)

            tables = ["polling", "log", "autoguider_log", "autoguider_info_log"]
            # 'images', 'autoguider_ref'

            db = sqlite3.connect(self.db_path)
            for table in tables:
                # backup table
                df = pd.read_sql_query(
                    f"SELECT * FROM {table} WHERE datetime > datetime('now', '-1 days')",
                    db,
                )

                if df.empty:
                    continue

                dt_str = (
                    df["datetime"]
                    .iloc[0]
                    .replace(":", "")
                    .replace("-", "")
                    .replace(" ", "_")
                    .split(".")[0]
                )
                df.to_csv(
                    os.path.join(
                        Config().paths.logs,
                        "archive",
                        f"{self.observatory_name}_{table}_{dt_str}.csv",
                    ),
                    index=False,
                )

            for table in tables:
                # once back up complete, delete rows older than 3 days ago from database
                # to minimize database size for speed
                self.cursor.execute(
                    f"DELETE FROM {table} WHERE datetime < datetime('now', '-3 days')"
                )

            db.close()

            self.logger.info("Database backed up")

        except Exception as e:
            # If logger has method report_device_issue, use it, else log normally
            if isinstance(self.logger, ObservatoryLogger):
                self.logger.report_device_issue(
                    device_type="Backup",
                    device_name="backup",
                    message="Error backing up database",
                    exception=e,
                )
            else:
                self.logger.error(f"Error backing up database: {str(e)}")

    def is_now_backup_time(self) -> bool:
        """Check if the current time matches the scheduled backup time."""
        return (
            datetime.now(UTC).hour == self.backup_time.hour
            and datetime.now(UTC).minute == self.backup_time.minute
        )

    def maybe_run_backup(self, thread_manager) -> None:
        """
        Check if it's time to run a backup and, if so, start it in a separate thread.
        Appends the backup thread to the thread_manager.
        """
        if self.is_now_backup_time():
            if self.run_backup:
                thread_manager.start_thread(
                    target=self.backup,
                    thread_type="Backup",
                    device_name="backup",
                    thread_id="backup",
                    daemon=True,
                )
                self.run_backup = False
        else:
            self.run_backup = True
