"""Configuration management for the Astra observatory automation system.

This module provides configuration classes for managing Astra settings,
observatory configurations, and asset paths. It handles YAML configuration
files, directory initialization, and exposes a singleton `Config` for
global configuration access.
"""

import filecmp
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Union

import pandas as pd
import yaml
from platformdirs import user_config_dir
from ruamel.yaml import YAML


class _Colors:
    """ANSI color codes for CLI output."""

    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    RESET = "\033[0m"

    @staticmethod
    def colorize(text: str, color: str) -> str:
        """Apply color to text if output device is a terminal."""
        if sys.stdout.isatty():
            return f"{color}{text}{_Colors.RESET}"
        return text


class Config:
    """Singleton class for managing Astra's configuration settings.

    This class loads configuration settings from a YAML file and provides
    methods to access and modify these settings. It ensures that only one
    instance of the configuration is created throughout the application.

    Attributes:
        observatory_name (str): The name of the observatory.
        folder_assets (Path): The path to the folder containing assets.
        gaia_db (Path): The path to the Gaia database
        paths (AssetPaths): An instance of AssetPaths containing paths to asset
            folders and log file.

    Note:
        If no configuration file is found, the user is prompted to provide
        the necessary information during initialization of the Config object.
        The configuration file is saved and the necessary files and folders
        are created.
    """

    """The path to the configuration YAML file."""
    CONFIG_PATH = (
        Path(user_config_dir("astra", ensure_exists=True)) / "astra_config.yml"
    )

    """The path to the directory containing template files."""
    TEMPLATE_DIR = Path(__file__).parent / "config" / "templates"

    """The format used for datetime strings."""
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    _instance: Optional["Config"] = None
    _initialized: bool = False
    _lock = Lock()

    def __new__(cls, *args: Any, **kwargs: Any) -> "Config":
        """Ensure singleton pattern - only one Config instance exists."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        observatory_name: Optional[str] = None,
        folder_assets: Optional[Union[Path, str]] = None,
        gaia_db: Optional[Union[Path, str]] = None,
        allow_default: bool = False,
        propagate_observatory_name: bool = False,
        reset: bool = False,
    ) -> None:
        """Initialise the configuration settings.

        Args:
            observatory_name (str): The name of the observatory.
            folder_assets (Path | str): The path to the folder containing assets.
            gaia_db (Path | str): The path to the Gaia database.
            allow_default (bool): Whether to raise a SystemExit
                if observatory configuration files were left unchanged.
            propagate_observatory_name (bool): Whether to automatically modify
                the observatory config files by substituting the observatory name.
                Mainly useful for testing.
            reset (bool): If True, resets the configuration by deleting the config
                file.
        """
        if reset:
            self.reset()

        # Fast-path check without lock
        if self.__class__._initialized:
            return

        with self.__class__._lock:
            if self.__class__._initialized:
                return

            if not self.__class__.CONFIG_PATH.exists():
                _ConfigInitialiser.run(observatory_name, folder_assets, gaia_db)

            config = self._load_from_file()

            self.observatory_name = config["observatory_name"]
            self.folder_assets = Path(config["folder_assets"])
            self.gaia_db = Path(config["gaia_db"])

            self.paths = AssetPaths(self.folder_assets)
            if not isinstance(self.paths, AssetPaths):
                raise TypeError(f"Expected AssetPaths, got {type(self.paths)}")

            self._initialize_observatory_files(
                allow_default=allow_default,
                propagate_observatory_name=propagate_observatory_name,
            )

            self._initialized = True

    @property
    def observatory_config(self) -> "ObservatoryConfig":
        """Load the observatory configuration."""
        return ObservatoryConfig.from_config(self)

    def reset(self, remove_assets: bool = False) -> None:
        """Reset configuration by removing config file and optionally assets.

        Args:
            remove_assets: If True, also removes the assets folder after confirmation.
        """
        if remove_assets:
            prompt = (
                _ConfigInitialiser._cinput(
                    f"Are you sure you want to remove {self.folder_assets}? [y/n]: "
                )
                .strip()
                .lower()
            )
            if prompt == "y":
                if self.folder_assets.exists():
                    self.folder_assets.rmdir()
                _ConfigInitialiser._print_success("Removed assets folder.")

        if self.CONFIG_PATH.exists():
            self.CONFIG_PATH.unlink()
            _ConfigInitialiser._print_success("Removed config file.")

        raise SystemExit("Astra base config has been reset.")

    def save(self) -> None:
        """Save current configuration settings to YAML file."""

        _ConfigInitialiser._validate_paths(
            folder_assets=self.folder_assets, gaia_db=self.gaia_db
        )
        config = {
            "folder_assets": str(self.folder_assets),
            "gaia_db": str(self.gaia_db),
            "observatory_name": str(self.observatory_name),
        }

        with open(self.CONFIG_PATH, "w") as file:
            yaml.dump(config, file)

    def as_datetime(self, date_string: str) -> datetime:
        """Convert string to datetime using configured format.

        Args:
            date_string: Date string to convert.

        Returns:
            datetime: Parsed datetime object.
        """
        return datetime.strptime(date_string, self.TIME_FORMAT)

    def _load_from_file(self) -> Dict[str, str]:
        """Load configuration from YAML file.

        Returns:
            dict: Configuration data from file.
        """
        with open(self.CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)

        return config

    def _initialize_observatory_files(
        self, allow_default: bool, propagate_observatory_name: bool
    ) -> None:
        """Initialize observatory configuration files from templates."""
        if not self.TEMPLATE_DIR.exists():
            raise FileNotFoundError(
                f"Template directory {self.TEMPLATE_DIR} not found."
            )

        unchanged_files = []

        # only csv and yml
        for template_file in [f for f in self.TEMPLATE_DIR.iterdir() if f.is_file()]:
            if not template_file.is_file():
                continue
            target_file = (
                self.paths.custom_observatories / template_file.name
                if template_file.suffix in [".py"]
                else self.paths.observatory_config
                / template_file.name.replace("observatory", self.observatory_name)
            )
            if not target_file.exists():
                target_file.write_bytes(template_file.read_bytes())

            if (
                filecmp.cmp(template_file, target_file, shallow=False)
                and not propagate_observatory_name
                and target_file.suffix in [".yml", ".csv"]
            ):
                unchanged_files.append(target_file.name)

            if propagate_observatory_name:
                self._modify_observatory_config_files(
                    target_file,
                    ["observatoryname", "ORIGIN"],
                    [self.observatory_name, self.observatory_name],
                )

        if unchanged_files:
            message_1 = (
                "\nWarning: Observatory config files have not been modified "
                "from default templates.\n"
            )
            if allow_default:
                print(_Colors.colorize(message_1, _Colors.YELLOW))
            else:
                message_2 = (
                    "Please update your observatory configuration files located in:\n"
                    f"{self.paths.observatory_config}\n\n"
                    f"Unchanged files: {', '.join(unchanged_files)}\n"
                )
                exit_message = "Exiting until observatory configuration is updated."
                print(_Colors.colorize(message_1, _Colors.YELLOW))
                print(message_2)
                raise SystemExit(_Colors.colorize(exit_message, _Colors.RED))

    def _modify_observatory_config_files(
        self, file_path, old_strings=[], new_strings=[]
    ):
        """Modify default template files by substituting specified strings."""
        with open(file_path, "r") as f:
            content = f.read()
        for old_string, new_string in zip(old_strings, new_strings):
            content = re.sub(r"(?<!\n)" + re.escape(old_string), new_string, content)
        with open(file_path, "w") as f:
            f.write(content)

    def __repr__(self) -> str:
        return (
            f"Config(\n"
            f"  folder_assets={self.folder_assets},\n"
            f"  gaia_db={self.gaia_db},\n"
            f"  observatory_name={self.observatory_name},\n"
            f"  paths={self.paths}\n"
            f")"
        )


class AssetPaths:
    """Container for asset directory paths and log file used by Astra.

    Manages the creation and organization of Astra's asset directories
    including configuration, schedules, images, and logs.
    """

    def __init__(self, folder_assets: Union[Path, str]) -> None:
        """Create AssetPaths and ensure on-disk folders and log file exist.

        Args:
            folder_assets (Path | str): Base path for Astra assets.
        """
        if isinstance(folder_assets, str):
            folder_assets = Path(folder_assets)

        self.assets = folder_assets
        self.custom_observatories = folder_assets / "custom_observatories"
        self.observatory_config = folder_assets / "observatory_config"
        self.schedules = folder_assets / "schedules"
        self.images = folder_assets / "images"
        self.logs = folder_assets / "logs"
        self.log_file = self.logs / "astra.log"

        self._initialize_folders_and_log_file()

    def archive_log_file(self) -> None:
        """Archive the current log file with a timestamp."""
        with open(self.log_file, "r") as file:
            first_line = file.readline()
            match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", first_line)
            if match:
                timestamp = match.group(1)
            else:
                timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")

        archive_file_path = self.logs / "archive" / f"{timestamp}_astra.log"
        archive_file_path.parent.mkdir(exist_ok=True)
        self.log_file.rename(archive_file_path)
        self.log_file.touch()

    def _initialize_folders_and_log_file(self) -> None:
        """Create necessary folders and the log file if they do not exist."""
        for folder in (
            self.assets,
            self.custom_observatories,
            self.observatory_config,
            self.schedules,
            self.logs,
            self.images,
        ):
            if not folder.exists():
                folder.mkdir(parents=True)
                print(_Colors.colorize(f"Created folder {folder}", _Colors.GREEN))

        self.log_file.parent.mkdir(parents=True, exist_ok=True)
        self.log_file.touch(exist_ok=True)

    def __repr__(self) -> str:
        return f"AssetPaths(assets={self.assets})"

    def __str__(self) -> str:
        return (
            f"AssetPaths(\n"
            f"  assets={self.assets},\n"
            f"  custom_observatories={self.custom_observatories},\n"
            f"  observatory_config={self.observatory_config},\n"
            f"  schedules={self.schedules},\n"
            f"  logs={self.logs},\n"
            f"  images={self.images},\n"
            f"  log_file={self.log_file}\n"
            f")"
        )


class _ConfigInitialiser:
    """Helper class for initial configuration setup through user prompts.

    Handles the first-time setup process including directory creation,
    user input validation, and initial configuration file generation.
    """

    DEFAULT_ASSETS_PATH = Path.home() / "Documents" / "Astra"

    # Gaia database options with magnitude cuts
    GAIA_DB_OPTIONS = {
        "1": {"records": "144", "size": "766.0 kB"},
        "2": {"records": "650", "size": "766.0 kB"},
        "3": {"records": "2K", "size": "766.0 kB"},
        "4": {"records": "6K", "size": "766.0 kB"},
        "5": {"records": "18K", "size": "2.0 MB"},
        "6": {"records": "58K", "size": "4.5 MB"},
        "7": {"records": "160K", "size": "10.8 MB"},
        "8": {"records": "426K", "size": "26.8 MB"},
        "9": {"records": "1M", "size": "67.2 MB"},
        "10": {"records": "3M", "size": "172.3 MB"},
        "11": {"records": "7M", "size": "425.2 MB"},
        "12": {"records": "16M", "size": "987.6 MB"},
        "13": {"records": "36M", "size": "2.2 GB"},
        "14": {"records": "79M", "size": "4.8 GB"},
        "15": {"records": "161M", "size": "9.8 GB"},
        "16": {"records": "297M", "size": "18.1 GB"},
    }
    GAIA_ZENODO_RECORD = "18214672"

    @staticmethod
    def _print_header(title: str) -> None:
        print("\n" + _Colors.colorize("=" * 60, _Colors.BLUE))
        print(_Colors.colorize(title.center(60), _Colors.BOLD))
        print(_Colors.colorize("=" * 60, _Colors.BLUE) + "\n")

    @staticmethod
    def _print_success(message: str) -> None:
        print(_Colors.colorize(f"✓ {message}", _Colors.GREEN))

    @staticmethod
    def _print_error(message: str) -> None:
        print(_Colors.colorize(f"✗ {message}", _Colors.RED))

    @staticmethod
    def _cinput(prompt: str) -> str:
        return input(_Colors.colorize(prompt, _Colors.CYAN))

    @staticmethod
    def run(
        observatory_name: Optional[str],
        folder_assets: Optional[Union[str, Path]],
        gaia_db: Optional[Union[str, Path]],
    ) -> None:
        """Create initial configuration through user prompts.

        Args:
            observatory_name: Name of the observatory.
            folder_assets: Path to assets folder.
            gaia_db: Path to Gaia database file.
        """
        _ConfigInitialiser._print_header("Welcome to Astra Configuration")
        if any(item is None for item in (observatory_name, folder_assets, gaia_db)):
            print(
                "Please provide the following information to set up your observatory.\n"
            )

        _ConfigInitialiser._validate_paths(folder_assets, gaia_db)
        Config.CONFIG_PATH.parent.mkdir(exist_ok=True)

        if folder_assets is None:
            folder_assets = _ConfigInitialiser._prompt_assets_path()

        if gaia_db is None:
            gaia_db = _ConfigInitialiser._prompt_gaia_db_path()

        if observatory_name is None:
            observatory_name = _ConfigInitialiser._cinput(
                "Please enter the name of the observatory: "
            ).strip()

        config = {
            "folder_assets": str(folder_assets),
            "gaia_db": str(gaia_db),
            "observatory_name": str(observatory_name),
        }

        with open(Config.CONFIG_PATH, "w") as file:
            yaml.dump(config, file)

        _ConfigInitialiser._print_success("Configuration file created successfully.")

    @staticmethod
    def _prompt_assets_path() -> Path:
        """Prompt user for assets folder location.

        Returns:
            Path: Validated path to assets folder.
        """
        while True:
            use_default = (
                _ConfigInitialiser._cinput(
                    f"Use default assets path ({_ConfigInitialiser.DEFAULT_ASSETS_PATH})? [y/n]: "
                )
                .strip()
                .lower()
            )

            if use_default == "y":
                return Path(_ConfigInitialiser.DEFAULT_ASSETS_PATH)
            elif use_default == "n":
                custom_path = Path(
                    _ConfigInitialiser._cinput(
                        "Please enter the desired path: "
                    ).strip()
                )
                if custom_path.exists():
                    return custom_path
                create_path = (
                    _ConfigInitialiser._cinput(
                        "Path does not exist. Do you want to create it? [y/n]: "
                    )
                    .strip()
                    .lower()
                )
                if create_path == "y":
                    custom_path.mkdir(parents=True, exist_ok=True)
                    return custom_path
            else:
                _ConfigInitialiser._print_error("Please enter 'y' or 'n'.")

    @staticmethod
    def _prompt_gaia_db_path() -> Optional[str]:
        """Prompt user for Gaia database location with download option.

        Returns:
            str or None: Path to Gaia database file or None if not using local DB.
        """
        _ConfigInitialiser._print_header("Gaia Database Configuration")
        print("The Gaia-2MASS catalog enables offline plate solving and")
        print("autofocus field selection. Choose a magnitude cut based on")
        print("your needs (higher = more stars, larger file).\n")

        use_gaia = (
            _ConfigInitialiser._cinput("Use Gaia database? [y/n]: ").strip().lower()
        )

        if use_gaia != "y":
            return ""

        # Check common locations for existing database
        common_paths = [
            Path.home() / "gaia_tmass_16_jm_cut.db",
            Path.home() / "Downloads" / "gaia_tmass_16_jm_cut.db",
            Path.cwd() / "gaia_tmass_16_jm_cut.db",
        ]

        for path in common_paths:
            if path.exists():
                _ConfigInitialiser._print_success(
                    f"Found existing Gaia database at: {path}"
                )
                use_existing = (
                    _ConfigInitialiser._cinput("Use this database? [Y/n]: ")
                    .strip()
                    .lower()
                )
                if use_existing != "n":
                    return str(path)

        # Offer download or manual path
        print("\nOptions:")
        print("  1. Download Gaia database now (choose magnitude cut)")
        print("  2. I already have it (enter path)")
        print("  3. Skip for now (can add later in config)")

        choice = _ConfigInitialiser._cinput("\nSelect option [1/2/3]: ").strip()

        if choice == "1":
            return _ConfigInitialiser._download_gaia_db()
        elif choice == "2":
            while True:
                path = _ConfigInitialiser._cinput(
                    "Enter path to Gaia database: "
                ).strip()
                if path and Path(path).exists():
                    return path
                elif not path:
                    return ""
                else:
                    _ConfigInitialiser._print_error(f"File not found: {path}")
        else:
            print("\nSkipping Gaia database setup.")
            print("You can download it later from:")
            print(f"https://zenodo.org/records/{_ConfigInitialiser.GAIA_ZENODO_RECORD}")
            return ""

    @staticmethod
    def _download_gaia_db() -> str:
        """Download Gaia database from Zenodo with user-selected magnitude cut.

        Returns:
            str: Path to downloaded database file, or empty string if failed.
        """
        _ConfigInitialiser._print_header("Select Gaia Database Magnitude Cut")
        print("\nMagnitude Cut | Stars       | File Size")
        print("-" * 60)

        for mag_cut, info in _ConfigInitialiser.GAIA_DB_OPTIONS.items():
            print(
                f"      {mag_cut:>2}      | {info['records']:>13} | {info['size']:>10}"
            )

        print("\nRecommendation: Magnitude 16 for most coverage")
        print("                Magnitude 10-15 for most small-medium setups")
        print("                Magnitude 1-9 for testing\n")

        while True:
            choice = _ConfigInitialiser._cinput(
                "Select magnitude cut [1-16] or 'c' to cancel: "
            ).strip()

            if choice.lower() == "c":
                return ""

            if choice in _ConfigInitialiser.GAIA_DB_OPTIONS:
                mag_cut = choice
                break
            else:
                _ConfigInitialiser._print_error(
                    "Invalid choice. Please enter a number between 1-16."
                )

        # Construct download URL and filename
        filename = f"gaia_tmass_{mag_cut}_jm_cut.db"
        url = f"https://zenodo.org/records/{_ConfigInitialiser.GAIA_ZENODO_RECORD}/files/{filename}?download=1"

        # Determine download path
        default_path = Path.home() / filename
        path_input = _ConfigInitialiser._cinput(
            f"\nDownload filepath [{default_path}]: "
        ).strip()
        download_path = Path(path_input) if path_input else default_path

        # Check if file already exists
        if download_path.exists():
            overwrite = (
                _ConfigInitialiser._cinput(
                    f"\nFile exists at {download_path}. Overwrite? [y/n]: "
                )
                .strip()
                .lower()
            )
            if overwrite != "y":
                return str(download_path)

        # Perform download
        print(
            f"\nDownloading magnitude {mag_cut} database ({_ConfigInitialiser.GAIA_DB_OPTIONS[mag_cut]['size']})..."
        )
        print(f"URL: {url}")
        print(f"Destination: {download_path}\n")

        try:
            import urllib.request

            def _progress_hook(block_num, block_size, total_size):
                """Display download progress."""
                downloaded = block_num * block_size
                if total_size > 0:
                    percent = min(100, (downloaded / total_size) * 100)
                    bar_length = 40
                    filled = int(bar_length * downloaded / total_size)
                    bar = "█" * filled + "░" * (bar_length - filled)

                    # Convert bytes to human readable
                    def human_size(bytes):
                        for unit in ["B", "KB", "MB", "GB"]:
                            if bytes < 1024:
                                return f"{bytes:.1f} {unit}"
                            bytes /= 1024
                        return f"{bytes:.1f} TB"

                    # Colorize progress bar
                    colored_bar = _Colors.colorize(bar, _Colors.GREEN)
                    print(
                        f"\r[{colored_bar}] {percent:.1f}% ({human_size(downloaded)} / {human_size(total_size)})   ",
                        end="",
                        flush=True,
                    )

            # Create parent directory if needed
            download_path.parent.mkdir(parents=True, exist_ok=True)

            # Download with progress
            urllib.request.urlretrieve(url, download_path, reporthook=_progress_hook)
            print()  # New line after progress bar

            _ConfigInitialiser._print_success(f"Download complete: {download_path}\n")
            return str(download_path)

        except Exception as e:
            _ConfigInitialiser._print_error(f"Download failed: {e}")
            print("\nYou can download manually from:")
            print(f"https://zenodo.org/records/{_ConfigInitialiser.GAIA_ZENODO_RECORD}")
            return ""

    @staticmethod
    def _validate_paths(
        folder_assets: Optional[Union[str, Path]], gaia_db: Optional[Union[str, Path]]
    ) -> None:
        """Validate user-provided path arguments.

        Args:
            folder_assets: Path to assets folder.
            gaia_db: Path to Gaia database file.

        Raises:
            TypeError: If paths are not str or Path types.
            FileNotFoundError: If gaia_db path doesn't exist.
        """
        if folder_assets is not None and not isinstance(folder_assets, (str, Path)):
            raise TypeError(f"Expected str or Path, got {type(folder_assets)}")

        if gaia_db is not None and not isinstance(gaia_db, (str, Path)):
            raise TypeError(f"Expected str or Path, got {type(gaia_db)}")

        if gaia_db is not None and not Path(gaia_db).exists():
            raise FileNotFoundError(f"File {gaia_db} does not exist.")


class ObservatoryConfig(dict):
    """Observatory-specific configuration management with YAML persistence.

    Extends dict to provide configuration loading, saving, backup creation,
    and automatic reload detection for observatory configuration files.

    Examples:
        >>> from astra.config import ObservatoryConfig
        >>> observatory_config = ObservatoryConfig.from_config()
    """

    def __init__(self, config_path: Union[str, Path], observatory_name: str) -> None:
        self.config_path: Path = (
            config_path if isinstance(config_path, Path) else Path(config_path)
        )
        self.observatory_name: str = observatory_name
        self._config_last_modified: Optional[float] = None
        self._yaml_data = None  # Store CommentedMap to preserve comments
        self.load()

    @property
    def file_path(self) -> Path:
        """Get path to the observatory configuration YAML file."""
        return self.config_path / f"{self.observatory_name}_config.yml"

    def load(self) -> None:
        """Load observatory configuration from YAML file.

        Uses ruamel.yaml to preserve comments and structure for later saving.
        """
        yaml_reader = YAML()
        yaml_reader.preserve_quotes = True
        yaml_reader.map_indent = 2
        yaml_reader.sequence_indent = 2

        with open(self.file_path, "r") as file:
            self._yaml_data = yaml_reader.load(file)

        # Update dict contents with the loaded data
        self.clear()
        if self._yaml_data is not None:
            self.update(self._yaml_data)

        self._config_last_modified = self.file_path.stat().st_mtime

    def reload(self) -> "ObservatoryConfig":
        """Reload configuration if file has been modified.

        Returns:
            ObservatoryConfig: Self for method chaining.
        """
        if self.is_outdated():
            self.load()
        return self

    def save(self, file_path: Optional[Union[str, Path]] = None) -> None:
        """Save configuration to YAML file with automatic backup.

        Uses ruamel.yaml to preserve comments, structure, and formatting
        from the original file.

        Args:
            file_path: Optional custom save path, defaults to original file path.
        """
        file_path = self.file_path if file_path is None else file_path
        self.save_backup()

        yaml_writer = YAML()
        yaml_writer.preserve_quotes = True
        yaml_writer.default_flow_style = False
        yaml_writer.map_indent = 2
        yaml_writer.sequence_indent = 2
        yaml_writer.sequence_dash_offset = 0
        yaml_writer.width = 4096

        # If we have the original CommentedMap, update it to preserve comments
        if self._yaml_data is not None:
            self._deep_update(self._yaml_data, dict(self))
            data_to_save = self._yaml_data
        else:
            # Fallback if no CommentedMap (shouldn't happen in normal use)
            data_to_save = dict(self)

        with open(file_path, "w") as file:
            yaml_writer.dump(data_to_save, file)

    def save_backup(self) -> None:
        """Create timestamped backup of current configuration file."""
        backup_path = self.backup_file_path()
        os.rename(self.file_path, backup_path)

    @staticmethod
    def _deep_update(target: dict, source: dict) -> None:
        """Deep update target dict with source dict values.

        Preserves ruamel.yaml CommentedMap structure and comments while updating values.
        Only updates existing keys or adds new ones; doesn't remove keys from target.

        Args:
            target: Dictionary to update (modified in place, preserves CommentedMap).
            source: Dictionary with new values to merge in.
        """
        for key, value in source.items():
            if (
                isinstance(value, dict)
                and key in target
                and isinstance(target[key], dict)
            ):
                # Recursively update nested dictionaries
                ObservatoryConfig._deep_update(target[key], value)
            else:
                # Update or add the value
                target[key] = value

    def backup_file_path(self, datetime_str: str = "") -> Path:
        """Get backup file path with timestamp.

        Args:
            datetime_str: Optional custom datetime string, defaults to current time.

        Returns:
            Path: Full path to backup file.
        """
        if not datetime_str:
            datetime_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_dir = self.config_path / "backups"
        backup_dir.mkdir(exist_ok=True)
        return backup_dir / f"{datetime_str}_{self.observatory_name}_config_backup.yml"

    def is_outdated(self) -> bool:
        """Check if configuration file has been modified since last load.

        Returns:
            bool: True if file has been modified externally.
        """
        current_mod_time = self.file_path.stat().st_mtime
        if self._config_last_modified is None:
            return True
        return current_mod_time != self._config_last_modified

    @classmethod
    def from_config(cls, config: Optional[Config] = None) -> "ObservatoryConfig":
        """Create ObservatoryConfig from main Config instance.

        Args:
            config: Main Config instance, creates new one if None.

        Returns:
            ObservatoryConfig: Configured instance for the observatory.

        Raises:
            TypeError: If config is not a Config instance.
        """
        if config is None:
            config = Config()

        if not isinstance(config, Config):
            raise TypeError(f"Expected Config, got {type(config)}")

        return cls(config.paths.observatory_config, config.observatory_name)

    def load_fits_config(self) -> pd.DataFrame:
        """Load the FITS header configuration for this observatory.

        Returns:
            pandas.DataFrame: DataFrame containing FITS header configuration
            indexed by the ``header`` column.
        """
        fits_config_path = (
            self.config_path / f"{self.observatory_name}_fits_header_config.csv"
        )
        return pd.read_csv(fits_config_path, index_col="header")

    def get_device_config(self, device_type: str, device_name: str) -> Dict[str, Any]:
        """Return configuration dict for a specific device.

        Args:
            device_type: Type of the device (e.g., 'Telescope', 'Camera').
            device_name: Name of the specific device.

        Returns:
            dict: Configuration dictionary for the specified device, or {}
                  if not found.
        """
        devices = self.get(device_type, [])
        if isinstance(devices, dict):
            raise TypeError(f"Expected list of devices, got dict for {device_type}")

        for device_config in devices:
            if device_config.get("name") == device_name:
                return device_config
        return {}
