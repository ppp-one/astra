"""Configuration management for Astra observatory automation system.

This module provides configuration classes for managing Astra's settings,
observatory configurations, and asset paths. It handles YAML configuration
files, directory initialization, and provides a singleton pattern for
global configuration access.

Classes:
    Config: Main configuration singleton for Astra settings
    AssetPaths: Container for asset directory paths
    ObservatoryConfig: Observatory-specific configuration management
    _ConfigInitialiser: Helper class for initial configuration setup
"""

import filecmp
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
import yaml
from ruamel.yaml import YAML


class Config:
    """Singleton class for managing Astra's configuration settings.

    This class loads configuration settings from a YAML file and provides
    methods to access and modify these settings. It ensures that only one
    instance of the configuration is created throughout the application.

    Attributes:
        CONFIG_PATH (Path): The path to the configuration YAML file.
        TEMPLATE_DIR (Path): The path to the directory containing template files.
        TIME_FORMAT (str): The format used for datetime strings.
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

    CONFIG_PATH = Path(__file__).parent / "config" / "astra_config.yml"
    TEMPLATE_DIR = Path(__file__).parent / "config" / "templates"
    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    _instance: Optional["Config"] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> "Config":
        """Ensure singleton pattern - only one Config instance exists."""
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
        """
        if not self.CONFIG_PATH.exists():
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
                input(f"Are you sure you want to remove {self.folder_assets}? [y/n]: ")
                .strip()
                .lower()
            )
            if prompt == "y":
                if self.folder_assets.exists():
                    self.folder_assets.rmdir()
                print("Removed assets folder.")

        self.CONFIG_PATH.unlink()
        print("Removed config file.")

        return None

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
            message = (
                "\nWarning: Observatory config files have not been modified "
                "from default templates. Please update the following files "
                "with your observatory's information in:\n\n"
                f"{self.paths.observatory_config}\n"
                f"Unchanged files: {', '.join(unchanged_files)}\n"
            )
            if allow_default:
                print(message)
            else:
                raise SystemExit(message)

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
                print(f"Created folder {folder}")

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
        if any(item is None for item in (observatory_name, folder_assets, gaia_db)):
            print("\nWelcome to Astra! Please provide the following information:\n")
        else:
            print("\nWelcome to Astra!")

        _ConfigInitialiser._validate_paths(folder_assets, gaia_db)
        Config.CONFIG_PATH.parent.mkdir(exist_ok=True)

        if folder_assets is None:
            folder_assets = _ConfigInitialiser._prompt_assets_path()

        if gaia_db is None:
            gaia_db = _ConfigInitialiser._prompt_gaia_db_path()

        if observatory_name is None:
            observatory_name = input(
                "\nPlease enter the name of the observatory: "
            ).strip()

        config = {
            "folder_assets": str(folder_assets),
            "gaia_db": str(gaia_db),
            "observatory_name": str(observatory_name),
        }

        with open(Config.CONFIG_PATH, "w") as file:
            yaml.dump(config, file)

        print("\nCreated config file.")

    @staticmethod
    def _prompt_assets_path() -> Path:
        """Prompt user for assets folder location.

        Returns:
            Path: Validated path to assets folder.
        """
        while True:
            use_default = (
                input(
                    "Use default assets path "
                    f"({_ConfigInitialiser.DEFAULT_ASSETS_PATH})? [y/n]: "
                )
                .strip()
                .lower()
            )

            if use_default == "y":
                return Path(_ConfigInitialiser.DEFAULT_ASSETS_PATH)
            elif use_default == "n":
                custom_path = Path(input("Please enter the desired path: ").strip())
                if custom_path.exists():
                    return custom_path
                create_path = (
                    input(
                        "Error: Path does not exist. Do you want to create it? [y/n]: "
                    )
                    .strip()
                    .lower()
                )
                if create_path == "y":
                    custom_path.mkdir(parents=True, exist_ok=True)
                    return custom_path
            else:
                print("Please enter 'y' or 'n'.")

    @staticmethod
    def _prompt_gaia_db_path() -> Optional[str]:
        """Prompt user for Gaia database location.

        Returns:
            str or None: Path to Gaia database file or None if not using local DB.
        """
        while True:
            use_local = input("\nUse local Gaia DB? [y/n]: ").strip().lower()

            if use_local == "y":
                db_path = Path(input("Please enter the path to Gaia DB: ").strip())
                if db_path.exists():
                    return str(db_path)
                print("Error: File does not exist. Please provide a valid path.")
            elif use_local == "n":
                return None
            else:
                print("Please enter 'y' or 'n'.")

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
        """
        Load the FITS header configuration as a pandas DataFrame.

        Args:
            observatory_name (str): Name of the observatory.

        Returns:
            pd.DataFrame: DataFrame containing FITS header configuration.
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
