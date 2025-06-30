import filecmp
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Dict, Optional

import yaml


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

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(
        self,
        observatory_name: Optional[str] = None,
        folder_assets: Optional[Path | str] = None,
        gaia_db: Optional[Path | str] = None,
        allow_default: bool = False,
    ):
        """Initialise the configuration settings.

        Args:
            observatory_name (str): The name of the observatory.
            folder_assets (Path | str): The path to the folder containing assets.
            gaia_db (Path | str): The path to the Gaia database.
            allow_default (bool): Whether to raise a SystemExit
                if observatory configuration files were left unchanged.
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

        self._initialize_observatory_files(allow_default=allow_default)

    def reset(self, remove_assets=False):
        """Resets the configuration, optionally removing associated asset folders."""
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

    def save(self):
        """Saves the current configuration settings to the YAML file."""

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
        """Convert a string to a datetime object using the configured format."""
        return datetime.strptime(date_string, self.TIME_FORMAT)

    def _load_from_file(self) -> Dict[str, str]:
        with open(self.CONFIG_PATH, "r") as file:
            config = yaml.safe_load(file)

        return config

    def _initialize_observatory_files(self, allow_default: bool):
        if not self.TEMPLATE_DIR.exists():
            raise FileNotFoundError(
                f"Template directory {self.TEMPLATE_DIR} not found."
            )

        unchanged_files = []

        for template_file in self.TEMPLATE_DIR.glob("*"):
            target_file = self.paths.observatory_config / template_file.name.replace(
                "observatory", self.observatory_name
            )
            if not target_file.exists():
                target_file.write_bytes(template_file.read_bytes())

            if filecmp.cmp(template_file, target_file, shallow=False):
                unchanged_files.append(target_file.name)

        if unchanged_files:
            message = (
                "\nWarning: Observatory config files have not been modified "
                "from default templates. Please update the following files "
                "with your observatory's information in:\n\n"
                f"{self.paths.observatory_config}\n"
            )
            if allow_default:
                print(message)
            else:
                raise SystemExit(message)

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
    """Container with paths to asset directories and the log_file used by Astra."""

    def __init__(self, folder_assets: Path | str):
        if isinstance(folder_assets, str):
            folder_assets = Path(folder_assets)

        self.assets = folder_assets
        self.observatory_config = folder_assets / "observatory_config"
        self.schedules = folder_assets / "schedules"
        self.images = folder_assets / "images"
        self.logs = folder_assets / "logs"
        self.log_file = self.logs / "astra.log"

        self._initialize_folders_and_log_file()

    def _initialize_folders_and_log_file(self):
        """Create necessary folders and the log file if they do not exist."""
        for folder in (
            self.assets,
            self.observatory_config,
            self.schedules,
            self.logs,
            self.images,
        ):
            if not folder.exists():
                folder.mkdir(parents=True)
                print(f"Created folder {folder}")

        self.log_file.touch(exist_ok=True)

    def __repr__(self) -> str:
        return f"AssetPaths(assets={self.assets})"

    def __str__(self) -> str:
        return (
            f"AssetPaths(\n"
            f"  assets={self.assets},\n"
            f"  observatory_config={self.observatory_config},\n"
            f"  schedules={self.schedules},\n"
            f"  logs={self.logs},\n"
            f"  images={self.images},\n"
            f"  log_file={self.log_file}\n"
            f")"
        )


class _ConfigInitialiser:
    """Initialises the configuration settings for Astra."""

    DEFAULT_ASSETS_PATH = Path.home() / "Documents" / "Astra"

    @staticmethod
    def run(
        observatory_name: Optional[str],
        folder_assets: Optional[str | Path],
        gaia_db: Optional[str | Path],
    ):
        """Create initial configuration through user prompts."""
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
        """Prompt user for assets folder location."""
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
    def _prompt_gaia_db_path() -> str | None:
        """Prompt user for Gaia DB location."""
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
        folder_assets: Optional[str | Path], gaia_db: Optional[str | Path]
    ):
        """Validate the arguments provided by the user."""
        if folder_assets is not None and not isinstance(folder_assets, (str, Path)):
            raise TypeError(f"Expected str or Path, got {type(folder_assets)}")

        if gaia_db is not None and not isinstance(gaia_db, (str, Path)):
            raise TypeError(f"Expected str or Path, got {type(gaia_db)}")

        if gaia_db is not None and not Path(gaia_db).exists():
            raise FileNotFoundError(f"File {gaia_db} does not exist.")


class ObservatoryConfig(dict):
    """
    Examples
    --------
    >>> from astra.config import ObservatoryConfig
    >>> observatory_config = ObservatoryConfig.from_config()
    """

    def __init__(self, config_path: str | Path, observatory_name: str):
        self.config_path = (
            config_path if isinstance(config_path, Path) else Path(config_path)
        )
        self.observatory_name = observatory_name
        self._config_last_modified = None
        self.load()

    @property
    def file_path(self) -> Path:
        """Returns the path to the observatory configuration yaml file."""
        return self.config_path / f"{self.observatory_name}_config.yml"

    def load(self):
        """Loads the observatory configuration file."""
        with open(self.file_path, "r") as file:
            config = yaml.safe_load(file)
        self.update(config)
        self._config_last_modified = self.file_path.stat().st_mtime

    def reload(self):
        """Reloads the observatory configuration file."""
        if self.is_outdated():
            self.load()
        return self

    def save(self, file_path: str | Path | None = None):
        """Saves the observatory configuration file."""
        config = dict(self)
        file_path = self.file_path if file_path is None else file_path
        self.save_backup()
        with open(file_path, "w") as file:
            yaml.dump(config, file)

    def save_backup(self):
        """Creates a backup of the observatory configuration file."""
        backup_path = self.backup_file_path()
        os.rename(self.file_path, backup_path)

    def backup_file_path(self, datetime_str: str = "") -> Path:
        """Returns the file path of a backup of the observatory configuration file."""
        if not datetime_str:
            datetime_str = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        backup_dir = self.config_path / "backups"
        backup_dir.mkdir(exist_ok=True)
        return backup_dir / f"{datetime_str}_{self.observatory_name}_config_backup.yml"

    def is_outdated(self) -> bool:
        current_mod_time = self.file_path.stat().st_mtime
        if self._config_last_modified is None:
            return True
        return current_mod_time != self._config_last_modified

    @classmethod
    def from_config(cls, config: Optional[Config] = None):
        if config is None:
            config = Config()

        if not isinstance(config, Config):
            raise TypeError(f"Expected Config, got {type(config)}")

        return cls(config.paths.observatory_config, config.observatory_name)
