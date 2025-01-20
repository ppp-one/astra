from datetime import datetime
from pathlib import Path
from typing import Optional, List
import yaml
from dataclasses import dataclass


@dataclass
class ConfigPaths:
    """Container for all configuration and asset paths."""

    folder_config: Path
    file_config: Path
    folder_assets: Path
    folder_observatory: Path
    folder_schedule: Path
    folder_log: Path
    folder_images: Path
    file_log: Path


class Config:
    """
    Configuration manager for Astra.

    Handles loading configuration settings, managing asset folders, and maintaining
    observatory-specific settings. Uses a YAML configuration file and creates necessary
    directory structures.
    """

    TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
    REQUIRED_FOLDERS = ["logs", "schedules", "observatory_config", "images"]
    DEFAULT_ASSETS_PATH = Path(__file__).parent.parent.parent / "assets"

    def __init__(self):
        """Initialize the configuration manager."""
        self.paths = self._initialize_paths()
        self.paths.folder_config.mkdir(exist_ok=True)
        self.config = self._load_config()
        self._setup_asset_folders()

    def _initialize_paths(self) -> ConfigPaths:
        """Initialize all path objects."""
        folder_config = Path(__file__).parent.parent
        folder_assets = Path(
            self.DEFAULT_ASSETS_PATH
        )  # Will be updated after config load

        return ConfigPaths(
            folder_config=folder_config,
            file_config=folder_config / "astra_config.yml",
            folder_assets=folder_assets,
            folder_observatory=folder_assets / "observatory_config",
            folder_schedule=folder_assets / "schedules",
            folder_log=folder_assets / "logs",
            folder_images=folder_assets / "images",
            file_log=folder_assets / "logs" / "astra.log",
        )

    def _load_config(self) -> dict:
        """Load or create the configuration file."""
        if not self.paths.file_config.exists():
            return self._create_initial_config()

        with open(self.paths.file_config, "r") as file:
            config = yaml.safe_load(file)

        # Update folder_assets path after loading config
        self.paths.folder_assets = Path(config["folder_assets"])
        return config

    def _create_initial_config(self) -> dict:
        """Create initial configuration through user prompts."""
        print("\nWelcome to Astra! Please provide the following information:\n")

        folder_assets = self._prompt_assets_path()
        gaia_db = self._prompt_gaia_db()
        observatory_name = input("\nPlease enter the name of the observatory: ").strip()

        config = {
            "folder_assets": str(folder_assets),
            "gaia_db": gaia_db,
            "observatory_name": observatory_name,
            "user_approved": False,
        }

        with open(self.paths.file_config, "w") as file:
            yaml.dump(config, file)
            print(f"\nCreated config file: {self.paths.file_config}")

        return config

    def _prompt_assets_path(self) -> Path:
        """Prompt user for assets folder location."""
        while True:
            use_default = (
                input(f"Use default assets path ({self.DEFAULT_ASSETS_PATH})? [y/n]: ")
                .strip()
                .lower()
            )

            if use_default == "y":
                return self.DEFAULT_ASSETS_PATH
            elif use_default == "n":
                custom_path = Path(input("Please enter the desired path: ").strip())
                if custom_path.exists():
                    return custom_path
                print("Error: Path does not exist. Please create it first.")
            else:
                print("Please enter 'y' or 'n'.")

    def _prompt_gaia_db(self) -> Optional[str]:
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

    def _setup_asset_folders(self) -> None:
        """Set up all required asset folders."""
        self.paths.folder_assets.mkdir(exist_ok=True)

        for folder_name in self.REQUIRED_FOLDERS:
            folder = self.paths.folder_assets / folder_name
            if not folder.exists():
                folder.mkdir()

                if folder_name == "observatory_config":
                    self._copy_template_configs(folder)

                print(f"Created folder: {folder}")

        if not self.config["user_approved"]:
            self._show_setup_message()

    def _copy_template_configs(self, target_folder: Path) -> None:
        """Copy template configuration files to the observatory config folder."""
        template_folder = Path(__file__).parent.parent / "template_configs"
        if not template_folder.exists():
            return

        observatory_name = self.config["observatory_name"]
        for template in template_folder.iterdir():
            new_name = template.name
            if template.suffix == ".csv":
                new_name = f"{observatory_name}_fits_header_config.csv"
            elif template.suffix == ".yml":
                new_name = f"{observatory_name}_config.yml"

            destination = target_folder / new_name
            destination.write_bytes(template.read_bytes())

    def _show_setup_message(self) -> None:
        """Display setup completion message."""
        print(f"\nPlease:")
        print(
            f"1. Edit the observatory config files in {self.paths.folder_observatory}"
        )
        print(f"2. Set 'user_approved: true' in {self.paths.file_config}")
        print("before proceeding.\n")
        exit()

    def as_datetime(self, date_string: str) -> datetime:
        """Convert a string to a datetime object using the configured format."""
        return datetime.strptime(date_string, self.TIME_FORMAT)
