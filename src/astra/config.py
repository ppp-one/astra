from pathlib import Path

import yaml


class Config:
    """
    Configuration class for astra. It loads the config file and creates the
    assets folder if they do not exist.
    """

    folder_config: str = Path.home() / ".astra"
    """folder where astra config files are located"""

    file_config: str = folder_config / "config.yaml"
    """config file path"""

    folder_assets: None
    """folder where working sub-folders like schedule, log... are located"""

    def __init__(self):
        self.folder_config.mkdir(exist_ok=True)
        self.config = self.load_config()
        self.check_assets_folders()

    @property
    def folder_telescope(self):
        """Folder where telescope configuration are stored."""
        return self.folder_assets / "telescope"

    @property
    def folder_schedule(self):
        """Folder where schedule files are stored."""
        return self.folder_assets / "schedule"

    @property
    def folder_log(self):
        """Folder where log files are stored."""
        return self.folder_assets / "log"

    def check_assets_folders(self, exist_ok=True):
        """Check if the assets folders exist, if not create them, i.e.
        telescope, schedule, log. Base folder is defined in the config file as
        ``folder_assets``.

        Parameters
        ----------
        exist_ok : bool, optional
            If True, do not raise an exception if the folder already exists.
            Default is True.
        """
        self.folder_assets = Path(self.config["folder_assets"])
        self.folder_assets.mkdir(exist_ok=exist_ok)

        for folder in ["log", "schedule", "telescope"]:
            if not (self.folder_assets / folder).exists():
                (self.folder_assets / folder).mkdir()
                print(f"Folder {self.folder_assets / folder} created")

    def check_config_file(self, exist_ok=True):
        """Check if the config file exists, if not create it.

        Parameters
        ----------
        exist_ok : bool, optional
            If True, do not raise an exception if the file already exists.
            Default is True.

        Raises
        ------
        FileExistsError
            If the file already exists and `exist_ok` is False.
        """
        if self.file_config.exists() and not exist_ok:
            raise FileExistsError("Config file already exists")
        else:
            config = {
                "folder_assets": str(Path(__file__).parent.parent.parent / "assets"),
                "gaia_db": None,
            }

            with open(self.file_config, "w") as file:
                yaml.dump(config, file)
                print(f"Config file created at {self.file_config}")

    def load_config(self):
        """Load the config file."""
        self.check_config_file()

        with open(self.file_config, "r") as file:
            return yaml.safe_load(file)
