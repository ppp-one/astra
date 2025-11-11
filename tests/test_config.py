import shutil
from pathlib import Path
from unittest.mock import ANY, MagicMock

import pytest
import yaml

from astra.config import AssetPaths, Config, _ConfigInitialiser


class TestConfigInitialiser:
    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path, monkeypatch):
        # This will run before each test
        self.config_path = tmp_path / "config" / "config.yaml"
        if self.config_path.exists():
            self.config_path.unlink()
        if not self.config_path.parent.exists():
            self.config_path.parent.mkdir()

        self.folder_assets = tmp_path / "assets"
        self.observatory_name = "Test Observatory"
        self.path_to_db = tmp_path / "gaia.db"
        self.path_to_db.touch()

        monkeypatch.setattr(Config, "CONFIG_PATH", self.config_path)
        monkeypatch.setattr(
            _ConfigInitialiser, "DEFAULT_ASSETS_PATH", Path.home() / "astra_test"
        )

    def test_run_with_all_parameters(self, capsys):
        """Test config initialization with all parameters provided."""
        _ConfigInitialiser.run(
            self.observatory_name, str(self.folder_assets), str(self.path_to_db)
        )

        assert self.config_path.exists(), "Config file was not created."

        with self.config_path.open() as f:
            config = yaml.safe_load(f)

        assert config["folder_assets"] == str(self.folder_assets)
        assert config["gaia_db"] == str(self.path_to_db)
        assert config["observatory_name"] == self.observatory_name

        captured = capsys.readouterr()
        assert "\nCreated config file." in captured.out

    def test_run_with_none_parameters(self, monkeypatch, capsys):
        """Test config initialization when no parameters are provided."""
        inputs = iter(
            ["y", "y", str(self.path_to_db), self.observatory_name]
        )  # Ensure all prompts are handled
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        monkeypatch.setattr(Path, "exists", lambda _: True)

        _ConfigInitialiser.run(None, None, None)

        assert self.config_path.exists(), "Config file was not created."

        with self.config_path.open() as f:
            config = yaml.safe_load(f)

        assert config["folder_assets"] == str(Path.home() / "astra_test")
        assert config["gaia_db"] == str(self.path_to_db)
        assert config["observatory_name"] == self.observatory_name

        captured = capsys.readouterr()
        assert "\nCreated config file." in captured.out

    def test_prompt_assets_path_creates_directory(self, monkeypatch):
        """Test asset directory creation if it does not exist."""
        inputs = iter(["n", "/invalid/path", "y"])  # Ensuring all prompts are covered
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        monkeypatch.setattr(Path, "exists", lambda _: False)

        mock_mkdir = MagicMock()
        monkeypatch.setattr(Path, "mkdir", mock_mkdir)

        assets_path = _ConfigInitialiser._prompt_assets_path()
        assert assets_path == Path("/invalid/path")
        mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)

    def test_prompt_gaia_db_path(self, monkeypatch):
        """Test Gaia DB path prompt when the path exists."""
        inputs = iter(["y", str(self.path_to_db)])  # Ensuring all prompts are covered
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        monkeypatch.setattr(Path, "exists", lambda _: True)

        db_path = _ConfigInitialiser._prompt_gaia_db_path()
        assert db_path == str(self.path_to_db)

    def test_validate_paths_valid(self):
        """Ensure valid paths pass validation."""
        _ConfigInitialiser._validate_paths(
            str(self.folder_assets), str(self.path_to_db)
        )

    def test_validate_paths_invalid_type(self):
        with pytest.raises(TypeError):
            _ConfigInitialiser._validate_paths(123, str(self.path_to_db))  # type: ignore

    def test_validate_paths_file_not_exist(self, monkeypatch):
        """Test that FileNotFoundError is raised if Gaia DB does not exist."""
        monkeypatch.setattr(Path, "exists", lambda _: False)

        with pytest.raises(FileNotFoundError):
            _ConfigInitialiser._validate_paths("/valid/assets", "/invalid/gaia.db")


class TestConfig:
    @pytest.fixture(autouse=True)
    def setup_method(self, tmp_path, monkeypatch):
        """Set up temporary paths before each test."""
        self.config_path = tmp_path / "config.yaml"
        self.folder_assets = tmp_path / "assets"
        self.gaia_db = tmp_path / "gaia.db"
        self.gaia_db.touch()  # Create an empty Gaia DB file
        self.template_dir = tmp_path / "templates"
        self.template_dir.mkdir()  # Create a fake template directory
        monkeypatch.setattr(Config, "TEMPLATE_DIR", self.template_dir)
        monkeypatch.setattr(Config, "CONFIG_PATH", self.config_path)

    def test_config_initialization_creates_config_if_missing(self):
        """Test if Config runs the initializer when the config file is missing."""
        _ = Config(
            "Test Observatory",
            str(self.folder_assets),
            str(self.gaia_db),
            allow_default=True,
        )

        assert self.config_path.exists(), "Config file should be created."
        with open(self.config_path) as f:
            loaded_config = yaml.safe_load(f)

        assert loaded_config["observatory_name"] == "Test Observatory"
        assert loaded_config["folder_assets"] == str(self.folder_assets)
        assert loaded_config["gaia_db"] == str(self.gaia_db)

    def test_config_initialization_loads_existing_config(self):
        """Test if Config loads settings properly from an existing config file."""
        config_data = {
            "observatory_name": "Mock Observatory",
            "folder_assets": str(self.folder_assets),
            "gaia_db": str(self.gaia_db),
        }
        with open(self.config_path, "w") as f:
            yaml.dump(config_data, f)

        config = Config(allow_default=True)

        assert config.observatory_name == "Mock Observatory"
        assert config.folder_assets == Path(self.folder_assets)
        assert config.gaia_db == Path(self.gaia_db)

    def test_config_reset_removes_config_file(self, monkeypatch):
        """Test if calling reset() removes the config file."""
        config = Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))

        assert self.config_path.exists(), "Config file should exist before reset."

        monkeypatch.setattr("builtins.input", lambda _: "y")
        monkeypatch.setattr(Path, "unlink", MagicMock())

        config.reset(remove_assets=False)
        Path.unlink.assert_called_once()

    def test_config_reset_removes_assets_folder(self, monkeypatch):
        """Test if reset() removes the assets folder when requested."""
        config = Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))

        monkeypatch.setattr("builtins.input", lambda _: "y")
        monkeypatch.setattr(Path, "rmdir", MagicMock())

        config.reset(remove_assets=True)
        Path.rmdir.assert_called_once()

    def test_config_save_updates_file(self, monkeypatch):
        """Test if calling save() correctly writes to the config file."""
        config = Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))

        mock_open = MagicMock()
        monkeypatch.setattr("builtins.open", mock_open)
        monkeypatch.setattr("yaml.dump", MagicMock())

        config.save()

        yaml.dump.assert_called_once_with(
            {
                "folder_assets": str(self.folder_assets),
                "gaia_db": str(self.gaia_db),
                "observatory_name": "Test Observatory",
            },
            ANY,
        )

    def test_config_as_datetime(self):
        """Test the as_datetime method for converting strings to datetime."""
        config = Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))
        dt_string = "2024-02-01 12:34:56"
        dt = config.as_datetime(dt_string)

        assert dt.year == 2024
        assert dt.month == 2
        assert dt.day == 1
        assert dt.hour == 12
        assert dt.minute == 34
        assert dt.second == 56

    def test_config_invalid_path_raises_typeerror(self):
        """Test that initializing Config with invalid paths raises TypeError."""
        with pytest.raises(TypeError):
            Config(
                observatory_name="Test Observatory",
                folder_assets=123,  # type: ignore
                gaia_db=self.gaia_db,
            )

        with pytest.raises(TypeError):
            Config(
                observatory_name="Test Observatory",
                folder_assets=self.folder_assets,
                gaia_db=456,  # type: ignore
            )

    def test_config_initialize_observatory_files_raises_error(self, monkeypatch):
        """Test that missing templates directory raises FileNotFoundError."""
        monkeypatch.setattr(Path, "exists", lambda _: False)

        with pytest.raises(FileNotFoundError):
            Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))
            Config("Test Observatory", str(self.folder_assets), str(self.gaia_db))


class TestAssetPaths:
    def test_initialize_folders_and_log_file(self, tmp_path):
        ap = AssetPaths(tmp_path)
        assert ap.assets == tmp_path
        assert ap.observatory_config.exists()
        assert ap.schedules.exists()
        assert ap.images.exists()
        assert ap.logs.exists()
        assert ap.log_file.exists()

    def test_archive_parses_date_at_line_start(self, tmp_path):
        ap = AssetPaths(tmp_path)
        # first line starts with the expected YYYY-MM-DD HH:MM:SS
        ap.log_file.write_text("2025-09-27 00:11:18 something\nnext\n")
        ap.archive_log_file()
        expected = ap.logs / "archive" / "2025-09-27 00:11:18_astra.log"
        assert expected.exists(), f"archive file not found: {expected}"
        # current log recreated and empty
        assert ap.log_file.exists()
        assert ap.log_file.read_text() == ""

    def test_archive_creates_archive_directory(self, tmp_path):
        ap = AssetPaths(tmp_path)
        # ensure archive dir absent initially
        archive_dir = ap.logs / "archive"
        if archive_dir.exists():
            shutil.rmtree(archive_dir)
        ap.log_file.write_text("2025-09-27 00:11:18 log\n")
        ap.archive_log_file()
        assert archive_dir.exists()
        # archived file present
        archived_files = list(archive_dir.glob("*_astra.log"))
        assert archived_files, "no archived files created"
