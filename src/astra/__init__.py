from astra.config import Config
import importlib.metadata

__all__ = ["ASTRA_VER", "Config"]

try:
    # Get the version of the installed 'astra' package
    ASTRA_VER = importlib.metadata.version("astra")
except importlib.metadata.PackageNotFoundError:
    # Fallback if the package is not installed (e.g., running from source)
    # You might want to log a warning here or set a default
    raise RuntimeError(
        "Astra package not found. Please ensure it is installed correctly."
    )
