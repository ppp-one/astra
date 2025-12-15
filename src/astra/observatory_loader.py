"""Load an Observatory subclass based on the observatory name.

Key capabilities:
    - Dynamically load custom Observatory subclasses from specified plugin paths
    - Match observatory names against class names and aliases
    - Fallback to default Observatory class if no custom class is found

"""

import importlib.util
import logging
from pathlib import Path
from typing import List, Optional, Type

from astra.config import Config
from astra.observatory import Observatory


class ObservatoryLoader:
    """
    Load an Observatory subclass based on the observatory name.

    Examples:
    >>> from astra.observatory_loader import ObservatoryLoader
    >>> ObservatoryLoader(observatory_name="MyObservatory").load()
    """

    def __init__(self, observatory_name: Optional[str] = None):
        self.observatory_name = observatory_name
        self.custom_observatories = Config().paths.custom_observatories

    def load(self) -> Type[Observatory]:
        """Return an Observatory class: plugin-provided subclass if available, else default."""
        if self.observatory_name is None:
            return Observatory

        target = self.observatory_name.lower()

        for observatory_path in self.custom_observatories.glob("*.py"):
            classes = self._try_load_from_path(observatory_path)
            for cls in classes:
                if not isinstance(cls, type) or not issubclass(cls, Observatory):
                    continue

                # Match against class name and aliases
                candidates = {cls.__name__.lower()} | {
                    item.lower()
                    for item in getattr(cls, "OBSERVATORY_ALIASES", [])
                    if isinstance(item, str)
                }

                if target in candidates:
                    return cls

        return Observatory

    def _try_load_from_path(self, path: Path) -> List[Type[Observatory]]:
        """Attempt to load all Observatory subclasses from the specified path."""
        found: List[Type[Observatory]] = []
        try:
            spec = importlib.util.spec_from_file_location(path.stem, path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (
                        isinstance(attr, type)
                        and issubclass(attr, Observatory)
                        and attr is not Observatory
                    ):
                        found.append(attr)
        except Exception as e:
            logging.error(f"Error loading module from {path}: {e}")
        return found
