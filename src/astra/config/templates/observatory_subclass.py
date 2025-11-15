from typing import List

from astra.observatory import Observatory


class ModifiedObservatory(Observatory):
    """Customised observatory class.

    Attributes:
        OBSERVATORY_ALIASES (List[str]): List of strings that the loader will match
            (case-insensitive) against the requested observatory name. The loader will
            also match the file stem and the class name.
    """

    OBSERVATORY_ALIASES: List[str] = []
