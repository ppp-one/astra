import numpy as np
import pytest

from astra import pointer


def test_no_twirl_match_gives_clear_error(monkeypatch):
    monkeypatch.setattr(pointer.twirl, "compute_wcs", lambda *args: None)
    stars = np.zeros((4, 2))
    gaia = np.zeros((8, 2))

    with pytest.raises(Exception, match="no match between the 4 image stars"):
        pointer.ImageStarMapping.from_gaia_coordinates(stars, gaia)
