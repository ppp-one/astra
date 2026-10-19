from unittest.mock import MagicMock

import pytest

from astra.utils import astelos_errors

CLEAN = "DRIVES|0::,SYSTEM|0::,AUXILIARY|0::,UNKNOWN|0::"
DOME_POWERED_ON = (
    "DRIVES|2:DOME[0]|2;DOME[1]|2:"
    "ERR_DeviceError|axis (0) unexpectedly changed to powered on state|2|DOME[0];"
    "ERR_DeviceError|axis (0) unexpectedly changed to powered on state|2|DOME[1],"
    "SYSTEM|0::,AUXILIARY|0::,UNKNOWN|0::"
)
UNKNOWN_ERROR = "DRIVES|2:HA|2:ERR_DeviceError|something new|2|HA,SYSTEM|0::"


def telescope_returning(reply):
    telescope = MagicMock()
    telescope.get.return_value = reply
    return telescope


@pytest.mark.parametrize(
    "reply, expected",
    [
        (CLEAN, True),
        (DOME_POWERED_ON, True),
        ("SYSTEM|4:LOCAL|4:ERR_DeviceWarn|Malformed telegram from GPS|4|LOCAL", True),
        ("Client not Connected", False),
        ("", False),
        ("1.0", False),
        (None, False),
    ],
)
def test_is_status_reply(reply, expected):
    assert astelos_errors.is_status_reply(reply) is expected


def test_check_error_clean_status():
    valid, errors, messages = astelos_errors.check_error(telescope_returning(CLEAN))
    assert valid is True
    assert len(errors) == 0
    assert messages == CLEAN


def test_check_error_allowed_errors():
    valid, errors, _ = astelos_errors.check_error(telescope_returning(DOME_POWERED_ON))
    assert valid is True
    assert len(errors) == 2


def test_check_error_unknown_error():
    valid, errors, _ = astelos_errors.check_error(telescope_returning(UNKNOWN_ERROR))
    assert valid is False
    assert len(errors) == 1


def test_check_error_driver_not_connected():
    """A driver that lost AsTelOS returns plain text. That is not "no errors"."""
    valid, errors, messages = astelos_errors.check_error(
        telescope_returning("Client not Connected")
    )
    assert valid is False
    assert len(errors) == 0
    assert messages == "Client not Connected"


def test_ack_error_stops_when_driver_not_connected(monkeypatch):
    monkeypatch.setattr(astelos_errors.time, "sleep", lambda seconds: None)
    telescope = MagicMock()
    telescope.get.side_effect = lambda command, **kwargs: (
        "Client not Connected" if command == "CommandString" else None
    )
    errors = astelos_errors.check_error(telescope_returning(DOME_POWERED_ON))[1]

    ack, messages = astelos_errors.ack_error(telescope, True, errors, DOME_POWERED_ON)

    assert ack is False
    assert messages == "Client not Connected"
