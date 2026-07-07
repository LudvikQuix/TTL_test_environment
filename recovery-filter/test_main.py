from datetime import timedelta

from main import decide, resolve_logger_level, resolve_ttl_kwargs


def test_resolve_ttl_kwargs_off():
    assert resolve_ttl_kwargs(False, 30) == {}
    assert resolve_ttl_kwargs(False, 0) == {}


def test_resolve_ttl_kwargs_on():
    assert resolve_ttl_kwargs(True, 30) == {"ttl": timedelta(seconds=30)}
    assert resolve_ttl_kwargs(True, 5) == {"ttl": timedelta(seconds=5)}


def test_resolve_logger_level_explicit_values():
    assert resolve_logger_level("off") == "off"
    assert resolve_logger_level("info") == "info"
    assert resolve_logger_level("debug") == "debug"


def test_resolve_logger_level_legacy_on_maps_to_info():
    assert resolve_logger_level("on") == "info"


def test_resolve_logger_level_unrecognized_falls_back_to_info():
    assert resolve_logger_level("") == "info"
    assert resolve_logger_level("garbage") == "info"
    assert resolve_logger_level(None) == "info"


def test_resolve_logger_level_case_insensitive():
    assert resolve_logger_level("DEBUG") == "debug"
    assert resolve_logger_level("Off") == "off"
    assert resolve_logger_level("ON") == "info"


def test_decide_no_stored_status_passes():
    assert decide(None, "ON") is True


def test_decide_same_status_blocks():
    assert decide("ON", "ON") is False
    assert decide("OFF", "OFF") is False


def test_decide_changed_status_passes():
    assert decide("ON", "OFF") is True
