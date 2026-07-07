from datetime import timedelta

from main import (
    decide,
    resolve_logger_level,
    resolve_new_status,
    resolve_ttl_kwargs,
    should_process,
)


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


def test_resolve_new_status_missing_field_returns_none():
    # RED: legacy messages produced before recovery-generator added the
    # "status" field only have seq/ts/pad — value["status"] would raise
    # KeyError. resolve_new_status() must safely return None instead.
    assert resolve_new_status({"seq": 1, "ts": 123, "pad": "x"}) is None


def test_resolve_new_status_present_returns_status():
    assert resolve_new_status({"status": "ON", "seq": 1}) == "ON"
    assert resolve_new_status({"status": "OFF"}) == "OFF"


def test_should_process_none_is_false():
    # GREEN: a missing status means the message should be skipped, not
    # passed and not blocked.
    assert should_process(None) is False


def test_should_process_real_status_is_true():
    assert should_process("ON") is True
    assert should_process("OFF") is True
