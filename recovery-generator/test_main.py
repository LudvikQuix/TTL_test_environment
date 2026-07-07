from main import next_status, should_flip, toggle


def test_toggle():
    assert toggle("ON") == "OFF"
    assert toggle("OFF") == "ON"


def test_should_flip_true():
    assert should_flip(0.01, 0.05) is True


def test_should_flip_false():
    assert should_flip(0.5, 0.05) is False


def test_next_status_seeds_on_when_no_prev():
    assert next_status(None, False) == "ON"
    assert next_status(None, True) == "ON"


def test_next_status_flips_when_flip_true():
    assert next_status("ON", True) == "OFF"
    assert next_status("OFF", True) == "ON"


def test_next_status_holds_steady_when_flip_false():
    assert next_status("ON", False) == "ON"
    assert next_status("OFF", False) == "OFF"
