from kbrbot.features.scan import next_user_checkpoint, should_skip_user


def test_scan_should_not_skip_unseen_user():
    assert should_skip_user("1232", {"12", "44"}) is False


def test_scan_should_skip_seen_user():
    assert should_skip_user("1232", {"1232", "44"}) is True


def test_next_user_checkpoint_clamps_bounds():
    assert next_user_checkpoint(0, 100) == 1
    assert next_user_checkpoint(50, 100) == 50
    assert next_user_checkpoint(200, 100) == 101
