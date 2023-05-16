import collections
from time import sleep

import pytest
import pytest_subtests  # noqa

from celery.exceptions import TimeoutError

from t.integration.conftest import get_redis_connection

RETRYABLE_EXCEPTIONS = (OSError, ConnectionError, TimeoutError)


def is_retryable_exception(exc):
    return isinstance(exc, RETRYABLE_EXCEPTIONS)


TIMEOUT = 60

_flaky = pytest.mark.flaky(reruns=5, reruns_delay=1, cause=is_retryable_exception)
_timeout = pytest.mark.timeout(timeout=300)


def flaky(fn):
    return _timeout(_flaky(fn))


def await_redis_echo(expected_msgs, redis_key="redis-echo", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to contain a string.
    """
    redis_connection = get_redis_connection()

    if isinstance(expected_msgs, (str, bytes, bytearray)):
        expected_msgs = (expected_msgs,)
    expected_msgs = collections.Counter(
        e if not isinstance(e, str) else e.encode("utf-8")
        for e in expected_msgs
    )

    # This can technically wait for `len(expected_msg_or_msgs) * timeout` :/
    while +expected_msgs:
        maybe_key_msg = redis_connection.blpop(redis_key, timeout)
        if maybe_key_msg is None:
            raise TimeoutError(
                "Fetching from {!r} timed out - still awaiting {!r}"
                .format(redis_key, dict(+expected_msgs))
            )
        retrieved_key, msg = maybe_key_msg
        assert retrieved_key.decode("utf-8") == redis_key
        expected_msgs[msg] -= 1  # silently accepts unexpected messages

    # There should be no more elements - block momentarily
    assert redis_connection.blpop(redis_key, min(1, timeout)) is None


def await_redis_list_message_length(expected_length, redis_key="redis-group-ids", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to contain a string.
    """
    sleep(1)
    redis_connection = get_redis_connection()

    check_interval = 0.1
    check_max = int(timeout / check_interval)

    for i in range(check_max + 1):
        length = redis_connection.llen(redis_key)

        if length == expected_length:
            break

        sleep(check_interval)
    else:
        raise TimeoutError(f'{redis_key!r} has length of {length}, but expected to be of length {expected_length}')

    sleep(min(1, timeout))
    assert redis_connection.llen(redis_key) == expected_length


def await_redis_count(expected_count, redis_key="redis-count", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to count to a value.
    """
    redis_connection = get_redis_connection()

    check_interval = 0.1
    check_max = int(timeout / check_interval)
    for i in range(check_max + 1):
        maybe_count = redis_connection.get(redis_key)
        # It's either `None` or a base-10 integer
        if maybe_count is not None:
            count = int(maybe_count)
            if count == expected_count:
                break
            elif i >= check_max:
                assert count == expected_count
        # try again later
        sleep(check_interval)
    else:
        raise TimeoutError(f"{redis_key!r} was never incremented")

    # There should be no more increments - block momentarily
    sleep(min(1, timeout))
    assert int(redis_connection.get(redis_key)) == expected_count


def compare_group_ids_in_redis(redis_key='redis-group-ids'):
    redis_connection = get_redis_connection()
    actual = redis_connection.lrange(redis_key, 0, -1)
    assert len(actual) >= 2, 'Expected at least 2 group ids in redis'
    assert actual[0] == actual[1], 'Expected group ids to be equal'


def assert_ids(r, expected_value, expected_root_id, expected_parent_id):
    root_id, parent_id, value = r.get(timeout=TIMEOUT)
    assert expected_value == value
    assert root_id == expected_root_id
    assert parent_id == expected_parent_id


def assert_ping(manager):
    ping_result = manager.inspect().ping()
    assert ping_result
    ping_val = list(ping_result.values())[0]
    assert ping_val == {"ok": "pong"}
