import pytest

from celery import signature
from celery.exceptions import TimeoutError
from t.integration.tasks import ExpectedException, fail, return_exception, retry_once
from t.integration.canvas.foundation import flaky, TIMEOUT


class test_link_error:
    @flaky
    def test_link_error_eager(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(args=("test",), link_error=return_exception.s())
        actual = result.get(timeout=TIMEOUT, propagate=False)
        assert actual == exception

    @flaky
    def test_link_error(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(args=("test",), link_error=return_exception.s())
        actual = result.get(timeout=TIMEOUT, propagate=False)
        assert actual == exception

    @flaky
    def test_link_error_callback_error_callback_retries_eager(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(
            args=("test",),
            link_error=retry_once.s(countdown=None)
        )
        assert result.get(timeout=TIMEOUT, propagate=False) == exception

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout instead of returning exception")
    def test_link_error_callback_retries(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply_async(
            args=("test",),
            link_error=retry_once.s(countdown=None)
        )
        assert result.get(timeout=TIMEOUT / 10, propagate=False) == exception

    @flaky
    def test_link_error_using_signature_eager(self):
        fail = signature('t.integration.tasks.fail', args=("test",))
        retrun_exception = signature('t.integration.tasks.return_exception')

        fail.link_error(retrun_exception)

        exception = ExpectedException("Task expected to fail", "test")
        assert (fail.apply().get(timeout=TIMEOUT, propagate=False), True) == (
            exception, True)

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout instead of returning exception")
    def test_link_error_using_signature(self):
        fail = signature('t.integration.tasks.fail', args=("test",))
        retrun_exception = signature('t.integration.tasks.return_exception')

        fail.link_error(retrun_exception)

        exception = ExpectedException("Task expected to fail", "test")
        assert (fail.delay().get(timeout=TIMEOUT / 10, propagate=False), True) == (
            exception, True)
