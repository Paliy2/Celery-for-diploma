from celery.result import ResultSet
from t.integration.tasks import add, raise_error
from t.integration.canvas.foundation import flaky, assert_ping, TIMEOUT


class test_result_set:

    @flaky
    def test_result_set(self, manager):
        assert_ping(manager)

        rs = ResultSet([add.delay(1, 1), add.delay(2, 2)])
        assert rs.get(timeout=TIMEOUT) == [2, 4]

    @flaky
    def test_result_set_error(self, manager):
        assert_ping(manager)

        rs = ResultSet([raise_error.delay(), add.delay(1, 1)])
        rs.get(timeout=TIMEOUT, propagate=False)

        assert rs.results[0].failed()
        assert rs.results[1].successful()
