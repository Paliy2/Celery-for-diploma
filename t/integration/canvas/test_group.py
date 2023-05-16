import uuid

import pytest

from celery import group, chord, chain
from celery.exceptions import TimeoutError
from celery.result import GroupResult
from t.integration.conftest import get_redis_connection
from t.integration.tasks import add, raise_error, ids, identity, redis_echo, fail, ExpectedException, redis_count, \
    add_replaced, fail_replaced, replace_with_chain
from t.integration.canvas.foundation import flaky, assert_ping, TIMEOUT, await_redis_echo, await_redis_count


class test_group:
    @flaky
    def test_ready_with_exception(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        g = group([add.s(1, 2), raise_error.s()])
        result = g.apply_async()
        while not result.ready():
            pass

    @flaky
    def test_empty_group_result(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        task = group([])
        result = task.apply_async()

        GroupResult.save(result)
        task = GroupResult.restore(result.id)
        assert task.results == []

    @flaky
    def test_parent_ids(self, manager):
        assert_ping(manager)

        g = (
            ids.si(i=1) |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(2, 50))
        )
        res = g()
        expected_root_id = res.parent.parent.id
        expected_parent_id = res.parent.id
        values = res.get(timeout=TIMEOUT)

        for i, r in enumerate(values):
            root_id, parent_id, value = r
            assert root_id == expected_root_id
            assert parent_id == expected_parent_id
            assert value == i + 2

    @flaky
    def test_nested_group(self, manager):
        assert_ping(manager)

        c = group(
            add.si(1, 10),
            group(
                add.si(1, 100),
                group(
                    add.si(1, 1000),
                    add.si(1, 2000),
                ),
            ),
        )
        res = c()

        assert res.get(timeout=TIMEOUT) == [11, 101, 1001, 2001]

    @flaky
    def test_large_group(self, manager):
        assert_ping(manager)

        c = group(identity.s(i) for i in range(1000))
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == list(range(1000))

    def test_group_lone(self, manager):
        """
        Test that a simple group completes.
        """
        sig = group(identity.s(42), identity.s(42))  # [42, 42]
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_group_group(self, manager):
        """
        Confirm that groups nested inside groups get unrolled.
        """
        sig = group(
            group(identity.s(42), identity.s(42)),  # [42, 42]
        )  # [42, 42] due to unrolling
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_group_chord_counting_simple(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_sig = identity.si(42)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[42]]

    def test_nested_group_chord_counting_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = chain((identity.si(1337),) * gchild_count)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[1337]]

    def test_nested_group_chord_counting_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = group((identity.si(1337),) * gchild_count)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[1337] * gchild_count]

    def test_nested_group_chord_counting_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = chord(
            (identity.si(1337),) * gchild_count, identity.si(31337),
        )
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[31337]]

    def test_nested_group_chord_counting_mixed(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        child_chord = chord(
            (
                identity.si(42),
                chain((identity.si(42),) * gchild_count),
                group((identity.si(42),) * gchild_count),
                chord((identity.si(42),) * gchild_count, identity.si(1337)),
            ),
            identity.s(),
        )
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected. The
        # group result gets unrolled into the encapsulating chord, hence the
        # weird unpacking below
        assert res.get(timeout=TIMEOUT) == [
            [42, 42, *((42,) * gchild_count), 1337]
        ]

    @pytest.mark.xfail(raises=TimeoutError, reason="#6734")
    def test_nested_group_chord_body_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_chord = chord(identity.si(42), chain((identity.s(),)))
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # The result can be expected to timeout since it seems like its
        # underlying promise might not be getting fulfilled (ref #6734). Pick a
        # short timeout since we don't want to block for ages and this is a
        # fairly simple signature which should run pretty quickly.
        expected_result = [[42]]
        with pytest.raises(TimeoutError) as expected_excinfo:
            res.get(timeout=TIMEOUT / 10)
        # Get the child `AsyncResult` manually so that we don't have to wait
        # again for the `GroupResult`
        assert res.children[0].get(timeout=TIMEOUT) == expected_result[0]
        assert res.get(timeout=TIMEOUT) == expected_result
        # Re-raise the expected exception so this test will XFAIL
        raise expected_excinfo.value

    def test_callback_called_by_group(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        callback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        callback = redis_echo.si(callback_msg, redis_key=redis_key)

        group_sig = group(identity.si(42), identity.si(1337))
        group_sig.link(callback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Group result is returned"):
            res = group_sig.delay()
            assert res.get(timeout=TIMEOUT) == [42, 1337]
        with subtests.test(msg="Callback is called after group is completed"):
            await_redis_echo({callback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_first(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)

        group_sig = group(fail.s(), identity.si(42))
        group_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_last(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)

        group_sig = group(identity.si(42), fail.s())
        group_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_multiple(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        expected_errback_count = 42
        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        # Include a mix of passing and failing tasks
        group_sig = group(
            *(identity.si(42) for _ in range(24)),  # arbitrary task count
            *(fail.s() for _ in range(expected_errback_count)),
        )
        group_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_count(expected_errback_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_children_with_callbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = identity.si(1337)
        child_sig.link(callback)
        group_sig = group(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [1337] * child_task_count
        with subtests.test(msg="Chain child task callbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_children_with_errbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = fail.si()
        child_sig.link_error(errback)
        group_sig = group(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain fails due to a child task dying"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain child task errbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_with_callback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        group_sig = group(add_replaced.si(42, 1337), identity.si(31337))
        group_sig.link(callback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [42 + 1337, 31337]
        with subtests.test(msg="Callback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_with_errback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        group_sig = group(add_replaced.si(42, 1337), fail.s())
        group_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_child_with_callback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_sig = add_replaced.si(42, 1337)
        child_sig.link(callback)
        group_sig = group(child_sig, identity.si(31337))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [42 + 1337, 31337]
        with subtests.test(msg="Callback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_child_with_errback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_sig = fail_replaced.si()
        child_sig.link_error(errback)
        group_sig = group(child_sig, identity.si(42))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_first(self, manager):
        orig_sig = group(replace_with_chain.si(42), identity.s(1337))
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_middle(self, manager):
        orig_sig = group(
            identity.s(42), replace_with_chain.s(1337), identity.s(31337)
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337, 31337]

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_last(self, manager):
        orig_sig = group(identity.s(42), replace_with_chain.s(1337))
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]
