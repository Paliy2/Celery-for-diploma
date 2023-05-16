import re
import tempfile
import uuid
from time import sleep, monotonic

import pytest

from celery import chord, group, chain
from celery.backends.base import BaseKeyValueStoreBackend
from celery.exceptions import TimeoutError
from celery.result import AsyncResult, GroupResult
from t.integration.conftest import get_active_redis_channels, get_redis_connection, TEST_BACKEND
from t.integration.tasks import add, tsum, delayed_sum, add_replaced, add_to_all, add_to_all_to_chord, identity, \
    add_chord_to_chord, ids, collect_ids, fail, print_unicode, errback_old_style, write_to_file_and_return_int, \
    return_priority, ExpectedException, redis_echo, errback_new_style, redis_count, replace_with_chain
from t.integration.canvas.foundation import flaky, TIMEOUT, assert_ids, await_redis_echo, await_redis_count


class test_chord:
    @flaky
    def test_simple_chord_with_a_delay_in_group_save(self, manager, monkeypatch):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not isinstance(manager.app.backend, BaseKeyValueStoreBackend):
            raise pytest.skip("The delay may only occur in the cache backend")

        x = BaseKeyValueStoreBackend._apply_chord_incr

        def apply_chord_incr_with_sleep(self, *args, **kwargs):
            sleep(1)
            x(self, *args, **kwargs)

        monkeypatch.setattr(BaseKeyValueStoreBackend,
                            '_apply_chord_incr',
                            apply_chord_incr_with_sleep)

        c = chord(header=[add.si(1, 1), add.si(1, 1)], body=tsum.s())

        result = c()
        assert result.get(timeout=TIMEOUT) == 4

    @pytest.mark.xfail(reason="async_results aren't performed in async way")
    def test_redis_subscribed_channels_leak(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        manager.app.backend.result_consumer.on_after_fork()
        initial_channels = get_active_redis_channels()
        initial_channels_count = len(initial_channels)
        total_chords = 10
        async_results = [
            chord([add.s(5, 6), add.s(6, 7)])(delayed_sum.s())
            for _ in range(total_chords)
        ]

        channels_before = get_active_redis_channels()
        manager.assert_result_tasks_in_progress_or_completed(async_results)

        channels_before_count = len(channels_before)
        assert set(channels_before) != set(initial_channels)
        assert channels_before_count > initial_channels_count

        # The total number of active Redis channels at this point
        # is the number of chord header tasks multiplied by the
        # total chord tasks, plus the initial channels
        # (existing from previous tests).
        chord_header_task_count = 2
        assert channels_before_count <= \
               chord_header_task_count * total_chords + initial_channels_count

        result_values = [
            result.get(timeout=TIMEOUT)
            for result in async_results
        ]
        assert result_values == [24] * total_chords

        channels_after = get_active_redis_channels()
        channels_after_count = len(channels_after)

        assert channels_after_count == initial_channels_count
        assert set(channels_after) == set(initial_channels)

    @flaky
    def test_replaced_nested_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([
            chord(
                [add.s(1, 2), add_replaced.s(3, 4)],
                add_to_all.s(5),
            ) | tsum.s(),
            chord(
                [add_replaced.s(6, 7), add.s(0, 0)],
                add_to_all.s(8),
            ) | tsum.s(),
        ], add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [29, 38]

    @flaky
    def test_add_to_chord(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = group([add_to_all_to_chord.s([1, 2, 3], 4)]) | identity.s()
        res = c()
        assert sorted(res.get()) == [0, 5, 6, 7]

    @flaky
    def test_add_chord_to_chord(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = group([add_chord_to_chord.s([1, 2, 3], 4)]) | identity.s()
        res = c()
        assert sorted(res.get()) == [0, 5 + 6 + 7]

    @flaky
    def test_eager_chord_inside_task(self, manager):
        from .tasks import chord_add

        prev = chord_add.app.conf.task_always_eager
        chord_add.app.conf.task_always_eager = True

        chord_add.apply_async(args=(4, 8), throw=True).get()

        chord_add.app.conf.task_always_eager = prev

    def test_group_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [12, 13, 14, 15]

    def test_group_kwargs(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c.apply_async(kwargs={"z": 1})
        assert res.get(timeout=TIMEOUT) == [13, 14, 15, 16]

    def test_group_args_and_kwargs(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = (
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c.apply_async(args=(4,), kwargs={"z": 1})
        if manager.app.conf.result_backend.startswith('redis'):
            # for a simple chord like the one above, redis does not guarantee
            # the ordering of the results as a performance trade off.
            assert set(res.get(timeout=TIMEOUT)) == {13, 14, 15, 16}
        else:
            assert res.get(timeout=TIMEOUT) == [13, 14, 15, 16]

    def test_nested_group_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = chain(
            add.si(1, 0),
            group(
                add.si(1, 100),
                chain(
                    add.si(1, 200),
                    group(
                        add.si(1, 1000),
                        add.si(1, 2000),
                    ),
                ),
            ),
            add.si(1, 10),
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == 11

    @flaky
    def test_single_task_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([add.s(2, 5)], body=add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [16]

        c2 = group([add.s(2, 5)]) | add_to_all.s(9)
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == [16]

    def test_empty_header_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([], body=add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == []

        c2 = group([]) | add_to_all.s(9)
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == []

    @flaky
    def test_nested_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([
            chord([add.s(1, 2), add.s(3, 4)], add.s([5])),
            chord([add.s(6, 7)], add.s([10]))
        ], add_to_all.s(['A']))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [[3, 7, 5, 'A'], [13, 10, 'A']]

        c2 = group([
            group([add.s(1, 2), add.s(3, 4)]) | add.s([5]),
            group([add.s(6, 7)]) | add.s([10]),
        ]) | add_to_all.s(['A'])
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == [[3, 7, 5, 'A'], [13, 10, 'A']]

        c = group([
            group([
                group([
                    group([
                        add.s(1, 2)
                    ]) | add.s([3])
                ]) | add.s([4])
            ]) | add.s([5])
        ]) | add.s([6])

        res = c()
        assert [[[[3, 3], 4], 5], 6] == res.get(timeout=TIMEOUT)

    @flaky
    def test_parent_ids(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = chain(
            root, ids.si(i=2),
            chord(
                group(ids.si(i=i) for i in range(3, 50)),
                chain(collect_ids.s(i=50) | ids.si(i=51)),
            ),
        )
        self.assert_parentids_chord(g(), expected_root_id)

    @flaky
    def test_parent_ids__OR(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = (
            root |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(3, 50)) |
            collect_ids.s(i=50) |
            ids.si(i=51)
        )
        self.assert_parentids_chord(g(), expected_root_id)

    def assert_parentids_chord(self, res, expected_root_id):
        assert isinstance(res, AsyncResult)
        assert isinstance(res.parent, AsyncResult)
        assert isinstance(res.parent.parent, GroupResult)
        assert isinstance(res.parent.parent.parent, AsyncResult)
        assert isinstance(res.parent.parent.parent.parent, AsyncResult)

        # first we check the last task
        assert_ids(res, 51, expected_root_id, res.parent.id)

        # then the chord callback
        prev, (root_id, parent_id, value) = res.parent.get(timeout=30)
        assert value == 50
        assert root_id == expected_root_id
        # started by one of the chord header tasks.
        assert parent_id in res.parent.parent.results

        # check what the chord callback recorded
        for i, p in enumerate(prev):
            root_id, parent_id, value = p
            assert root_id == expected_root_id
            assert parent_id == res.parent.parent.parent.id

        # ids(i=2)
        root_id, parent_id, value = res.parent.parent.parent.get(timeout=30)
        assert value == 2
        assert parent_id == res.parent.parent.parent.parent.id
        assert root_id == expected_root_id

        # ids(i=1)
        root_id, parent_id, value = res.parent.parent.parent.parent.get(
            timeout=30)
        assert value == 1
        assert root_id == expected_root_id
        assert parent_id is None

    def test_chord_on_error(self, manager):
        from celery import states

        from .tasks import ExpectedException

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        # Run the chord and wait for the error callback to finish. Note that
        # this only works for old style callbacks since they get dispatched to
        # run async while new style errbacks are called synchronously so that
        # they can be passed the request object for the failing task.
        c1 = chord(
            header=[add.s(1, 2), add.s(3, 4), fail.s()],
            body=print_unicode.s('This should not be called').on_error(
                errback_old_style.s()),
        )
        res = c1()
        with pytest.raises(ExpectedException):
            res.get(propagate=True)

        # Got to wait for children to populate.
        check = (
            lambda: res.children,
            lambda: res.children[0].children,
            lambda: res.children[0].children[0].result,
        )
        start = monotonic()
        while not all(f() for f in check):
            if monotonic() > start + TIMEOUT:
                raise TimeoutError("Timed out waiting for children")
            sleep(0.1)

        # Extract the results of the successful tasks from the chord.
        #
        # We could do this inside the error handler, and probably would in a
        #  real system, but for the purposes of the test it's obnoxious to get
        #  data out of the error handler.
        #
        # So for clarity of our test, we instead do it here.

        # Use the error callback's result to find the failed task.
        uuid_patt = re.compile(
            r"[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}"
        )
        callback_chord_exc = AsyncResult(
            res.children[0].children[0].result
        ).result
        failed_task_id = uuid_patt.search(str(callback_chord_exc))
        assert (failed_task_id is not None), "No task ID in %r" % callback_chord_exc
        failed_task_id = failed_task_id.group()

        # Use new group_id result metadata to get group ID.
        failed_task_result = AsyncResult(failed_task_id)
        original_group_id = failed_task_result._get_task_meta()['group_id']

        # Use group ID to get preserved group result.
        backend = fail.app.backend
        j_key = backend.get_key_for_group(original_group_id, '.j')
        redis_connection = get_redis_connection()
        # The redis key is either a list or zset depending on configuration
        if manager.app.conf.result_backend_transport_options.get(
            'result_chord_ordered', True
        ):
            job_results = redis_connection.zrange(j_key, 0, 3)
        else:
            job_results = redis_connection.lrange(j_key, 0, 3)
        chord_results = [backend.decode(t) for t in job_results]

        # Validate group result
        assert [cr[3] for cr in chord_results if cr[2] == states.SUCCESS] == \
               [3, 7]

        assert len([cr for cr in chord_results if cr[2] != states.SUCCESS]
                   ) == 1

    @flaky
    @pytest.mark.parametrize('size', [3, 4, 5, 6, 7, 8, 9])
    def test_generator(self, manager, size):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        def assert_generator(file_name):
            for i in range(size):
                sleep(1)
                if i == size - 1:
                    with open(file_name) as file_handle:
                        # ensures chord header generators tasks are processed incrementally #3021
                        assert file_handle.readline() == '0\n', "Chord header was unrolled too early"

                yield write_to_file_and_return_int.s(file_name, i)

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            file_name = tmp_file.name
            c = chord(assert_generator(file_name), tsum.s())
            assert c().get(timeout=TIMEOUT) == size * (size - 1) // 2

    @flaky
    def test_parallel_chords(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord(group(add.s(1, 2), add.s(3, 4)), tsum.s())
        c2 = chord(group(add.s(1, 2), add.s(3, 4)), tsum.s())
        g = group(c1, c2)
        r = g.delay()

        assert r.get(timeout=TIMEOUT) == [10, 10]

    @flaky
    def test_chord_in_chords_with_chains(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = chord(
            group([
                chain(
                    add.si(1, 2),
                    chord(
                        group([add.si(1, 2), add.si(1, 2)]),
                        add.si(1, 2),
                    ),
                ),
                chain(
                    add.si(1, 2),
                    chord(
                        group([add.si(1, 2), add.si(1, 2)]),
                        add.si(1, 2),
                    ),
                ),
            ]),
            add.si(2, 2)
        )

        r = c.delay()

        assert r.get(timeout=TIMEOUT) == 4

    @flaky
    def test_chain_chord_chain_chord(self, manager):
        # test for #2573
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = chain(
            identity.si(1),
            chord(
                [
                    identity.si(2),
                    chain(
                        identity.si(3),
                        chord(
                            [identity.si(4), identity.si(5)],
                            identity.si(6)
                        )
                    )
                ],
                identity.si(7)
            )
        )
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 7

    @pytest.mark.xfail(reason="Issue #6176")
    def test_chord_in_chain_with_args(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chain(
            chord(
                [identity.s(), identity.s()],
                identity.s(),
            ),
            identity.s(),
        )
        res1 = c1.apply_async(args=(1,))
        assert res1.get(timeout=TIMEOUT) == [1, 1]
        res1 = c1.apply(args=(1,))
        assert res1.get(timeout=TIMEOUT) == [1, 1]

    @pytest.mark.xfail(reason="Issue #6200")
    def test_chain_in_chain_with_args(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chain(  # NOTE: This chain should have only 1 chain inside it
            chain(
                identity.s(),
                identity.s(),
            ),
        )

        res1 = c1.apply_async(args=(1,))
        assert res1.get(timeout=TIMEOUT) == 1
        res1 = c1.apply(args=(1,))
        assert res1.get(timeout=TIMEOUT) == 1

    @flaky
    def test_large_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(identity.si(i) for i in range(1000)) | tsum.s()
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 499500

    @flaky
    def test_chain_to_a_chord_with_large_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = identity.si(1) | group(
            identity.s() for _ in range(1000)) | tsum.s()
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 1000

    @flaky
    def test_priority(self, manager):
        c = chain(return_priority.signature(priority=3))()
        assert c.get(timeout=TIMEOUT) == "Priority: 3"

    @flaky
    def test_priority_chain(self, manager):
        c = return_priority.signature(priority=3) | return_priority.signature(
            priority=5)
        assert c().get(timeout=TIMEOUT) == "Priority: 5"

    def test_nested_chord_group(self, manager):
        """
        Confirm that groups nested inside chords get unrolled.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chord(
            (
                group(identity.s(42), identity.s(42)),  # [42, 42]
            ),
            identity.s()  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_chord_group_chain_group_tail(self, manager):
        """
        Sanity check that a deeply nested group is completed as expected.

        Groups at the end of chains nested in chords have had issues and this
        simple test sanity check that such a task structure can be completed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chord(
            group(
                chain(
                    identity.s(42),  # 42
                    group(
                        identity.s(),  # 42
                        identity.s(),  # 42
                    ),  # [42, 42]
                ),  # [42, 42]
            ),  # [[42, 42]] since the chain prevents unrolling
            identity.s(),  # [[42, 42]]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [[42, 42]]

    @pytest.mark.xfail(TEST_BACKEND.startswith('redis://'), reason="Issue #6437")
    def test_error_propagates_from_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = add.s(1, 1) | fail.s() | group(add.s(1), add.s(1))
        res = sig.delay()

        with pytest.raises(ExpectedException):
            res.get(timeout=TIMEOUT)

    def test_error_propagates_from_chord2(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = add.s(1, 1) | add.s(1) | group(add.s(1), fail.s())
        res = sig.delay()

        with pytest.raises(ExpectedException):
            res.get(timeout=TIMEOUT)

    def test_error_propagates_to_chord_from_simple(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_simple(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple header task fails"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple body task fails"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_simple(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple header task fails"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple body task fails"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_chain(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = chain(identity.si(42), fail.s(), identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_chain(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = chain(identity.si(42), fail.s(), identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails before the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails before the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_chain(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = chain(identity.si(42), fail_sig, identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails before the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails before the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_chain_tail(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = chain(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(
            msg="Error propagates from body chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_chain_tail(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = chain(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails at the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_chain_tail(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = chain(identity.si(42), fail_sig)

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_group(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = group(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_group(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = group(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_group(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = group(identity.si(42), fail_sig)

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_immutable_errback_called_by_chord_from_group_fail_multiple(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(24)),  # arbitrary task count
            *(fail.s() for _ in range(fail_task_count)),
        )

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from header group"):
            redis_connection.delete(redis_key)
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            # NOTE: Here we only expect the errback to be called once since it
            # is attached to the chord body which is a single task!
            await_redis_count(1, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            # NOTE: Here we expect the errback to be called once per failing
            # task in the chord body since it is a group
            await_redis_count(fail_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize("errback_task", [errback_old_style, errback_new_style])
    def test_mutable_errback_called_by_chord_from_group_fail_multiple_on_header_failure(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        # We have to use failing task signatures with unique task IDs to ensure
        # the chord can complete when they are used as part of its header!
        fail_sigs = tuple(
            fail.s() for _ in range(fail_task_count)
        )
        errback = errback_task.s()
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(8)),  # arbitrary task count
            *fail_sigs,
        )

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            sleep(1)
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            # NOTE: Here we only expect the errback to be called once since it
            # is attached to the chord body which is a single task!
            await_redis_count(1, redis_key=expected_redis_key)

    @pytest.mark.parametrize("errback_task", [errback_old_style, errback_new_style])
    def test_mutable_errback_called_by_chord_from_group_fail_multiple_on_body_failure(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        # We have to use failing task signatures with unique task IDs to ensure
        # the chord can complete when they are used as part of its header!
        fail_sigs = tuple(
            fail.s() for _ in range(fail_task_count)
        )
        fail_sig_ids = tuple(s.freeze().id for s in fail_sigs)
        errback = errback_task.s()
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(8)),  # arbitrary task count
            *fail_sigs,
        )

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        for fail_sig_id in fail_sig_ids:
            redis_connection.delete(fail_sig_id)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            sleep(1)
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            # NOTE: Here we expect the errback to be called once per failing
            # task in the chord body since it is a group, and each task has a
            # unique task ID
            for i, fail_sig_id in enumerate(fail_sig_ids):
                await_redis_count(
                    1, redis_key=fail_sig_id,
                    # After the first one is seen, check the rest with no
                    # timeout since waiting to confirm that each one doesn't
                    # get over-incremented will take a long time
                    timeout=TIMEOUT if i == 0 else 0,
                )
        for fail_sig_id in fail_sig_ids:
            redis_connection.delete(fail_sig_id)

    def test_chord_header_task_replaced_with_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            replace_with_chain.si(42),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_header_child_replaced_with_chain_first(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (replace_with_chain.si(42), identity.s(1337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    def test_chord_header_child_replaced_with_chain_middle(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (identity.s(42), replace_with_chain.s(1337), identity.s(31337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337, 31337]

    def test_chord_header_child_replaced_with_chain_last(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (identity.s(42), replace_with_chain.s(1337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    def test_chord_body_task_replaced_with_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            replace_with_chain.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_first(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(replace_with_chain.s(), identity.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_middle(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(identity.s(), replace_with_chain.s(), identity.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_last(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(identity.s(), replace_with_chain.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_enabling_flag_allow_error_cb_on_chord_header(self, manager, subtests):
        """
        Test that the flag allow_error_callback_on_chord_header works as
        expected. To confirm this, we create a chord with a failing header
        task, and check that the body does not execute when the header task fails.
        This allows preventing the body from executing when the chord header fails
        when the flag is turned on. In addition, we make sure the body error callback
        is also executed when the header fails and the flag is turned on.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = True

        header_errback_msg = 'header errback called'
        header_errback_key = 'echo_header_errback'
        header_errback_sig = redis_echo.si(header_errback_msg, redis_key=header_errback_key)

        body_errback_msg = 'body errback called'
        body_errback_key = 'echo_body_errback'
        body_errback_sig = redis_echo.si(body_errback_msg, redis_key=body_errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            (fail.si(),),
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            chord_sig = chord(header, body_sig)
            # link error to chord header ONLY
            [header_task.link_error(header_errback_sig) for header_task in chord_sig.tasks]
            # link error to chord body ONLY
            chord_sig.body.link_error(body_errback_sig)
            redis_connection.delete(header_errback_key, body_errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm the errback was called for each failed header task + body'):
                # confirm the errback was called for each task in the chord header
                failed_header_tasks_count = len(list(filter(lambda f_sig: f_sig == fail.si(), header)))
                expected_header_errbacks = tuple(header_errback_msg for _ in range(failed_header_tasks_count))
                await_redis_echo(expected_header_errbacks, redis_key=header_errback_key)

                # confirm the errback was called for the chord body
                await_redis_echo((body_errback_msg,), redis_key=body_errback_key)

            redis_connection.delete(header_errback_key, body_errback_key)

    def test_disabling_flag_allow_error_cb_on_chord_header(self, manager, subtests):
        """
        Confirm that when allow_error_callback_on_chord_header is disabled, the default
        behavior is kept.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = False

        errback_msg = 'errback called'
        errback_key = 'echo_errback'
        errback_sig = redis_echo.si(errback_msg, redis_key=errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            (fail.si(),),
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            chord_sig = chord(header, body_sig)
            chord_sig.link_error(errback_sig)
            redis_connection.delete(errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm only one errback was called'):
                await_redis_echo((errback_msg,), redis_key=errback_key, timeout=10)
                with pytest.raises(TimeoutError):
                    await_redis_echo((errback_msg,), redis_key=errback_key, timeout=10)

            # Cleanup
            redis_connection.delete(errback_key)

    def test_flag_allow_error_cb_on_chord_header_on_upgraded_chord(self, manager, subtests):
        """
        Confirm that allow_error_callback_on_chord_header flag supports upgraded chords
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = True

        errback_msg = 'errback called'
        errback_key = 'echo_errback'
        errback_sig = redis_echo.si(errback_msg, redis_key=errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            # (fail.si(),),  <-- this is not supported because it's not a valid chord header (only one task)
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            implicit_chord_sig = chain(group(list(header)), body_sig)
            implicit_chord_sig.link_error(errback_sig)
            redis_connection.delete(errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = implicit_chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm the errback was called for each failed header task + body'):
                # confirm the errback was called for each task in the chord header
                failed_header_tasks_count = len(list(filter(lambda f_sig: f_sig.name == fail.si().name, header)))
                expected_errbacks_count = failed_header_tasks_count + 1  # +1 for the body
                expected_errbacks = tuple(errback_msg for _ in range(expected_errbacks_count))
                await_redis_echo(expected_errbacks, redis_key=errback_key)

                # confirm there are not leftovers
                assert not redis_connection.exists(errback_key)

            # Cleanup
            redis_connection.delete(errback_key)
