import uuid
from datetime import datetime, timedelta

import pytest

from celery import chain, group, signature, chord
from celery.exceptions import TimeoutError, ImproperlyConfigured
from celery.signals import before_task_publish
from t.integration.conftest import get_redis_connection
from t.integration.tasks import add, add_replaced, fail, redis_echo, identity, second_order_replace1, ids, \
    delayed_sum_with_soft_guard, delayed_sum, tsum, print_unicode, build_chain_inside_task, replace_with_chain, \
    replace_with_chain_which_raises, replace_with_empty_chain, redis_count, ExpectedException, fail_replaced, \
    redis_echo_group_id
from t.integration.canvas.foundation import flaky, TIMEOUT, assert_ping, await_redis_echo, await_redis_count, \
    await_redis_list_message_length, compare_group_ids_in_redis


class test_chain:

    @flaky
    def test_simple_chain(self, manager):
        c = add.s(4, 4) | add.s(8) | add.s(16)
        assert c().get(timeout=TIMEOUT) == 32

    @flaky
    def test_single_chain(self, manager):
        c = chain(add.s(3, 4))()
        assert c.get(timeout=TIMEOUT) == 7

    @flaky
    def test_complex_chain(self, manager):
        g = group(add.s(i) for i in range(4))
        c = (
            add.s(2, 2) | (
                add.s(4) | add_replaced.s(8) | add.s(16) | add.s(32)
            ) | g
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [64, 65, 66, 67]

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout")
    def test_group_results_in_chain(self, manager):
        # This adds in an explicit test for the special case added in commit
        # 1e3fcaa969de6ad32b52a3ed8e74281e5e5360e6
        c = (
            group(
                add.s(1, 2) | group(
                    add.s(1), add.s(2)
                )
            )
        )
        res = c()
        assert res.get(timeout=TIMEOUT / 10) == [4, 5]

    def test_chain_of_chain_with_a_single_task(self, manager):
        sig = signature('any_taskname', queue='any_q')
        chain([chain(sig)]).apply_async()

    def test_chain_on_error(self, manager):
        from ..tasks import ExpectedException

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        # Run the chord and wait for the error callback to finish.
        c1 = chain(
            add.s(1, 2), fail.s(), add.s(3, 4),
        )
        res = c1()

        with pytest.raises(ExpectedException):
            res.get(propagate=True)

        with pytest.raises(ExpectedException):
            res.parent.get(propagate=True)

    @flaky
    def test_chain_inside_group_receives_arguments(self, manager):
        c = (
            add.s(5, 6) |
            group((add.s(1) | add.s(2), add.s(3)))
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [14, 14]

    @flaky
    def test_eager_chain_inside_task(self, manager):
        from ..tasks import chain_add

        prev = chain_add.app.conf.task_always_eager
        chain_add.app.conf.task_always_eager = True

        chain_add.apply_async(args=(4, 8), throw=True).get()

        chain_add.app.conf.task_always_eager = prev

    @flaky
    def test_group_chord_group_chain(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')
        before = group(redis_echo.si(f'before {i}') for i in range(3))
        connect = redis_echo.si('connect')
        after = group(redis_echo.si(f'after {i}') for i in range(2))

        result = (before | connect | after).delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(redis_connection.lrange('redis-echo', 0, -1))
        before_items = {b'before 0', b'before 1', b'before 2'}
        after_items = {b'after 0', b'after 1'}

        assert set(redis_messages[:3]) == before_items
        assert redis_messages[3] == b'connect'
        assert set(redis_messages[4:]) == after_items
        redis_connection.delete('redis-echo')

    @flaky
    def test_group_result_not_has_cache(self, manager):
        t1 = identity.si(1)
        t2 = identity.si(2)
        gt = group([identity.si(3), identity.si(4)])
        ct = chain(identity.si(5), gt)
        task = group(t1, t2, ct)
        result = task.delay()
        assert result.get(timeout=TIMEOUT) == [1, 2, [3, 4]]

    @flaky
    def test_second_order_replace(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        result = second_order_replace1.delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(redis_connection.lrange('redis-echo', 0, -1))

        expected_messages = [b'In A', b'In B', b'In/Out C', b'Out B',
                             b'Out A']
        assert redis_messages == expected_messages

    @flaky
    def test_parent_ids(self, manager, num=10):
        assert_ping(manager)

        c = chain(ids.si(i=i) for i in range(num))
        c.freeze()
        res = c()
        try:
            res.get(timeout=TIMEOUT)
        except TimeoutError:
            print(manager.inspect().active())
            print(manager.inspect().reserved())
            print(manager.inspect().stats())
            raise
        self.assert_ids(res, num - 1)

    def assert_ids(self, res, size):
        i, root = size, res
        while root.parent:
            root = root.parent
        node = res
        while node:
            root_id, parent_id, value = node.get(timeout=30)
            assert value == i
            if node.parent:
                assert parent_id == node.parent.id
            assert root_id == root.id
            node = node.parent
            i -= 1

    def test_chord_soft_timeout_recuperation(self, manager):
        """Test that if soft timeout happens in task but is managed by task,
        chord still get results normally
        """
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = chord([
            # return 3
            add.s(1, 2),
            # return 0 after managing soft timeout
            delayed_sum_with_soft_guard.s(
                [100], pause_time=2
            ).set(
                soft_time_limit=1
            ),
        ])
        result = c(delayed_sum.s(pause_time=0)).get()
        assert result == 3

    def test_chain_error_handler_with_eta(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        eta = datetime.utcnow() + timedelta(seconds=10)
        c = chain(
            group(
                add.s(1, 2),
                add.s(3, 4),
            ),
            tsum.s()
        ).on_error(print_unicode.s()).apply_async(eta=eta)

        result = c.get()
        assert result == 10

    @flaky
    def test_groupresult_serialization(self, manager):
        """Test GroupResult is correctly serialized
        to save in the result backend"""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        async_result = build_chain_inside_task.delay()
        result = async_result.get()
        assert len(result) == 2
        assert isinstance(result[0][1], list)

    @flaky
    def test_chain_of_task_a_group_and_a_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | group(tsum.s(), tsum.s())
        c = c | tsum.s()

        res = c()
        assert res.get(timeout=TIMEOUT) == 8

    @flaky
    def test_chain_of_chords_as_groups_chained_to_a_task_with_two_tasks(self,
                                                                        manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()

        res = c()
        assert res.get(timeout=TIMEOUT) == 12

    @flaky
    def test_chain_of_chords_with_two_tasks(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | chord(group(add.s(1), add.s(1)), tsum.s())

        res = c()
        assert res.get(timeout=TIMEOUT) == 12

    @flaky
    def test_chain_of_a_chord_and_a_group_with_two_tasks(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [6, 6]

    @flaky
    def test_chain_of_a_chord_and_a_task_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [6, 6]

    @flaky
    def test_chain_of_a_chord_and_two_tasks_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [7, 7]

    @flaky
    def test_chain_of_a_chord_and_three_tasks_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [8, 8]

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout")
    def test_nested_chain_group_lone(self, manager):
        """
        Test that a lone group in a chain completes.
        """
        sig = chain(
            group(identity.s(42), identity.s(42)),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT / 10) == [42, 42]

    def test_nested_chain_group_mid(self, manager):
        """
        Test that a mid-point group in a chain completes.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            identity.s(42),  # 42
            group(identity.s(), identity.s()),  # [42, 42]
            identity.s(),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_chain_group_last(self, manager):
        """
        Test that a final group in a chain with preceding tasks completes.
        """
        sig = chain(
            identity.s(42),  # 42
            group(identity.s(), identity.s()),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_chain_replaced_with_a_chain_and_a_callback(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain callback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will pass its args though
            replace_with_chain.s(link_msg=link_msg),
            add.s('world'),
        )
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == 'Hello world'
        await_redis_echo({link_msg, })

    def test_chain_replaced_with_a_chain_and_an_error_callback(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain errback'
        c = chain(
            identity.s('Hello '),
            replace_with_chain_which_raises.s(link_msg=link_msg),
            add.s(' will never be seen :(')
        )
        res = c.delay()

        with pytest.raises(ValueError):
            res.get(timeout=TIMEOUT)
        await_redis_echo({link_msg, })

    def test_chain_with_cb_replaced_with_chain_with_cb(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain callback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will pass its args though
            replace_with_chain.s(link_msg=link_msg),
            add.s('world'),
        )
        c.link(redis_echo.s())
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == 'Hello world'
        await_redis_echo({link_msg, 'Hello world'})

    def test_chain_flattening_keep_links_of_inner_chain(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()

        link_b_msg = 'link_b called'
        link_b_key = 'echo_link_b'
        link_b_sig = redis_echo.si(link_b_msg, redis_key=link_b_key)

        def link_chain(sig):
            sig.link(link_b_sig)
            sig.link_error(identity.s('link_ab'))
            return sig

        inner_chain = link_chain(chain(identity.s('a'), add.s('b')))
        flat_chain = chain(inner_chain, add.s('c'))
        redis_connection.delete(link_b_key)
        res = flat_chain.delay()

        assert res.get(timeout=TIMEOUT) == 'abc'
        await_redis_echo((link_b_msg,), redis_key=link_b_key)

    def test_chain_with_eb_replaced_with_chain_with_eb(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        inner_link_msg = 'Internal chain errback'
        outer_link_msg = 'External chain errback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will die and break the encapsulating chain
            replace_with_chain_which_raises.s(link_msg=inner_link_msg),
            add.s('world'),
        )
        c.link_error(redis_echo.si(outer_link_msg))
        res = c.delay()

        with subtests.test(msg="Chain fails due to a child task dying"):
            with pytest.raises(ValueError):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain and child task callbacks are called"):
            await_redis_echo({inner_link_msg, outer_link_msg})

    def test_replace_chain_with_empty_chain(self, manager):
        r = chain(identity.s(1), replace_with_empty_chain.s()).delay()

        with pytest.raises(ImproperlyConfigured,
                           match="Cannot replace with an empty chain"):
            r.get(timeout=TIMEOUT)

    def test_chain_children_with_callbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = identity.si(1337)
        child_sig.link(callback)
        chain_sig = chain(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 1337
        with subtests.test(msg="Chain child task callbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_children_with_errbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = fail.si()
        child_sig.link_error(errback)
        chain_sig = chain(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain fails due to a child task dying"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain child task errbacks are called"):
            # Only the first child task gets a change to run and fail
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_with_callback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        chain_sig = chain(add_replaced.si(42, 1337), identity.s())
        chain_sig.link(callback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 42 + 1337
        with subtests.test(msg="Callback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_with_errback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        chain_sig = chain(add_replaced.si(42, 1337), fail.s())
        chain_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_child_with_callback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_sig = add_replaced.si(42, 1337)
        child_sig.link(callback)
        chain_sig = chain(child_sig, identity.s())

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 42 + 1337
        with subtests.test(msg="Callback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_child_with_errback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_sig = fail_replaced.si()
        child_sig.link_error(errback)
        chain_sig = chain(child_sig, identity.si(42))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_task_replaced_with_chain(self, manager):
        orig_sig = replace_with_chain.si(42)
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    def test_chain_child_replaced_with_chain_first(self, manager):
        orig_sig = chain(replace_with_chain.si(42), identity.s())
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    def test_chain_child_replaced_with_chain_middle(self, manager):
        orig_sig = chain(
            identity.s(42), replace_with_chain.s(), identity.s()
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_chain_child_replaced_with_chain_last(self, manager):
        orig_sig = chain(identity.s(42), replace_with_chain.s())
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    @pytest.mark.parametrize('redis_key', ['redis-group-ids'])
    def test_chord_header_id_duplicated_on_rabbitmq_msg_duplication(self, manager, subtests, celery_session_app,
                                                                    redis_key):
        """
        When a task that predates a chord in a chain was duplicated by Rabbitmq (for whatever reason),
        the chord header id was not duplicated. This caused the chord header to have a different id.
        This test ensures that the chord header's id preserves itself in face of such an edge case.
        To validate the correct behavior is implemented, we collect the original and duplicated chord header ids
        in redis, to ensure that they are the same.
        """

        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if manager.app.conf.broker_url.startswith('redis'):
            raise pytest.xfail('Redis broker does not duplicate the task (t1)')

        # Republish t1 to cause the chain to be executed twice
        @before_task_publish.connect
        def before_task_publish_handler(sender=None, body=None, exchange=None, routing_key=None, headers=None,
                                        properties=None,
                                        declare=None, retry_policy=None, **kwargs):
            """ We want to republish t1 to ensure that the chain is executed twice """

            metadata = {
                'body': body,
                'exchange': exchange,
                'routing_key': routing_key,
                'properties': properties,
                'headers': headers,
            }

            with celery_session_app.producer_pool.acquire(block=True) as producer:
                # Publish t1 to the message broker, just before it's going to be published which causes duplication
                return producer.publish(
                    metadata['body'],
                    exchange=metadata['exchange'],
                    routing_key=metadata['routing_key'],
                    retry=None,
                    retry_policy=retry_policy,
                    serializer='json',
                    delivery_mode=None,
                    headers=headers,
                    **kwargs
                )

        # Clean redis key
        redis_connection = get_redis_connection()
        if redis_connection.exists(redis_key):
            redis_connection.delete(redis_key)

        # Prepare tasks
        t1, t2, t3, t4 = identity.s(42), redis_echo_group_id.s(), identity.s(), identity.s()
        c = chain(t1, chord([t2, t3], t4))

        # Delay chain
        r1 = c.delay()
        r1.get(timeout=TIMEOUT)

        # Cleanup
        before_task_publish.disconnect(before_task_publish_handler)

        with subtests.test(msg='Compare group ids via redis list'):
            await_redis_list_message_length(2, redis_key=redis_key, timeout=15)
            compare_group_ids_in_redis(redis_key=redis_key)

        # Cleanup
        redis_connection = get_redis_connection()
        redis_connection.delete(redis_key)

    def test_chaining_upgraded_chords_pure_groups(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            # letting the chain upgrade the chord, reproduces the issue in _chord.__or__
            group(
                redis_echo.si('1', redis_key=redis_key),
                redis_echo.si('2', redis_key=redis_key),
                redis_echo.si('3', redis_key=redis_key),
            ),
            group(
                redis_echo.si('4', redis_key=redis_key),
                redis_echo.si('5', redis_key=redis_key),
                redis_echo.si('6', redis_key=redis_key),
            ),
            group(
                redis_echo.si('7', redis_key=redis_key),
            ),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')

    def test_chaining_upgraded_chords_starting_with_chord(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            # by manually upgrading the chord to a group, we can reproduce the issue in _chain.__or__
            chord(group([redis_echo.si('1', redis_key=redis_key),
                         redis_echo.si('2', redis_key=redis_key),
                         redis_echo.si('3', redis_key=redis_key)]),
                  group([redis_echo.si('4', redis_key=redis_key),
                         redis_echo.si('5', redis_key=redis_key),
                         redis_echo.si('6', redis_key=redis_key)])),
            group(
                redis_echo.si('7', redis_key=redis_key),
            ),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')

    def test_chaining_upgraded_chords_mixed_canvas(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            chord(group([redis_echo.si('1', redis_key=redis_key),
                         redis_echo.si('2', redis_key=redis_key),
                         redis_echo.si('3', redis_key=redis_key)]),
                  group([redis_echo.si('4', redis_key=redis_key),
                         redis_echo.si('5', redis_key=redis_key),
                         redis_echo.si('6', redis_key=redis_key)])),
            redis_echo.si('7', redis_key=redis_key),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')
