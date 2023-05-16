import pytest

from celery import chain
from t.integration import tasks
from t.integration.canvas.foundation import TIMEOUT


class test_signature_serialization:
    """
    Confirm nested signatures can be rebuilt after passing through a backend.

    These tests are expected to finish and return `None` or raise an exception
    in the error case. The exception indicates that some element of a nested
    signature object was not properly deserialized from its dictionary
    representation, and would explode later on if it were used as a signature.
    """

    def test_rebuild_nested_chain_chain(self, manager):
        sig = chain(
            tasks.return_nested_signature_chain_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chain_group(self, manager):
        sig = chain(
            tasks.return_nested_signature_chain_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chain_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chain_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_chain(self, manager):
        sig = chain(
            tasks.return_nested_signature_group_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_group(self, manager):
        sig = chain(
            tasks.return_nested_signature_group_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_group_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)
