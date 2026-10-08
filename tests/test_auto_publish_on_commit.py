"""@auto_publish defers publishing until the surrounding transaction commits."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from django.db import connection, models, transaction
from pydantic import BaseModel

from celery_salt.core.events import SaltEvent
from celery_salt.django.decorators import auto_publish

deferred_client = MagicMock()
immediate_client = MagicMock()


class WidgetCreatedEvent(SaltEvent):
    class Schema(BaseModel):
        id: int
        name: str

    class Meta:
        topic = "tests.widget.created"


@auto_publish(client=deferred_client)
class DeferredWidget(models.Model):
    name = models.CharField(max_length=50)

    class Meta:
        app_label = "celery_salt_django"


@auto_publish(client=immediate_client, publish_on_commit=False)
class ImmediateWidget(models.Model):
    name = models.CharField(max_length=50)

    class Meta:
        app_label = "celery_salt_django"


@auto_publish(
    event_classes={"created": WidgetCreatedEvent},
    payload_provider=lambda widget, event_type: {"id": widget.pk, "name": widget.name},
)
class EventWidget(models.Model):
    name = models.CharField(max_length=50)

    class Meta:
        app_label = "celery_salt_django"


MODELS = (DeferredWidget, ImmediateWidget, EventWidget)


@pytest.fixture
def widget_tables(transactional_db: None) -> Iterator[None]:
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.create_model(model)
    deferred_client.reset_mock()
    immediate_client.reset_mock()
    yield
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.delete_model(model)


def published_topics(client: MagicMock) -> list[str]:
    return [call.args[0] for call in client.publish.call_args_list]


@pytest.mark.usefixtures("widget_tables")
class TestAutoPublishOnCommit:
    def test_publish_waits_for_commit(self) -> None:
        with transaction.atomic():
            DeferredWidget.objects.create(name="a")
            assert deferred_client.publish.call_count == 0

        assert published_topics(deferred_client) == [
            "celery_salt_django.deferredwidget.created"
        ]

    def test_rollback_publishes_nothing(self) -> None:
        with pytest.raises(RuntimeError), transaction.atomic():
            DeferredWidget.objects.create(name="a")
            raise RuntimeError("rollback")

        assert deferred_client.publish.call_count == 0

    def test_autocommit_publishes_immediately(self) -> None:
        widget = DeferredWidget.objects.create(name="a")
        widget.name = "b"
        widget.save()
        widget.delete()

        assert published_topics(deferred_client) == [
            "celery_salt_django.deferredwidget.created",
            "celery_salt_django.deferredwidget.updated",
            "celery_salt_django.deferredwidget.deleted",
        ]

    def test_payload_is_captured_at_save_time(self) -> None:
        with transaction.atomic():
            widget = DeferredWidget.objects.create(name="first")
            widget.name = "second"

        payload = deferred_client.publish.call_args.args[1]
        assert payload["name"] == "first"

    def test_deleted_payload_keeps_pk(self) -> None:
        widget = DeferredWidget.objects.create(name="a")
        widget_pk = widget.pk
        deferred_client.reset_mock()

        with transaction.atomic():
            widget.delete()

        payload = deferred_client.publish.call_args.args[1]
        assert payload["id"] == str(widget_pk)

    def test_publish_on_commit_false_publishes_inside_transaction(self) -> None:
        with transaction.atomic():
            ImmediateWidget.objects.create(name="a")
            assert immediate_client.publish.call_count == 1

    def test_publish_failure_does_not_break_commit(self) -> None:
        deferred_client.publish.side_effect = ConnectionError("broker down")
        try:
            with transaction.atomic():
                DeferredWidget.objects.create(name="a")
        finally:
            deferred_client.publish.side_effect = None

        assert DeferredWidget.objects.filter(name="a").exists()

    def test_event_class_publishes_after_commit(self) -> None:
        with patch.object(WidgetCreatedEvent, "publish") as publish:
            with transaction.atomic():
                widget = EventWidget.objects.create(name="a")
                assert publish.call_count == 0

            publish.assert_called_once_with()
        assert widget.pk is not None
