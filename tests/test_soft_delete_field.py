"""@auto_publish maps a soft delete (saving soft_delete_field non-null) to a deleted event."""

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from django.db import connection, models
from django.utils import timezone

from celery_salt.django.decorators import auto_publish

client = MagicMock()


@auto_publish(client=client, soft_delete_field="deleted_at")
class SoftDeleteWidget(models.Model):
    name = models.CharField(max_length=50)
    deleted_at = models.DateTimeField(null=True, default=None)

    class Meta:
        app_label = "celery_salt_django"


MODELS = (SoftDeleteWidget,)


@pytest.fixture
def widget_tables(transactional_db: None) -> Iterator[None]:
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.create_model(model)
    client.reset_mock()
    yield
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.delete_model(model)


def published_topics(client: MagicMock) -> list[str]:
    return [call.args[0] for call in client.publish.call_args_list]


@pytest.mark.usefixtures("widget_tables")
class TestSoftDeleteField:
    def test_setting_field_non_null_publishes_deleted_not_updated(self) -> None:
        widget = SoftDeleteWidget.objects.create(name="a")
        client.reset_mock()

        widget.deleted_at = timezone.now()
        widget.save()

        assert published_topics(client) == [
            "celery_salt_django.softdeletewidget.deleted"
        ]

    def test_ordinary_update_still_publishes_updated(self) -> None:
        widget = SoftDeleteWidget.objects.create(name="a")
        client.reset_mock()

        widget.name = "b"
        widget.save()

        assert published_topics(client) == [
            "celery_salt_django.softdeletewidget.updated"
        ]

    def test_resaving_already_soft_deleted_row_keeps_publishing_deleted(self) -> None:
        widget = SoftDeleteWidget.objects.create(name="a")
        widget.deleted_at = timezone.now()
        widget.save()
        client.reset_mock()

        widget.name = "b"
        widget.save()

        assert published_topics(client) == [
            "celery_salt_django.softdeletewidget.deleted"
        ]

    def test_create_with_field_already_set_still_publishes_created(self) -> None:
        SoftDeleteWidget.objects.create(name="a", deleted_at=timezone.now())

        assert published_topics(client) == [
            "celery_salt_django.softdeletewidget.created"
        ]


class TestSoftDeleteFieldValidation:
    def test_rejects_field_that_does_not_exist_on_model(self) -> None:
        with pytest.raises(ValueError, match="soft_delete_field"):

            @auto_publish(client=MagicMock(), soft_delete_field="not_a_real_field")
            class BadWidget(models.Model):
                name = models.CharField(max_length=50)

                class Meta:
                    app_label = "celery_salt_django"
