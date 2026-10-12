"""@auto_publish records a dropped-publish metric for every swallowed failure."""

from collections.abc import Iterator
from unittest.mock import MagicMock

import pytest
from django.db import connection, models
from pydantic import BaseModel

from celery_salt.core.events import SaltEvent
from celery_salt.django.decorators import auto_publish
from celery_salt.metrics.collectors import get_metrics_collector

raw_client = MagicMock()


class MetricWidgetCreatedEvent(SaltEvent):
    class Schema(BaseModel):
        id: int
        name: str

    class Meta:
        topic = "tests.metric_widget.created"


def failing_payload_provider(widget, event_type):
    raise RuntimeError("provider boom")


@auto_publish(client=raw_client)
class RawMetricWidget(models.Model):
    name = models.CharField(max_length=50)

    class Meta:
        app_label = "celery_salt_django"


@auto_publish(
    event_classes={"created": MetricWidgetCreatedEvent},
    payload_provider=failing_payload_provider,
)
class EventMetricWidget(models.Model):
    name = models.CharField(max_length=50)

    class Meta:
        app_label = "celery_salt_django"


MODELS = (RawMetricWidget, EventMetricWidget)


@pytest.fixture
def widget_tables(transactional_db: None) -> Iterator[None]:
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.create_model(model)
    raw_client.reset_mock()
    get_metrics_collector().clear_metrics()
    yield
    with connection.schema_editor() as editor:
        for model in MODELS:
            editor.delete_model(model)


@pytest.mark.usefixtures("widget_tables")
class TestDroppedPublishMetrics:
    def test_broker_publish_failure_records_dropped_publish_metric(self) -> None:
        raw_client.publish.side_effect = ConnectionError("broker down")
        try:
            RawMetricWidget.objects.create(name="a")
        finally:
            raw_client.publish.side_effect = None

        errors = get_metrics_collector().get_recent_errors()
        assert len(errors) == 1
        assert errors[0]["topic"] == "celery_salt_django.rawmetricwidget.created"
        assert errors[0]["error_type"] == "dropped_publish"

    def test_payload_provider_failure_records_dropped_publish_metric(self) -> None:
        EventMetricWidget.objects.create(name="a")

        errors = get_metrics_collector().get_recent_errors()
        assert len(errors) == 1
        assert errors[0]["topic"] == "tests.metric_widget.created"
        assert errors[0]["error_type"] == "dropped_publish"

    def test_successful_publish_records_no_metric(self) -> None:
        RawMetricWidget.objects.create(name="a")

        assert get_metrics_collector().get_recent_errors() == []
