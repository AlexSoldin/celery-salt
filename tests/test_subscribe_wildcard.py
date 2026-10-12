"""@subscribe accepts wildcard topics ("#", "user.*") without requiring a schema."""

import pytest

from celery_salt.core.decorators import subscribe
from celery_salt.core.exceptions import SchemaRegistryUnavailableError
from celery_salt.core.registry import InMemorySchemaRegistry, set_schema_registry
from celery_salt.integrations import registry as handler_registry_module
from celery_salt.integrations.registry import HandlerRegistry, get_handler_registry


class TestSubscribeWildcard:
    def setup_method(self) -> None:
        set_schema_registry(InMemorySchemaRegistry())
        handler_registry_module._global_handler_registry = HandlerRegistry()

    def test_hash_wildcard_does_not_require_a_registered_schema(self) -> None:
        @subscribe("#")
        def catch_all_handler(data):
            return "processed"

        assert callable(catch_all_handler)

    def test_hash_wildcard_passes_through_arbitrary_payload(self) -> None:
        received = {}

        @subscribe("#")
        def payload_capturing_handler(data):
            received["payload"] = data
            return "processed"

        payload_capturing_handler.run({"anything": "goes", "count": 3})

        assert received["payload"].anything == "goes"
        assert received["payload"].count == 3

    def test_hash_wildcard_registers_as_pattern_handler_matching_any_topic(
        self,
    ) -> None:
        @subscribe("#")
        def pattern_registered_handler(data):
            return "processed"

        handlers = get_handler_registry().get_handlers("literally.any.topic.at.all")
        assert len(handlers) == 1
        assert handlers[0]["function"] is pattern_registered_handler

    def test_star_wildcard_also_skips_schema_validation(self) -> None:
        @subscribe("user.*")
        def star_handler(data):
            return "processed"

        assert callable(star_handler)
        handlers = get_handler_registry().get_handlers("user.signup")
        assert len(handlers) == 1

    def test_non_wildcard_topic_without_schema_still_raises(self) -> None:
        with pytest.raises(SchemaRegistryUnavailableError):

            @subscribe("no.schema.registered.for.this.topic")
            def handler(data):
                return "processed"
