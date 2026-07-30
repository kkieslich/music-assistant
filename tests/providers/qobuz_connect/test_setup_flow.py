"""Tests for the Qobuz Connect setup flow."""

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock

from music_assistant.models.setup_flow import SetupFlowError
from music_assistant.providers.qobuz_connect import (
    CONF_HTTP_PORT,
    CONF_INITIAL_VOLUME,
    CONF_MAX_QUALITY,
    CONF_PUBLISH_NAME,
    CONF_QOBUZ_PROVIDER,
    CONF_TARGET_PLAYER,
)
from music_assistant.providers.qobuz_connect.setup_flow import _suggest_http_port, run_setup

if TYPE_CHECKING:
    from music_assistant.models.setup_flow import SetupSession


class FakeSetupSession:
    """Minimal setup session that records flow inputs and output."""

    def __init__(
        self,
        submitted: dict[str, Any],
        finish_error: SetupFlowError | None = None,
    ) -> None:
        """Initialize the fake session with form values and an optional first finish error."""
        self.submitted = submitted
        self.context = SimpleNamespace(instance_id=None, setup_data={}, values={})
        self.mass = MagicMock()
        self.mass.players.all_players.return_value = []

        async def get_provider_configs(*, provider_domain: str) -> list[Any]:
            assert provider_domain in {"qobuz", "qobuz_connect"}
            return []

        self.mass.config.get_provider_configs.side_effect = get_provider_configs
        self.finish_error = finish_error
        self.finished_values: dict[str, Any] | None = None
        self.form_calls: list[dict[str, Any]] = []

    async def form(self, entries: list[Any], **kwargs: Any) -> dict[str, Any]:
        """Record form state and return the literal submitted values."""
        self.form_calls.append({"entries": entries, **kwargs})
        return dict(self.submitted)

    async def finish(self, values: dict[str, Any]) -> None:
        """Record the submitted setup data after any configured first failure."""
        if self.finish_error is not None:
            error = self.finish_error
            self.finish_error = None
            raise error
        self.finished_values = dict(values)


async def test_setup_flow_finishes_with_submitted_setup_data() -> None:
    """The flow persists exactly the setup values submitted by the user."""
    session = FakeSetupSession(
        submitted={
            CONF_QOBUZ_PROVIDER: "qobuz--one",
            CONF_TARGET_PLAYER: "__auto__",
            CONF_PUBLISH_NAME: "Living room",
            CONF_HTTP_PORT: 8695,
            CONF_INITIAL_VOLUME: 25,
        }
    )

    await run_setup(cast("SetupSession", session))

    assert session.finished_values == session.submitted


async def test_setup_flow_retries_with_submitted_values_after_finish_error() -> None:
    """A rejected finish rerenders the form without discarding the user's values."""
    session = FakeSetupSession(
        submitted={
            CONF_QOBUZ_PROVIDER: "qobuz--one",
            CONF_TARGET_PLAYER: "__auto__",
            CONF_PUBLISH_NAME: "Living room",
            CONF_HTTP_PORT: 8695,
            CONF_INITIAL_VOLUME: 25,
        },
        finish_error=SetupFlowError("Port unavailable", translation_key="port_unavailable"),
    )

    await run_setup(cast("SetupSession", session))

    assert session.form_calls[1]["errors"] == {"base": "port_unavailable"}
    assert {
        entry.key: entry.value for entry in session.form_calls[1]["entries"]
    } == session.submitted


async def test_setup_flow_finish_excludes_runtime_provider_values() -> None:
    """Reconfigure prefills runtime options but persists only setup-owned fields."""
    session = FakeSetupSession(
        submitted={
            CONF_QOBUZ_PROVIDER: "qobuz--one",
            CONF_TARGET_PLAYER: "__auto__",
            CONF_PUBLISH_NAME: "Living room",
            CONF_HTTP_PORT: 8695,
            CONF_INITIAL_VOLUME: 25,
        }
    )
    session.context.values = {
        CONF_MAX_QUALITY: "7",
        "log_level": "debug",
    }

    await run_setup(cast("SetupSession", session))

    assert session.finished_values == session.submitted


async def test_suggest_http_port_prefers_current_setup_data() -> None:
    """Current-format sibling setup data reserves its configured port."""
    mass = MagicMock()
    sibling = SimpleNamespace(
        instance_id="qobuz_connect--one",
        setup_data={CONF_HTTP_PORT: 8695},
    )
    mass.config.get_provider_configs = AsyncMock(return_value=[sibling])
    mass.config.get_raw_provider_config_value.return_value = 8795

    port = await _suggest_http_port(mass, instance_id=None)

    assert port == 8696
    mass.config.get_raw_provider_config_value.assert_not_called()


async def test_suggest_http_port_reads_legacy_raw_provider_value() -> None:
    """Legacy sibling values are read from storage when list responses omit values."""
    mass = MagicMock()
    sibling = SimpleNamespace(
        instance_id="qobuz_connect--legacy",
        setup_data={},
    )
    mass.config.get_provider_configs = AsyncMock(return_value=[sibling])
    mass.config.get_raw_provider_config_value.return_value = 8695

    port = await _suggest_http_port(mass, instance_id=None)

    assert port == 8696
    mass.config.get_raw_provider_config_value.assert_called_once_with(
        "qobuz_connect--legacy",
        CONF_HTTP_PORT,
    )
