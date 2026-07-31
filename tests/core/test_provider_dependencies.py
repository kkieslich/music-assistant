"""Tests for provider dependency handling at the loader boundary."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from music_assistant_models.config_entries import ProviderConfig
from music_assistant_models.enums import ProviderType
from music_assistant_models.errors import SetupFailedError
from music_assistant_models.provider import ProviderManifest

from music_assistant.constants import CONF_PROVIDERS
from music_assistant.mass import MusicAssistant

QOBUZ_INSTANCE = "qobuz--only"
CONNECT_INSTANCE = "qobuz_connect--legacy"


def _manifest(
    domain: str, provider_type: ProviderType, depends_on: str | None = None
) -> ProviderManifest:
    """Build a minimal provider manifest for loader tests."""
    return ProviderManifest(
        type=provider_type,
        domain=domain,
        name=domain,
        description=domain,
        codeowners=[],
        multi_instance=True,
        depends_on=depends_on,
    )


def _provider_config(domain: str, provider_type: ProviderType, instance_id: str) -> ProviderConfig:
    """Build an enabled provider configuration for loader tests."""
    return ProviderConfig(
        values={},
        type=provider_type,
        domain=domain,
        instance_id=instance_id,
        enabled=True,
    )


def _store_config(
    mass: MusicAssistant,
    domain: str,
    provider_type: ProviderType,
    instance_id: str,
    *,
    enabled: bool,
) -> None:
    """Store the raw fields used by dependency config discovery and error persistence."""
    mass.config.set(
        f"{CONF_PROVIDERS}/{instance_id}",
        {
            "type": provider_type.value,
            "domain": domain,
            "instance_id": instance_id,
            "enabled": enabled,
            "values": {},
        },
    )


@pytest.mark.parametrize("qobuz_enabled", [None, False], ids=["missing", "disabled"])
async def test_loader_records_actionable_error_without_enabled_dependency(
    mass_minimal: MusicAssistant, qobuz_enabled: bool | None
) -> None:
    """Missing or disabled Qobuz config produces an error and schedules a retry."""
    mass_minimal._provider_manifests.update(
        {
            "qobuz": _manifest("qobuz", ProviderType.MUSIC),
            "qobuz_connect": _manifest("qobuz_connect", ProviderType.PLUGIN, depends_on="qobuz"),
        }
    )
    _store_config(
        mass_minimal,
        "qobuz_connect",
        ProviderType.PLUGIN,
        CONNECT_INSTANCE,
        enabled=True,
    )
    if qobuz_enabled is not None:
        _store_config(
            mass_minimal,
            "qobuz",
            ProviderType.MUSIC,
            QOBUZ_INSTANCE,
            enabled=qobuz_enabled,
        )
    connect_config = _provider_config("qobuz_connect", ProviderType.PLUGIN, CONNECT_INSTANCE)

    with (
        patch.object(mass_minimal, "unload_provider", AsyncMock()),
        patch.object(
            mass_minimal.config,
            "get_provider_config",
            AsyncMock(return_value=connect_config),
        ),
    ):
        await mass_minimal.load_provider(CONNECT_INSTANCE, allow_retry=True)

    stored_error = mass_minimal.config.get(f"{CONF_PROVIDERS}/{CONNECT_INSTANCE}/last_error")
    assert stored_error is not None
    assert "reconfigure" in stored_error["message"].lower()
    timer_id = f"load_provider_{CONNECT_INSTANCE}"
    assert timer_id in mass_minimal._tracked_timers
    mass_minimal.cancel_timer(timer_id)


async def test_loader_waits_for_enabled_dependency_then_continues_loading(
    mass_minimal: MusicAssistant,
) -> None:
    """An enabled dependency still loading is deferred and can load normally on retry."""
    mass_minimal._provider_manifests.update(
        {
            "qobuz": _manifest("qobuz", ProviderType.MUSIC),
            "qobuz_connect": _manifest("qobuz_connect", ProviderType.PLUGIN, depends_on="qobuz"),
        }
    )
    _store_config(
        mass_minimal,
        "qobuz_connect",
        ProviderType.PLUGIN,
        CONNECT_INSTANCE,
        enabled=True,
    )
    _store_config(
        mass_minimal,
        "qobuz",
        ProviderType.MUSIC,
        QOBUZ_INSTANCE,
        enabled=True,
    )
    connect_config = _provider_config("qobuz_connect", ProviderType.PLUGIN, CONNECT_INSTANCE)

    with patch.object(mass_minimal, "unload_provider", AsyncMock()):
        await mass_minimal._load_provider(connect_config)

        dependency = AsyncMock()
        dependency.available = True
        dependency.domain = "qobuz"
        dependency.instance_id = QOBUZ_INSTANCE
        mass_minimal._providers[QOBUZ_INSTANCE] = dependency
        with (
            patch(
                "music_assistant.mass.load_provider_module",
                AsyncMock(side_effect=SetupFailedError("dependency gate passed")),
            ),
            pytest.raises(SetupFailedError, match="dependency gate passed"),
        ):
            await mass_minimal._load_provider(connect_config)
