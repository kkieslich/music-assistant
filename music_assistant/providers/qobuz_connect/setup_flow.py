"""Setup flow entry definitions for the Qobuz Connect plugin."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from music_assistant_models.config_entries import ConfigEntry, ConfigValueOption
from music_assistant_models.enums import ConfigEntryType

from music_assistant.models.setup_flow import SetupFlowError

from . import (
    CONF_HTTP_PORT,
    CONF_INITIAL_VOLUME,
    CONF_PUBLISH_NAME,
    CONF_QOBUZ_PROVIDER,
    CONF_TARGET_PLAYER,
    DEFAULT_INITIAL_VOLUME,
    PLAYER_ID_AUTO,
)

if TYPE_CHECKING:
    from music_assistant_models.config_entries import ConfigValueType, ProviderConfig

    from music_assistant.mass import MusicAssistant
    from music_assistant.models.setup_flow import SetupSession

_SETUP_KEYS = (
    CONF_QOBUZ_PROVIDER,
    CONF_TARGET_PLAYER,
    CONF_PUBLISH_NAME,
    CONF_HTTP_PORT,
    CONF_INITIAL_VOLUME,
)


async def run_setup(session: SetupSession) -> None:
    """
    Configure a Qobuz Connect receiver instance.

    :param session: The setup session driving the flow.
    """
    setup_data = {**session.context.values, **session.context.setup_data}
    errors: dict[str, str] | None = None
    while True:
        entries = await build_setup_entries(
            session.mass,
            instance_id=session.context.instance_id,
            values=setup_data,
        )
        submitted = await session.form(
            list(entries),
            step_id="user",
            errors=errors,
            last_step=True,
        )
        setup_data.update(submitted)
        try:
            await session.finish({key: setup_data[key] for key in _SETUP_KEYS})
            return
        except SetupFlowError as err:
            errors = {"base": err.translation_key or str(err)}


async def build_setup_entries(
    mass: MusicAssistant,
    instance_id: str | None = None,
    values: dict[str, ConfigValueType] | None = None,
) -> tuple[ConfigEntry, ...]:
    """Build the setup-flow fields for a Qobuz Connect receiver."""
    prefill = values or {}
    qobuz_configs = await mass.config.get_provider_configs(provider_domain="qobuz")
    default_qobuz_provider = qobuz_configs[0].instance_id if qobuz_configs else None
    qobuz_provider = prefill.get(CONF_QOBUZ_PROVIDER, default_qobuz_provider)
    target_player = prefill.get(CONF_TARGET_PLAYER, PLAYER_ID_AUTO)
    publish_name = prefill.get(CONF_PUBLISH_NAME, "Music Assistant")
    http_port = prefill.get(CONF_HTTP_PORT, await _suggest_http_port(mass, instance_id))
    initial_volume = prefill.get(CONF_INITIAL_VOLUME, DEFAULT_INITIAL_VOLUME)
    return (
        ConfigEntry(
            key=CONF_QOBUZ_PROVIDER,
            type=ConfigEntryType.STRING,
            default_value=qobuz_provider,
            value=qobuz_provider,
            required=True,
            options=[
                ConfigValueOption(config.instance_id, title=config.name) for config in qobuz_configs
            ],
        ),
        ConfigEntry(
            key=CONF_TARGET_PLAYER,
            type=ConfigEntryType.STRING,
            default_value=target_player,
            value=target_player,
            required=True,
            options=[
                ConfigValueOption(PLAYER_ID_AUTO),
                *(
                    ConfigValueOption(player.player_id, title=player.display_name)
                    for player in sorted(
                        mass.players.all_players(False, False),
                        key=lambda player: player.display_name,
                    )
                ),
            ],
        ),
        ConfigEntry(
            key=CONF_PUBLISH_NAME,
            type=ConfigEntryType.STRING,
            default_value=publish_name,
            value=publish_name,
            required=True,
        ),
        ConfigEntry(
            key=CONF_HTTP_PORT,
            type=ConfigEntryType.INTEGER,
            default_value=http_port,
            value=http_port,
            required=True,
        ),
        ConfigEntry(
            key=CONF_INITIAL_VOLUME,
            type=ConfigEntryType.INTEGER,
            default_value=initial_volume,
            value=initial_volume,
            required=True,
        ),
    )


async def _suggest_http_port(mass: MusicAssistant, instance_id: str | None) -> int:
    """Return the first default HTTP port not used by another Connect instance."""
    configs = await mass.config.get_provider_configs(provider_domain="qobuz_connect")
    used_ports = {
        int(cast("int | str", port))
        for config in configs
        if config.instance_id != instance_id
        and (port := _get_setup_or_legacy_value(mass, config, CONF_HTTP_PORT)) is not None
    }
    port = 8695
    while port in used_ports:
        port += 1
    return port


def _get_setup_or_legacy_value(
    mass: MusicAssistant, config: ProviderConfig, key: str
) -> ConfigValueType:
    """Return setup data first, then a legacy provider option value."""
    setup_data = getattr(config, "setup_data", {}) or {}
    if (setup_value := setup_data.get(key)) is not None:
        return cast("ConfigValueType", setup_value)
    return mass.config.get_raw_provider_config_value(config.instance_id, key)
