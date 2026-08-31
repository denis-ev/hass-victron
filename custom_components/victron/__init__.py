"""The victron integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er

from .const import CONF_HOST, CONF_INTERVAL, CONF_PORT, DOMAIN, SCAN_REGISTERS
from .coordinator import victronEnergyDeviceUpdateCoordinator as Coordinator

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.SWITCH,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.BINARY_SENSOR,
    Platform.BUTTON,
]


async def async_setup_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Set up victron from a config entry."""

    hass.data.setdefault(DOMAIN, {})

    coordinator = Coordinator(
        hass,
        config_entry,
        config_entry.options[CONF_HOST],
        config_entry.options[CONF_PORT],
        config_entry.data[SCAN_REGISTERS],
        config_entry.options[CONF_INTERVAL],
    )
    # try:
    #     await coordinator.async_config_entry_first_refresh()
    # except ConfigEntryNotReady:
    #     await coordinator.api.close()
    #     raise

    # Finalize
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN][config_entry.entry_id] = coordinator

    # Register a device for the hub itself so per-slave devices can be
    # linked to it via via_device, and so each hub is visually distinct
    # once multiple config entries exist.
    device_registry = dr.async_get(hass)
    device_registry.async_get_or_create(
        config_entry_id=config_entry.entry_id,
        identifiers={(DOMAIN, config_entry.entry_id)},
        name=config_entry.title,
        manufacturer="victron",
    )

    await coordinator.async_config_entry_first_refresh()
    config_entry.async_on_unload(config_entry.add_update_listener(update_listener))
    await hass.config_entries.async_forward_entry_setups(config_entry, PLATFORMS)

    return True


async def async_migrate_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Migrate an old config entry to the current unique_id/device scheme.

    Version 1 entries used a unique_id of "{slave}_{key}" and device
    identifiers of the bare slave number, both of which collide across
    multiple hubs. Version 2 prefixes both with the config entry id.
    """
    _LOGGER.debug(
        "Migrating victron config entry %s from version %s",
        config_entry.entry_id,
        config_entry.version,
    )

    if config_entry.version == 1:
        entity_registry = er.async_get(hass)
        device_registry = dr.async_get(hass)
        prefix = f"{config_entry.entry_id}_"

        for entity_entry in er.async_entries_for_config_entry(
            entity_registry, config_entry.entry_id
        ):
            if entity_entry.unique_id.startswith(prefix):
                continue  # already migrated
            entity_registry.async_update_entity(
                entity_entry.entity_id,
                new_unique_id=f"{prefix}{entity_entry.unique_id}",
            )

        for device_entry in dr.async_entries_for_config_entry(
            device_registry, config_entry.entry_id
        ):
            new_identifiers = set()
            changed = False
            for domain, identifier in device_entry.identifiers:
                if (
                    domain == DOMAIN
                    and identifier != config_entry.entry_id
                    and not identifier.startswith(prefix)
                ):
                    new_identifiers.add((domain, f"{prefix}{identifier}"))
                    changed = True
                else:
                    new_identifiers.add((domain, identifier))
            if changed:
                device_registry.async_update_device(
                    device_entry.id, new_identifiers=new_identifiers
                )

        hass.config_entries.async_update_entry(config_entry, version=2)

    _LOGGER.debug(
        "Migration of victron config entry %s to version %s successful",
        config_entry.entry_id,
        config_entry.version,
    )
    return True


async def async_unload_entry(hass: HomeAssistant, config_entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    if unload_ok := await hass.config_entries.async_unload_platforms(
        config_entry, PLATFORMS
    ):
        hass.data[DOMAIN].pop(config_entry.entry_id)

    return unload_ok


async def update_listener(hass: HomeAssistant, config_entry: ConfigEntry) -> None:
    """Update listener."""
    await hass.config_entries.async_reload(config_entry.entry_id)
