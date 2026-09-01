"""Module defines entity descriptions for Victron components."""

from collections.abc import Callable
from dataclasses import dataclass

from homeassistant.helpers.entity import EntityDescription
from homeassistant.helpers.typing import StateType

from .const import DOMAIN


def strip_domain_prefix(hub_slug: str) -> str:
    """Strip a redundant leading DOMAIN token from a slugified hub name.

    Config entry titles often already start with "Victron" (e.g. "Victron
    Cerbo - Coobina"), and every entity_id is separately prefixed with
    f"{DOMAIN}_" (DOMAIN == "victron") when it's built - without this,
    the two combine into a doubled victron_victron_... prefix (field
    report F5). Only strips when something meaningful remains afterwards,
    so a hub literally named just "Victron" doesn't collapse into an
    empty/no-op prefix.
    """
    if hub_slug == DOMAIN:
        return hub_slug
    prefix = f"{DOMAIN}_"
    if hub_slug.startswith(prefix):
        stripped = hub_slug[len(prefix) :]
        if stripped:
            return stripped
    return hub_slug


@dataclass
class VictronBaseEntityDescription(EntityDescription):
    """An extension of EntityDescription for Victron components."""

    @staticmethod
    def lambda_func():
        """Return an entitydescription."""
        return lambda data, slave, key: data["data"][str(slave) + "." + str(key)]

    slave: int = None
    value_fn: Callable[[dict], StateType] = lambda_func()


@dataclass
class VictronWriteBaseEntityDescription(VictronBaseEntityDescription):
    """An extension of VictronBaseEntityDescription for writeable Victron components."""

    address: int = None
