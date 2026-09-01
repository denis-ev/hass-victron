"""Define the Victron Energy Device Update Coordinator."""

from __future__ import annotations

from collections import OrderedDict
from datetime import timedelta
import logging

import pymodbus

if "3.7.0" <= pymodbus.__version__ <= "3.7.4":
    from pymodbus.pdu.register_read_message import ReadHoldingRegistersResponse
else:
    from pymodbus.pdu.register_message import ReadHoldingRegistersResponse

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import (
    DOMAIN,
    INT16,
    INT32,
    INT64,
    STRING,
    UINT16,
    UINT32,
    UINT64,
    RegisterInfo,
    register_info_dict,
)
from .hub import VictronHub

_LOGGER = logging.getLogger(__name__)


class victronEnergyDeviceUpdateCoordinator(DataUpdateCoordinator):
    """Gather data for the energy device."""

    api: VictronHub
    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        host: str,
        port: str,
        decodeInfo: OrderedDict,
        interval: int,
        timeout: float | None = None,
    ) -> None:
        """Initialize Update Coordinator."""

        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=f"{DOMAIN} ({config_entry.title})",
            update_interval=timedelta(seconds=interval),
        )
        self.config_entry = config_entry
        self.api = VictronHub(host, port, timeout=timeout)
        self.decodeInfo = decodeInfo
        self.interval = interval
        # (unit, register_set_name) pairs currently failing to respond.
        # Used to log the "no valid data" warning only on transition to
        # and from unavailable instead of every poll cycle forever - see
        # field report F7: a permanently-missing unit turned this into
        # the only thing in the log, every 30s, indefinitely.
        self._unavailable_register_sets: set = set()

    async def async_setup(self) -> None:
        """Open the Modbus TCP connection.

        ModbusTcpClient.connect() is a blocking socket call; run it in the
        executor instead of the event loop. Must be awaited by
        async_setup_entry before any register reads are attempted.
        """
        await self.hass.async_add_executor_job(self.api.connect)

    # async def force_update_data(self) -> None:
    #     data = await self._async_update_data()
    #     self.async_set_updated_data(data)

    async def _async_update_data(self) -> dict:
        """Fetch all device and sensor data from api."""
        data = ""
        """Get the latest data from victron"""
        self.logger.debug("Fetching victron data")
        self.logger.debug(self.decodeInfo)

        parsed_data = OrderedDict()
        unavailable_entities = OrderedDict()

        if self.data is None:
            self.data = {"data": OrderedDict(), "availability": OrderedDict()}

        for unit, registerInfo in self.decodeInfo.items():
            for name in registerInfo:
                data = await self.fetch_registers(unit, register_info_dict[name])
                # TODO safety check if result is actual data if not unavailable
                transition_key = (unit, name)
                if data.isError():
                    # raise error
                    # TODO change this to work with partial updates
                    for key in register_info_dict[name]:
                        full_key = str(unit) + "." + key
                        # self.data["data"][full_key] = None
                        unavailable_entities[full_key] = False

                    if transition_key not in self._unavailable_register_sets:
                        self._unavailable_register_sets.add(transition_key)
                        _LOGGER.warning(
                            "No valid data returned for entities of slave: %s (if the device continues to no longer update) check if the device was physically removed. Before opening an issue please force a rescan to attempt to resolve this issue",
                            unit,
                        )
                else:
                    if transition_key in self._unavailable_register_sets:
                        self._unavailable_register_sets.discard(transition_key)
                        _LOGGER.info(
                            "Slave %s register set %s is responding again",
                            unit,
                            name,
                        )
                    parsed_data = OrderedDict(
                        list(parsed_data.items())
                        + list(
                            self.parse_register_data(
                                data, register_info_dict[name], unit
                            ).items()
                        )
                    )
                    for key in register_info_dict[name]:
                        full_key = str(unit) + "." + key
                        unavailable_entities[full_key] = True

        return {
            "register_set": self.decodeInfo,
            "data": parsed_data,
            "availability": unavailable_entities,
        }

    def parse_register_data(
        self,
        buffer: ReadHoldingRegistersResponse,
        registerInfo: OrderedDict[str, RegisterInfo],
        unit: int,
    ) -> dict:
        """Parse the register data using convert_from_registers."""
        decoded_data = OrderedDict()
        registers = buffer.registers
        offset = 0

        for key, value in registerInfo.items():
            full_key = f"{unit}.{key}"
            count = 0
            if value.dataType in (INT16, UINT16):
                count = 1
            elif value.dataType in (INT32, UINT32):
                count = 2
            elif value.dataType in (INT64, UINT64):
                count = 4
            elif isinstance(value.dataType, STRING):
                count = value.dataType.length
            segment = registers[offset : offset + count]

            if isinstance(value.dataType, STRING):
                raw = self.api.convert_string_from_register(segment)
                decoded_data[full_key] = raw
            else:
                raw = self.api.convert_number_from_register(segment, value.dataType)
                # _LOGGER.warning("trying to decode %s with value %s", key, raw)
                decoded_data[full_key] = self.decode_scaling(
                    raw, value.scale, value.unit
                )
            offset += count

        return decoded_data

    def decode_scaling(self, number, scale, unit):
        """Decode the scaling."""
        if unit == "" and scale == 1:
            return round(number)
        return number / scale

    def encode_scaling(self, value, unit, scale):
        """Encode the scaling."""
        if scale == 0:
            return value
        if unit == "" and scale == 1:
            return int(round(value))
        return int(value * scale)

    def get_data(self):
        """Return the data."""
        return self.data

    async def async_update_local_entry(self, key, value):
        """Update the local entry."""
        data = self.data
        data["data"][key] = value
        self.async_set_updated_data(data)

        await self.async_request_refresh()

    def processed_data(self):
        """Return the processed data."""
        return self.data

    async def fetch_registers(self, unit, registerData):
        """Fetch the registers."""
        try:
            # run api_update in async job
            return await self.hass.async_add_executor_job(
                self.api_update, unit, registerData
            )

        except HomeAssistantError as e:
            raise UpdateFailed("Fetching registers failed") from e

    async def write_register(self, unit, address, value):
        """Write to the register.

        api_write() -> self.api.write_register() is a blocking Modbus
        socket call; run it in the executor instead of the event loop.
        """
        await self.hass.async_add_executor_job(
            self.api_write, unit, address, value
        )

    def api_write(self, unit, address, value):
        """Write to the api."""
        # recycle connection
        return self.api.write_register(unit=unit, address=address, value=value)

    def api_update(self, unit, registerInfo):
        """Update the api."""
        # recycle connection
        return self.api.read_holding_registers(
            unit=unit,
            address=self.api.get_first_register_id(registerInfo),
            count=self.api.calculate_register_count(registerInfo),
        )


class DecodeDataTypeUnsupported(Exception):
    """Exception for unsupported data type."""


class DataEntry:
    """Data entry class."""

    def __init__(self, unit, value) -> None:
        """Initialize the data entry."""
        self.unit = unit
        self.value = value
