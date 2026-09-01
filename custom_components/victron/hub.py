"""Support for Victron Energy devices."""

from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import threading

from packaging import version
import pymodbus
from pymodbus.client import ModbusTcpClient

from homeassistant.exceptions import HomeAssistantError

from .const import (
    INT16,
    INT32,
    INT64,
    STRING,
    UINT16,
    UINT32,
    UINT64,
    register_info_dict,
    valid_unit_ids,
)

_LOGGER = logging.getLogger(__name__)

# Discovery scan concurrency: valid_unit_ids has 150+ candidate ids and
# register_info_dict has ~75 register blocks, so a fully sequential scan
# (the previous behaviour) can take a very long time, especially for
# unit ids that don't respond at all and eat the full per-request timeout
# on every attempt. Probing several unit ids at once (each on its own
# short-lived connection) cuts wall-clock scan time roughly by this
# factor. Kept conservative and not user-configurable: the Venus OS
# Modbus TCP daemon's concurrent-connection headroom isn't documented,
# and this only runs for the (infrequent) discovery scan, not the
# regular polling coordinator.
#
# A unit whose connection fails outright (as opposed to connecting fine
# and simply having no matching registers) is more likely to be hitting
# the device's own connection-slot contention from running several scan
# workers at once than to genuinely not exist. Rather than block a
# worker thread retrying with time.sleep() (which just ties up a
# concurrency slot doing nothing), determine_present_devices() reports
# those unit ids back to the caller so the recheck can happen at the
# async layer instead - see scan_connected_devices() in config_flow.py.
SCAN_CONCURRENCY = 2


class VictronHub:
    """Victron Hub."""

    def __init__(self, host: str, port: int, timeout: float | None = None) -> None:
        """Initialize.

        timeout is the pymodbus per-request timeout in seconds; None uses
        pymodbus's own ModbusTcpClient default.
        """
        self.host = host
        self.port = port
        self.timeout = timeout
        client_kwargs = {"host": self.host, "port": self.port}
        if timeout is not None:
            client_kwargs["timeout"] = timeout
        self._client = ModbusTcpClient(**client_kwargs)
        self._lock = threading.Lock()

    def is_still_connected(self):
        """Check if the connection is still open."""
        return self._client.is_socket_open()

    def convert_string_from_register(self, segment, string_encoding="ascii"):
        """Convert from registers to the appropriate data type."""
        if (
            version.parse("3.8.0")
            <= version.parse(pymodbus.__version__)
            <= version.parse("3.8.4")
        ):
            return self._client.convert_from_registers(
                segment, self._client.DATATYPE.STRING
            ).split("\x00")[0]
        return self._client.convert_from_registers(
            segment, self._client.DATATYPE.STRING, string_encoding=string_encoding
        ).split("\x00")[0]

    def convert_number_from_register(self, segment, dataType):
        """Convert from registers to the appropriate data type."""
        if dataType == UINT16:
            raw = self._client.convert_from_registers(
                segment, data_type=self._client.DATATYPE.UINT16
            )
        elif dataType == INT16:
            raw = self._client.convert_from_registers(
                segment, data_type=self._client.DATATYPE.INT16
            )
        elif dataType == UINT32:
            raw = self._client.convert_from_registers(
                segment, data_type=self._client.DATATYPE.UINT32
            )
        elif dataType == INT32:
            raw = self._client.convert_from_registers(
                segment, data_type=self._client.DATATYPE.INT32
            )
        return raw

    def connect(self):
        """Connect to the Modbus TCP server."""
        return self._client.connect()

    def disconnect(self):
        """Disconnect from the Modbus TCP server."""
        if self._client.is_socket_open():
            return self._client.close()
        return None

    def write_register(self, unit, address, value):
        """Write a register.

        Guarded by self._lock: reads and writes both run on executor threads
        (via hass.async_add_executor_job) and share one ModbusTcpClient
        socket, which is not safe for concurrent request/response pairs.
        """
        slave = int(unit) if unit else 1
        with self._lock:
            return self._client.write_register(
                address=address, value=value, device_id=slave
            )

    def read_holding_registers(self, unit, address, count):
        """Read holding registers.

        See write_register's docstring for why this is lock-guarded.
        """
        slave = int(unit) if unit else 1
        _LOGGER.debug("Reading unit %s address %s count %s", unit, address, count)
        with self._lock:
            return self._client.read_holding_registers(
                address=address, count=count, device_id=slave
            )

    def calculate_register_count(self, registerInfoDict: OrderedDict):
        """Calculate the number of registers to read."""
        first_key = next(iter(registerInfoDict))
        last_key = next(reversed(registerInfoDict))
        end_correction = 1
        if registerInfoDict[last_key].dataType in (INT32, UINT32):
            end_correction = 2
        elif registerInfoDict[last_key].dataType in (INT64, UINT64):
            end_correction = 4
        elif isinstance(registerInfoDict[last_key].dataType, STRING):
            end_correction = registerInfoDict[last_key].dataType.length

        return (
            registerInfoDict[last_key].register - registerInfoDict[first_key].register
        ) + end_correction

    def get_first_register_id(self, registerInfoDict: OrderedDict):
        """Return first register id."""
        first_register = next(iter(registerInfoDict))
        return registerInfoDict[first_register].register

    def determine_present_devices(self, units=None):
        """Determine which devices are present.

        Probes are spread across up to SCAN_CONCURRENCY unit ids at a time
        (each on its own short-lived connection via _scan_unit) instead of
        one long sequential loop; see SCAN_CONCURRENCY's comment for why.
        This method itself still blocks the calling thread until every
        unit id has been probed - callers already run it in the executor
        (config_flow.validate_input) so that's fine.

        units defaults to every valid_unit_ids candidate; pass a smaller
        iterable to re-probe only specific unit ids (used by
        scan_connected_devices's async recheck of units whose connection
        failed on a previous pass).

        Returns (valid_devices, failed_units): failed_units is every unit
        id whose connection itself failed (as opposed to connecting fine
        and finding no matching registers), so the caller can decide
        whether to recheck them rather than conclude they're absent.
        """
        _LOGGER.debug("Determining present devices")

        units = list(valid_unit_ids) if units is None else list(units)
        valid_devices = {}
        failed_units = []
        max_workers = min(SCAN_CONCURRENCY, len(units)) or 1
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_unit = {
                executor.submit(self._scan_unit, unit): unit for unit in units
            }
            for future in as_completed(future_to_unit):
                unit = future_to_unit[future]
                try:
                    working_registers, connection_failed = future.result()
                except Exception:  # noqa: BLE001 - one unit's failure must not abort the rest of the scan
                    _LOGGER.exception("Scan of unit %s failed", unit)
                    failed_units.append(unit)
                    continue

                if connection_failed:
                    failed_units.append(unit)
                elif working_registers:
                    valid_devices[unit] = working_registers
                else:
                    _LOGGER.debug("no registers found for unit: %s", unit)

        return valid_devices, failed_units

    def _scan_unit(self, unit) -> tuple[list, bool]:
        """Probe every register block for a single unit id on its own connection.

        Runs on a worker thread from determine_present_devices's thread
        pool. Uses a dedicated VictronHub/connection rather than self so
        multiple units can be probed concurrently without serializing on
        self._lock, which would defeat the point of parallelizing.

        Single connection attempt only - no blocking retry here, since
        sleeping inside a thread pool worker just ties up a concurrency
        slot. Returns (working_registers, connection_failed) so the
        caller can distinguish "connection itself failed" from
        "connected fine, nothing found" and decide whether a recheck
        makes sense.

        Two-pass register probing (field report F3): a genuinely absent
        unit answers every single register block with a fast Modbus-level
        error (tens of ms, not a timeout), so retrying each of the ~75
        blocks 3 times each was pure probe-count waste for the vast
        majority of candidate unit ids, which don't exist on any given
        system. Pass one tries every block once; if literally nothing
        responds, the unit is almost certainly absent and there is
        nothing to gain from retrying blocks that were never going to
        answer. Only once a unit proves it's alive - at least one block
        answered - do the remaining not-yet-successful blocks get the
        full multi-attempt consensus retry, since a real unit's
        per-block failure is worth treating as possibly transient.

        WAN-deployed GX devices (a real-world caveat raised against the
        first version of this two-pass design) can hit a connection-level
        blip spanning this unit's entire first pass, making a genuinely
        present unit fail every single first-pass probe - a same-
        connection retry wouldn't catch that if the connection itself was
        the problem. So if pass one comes back completely empty, this
        reconnects fresh and tries once more against a single register
        before concluding absence: one extra probe per genuinely-absent
        unit is negligible against the thousands saved, and it restores
        tolerance for a transient that spans the whole first pass.
        """
        hub = VictronHub(self.host, self.port, timeout=self.timeout)
        working_registers = []
        try:
            if not hub.connect():
                _LOGGER.debug("Scan connection failed for unit %s", unit)
                return working_registers, True

            pending_blocks = []
            for key, register_definition in register_info_dict.items():
                _LOGGER.debug("Checking unit %s for register set %s", unit, key)
                # VE.CAN device zero is present under unit 100. This seperates non system / settings entities into the seperate can device
                if unit == 100 and not key.startswith(("settings", "system")):
                    continue

                try:
                    status = hub._probe_block_supported(  # noqa: SLF001 - same class, dedicated scan connection
                        unit, register_definition, attempts=1
                    )
                except HomeAssistantError as e:
                    _LOGGER.error(e)
                    continue

                if status is True:
                    working_registers.append(key)
                else:
                    pending_blocks.append((key, register_definition))

            if not working_registers and pending_blocks:
                hub.disconnect()
                if hub.connect():
                    recheck_key, recheck_definition = pending_blocks[0]
                    try:
                        status = hub._probe_block_supported(  # noqa: SLF001 - same class, dedicated scan connection
                            unit, recheck_definition, attempts=1
                        )
                    except HomeAssistantError as e:
                        _LOGGER.error(e)
                        status = None
                    if status is True:
                        working_registers.append(recheck_key)
                        pending_blocks = pending_blocks[1:]
                        _LOGGER.debug(
                            "Unit %s answered on a reconnect-and-retry after "
                            "failing every block on the first pass; "
                            "treating as present",
                            unit,
                        )

            if not working_registers:
                _LOGGER.debug(
                    "Unit %s did not respond to any of %s register blocks "
                    "on the first pass or a reconnect retry; treating as "
                    "not present instead of retrying every block",
                    unit,
                    len(pending_blocks),
                )
            else:
                for key, register_definition in pending_blocks:
                    try:
                        status = hub._probe_block_supported(  # noqa: SLF001 - same class, dedicated scan connection
                            unit, register_definition, attempts=2
                        )
                    except HomeAssistantError as e:
                        _LOGGER.error(e)
                        continue
                    if status is True:
                        working_registers.append(key)
                    else:
                        _LOGGER.debug(
                            "register set %s on unit %s did not respond on "
                            "any probe attempt; treating as not present",
                            key,
                            unit,
                        )
        finally:
            hub.disconnect()

        return working_registers, False

    def _probe_block_supported(
        self, unit, register_definition: OrderedDict, attempts: int = 3
    ):
        """Probe a register block multiple times.

        Returns True if any read succeeded (a well-formed, non-error
        Modbus response), None if every attempt errored or raised.

        Deliberately does NOT inspect whether TextReadEntityType registers
        in the block decode against their known enum - it used to, and a
        block was excluded entirely if every successful read had at least
        one undecodable enum value anywhere in it. Register blocks here
        can bundle several unrelated registers together (e.g.
        solarcharger_registers has battery_voltage/current/temperature
        and pv_voltage/current alongside state/alarm/errorcode enum
        registers), so one enum register reporting a firmware value this
        integration doesn't know about yet silently deleted every other,
        perfectly good, scalar register sharing that block - see the
        field report (F2) this change addresses: an entire charger's
        battery telemetry disappeared this way. An undecodable enum value
        is now handled gracefully at the entity level instead (see
        sensor.py), which doesn't require throwing away sibling data to
        avoid it.
        """
        address = self.get_first_register_id(register_definition)
        count = self.calculate_register_count(register_definition)
        for _ in range(attempts):
            try:
                result = self.read_holding_registers(unit, address, count)
            except Exception:  # noqa: BLE001 - bounded retry; transient errors are expected
                continue
            if result.isError():
                continue
            return True
        return None

    def revalidate_register_set(self, stored: dict) -> dict:
        """Drop stored register blocks that no longer exist in this version.

        Older config entries can reference register set keys that have
        since been renamed or removed from register_info_dict; this
        drops those so async_setup_entry doesn't hit a KeyError building
        entities from a key that no longer resolves to anything.

        No longer probes the device to prune blocks by decodability - see
        _probe_block_supported's docstring for why that was removed.
        """
        pruned: dict = {}
        for unit, blocks in stored.items():
            kept = [key for key in blocks if key in register_info_dict]
            if kept:
                pruned[unit] = kept
        return pruned
