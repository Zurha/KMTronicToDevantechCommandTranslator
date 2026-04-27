#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import signal
import sys
import time
from typing import Callable, Dict, Optional, Tuple

import serial


SERIAL_ERRORS = (serial.SerialException, OSError)


class RelayUnavailableError(RuntimeError):
    """Raised when the Devantech relay cannot be reached right now."""


class DevantechUsbRly02:
    CMD_GET_RELAY_STATES = 0x5B
    CMD_RELAY_1_ON = 0x65
    CMD_RELAY_2_ON = 0x66
    CMD_RELAY_1_OFF = 0x6F
    CMD_RELAY_2_OFF = 0x70

    def __init__(
        self,
        port: str,
        baudrate: int,
        timeout: float,
        reconnect_delay: float,
    ) -> None:
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.reconnect_delay = reconnect_delay
        self.ser: Optional[serial.Serial] = None
        self.cached_states: Tuple[bool, bool] = (False, False)
        self._last_open_attempt = 0.0
        self.ensure_connected(force=True)

    def close(self) -> None:
        self._close_serial()

    def ensure_connected(self, force: bool = False) -> bool:
        if self.ser is not None and self.ser.is_open:
            return True

        now = time.monotonic()
        if not force and now - self._last_open_attempt < self.reconnect_delay:
            return False

        self._last_open_attempt = now
        try:
            self.ser = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=self.timeout,
            )
        except SERIAL_ERRORS as exc:
            self._close_serial()
            logging.warning(
                "USB-RLY02-SN on %s is not available yet; retrying: %s",
                self.port,
                exc,
            )
            return False

        logging.info("Connected to USB-RLY02-SN on %s", self.port)
        return True

    def set_relay(self, relay: int, is_on: bool) -> None:
        if relay not in (1, 2):
            raise ValueError(f"Unsupported Devantech relay index: {relay}")

        cmd_map = {
            (1, True): self.CMD_RELAY_1_ON,
            (1, False): self.CMD_RELAY_1_OFF,
            (2, True): self.CMD_RELAY_2_ON,
            (2, False): self.CMD_RELAY_2_OFF,
        }
        cmd = cmd_map[(relay, is_on)]

        try:
            ser = self._connected_serial()
            ser.write(bytes([cmd]))
            ser.flush()
        except SERIAL_ERRORS as exc:
            self._mark_disconnected("Relay write failed", exc)
            raise RelayUnavailableError(
                f"USB-RLY02-SN on {self.port} disconnected during relay write"
            ) from exc

        states = list(self.cached_states)
        states[relay - 1] = is_on
        self.cached_states = (states[0], states[1])

    def get_states(self) -> Tuple[bool, bool]:
        try:
            ser = self._connected_serial()
            ser.reset_input_buffer()
            ser.write(bytes([self.CMD_GET_RELAY_STATES]))
            ser.flush()
            raw = ser.read(1)
        except SERIAL_ERRORS as exc:
            self._mark_disconnected("Relay state poll failed", exc)
            raise RelayUnavailableError(
                f"USB-RLY02-SN on {self.port} disconnected during state poll"
            ) from exc

        if len(raw) != 1:
            self._mark_disconnected("Relay state poll timed out")
            raise RelayUnavailableError("No state byte returned by USB-RLY02-SN")

        value = raw[0]
        states = (bool(value & 0x01), bool(value & 0x02))
        self.cached_states = states
        return states

    def _connected_serial(self) -> serial.Serial:
        if not self.ensure_connected():
            raise RelayUnavailableError(
                f"USB-RLY02-SN on {self.port} is unavailable; retrying in "
                f"{self.reconnect_delay:.1f}s"
            )
        if self.ser is None:
            raise RelayUnavailableError(f"USB-RLY02-SN on {self.port} is unavailable")
        return self.ser

    def _mark_disconnected(
        self,
        message: str,
        exc: Optional[BaseException] = None,
    ) -> None:
        if exc is None:
            logging.warning("%s on %s; closing port and retrying", message, self.port)
        else:
            logging.warning(
                "%s on %s; closing port and retrying: %s",
                message,
                self.port,
                exc,
            )
        self._close_serial()

    def _close_serial(self) -> None:
        ser = self.ser
        self.ser = None
        if ser is None:
            return
        try:
            if ser.is_open:
                ser.close()
        except SERIAL_ERRORS as exc:
            logging.debug("Error while closing Devantech port %s: %s", self.port, exc)


class KmtronicProxy:
    def __init__(
        self,
        listen_port: str,
        listen_baud: int,
        devantech_port: str,
        devantech_baud: int,
        km_to_dev_map: Dict[int, int],
        km_status_count: int,
        serial_timeout: float,
        devantech_timeout: float,
        reconnect_delay: float,
    ) -> None:
        self.listen_port = listen_port
        self.listen_baud = listen_baud
        self.serial_timeout = serial_timeout
        self.reconnect_delay = reconnect_delay
        self.km_ser: Optional[serial.Serial] = None
        self._last_km_open_attempt = 0.0
        self.dev = DevantechUsbRly02(
            port=devantech_port,
            baudrate=devantech_baud,
            timeout=devantech_timeout,
            reconnect_delay=reconnect_delay,
        )
        self.km_to_dev_map = km_to_dev_map
        self.km_status_count = km_status_count
        self.running = True
        self.buffer = bytearray()
        self._last_status_fallback_log = 0.0
        self._ensure_km_open(force=True)

    def close(self) -> None:
        self._close_km_serial()
        self.dev.close()

    def stop(self) -> None:
        self.running = False

    def run(self) -> None:
        logging.info("Listening for KMTronic commands on %s", self.listen_port)
        logging.info("Forwarding to USB-RLY02-SN on %s", self.dev.port)

        while self.running:
            if not self._ensure_km_open():
                self._sleep_while_running(self.reconnect_delay)
                continue

            try:
                if self.km_ser is None:
                    continue
                chunk = self.km_ser.read(64)
            except SERIAL_ERRORS as exc:
                logging.warning(
                    "KMTronic serial read failed on %s; reconnecting: %s",
                    self.listen_port,
                    exc,
                )
                self._close_km_serial()
                self._sleep_while_running(self.reconnect_delay)
                continue

            if not chunk:
                continue

            self.buffer.extend(chunk)
            try:
                self._process_buffer()
            except Exception:
                logging.exception(
                    "Unexpected error while processing KMTronic data; "
                    "clearing buffered bytes and continuing"
                )
                self.buffer.clear()

    def _ensure_km_open(self, force: bool = False) -> bool:
        if self.km_ser is not None and self.km_ser.is_open:
            return True

        now = time.monotonic()
        if not force and now - self._last_km_open_attempt < self.reconnect_delay:
            return False

        self._last_km_open_attempt = now
        try:
            self.km_ser = serial.Serial(
                port=self.listen_port,
                baudrate=self.listen_baud,
                timeout=self.serial_timeout,
            )
        except SERIAL_ERRORS as exc:
            self._close_km_serial()
            logging.warning(
                "KMTronic listen port %s is not available yet; retrying: %s",
                self.listen_port,
                exc,
            )
            return False

        logging.info("Connected to KMTronic listen port %s", self.listen_port)
        return True

    def _close_km_serial(self) -> None:
        ser = self.km_ser
        self.km_ser = None
        if ser is None:
            return
        try:
            if ser.is_open:
                ser.close()
        except SERIAL_ERRORS as exc:
            logging.debug("Error while closing KMTronic port %s: %s", self.listen_port, exc)

    def _sleep_while_running(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while self.running and time.monotonic() < deadline:
            time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))

    def _process_buffer(self) -> None:
        while len(self.buffer) >= 3:
            if self.buffer[0] != 0xFF:
                dropped = self.buffer.pop(0)
                logging.debug("Dropped non-sync byte: 0x%02X", dropped)
                continue

            frame = bytes(self.buffer[:3])
            del self.buffer[:3]
            self._handle_frame(frame)

    def _handle_frame(self, frame: bytes) -> None:
        _, relay, command = frame
        logging.debug("RX KMTronic frame: %s", frame.hex(" ").upper())

        if relay == 0x09 and command == 0x00:
            self._reply_all_status()
            return

        if command == 0x00:
            self._set_relay(relay, False)
            return

        if command == 0x01:
            self._set_relay(relay, True)
            return

        if command == 0x03:
            self._reply_single_status(relay)
            return

        logging.warning(
            "Unsupported KMTronic command: relay=%d command=0x%02X",
            relay,
            command,
        )

    def _set_relay(self, km_relay: int, is_on: bool) -> None:
        dev_relay = self.km_to_dev_map.get(km_relay)
        if dev_relay is None:
            logging.warning("No mapping configured for KMTronic relay %d", km_relay)
            return

        try:
            self.dev.set_relay(dev_relay, is_on)
        except RelayUnavailableError as exc:
            logging.error(
                "Could not set KM relay %d -> Dev relay %d to %s; "
                "bridge remains running and will retry the relay connection: %s",
                km_relay,
                dev_relay,
                "ON" if is_on else "OFF",
                exc,
            )
            return

        logging.info(
            "KM relay %d -> Dev relay %d set to %s",
            km_relay,
            dev_relay,
            "ON" if is_on else "OFF",
        )

    def _reply_single_status(self, km_relay: int) -> None:
        dev_relay = self.km_to_dev_map.get(km_relay)
        status = 0

        if dev_relay is not None:
            states = self._get_dev_states_or_cache()
            status = 1 if states[dev_relay - 1] else 0

        reply = bytes([0xFF, km_relay & 0xFF, status])
        self._write_km_reply(reply)
        logging.debug("TX KMTronic status reply: %s", reply.hex(" ").upper())

    def _reply_all_status(self) -> None:
        states = self._get_dev_states_or_cache()

        payload = bytearray()
        for km_relay in range(1, self.km_status_count + 1):
            dev_relay = self.km_to_dev_map.get(km_relay)
            if dev_relay is None:
                payload.append(0x00)
            else:
                payload.append(0x01 if states[dev_relay - 1] else 0x00)

        self._write_km_reply(bytes(payload))
        logging.debug("TX KMTronic multi-status reply: %s", payload.hex(" ").upper())

    def _get_dev_states_or_cache(self) -> Tuple[bool, bool]:
        try:
            return self.dev.get_states()
        except RelayUnavailableError as exc:
            now = time.monotonic()
            log_message = "Devantech state unavailable, using cached states: %s"
            if now - self._last_status_fallback_log >= self.reconnect_delay:
                logging.warning(log_message, exc)
                self._last_status_fallback_log = now
            else:
                logging.debug(log_message, exc)
            return self.dev.cached_states

    def _write_km_reply(self, reply: bytes) -> None:
        if not self._ensure_km_open():
            logging.warning(
                "Cannot send KMTronic reply because listen port %s is unavailable",
                self.listen_port,
            )
            return

        try:
            if self.km_ser is None:
                return
            self.km_ser.write(reply)
            self.km_ser.flush()
        except SERIAL_ERRORS as exc:
            logging.warning(
                "KMTronic serial write failed on %s; reconnecting: %s",
                self.listen_port,
                exc,
            )
            self._close_km_serial()


def parse_relay_map(value: str) -> Dict[int, int]:
    result: Dict[int, int] = {}
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise argparse.ArgumentTypeError(
                "Invalid relay-map format. Use format like 1:1,2:2"
            )
        km_raw, dev_raw = part.split(":", 1)
        try:
            km_relay = int(km_raw.strip())
            dev_relay = int(dev_raw.strip())
        except ValueError as exc:
            raise argparse.ArgumentTypeError(
                "relay-map values must be numbers, for example 1:1,2:2"
            ) from exc

        if km_relay < 1:
            raise argparse.ArgumentTypeError("KMTronic relay indexes must be >= 1")
        if km_relay > 255:
            raise argparse.ArgumentTypeError("KMTronic relay indexes must be <= 255")
        if dev_relay not in (1, 2):
            raise argparse.ArgumentTypeError("Devantech relay index must be 1 or 2")

        result[km_relay] = dev_relay

    if not result:
        raise argparse.ArgumentTypeError("relay-map cannot be empty")
    return result


def parse_positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Value must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("Value must be a positive integer")
    return parsed


def parse_km_status_count(value: str) -> int:
    parsed = parse_positive_int(value)
    if parsed > 64:
        raise argparse.ArgumentTypeError("km-status-count must be between 1 and 64")
    return parsed


def parse_non_negative_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Value must be a number") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("Value must be 0 or greater")
    return parsed


def parse_positive_float(value: str) -> float:
    parsed = parse_non_negative_float(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("Value must be greater than 0")
    return parsed


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="KMTronic serial protocol bridge -> Devantech USB-RLY02-SN"
    )
    parser.add_argument(
        "--listen-port",
        required=True,
        help="Virtual COM port opened by this script",
    )
    parser.add_argument(
        "--devantech-port",
        required=True,
        help="Real COM port of USB-RLY02-SN",
    )
    parser.add_argument(
        "--listen-baud",
        type=int,
        default=9600,
        help="KMTronic side baud rate",
    )
    parser.add_argument(
        "--devantech-baud",
        type=int,
        default=9600,
        help="Devantech side baud rate",
    )
    parser.add_argument(
        "--relay-map",
        type=parse_relay_map,
        default=parse_relay_map("1:1,2:2"),
        help="Map KM relays to Dev relays, example: 1:1,2:2",
    )
    parser.add_argument(
        "--km-status-count",
        type=parse_km_status_count,
        default=2,
        help="Number of bytes to return for FF 09 00 status request",
    )
    parser.add_argument(
        "--listen-timeout",
        type=parse_positive_float,
        default=0.1,
        help="Serial read timeout for the virtual KMTronic COM port",
    )
    parser.add_argument(
        "--devantech-timeout",
        type=parse_positive_float,
        default=0.5,
        help="Serial read timeout for the Devantech relay COM port",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=parse_positive_float,
        default=2.0,
        help="Seconds to wait between COM port reconnect attempts",
    )
    parser.add_argument(
        "--log-file",
        help="Optional rotating log file path for unattended production runs",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def configure_logging(verbose: bool, log_file: Optional[str]) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file_error: Optional[OSError] = None
    if log_file:
        try:
            log_path = Path(log_file)
            if log_path.parent != Path("."):
                log_path.parent.mkdir(parents=True, exist_ok=True)
            handlers.append(
                RotatingFileHandler(
                    log_path,
                    maxBytes=1_000_000,
                    backupCount=5,
                    encoding="utf-8",
                )
            )
        except OSError as exc:
            log_file_error = exc

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=handlers,
    )
    if log_file_error is not None:
        logging.warning("Could not open log file %s: %s", log_file, log_file_error)


def sleep_until_stopped(seconds: float, is_stopped: Callable[[], bool]) -> None:
    deadline = time.monotonic() + seconds
    while not is_stopped() and time.monotonic() < deadline:
        time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    configure_logging(args.verbose, args.log_file)

    state = {"stopping": False, "proxy": None}

    def _stop_handler(signum, frame):
        logging.info("Signal %s received, stopping...", signum)
        state["stopping"] = True
        proxy = state["proxy"]
        if proxy is not None:
            proxy.stop()

    signal.signal(signal.SIGINT, _stop_handler)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _stop_handler)

    while not state["stopping"]:
        proxy = None
        try:
            proxy = KmtronicProxy(
                listen_port=args.listen_port,
                listen_baud=args.listen_baud,
                devantech_port=args.devantech_port,
                devantech_baud=args.devantech_baud,
                km_to_dev_map=args.relay_map,
                km_status_count=args.km_status_count,
                serial_timeout=args.listen_timeout,
                devantech_timeout=args.devantech_timeout,
                reconnect_delay=args.reconnect_delay,
            )
            state["proxy"] = proxy
            proxy.run()
        except Exception:
            if state["stopping"]:
                break
            logging.exception(
                "Unexpected bridge error; restarting in %.1fs",
                args.reconnect_delay,
            )
        finally:
            if proxy is not None:
                proxy.close()
            state["proxy"] = None

        if not state["stopping"]:
            sleep_until_stopped(args.reconnect_delay, lambda: state["stopping"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
