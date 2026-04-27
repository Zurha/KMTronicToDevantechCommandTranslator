import argparse
import sys
import types
import unittest


class FakeSerialException(Exception):
    pass


fake_serial_module = types.SimpleNamespace(SerialException=FakeSerialException)
sys.modules["serial"] = fake_serial_module

import translator


class FakeSerial:
    def __init__(self, port, baudrate, timeout):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.is_open = True
        self.writes = []
        self.read_chunks = []
        self.fail_write = False
        self.fail_read = False
        self.on_read = None

    def close(self):
        self.is_open = False

    def flush(self):
        if self.fail_write:
            raise FakeSerialException("flush failed")

    def read(self, size):
        if self.fail_read:
            raise FakeSerialException("read failed")
        if self.on_read is not None:
            self.on_read(self)
        if self.read_chunks:
            next_chunk = self.read_chunks.pop(0)
            if isinstance(next_chunk, BaseException):
                raise next_chunk
            return next_chunk
        return b""

    def reset_input_buffer(self):
        pass

    def write(self, data):
        if self.fail_write:
            raise FakeSerialException("write failed")
        self.writes.append(bytes(data))
        return len(data)


class FakeSerialFactory:
    def __init__(self):
        self.created = {}
        self.configurators = {}

    def set_configurators(self, port, configurators):
        self.configurators[port] = list(configurators)

    def __call__(self, port, baudrate, timeout):
        ser = FakeSerial(port, baudrate, timeout)
        configurators = self.configurators.get(port, [])
        if configurators:
            configurators.pop(0)(ser)
        self.created.setdefault(port, []).append(ser)
        return ser


class TranslatorReliabilityTest(unittest.TestCase):
    def setUp(self):
        self.factory = FakeSerialFactory()
        translator.serial.Serial = self.factory

    def build_proxy(self):
        return translator.KmtronicProxy(
            listen_port="COM6",
            listen_baud=9600,
            devantech_port="COM7",
            devantech_baud=9600,
            km_to_dev_map={1: 1, 2: 2},
            km_status_count=2,
            serial_timeout=0.1,
            devantech_timeout=0.1,
            reconnect_delay=0,
        )

    def test_devantech_write_failure_does_not_stop_bridge_and_reconnects(self):
        proxy = self.build_proxy()
        dev_first = self.factory.created["COM7"][0]
        dev_first.fail_write = True

        proxy._handle_frame(bytes([0xFF, 0x01, 0x01]))

        self.assertTrue(proxy.running)
        self.assertFalse(dev_first.is_open)
        self.assertEqual(proxy.dev.cached_states, (False, False))

        proxy._handle_frame(bytes([0xFF, 0x01, 0x01]))

        dev_second = self.factory.created["COM7"][1]
        self.assertEqual(
            dev_second.writes,
            [bytes([translator.DevantechUsbRly02.CMD_RELAY_1_ON])],
        )
        self.assertEqual(proxy.dev.cached_states, (True, False))

    def test_status_request_falls_back_to_cached_state_when_relay_is_lost(self):
        proxy = self.build_proxy()
        proxy.dev.cached_states = (True, False)
        dev = self.factory.created["COM7"][0]

        proxy._handle_frame(bytes([0xFF, 0x01, 0x03]))

        km = self.factory.created["COM6"][0]
        self.assertEqual(km.writes[-1], bytes([0xFF, 0x01, 0x01]))
        self.assertFalse(dev.is_open)

    def test_listen_port_read_failure_reconnects_instead_of_exiting(self):
        holder = {}

        self.factory.set_configurators(
            "COM6",
            [
                lambda ser: setattr(ser, "fail_read", True),
                lambda ser: setattr(
                    ser,
                    "on_read",
                    lambda _ser: holder["proxy"].stop(),
                ),
            ],
        )

        proxy = self.build_proxy()
        holder["proxy"] = proxy

        proxy.run()

        self.assertGreaterEqual(len(self.factory.created["COM6"]), 2)
        self.assertFalse(self.factory.created["COM6"][0].is_open)

    def test_invalid_relay_map_values_are_reported_as_argparse_errors(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            translator.parse_relay_map("x:1")
        with self.assertRaises(argparse.ArgumentTypeError):
            translator.parse_relay_map("1:x")
        with self.assertRaises(argparse.ArgumentTypeError):
            translator.parse_relay_map("256:1")
        with self.assertRaises(argparse.ArgumentTypeError):
            translator.parse_km_status_count("65")


if __name__ == "__main__":
    unittest.main()
