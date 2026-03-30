import argparse
import asyncio
import os

from bleak import BleakScanner


def _extract_legacy_metadata_rssi(device):
    try:
        md = getattr(device, "metadata", None) or {}
        return md.get("rssi")
    except Exception:
        return None


async def _callback_probe(address: str, timeout: float) -> dict:
    target = str(address).upper()
    latest = {"seen": False, "name": None, "address": target, "adv_rssi": None, "device_rssi": None, "metadata_rssi": None}

    def _cb(device, advertisement_data):
        d_addr = str(getattr(device, "address", "") or "").upper()
        if d_addr != target:
            return
        latest["seen"] = True
        latest["name"] = getattr(device, "name", None)
        latest["address"] = getattr(device, "address", target)
        latest["adv_rssi"] = getattr(advertisement_data, "rssi", None)
        latest["device_rssi"] = getattr(device, "rssi", None)
        latest["metadata_rssi"] = _extract_legacy_metadata_rssi(device)

    scanner = BleakScanner(detection_callback=_cb)
    await scanner.start()
    try:
        await asyncio.sleep(timeout)
    finally:
        await scanner.stop()
    return latest


async def main(address: str, timeout: float, callback_timeout: float, enable_bleak_logging: bool) -> int:
    if enable_bleak_logging:
        os.environ["BLEAK_LOGGING"] = "1"
    addr_up = str(address).upper()
    print(f"[probe] scanning for {addr_up} (discover timeout={timeout:.1f}s)")
    devices = await BleakScanner.discover(timeout=timeout)
    found = False
    for d in devices:
        d_addr = str(getattr(d, "address", "") or "")
        if d_addr.upper() != addr_up:
            continue
        found = True
        print(
            "[probe] discover match:",
            {
                "name": getattr(d, "name", None),
                "address": d_addr,
                "rssi": getattr(d, "rssi", None),
                "metadata_rssi": _extract_legacy_metadata_rssi(d),
            },
        )
        break
    if not found:
        print("[probe] discover did not find target address")

    print(f"[probe] find_device_by_address for {addr_up}")
    dev = await BleakScanner.find_device_by_address(address, timeout=timeout)
    if dev is None:
        print("[probe] find_device_by_address returned None")
        return 2
    print(
        "[probe] find_device_by_address result:",
        {
            "name": getattr(dev, "name", None),
            "address": getattr(dev, "address", None),
            "rssi": getattr(dev, "rssi", None),
            "metadata_rssi": _extract_legacy_metadata_rssi(dev),
        },
    )
    print(f"[probe] callback scan for {addr_up} (timeout={callback_timeout:.1f}s)")
    cb = await _callback_probe(address, callback_timeout)
    print("[probe] callback result:", cb)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BLE RSSI diagnostics for a specific device address.")
    parser.add_argument("address", help="BLE address, e.g. 24:AC:AC:12:85:C3")
    parser.add_argument("--timeout", type=float, default=6.0, help="Scan timeout in seconds")
    parser.add_argument("--callback-timeout", type=float, default=8.0, help="Callback scan timeout in seconds")
    parser.add_argument("--bleak-logging", action="store_true", help="Enable BLEAK_LOGGING=1 during probe")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.address, args.timeout, args.callback_timeout, args.bleak_logging)))
