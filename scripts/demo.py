#!/usr/bin/env python3
"""Exercise an isolated gateway and receiver against a real Mosquitto broker."""

import argparse
from contextlib import ExitStack, closing
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parent.parent
TOPIC = "edge/v1/demo"


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def stop(process, hard=False):
    if process.poll() is None:
        (process.kill if hard else process.terminate)()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def wait_for(description, check, timeout=20):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.05)
    raise RuntimeError(f"timed out waiting for {description}")


def available_ports():
    with socket.socket() as http_socket, socket.socket() as mqtt_socket:
        http_socket.bind(("127.0.0.1", 0))
        mqtt_socket.bind(("127.0.0.1", 0))
        return http_socket.getsockname()[1], mqtt_socket.getsockname()[1]


def request(url, path, data=None):
    body = None if data is None else json.dumps(data).encode()
    outgoing = Request(url + path, body, {"Content-Type": "application/json"})
    try:
        incoming = urlopen(outgoing, timeout=3)
    except HTTPError as error:
        incoming = error
    with incoming:
        return incoming.code, json.load(incoming), incoming.headers


def inbox_rows(path):
    with closing(sqlite3.connect(path, timeout=2)) as conn:
        return conn.execute("SELECT id, payload, attempts FROM inbox").fetchall()


def run(args, work, cleanup):
    http_port, mqtt_port = available_ports()
    base_url = f"http://127.0.0.1:{http_port}"
    gateway_db = work / "gateway.sqlite3"
    receiver_db = work / "receiver.sqlite3"
    broker_config = work / "mosquitto.conf"
    acl = work / "mosquitto.acl"
    # Mosquitto can drop privileges when this demo is run as root.
    if os.geteuid() == 0:
        work.chmod(0o755)
    online = min(32, args.events // 4)
    offline = args.events - online
    env = {key: value for key, value in os.environ.items() if not key.startswith("IOT_")}
    env.update(
        IOT_GATEWAY_ID="demo",
        IOT_HTTP_ADDR=f"127.0.0.1:{http_port}",
        IOT_MQTT_PORT=str(mqtt_port),
        IOT_MAX_PENDING=str(offline),
    )

    def spawn(command, name, variables=env):
        with (work / f"{name}.log").open("wb") as log:
            process = subprocess.Popen(command, env=variables, stdout=log, stderr=subprocess.STDOUT)
        cleanup.callback(stop, process)
        return process

    def broker(name, allow_receipts=True):
        config = f"listener {mqtt_port} 127.0.0.1\nallow_anonymous true\npersistence false\n"
        if not allow_receipts:
            acl.write_text(f"topic readwrite {TOPIC}/telemetry\ntopic read {TOPIC}/receipt\n")
            config += f"acl_file {acl}\n"
        broker_config.write_text(config)
        process = spawn([args.broker, "-c", str(broker_config)], name)

        def listening():
            require(process.poll() is None, f"broker {name} exited")
            try:
                with socket.create_connection(("127.0.0.1", mqtt_port), timeout=0.2):
                    return True
            except OSError:
                return False

        wait_for(f"{name} to listen", listening)
        return process

    def service(role, db_path, name):
        return spawn([str(args.binary), role], name, dict(env, IOT_DB_PATH=str(db_path)))

    def status():
        try:
            code, body, _ = request(base_url, "/status")
            return body if code == 200 else {}
        except (URLError, TimeoutError, ConnectionError):
            return {}

    readings = {
        f"event-{index:06}": {
            "id": f"event-{index:06}",
            "device_id": "temperature-probe-01",
            "timestamp_ms": 1760000000000 + index * 100,
            "metric": "temperature.c",
            "value": 20 + index / 1000,
        }
        for index in range(args.events)
    }
    ids = list(readings)
    confirmed = set()

    def submit(event_id, payload=None, expected=202):
        code, body, headers = request(base_url, "/v1/telemetry", readings[event_id] if payload is None else payload)
        require(code == expected, f"HTTP {code} for {event_id}, expected {expected}: {body}")
        if expected == 202:
            require(body == {"id": event_id}, f"unexpected admission receipt: {body}")
            confirmed.add(event_id)
        if expected == 503:
            require(headers.get("Retry-After") == "1", "HTTP backpressure lacks Retry-After")

    first_broker = broker("broker-online")
    gateway = service("gateway", gateway_db, "gateway-before-crash")
    receiver = service("receiver", receiver_db, "receiver-before-crash")
    wait_for("gateway MQTT subscription", lambda: status().get("mqtt_subscribed"))
    for event_id in ids[:online]:
        submit(event_id)
    wait_for("initial online delivery", lambda: status().get("delivered") == online)

    stop(first_broker)
    wait_for("gateway to detect lost MQTT transport", lambda: status().get("mqtt_subscribed") is False)
    for event_id in ids[online:]:
        submit(event_id)
    submit(ids[-1])
    submit(ids[-1], dict(readings[ids[-1]], value=999.0), 409)
    submit("overflow", dict(readings[ids[-1]], id="overflow"), 503)
    require(status().get("pending") == offline, "offline events did not remain pending")
    print(f"Broker offline. Gateway accepted {offline} new events and applied backpressure at capacity.", flush=True)

    stop(gateway, hard=True)
    gateway = service("gateway", gateway_db, "gateway-after-crash")
    wait_for("gateway to recover its durable queue", lambda: status().get("pending") == offline)
    print(f"Gateway restarted after SIGKILL with all {offline} pending events.", flush=True)

    restricted_broker = broker("broker-without-receipts", allow_receipts=False)

    def committed_without_receipts():
        return {event_id for event_id, _, _ in inbox_rows(receiver_db)} - set(ids[:online])

    withheld = wait_for("receiver to commit while receipt publishing is blocked", committed_without_receipts)
    gateway_status = status()
    require(gateway_status.get("pending") == offline, "gateway cleared records without application receipts")
    require(gateway_status.get("delivered") == online, "unexpected delivery count while receipts blocked")
    stop(restricted_broker)
    stop(receiver, hard=True)
    receiver = service("receiver", receiver_db, "receiver-after-crash")
    print(f"Receiver committed {len(withheld)} events with receipts blocked, then restarted after SIGKILL.", flush=True)

    reconnect_start = time.monotonic()
    broker("broker-recovered")
    wait_for(
        "the recovered queue to drain",
        lambda: status().get("delivered") == args.events,
        timeout=45,
    )
    recovery_seconds = time.monotonic() - reconnect_start

    final_status = status()
    stored_rows = inbox_rows(receiver_db)
    records = {event_id: (json.loads(payload), attempts) for event_id, payload, attempts in stored_rows}
    stored_duplicates = len(stored_rows) - len(records)
    require(stored_duplicates == 0, "the receiver stored duplicate records")
    require(confirmed == set(records), "confirmed input IDs and stored output IDs differ")
    require(all(records[event_id][0] == readings[event_id] for event_id in confirmed), "output payloads differ")
    require(all(records[event_id][1] >= 2 for event_id in withheld), "unconfirmed events were not replayed")
    require(final_status.get("pending") == 0, "gateway did not finish draining")
    submit(ids[-1])
    submit(ids[-1], dict(readings[ids[-1]], value=999.0), 409)
    require(status().get("accepted") == args.events, "a repeated submission was counted again")

    return {
        "accepted_ids": len(confirmed),
        "stored_unique_ids": len(records),
        "missing_ids": len(confirmed - set(records)),
        "unexpected_ids": len(set(records) - confirmed),
        "duplicate_stored_records": stored_duplicates,
        "duplicate_deliveries_seen": sum(attempts - 1 for _, attempts in records.values()),
        "queued_during_outage": offline,
        "committed_without_receipt_before_restart": len(withheld),
        "remaining_pending": final_status["pending"],
        "recovery_seconds": round(recovery_seconds, 3),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/iot-edge-gateway")
    parser.add_argument("--broker", default="mosquitto")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    args.binary = args.binary.resolve()
    if args.events < 8:
        parser.error("--events must be at least 8")

    with tempfile.TemporaryDirectory(prefix="iot-gateway-demo-") as directory:
        work = Path(directory)
        try:
            with ExitStack() as cleanup:
                report = run(args, work, cleanup)
        except Exception:
            for log in sorted(work.glob("*.log")):
                print(f"\n{log.name}\n" + "\n".join(log.read_text(errors="replace").splitlines()[-25:]), file=sys.stderr)
            raise
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.report:
        args.report.write_text(rendered + "\n")


if __name__ == "__main__":
    main()
