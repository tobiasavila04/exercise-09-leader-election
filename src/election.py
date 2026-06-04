import os
import threading
import time
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

MY_ID = int(os.getenv("MY_ID", "0"))
MY_URL = os.getenv("MY_URL", "http://127.0.0.1:8000")
PEERS_CFG = os.getenv("PEERS_CFG", "")
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5"))
REQ_TIMEOUT = float(os.getenv("REQ_TIMEOUT", "2"))

leader_info = None
last_ok = None
electing = False
monitor_on = False
mtx = threading.Lock()


def _fmt(raw):
    raw = raw.strip()
    if raw and not raw.startswith("http"):
        raw = f"http://{raw}"
    return raw.rstrip("/")


def _peers(raw):
    result = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" in chunk:
            pid, purl = chunk.split("=", 1)
            try:
                pid = int(pid.strip())
            except ValueError:
                pid = None
            result.append({"id": pid, "url": _fmt(purl)})
        else:
            result.append({"id": None, "url": _fmt(chunk)})
    return [p for p in result if p.get("url")]


PEERS = _peers(PEERS_CFG)


def _others():
    return [p for p in PEERS if p["url"] != MY_URL]


def _higher():
    h = [p for p in _others() if p.get("id") is not None and p["id"] > MY_ID]
    return sorted(h, key=lambda x: x["id"]) if h else [p for p in _others() if p.get("id") is None]


def _call(method, base, path, data=None):
    url = f"{base.rstrip('/')}{path}"
    if method == "post":
        return requests.post(url, json=data or {}, timeout=REQ_TIMEOUT)
    return requests.get(url, timeout=REQ_TIMEOUT)


def state():
    with mtx:
        return {
            "my_id": MY_ID,
            "my_url": MY_URL,
            "leader": leader_info,
            "electing": electing,
            "last_seen": last_ok.isoformat() if last_ok else None,
            "peers": _others(),
        }


def elect():
    global leader_info, electing
    with mtx:
        if electing:
            return {"status": "busy"}
        electing = True
        leader_info = None

    targets = _higher()
    if not targets:
        return _win()

    alive = False
    for t in targets:
        try:
            r = _call("post", t["url"], "/election", {"from": MY_ID})
            if r.ok:
                alive = True
        except requests.RequestException:
            continue

    if alive:
        return {"status": "waiting"}

    return _win()


def on_election(from_id):
    if from_id is None:
        from_id = 0
    if from_id < MY_ID or MY_ID == 0:
        with mtx:
            if not electing:
                t = threading.Thread(target=elect, daemon=True)
                t.start()
        return {"ok": True}
    return {"ok": False}


def _win():
    global leader_info, last_ok, electing
    with mtx:
        leader_info = {"id": MY_ID, "url": MY_URL}
        last_ok = datetime.now(timezone.utc)
        electing = False

    for p in _others():
        try:
            _call("post", p["url"], "/coordinator", {"id": MY_ID, "url": MY_URL})
        except requests.RequestException:
            continue

    return {"leader_id": MY_ID, "leader_url": MY_URL}


def on_coordinator(cid, curl):
    global leader_info, last_ok, electing
    if cid is None or not curl:
        return {"status": "bad"}
    curl = _fmt(curl)
    with mtx:
        if cid < MY_ID and MY_ID != 0:
            threading.Thread(target=elect, daemon=True).start()
            return {"status": "ignored"}
        leader_info = {"id": cid, "url": curl}
        last_ok = datetime.now(timezone.utc)
        electing = False
    return {"status": "ok", "leader_id": cid}


def check():
    global leader_info, last_ok, electing
    with mtx:
        if electing:
            return {"status": "electing"}
        ldr = leader_info

    if ldr is None:
        return elect()

    try:
        r = _call("get", ldr["url"], "/ping")
        if r.ok:
            with mtx:
                last_ok = datetime.now(timezone.utc)
            return {"status": "alive", "id": ldr["id"]}
    except requests.RequestException:
        pass

    with mtx:
        leader_info = None
        electing = False
    return elect()


def _loop():
    while True:
        check()
        time.sleep(CHECK_INTERVAL)


def start():
    global monitor_on
    with mtx:
        if monitor_on:
            return
        monitor_on = True
        threading.Thread(target=_loop, daemon=True).start()
