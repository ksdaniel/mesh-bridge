"""Meshtastic bridge: listens for text messages from a node over WiFi (TCP)
and exposes a small HTTP API + web UI for reading and sending them."""

import logging
import os
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path

import meshtastic.tcp_interface
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pubsub import pub
from pydantic import BaseModel, Field

MESHTASTIC_HOST = os.getenv("MESHTASTIC_HOST", "10.0.0.153")
DB_PATH = os.getenv("DB_PATH", "/data/messages.db")
RECONNECT_SECONDS = 10
MAX_PAYLOAD_BYTES = 228  # Meshtastic text payload limit

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mesh-bridge")


# --- storage -----------------------------------------------------------------

class Store:
    def __init__(self, path: str):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                packet_id  INTEGER,
                direction  TEXT NOT NULL,          -- 'in' or 'out'
                from_id    TEXT,
                from_name  TEXT,
                to_id      TEXT,
                channel    INTEGER,
                text       TEXT NOT NULL,
                rx_snr     REAL,
                rx_rssi    INTEGER,
                created_at INTEGER NOT NULL,
                UNIQUE (packet_id, from_id)
            )""")
        self.db.commit()

    def add(self, **msg) -> dict | None:
        cols = ", ".join(msg)
        marks = ", ".join("?" for _ in msg)
        with self.lock:
            cur = self.db.execute(
                f"INSERT OR IGNORE INTO messages ({cols}) VALUES ({marks})", tuple(msg.values()))
            self.db.commit()
            if cur.rowcount == 0:  # duplicate (mesh retransmission)
                return None
            row = self.db.execute("SELECT * FROM messages WHERE id = ?", (cur.lastrowid,)).fetchone()
        return dict(row)

    def list(self, since_id: int, limit: int) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM (SELECT * FROM messages WHERE id > ? ORDER BY id DESC LIMIT ?) ORDER BY id",
                (since_id, limit)).fetchall()
        return [dict(r) for r in rows]


# --- radio connection --------------------------------------------------------

class Radio:
    """Keeps a TCP connection to the node open, reconnecting when it drops."""

    def __init__(self, host: str, store: Store):
        self.host = host
        self.store = store
        self.iface = None
        self.lost = threading.Event()
        pub.subscribe(self._on_text, "meshtastic.receive.text")
        pub.subscribe(self._on_lost, "meshtastic.connection.lost")
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            try:
                log.info("Connecting to %s ...", self.host)
                self.lost.clear()
                self.iface = meshtastic.tcp_interface.TCPInterface(hostname=self.host)
                log.info("Connected to %s as %s", self.host, self.my_id)
                self.lost.wait()
                log.warning("Connection lost")
            except Exception as e:
                log.warning("Connection failed: %s", e)
            iface, self.iface = self.iface, None
            if iface:
                try:
                    iface.close()
                except Exception:
                    pass
            time.sleep(RECONNECT_SECONDS)

    def _on_lost(self, interface, topic=pub.AUTO_TOPIC):
        if interface is self.iface:
            self.lost.set()  # close/reconnect happens in _run, not on the reader thread

    def _on_text(self, packet, interface):
        from_id = packet.get("fromId") or f"!{packet['from']:08x}"
        msg = self.store.add(
            packet_id=packet.get("id"),
            direction="in",
            from_id=from_id,
            from_name=self.node_name(from_id),
            to_id=packet.get("toId"),
            channel=packet.get("channel", 0),
            text=packet["decoded"]["text"],
            rx_snr=packet.get("rxSnr"),
            rx_rssi=packet.get("rxRssi"),
            created_at=int(packet.get("rxTime") or time.time()),
        )
        if msg:
            log.info("Message from %s: %s", from_id, msg["text"])

    @property
    def connected(self) -> bool:
        return self.iface is not None and not self.lost.is_set()

    @property
    def my_id(self) -> str | None:
        user = (self.iface.getMyNodeInfo() or {}).get("user", {}) if self.iface else {}
        return user.get("id")

    def node_name(self, node_id: str) -> str | None:
        nodes = (self.iface.nodes or {}) if self.iface else {}
        return nodes.get(node_id, {}).get("user", {}).get("longName")

    def nodes(self) -> list[dict]:
        nodes = (self.iface.nodes or {}).values() if self.iface else []
        return [
            {"id": n["user"]["id"], "long_name": n["user"].get("longName"),
             "short_name": n["user"].get("shortName"), "last_heard": n.get("lastHeard")}
            for n in nodes if "user" in n
        ]

    def send(self, text: str, to: str, channel: int) -> dict:
        if not self.connected:
            raise HTTPException(503, "Not connected to the Meshtastic node")
        pkt = self.iface.sendText(text, destinationId=to, channelIndex=channel)
        return self.store.add(
            packet_id=pkt.id,
            direction="out",
            from_id=self.my_id,
            from_name=self.node_name(self.my_id),
            to_id=to,
            channel=channel,
            text=text,
            created_at=int(time.time()),
        )


# --- API ---------------------------------------------------------------------

store: Store
radio: Radio


@asynccontextmanager
async def lifespan(app: FastAPI):
    global store, radio
    store = Store(DB_PATH)
    radio = Radio(MESHTASTIC_HOST, store)
    yield


app = FastAPI(title="Meshtastic Bridge", lifespan=lifespan)


class SendRequest(BaseModel):
    text: str = Field(min_length=1)
    to: str = "^all"  # "^all" for broadcast, or a node id like "!1d69cba4"
    channel: int = Field(0, ge=0, le=7)


@app.get("/api/status")
def status():
    return {"connected": radio.connected, "host": MESHTASTIC_HOST,
            "node_id": radio.my_id, "node_name": radio.node_name(radio.my_id)}


@app.get("/api/messages")
def list_messages(since_id: int = 0, limit: int = 200):
    return store.list(since_id, min(max(limit, 1), 1000))


@app.post("/api/messages", status_code=201)
def send_message(req: SendRequest):
    if len(req.text.encode()) > MAX_PAYLOAD_BYTES:
        raise HTTPException(422, f"Message is longer than {MAX_PAYLOAD_BYTES} bytes")
    return radio.send(req.text, req.to, req.channel)


@app.get("/api/nodes")
def list_nodes():
    return radio.nodes()


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
