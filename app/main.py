"""Meshtastic bridge: listens for text messages from a node over WiFi (TCP)
and exposes a small HTTP API + web UI for reading and sending them."""

import json
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
from meshtastic.protobuf import mesh_pb2, portnums_pb2
from pubsub import pub
from pydantic import BaseModel, Field

MESHTASTIC_HOST = os.getenv("MESHTASTIC_HOST", "10.0.0.153")
DB_PATH = os.getenv("DB_PATH", "/data/messages.db")
RECONNECT_SECONDS = 10
MAX_PAYLOAD_BYTES = 228  # Meshtastic text payload limit
TRACEROUTE_TIMEOUT = 120  # seconds before a traceroute with no reply counts as lost
UNKNOWN_SNR = -128
PACKET_LOG_MAX = int(os.getenv("PACKET_LOG_MAX", "50000"))  # oldest packets beyond this are dropped

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
        # Delivery info for sent messages, filled in when the ACK/NAK comes back.
        have = {r["name"] for r in self.db.execute("PRAGMA table_info(messages)")}
        for col, typ in [("ack_status", "TEXT"),  # pending | delivered | relayed | failed
                         ("ack_error", "TEXT"), ("ack_from", "TEXT"),
                         ("ack_hops", "INTEGER"), ("ack_relay", "INTEGER")]:
            if col not in have:
                self.db.execute(f"ALTER TABLE messages ADD COLUMN {col} {typ}")
        # message_id used to be NOT NULL; traceroutes started from the map have none.
        tr_cols = {r["name"]: r for r in self.db.execute("PRAGMA table_info(traceroutes)")}
        rebuild = bool(tr_cols) and tr_cols["message_id"]["notnull"]
        if rebuild:
            self.db.execute("ALTER TABLE traceroutes RENAME TO traceroutes_old")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS traceroutes (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER REFERENCES messages(id),  -- NULL when started from the map
                target_id  TEXT NOT NULL,
                status     TEXT NOT NULL,          -- pending | done | failed | timeout
                error      TEXT,
                towards    TEXT,                   -- JSON [{id, name, snr}], us -> target
                back       TEXT,                   -- JSON [{id, name, snr}], target -> us
                created_at INTEGER NOT NULL
            )""")
        if rebuild:
            self.db.execute("INSERT INTO traceroutes SELECT * FROM traceroutes_old")
            self.db.execute("DROP TABLE traceroutes_old")
        # Every packet heard over the radio, for the log page.
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS packets (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                packet_id  INTEGER,
                from_id    TEXT,
                from_name  TEXT,
                to_id      TEXT,
                channel    INTEGER,
                portnum    TEXT NOT NULL,          -- e.g. TEXT_MESSAGE_APP, or ENCRYPTED
                rx_snr     REAL,
                rx_rssi    INTEGER,
                hops       INTEGER,
                via_mqtt   INTEGER,
                data       TEXT NOT NULL,          -- JSON of the whole packet
                created_at INTEGER NOT NULL
            )""")
        self.db.execute("CREATE INDEX IF NOT EXISTS packets_portnum ON packets (portnum, id)")
        self.db.execute("CREATE INDEX IF NOT EXISTS packets_from ON packets (from_id, id)")
        self.packet_inserts = 0
        # Links reported by nodes running the NeighborInfo module.
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS neighbors (
                node_id     TEXT NOT NULL,
                neighbor_id TEXT NOT NULL,
                snr         REAL,
                updated_at  INTEGER NOT NULL,
                PRIMARY KEY (node_id, neighbor_id)
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

    def get(self, msg_id: int) -> dict | None:
        with self.lock:
            row = self.db.execute("SELECT * FROM messages WHERE id = ?", (msg_id,)).fetchone()
        return dict(row) if row else None

    def update_sent(self, packet_id: int, **fields):
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self.lock:
            self.db.execute(f"UPDATE messages SET {sets} WHERE packet_id = ? AND direction = 'out'",
                            (*fields.values(), packet_id))
            self.db.commit()

    def add_traceroute(self, message_id: int | None, target_id: str) -> int:
        with self.lock:
            cur = self.db.execute(
                "INSERT INTO traceroutes (message_id, target_id, status, created_at) VALUES (?, ?, 'pending', ?)",
                (message_id, target_id, int(time.time())))
            self.db.commit()
        return cur.lastrowid

    def update_traceroute(self, tr_id: int, **fields):
        fields = {k: json.dumps(v) if k in ("towards", "back") else v for k, v in fields.items()}
        sets = ", ".join(f"{k} = ?" for k in fields)
        with self.lock:
            self.db.execute(f"UPDATE traceroutes SET {sets} WHERE id = ?", (*fields.values(), tr_id))
            self.db.commit()

    def _traceroute_rows(self, where: str, args: tuple) -> list[dict]:
        with self.lock:
            self.db.execute(
                "UPDATE traceroutes SET status = 'timeout' WHERE status = 'pending' AND created_at < ?",
                (int(time.time()) - TRACEROUTE_TIMEOUT,))
            self.db.commit()
            rows = self.db.execute(f"SELECT * FROM traceroutes WHERE {where} ORDER BY id", args).fetchall()
        out = []
        for r in rows:
            r = dict(r)
            r["towards"] = json.loads(r["towards"]) if r["towards"] else None
            r["back"] = json.loads(r["back"]) if r["back"] else None
            out.append(r)
        return out

    def traceroutes(self, message_id: int) -> list[dict]:
        return self._traceroute_rows("message_id = ?", (message_id,))

    def latest_traceroutes(self) -> list[dict]:
        """Per target: the newest traceroute (to show whether one is running)
        and the newest successful one (the route drawn on the map)."""
        return self._traceroute_rows(
            "id IN (SELECT MAX(id) FROM traceroutes GROUP BY target_id)"
            " OR id IN (SELECT MAX(id) FROM traceroutes WHERE status = 'done' GROUP BY target_id)", ())

    def set_neighbors(self, node_id: str, neighbors: list[tuple[str, float | None]]):
        now = int(time.time())
        with self.lock:
            self.db.execute("DELETE FROM neighbors WHERE node_id = ?", (node_id,))
            self.db.executemany(
                "INSERT OR REPLACE INTO neighbors (node_id, neighbor_id, snr, updated_at) VALUES (?, ?, ?, ?)",
                [(node_id, nb, snr, now) for nb, snr in neighbors])
            self.db.commit()

    def add_packet(self, **pkt):
        cols = ", ".join(pkt)
        marks = ", ".join("?" for _ in pkt)
        with self.lock:
            self.db.execute(f"INSERT INTO packets ({cols}) VALUES ({marks})", tuple(pkt.values()))
            self.packet_inserts += 1
            if self.packet_inserts % 500 == 0:
                self.db.execute("DELETE FROM packets WHERE id <= (SELECT MAX(id) FROM packets) - ?",
                                (PACKET_LOG_MAX,))
            self.db.commit()

    def packets(self, since_id: int, before_id: int | None, limit: int,
                portnum: str | None, from_id: str | None) -> list[dict]:
        """Newest first. since_id returns only newer packets, before_id pages back."""
        where, args = ["id > ?"], [since_id]
        if before_id is not None:
            where.append("id < ?"); args.append(before_id)
        if portnum:
            where.append("portnum = ?"); args.append(portnum)
        if from_id:
            where.append("from_id = ?"); args.append(from_id)
        with self.lock:
            rows = self.db.execute(
                f"SELECT * FROM packets WHERE {' AND '.join(where)} ORDER BY id DESC LIMIT ?",
                (*args, limit)).fetchall()
        out = []
        for r in rows:
            r = dict(r)
            r["data"] = json.loads(r["data"])
            out.append(r)
        return out

    def packet_types(self) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT portnum, COUNT(*) AS count FROM packets GROUP BY portnum ORDER BY count DESC").fetchall()
        return [dict(r) for r in rows]

    def neighbors(self, max_age: int) -> list[dict]:
        with self.lock:
            rows = self.db.execute("SELECT * FROM neighbors WHERE updated_at > ?",
                                   (int(time.time()) - max_age,)).fetchall()
        return [dict(r) for r in rows]

    def list(self, since_id: int, limit: int) -> list[dict]:
        with self.lock:
            rows = self.db.execute(
                "SELECT * FROM (SELECT * FROM messages WHERE id > ? ORDER BY id DESC LIMIT ?) ORDER BY id",
                (since_id, limit)).fetchall()
        return [dict(r) for r in rows]


# --- radio connection --------------------------------------------------------

def _jsonable(v):
    """Packet dicts carry raw protobufs and bytes; keep only what JSON can hold."""
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items() if k != "raw"}
    if isinstance(v, (list, tuple)):
        return [_jsonable(x) for x in v]
    if isinstance(v, bytes):
        return v.hex()
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


class Radio:
    """Keeps a TCP connection to the node open, reconnecting when it drops."""

    def __init__(self, host: str, store: Store):
        self.host = host
        self.store = store
        self.iface = None
        self.lost = threading.Event()
        pub.subscribe(self._on_text, "meshtastic.receive.text")
        pub.subscribe(self._on_neighborinfo, "meshtastic.receive.neighborinfo")
        pub.subscribe(self._on_packet, "meshtastic.receive")  # parent topic: every packet
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

    def _on_packet(self, packet, interface):
        try:
            from_id = packet.get("fromId") or f"!{packet.get('from', 0):08x}"
            decoded = packet.get("decoded")
            hops = packet["hopStart"] - packet.get("hopLimit", 0) if "hopStart" in packet else None
            self.store.add_packet(
                packet_id=packet.get("id"),
                from_id=from_id,
                from_name=self.node_name(from_id),
                to_id=packet.get("toId") or f"!{packet.get('to', 0):08x}",
                channel=packet.get("channel", 0),
                portnum=decoded.get("portnum", "UNKNOWN_APP") if decoded else "ENCRYPTED",
                rx_snr=packet.get("rxSnr"),
                rx_rssi=packet.get("rxRssi"),
                hops=hops,
                via_mqtt=1 if packet.get("viaMqtt") else 0,
                data=json.dumps(_jsonable(packet)),
                created_at=int(packet.get("rxTime") or time.time()),
            )
        except Exception:
            log.exception("Could not log packet")

    def _on_neighborinfo(self, packet, interface):
        info = packet["decoded"]["neighborinfo"]
        node_id = f"!{info.get('nodeId', packet['from']):08x}"
        neighbors = [(f"!{n['nodeId']:08x}", n.get("snr")) for n in info.get("neighbors", [])]
        self.store.set_neighbors(node_id, neighbors)
        log.info("Neighbour info from %s: %d neighbours", node_id, len(neighbors))

    @property
    def connected(self) -> bool:
        return self.iface is not None and not self.lost.is_set()

    @property
    def my_id(self) -> str | None:
        user = (self.iface.getMyNodeInfo() or {}).get("user", {}) if self.iface else {}
        return user.get("id")

    @property
    def my_num(self) -> int | None:
        return self.iface.myInfo.my_node_num if self.iface and self.iface.myInfo else None

    def node_name(self, node_id: str) -> str | None:
        nodes = (self.iface.nodes or {}) if self.iface else {}
        return nodes.get(node_id, {}).get("user", {}).get("longName")

    def relay_candidates(self, relay_byte: int) -> list[dict]:
        """Packets only carry the last byte of the relaying node's number, so
        return every known node that matches it."""
        nodes = (self.iface.nodes or {}).values() if self.iface else []
        return [{"id": n["user"]["id"], "name": n["user"].get("longName")}
                for n in nodes if "user" in n and n.get("num", 0) & 0xFF == relay_byte]

    def nodes(self) -> list[dict]:
        nodes = (self.iface.nodes or {}).values() if self.iface else []
        me = self.my_id
        out = []
        for n in nodes:
            if "user" not in n:
                continue
            pos = n.get("position", {})
            has_pos = pos.get("latitude") and pos.get("longitude")  # 0/0 means "no fix"
            out.append({
                "id": n["user"]["id"], "long_name": n["user"].get("longName"),
                "short_name": n["user"].get("shortName"), "last_heard": n.get("lastHeard"),
                "is_me": n["user"]["id"] == me,
                "hops_away": 0 if n["user"]["id"] == me else n.get("hopsAway"),
                "snr": n.get("snr"),
                "lat": pos["latitude"] if has_pos else None,
                "lon": pos["longitude"] if has_pos else None,
                "altitude": pos.get("altitude"),
            })
        return out

    def send(self, text: str, to: str, channel: int) -> dict:
        if not self.connected:
            raise HTTPException(503, "Not connected to the Meshtastic node")
        pkt = self.iface.sendData(
            text.encode(), destinationId=to, portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
            wantAck=True, onResponse=self._on_ack, onResponseAckPermitted=True, channelIndex=channel)
        return self.store.add(
            packet_id=pkt.id,
            direction="out",
            from_id=self.my_id,
            from_name=self.node_name(self.my_id),
            to_id=to,
            channel=channel,
            text=text,
            created_at=int(time.time()),
            ack_status="pending",
        )

    def _on_ack(self, packet: dict):
        """ACK/NAK for a sent text. A DM is ACKed by the destination itself; for a
        broadcast our own node reports an implicit ACK once it hears a neighbour
        rebroadcast it."""
        error = packet["decoded"].get("routing", {}).get("errorReason", "NONE")
        from_num = packet["from"]
        if error != "NONE":
            status = "failed"
        elif from_num == self.my_num:
            status = "relayed"
        else:
            status = "delivered"
        hops = packet["hopStart"] - packet.get("hopLimit", 0) if "hopStart" in packet else None
        self.store.update_sent(
            packet["decoded"]["requestId"],
            ack_status=status,
            ack_error=None if error == "NONE" else error,
            ack_from=f"!{from_num:08x}",
            ack_hops=hops,
            ack_relay=packet.get("relayNode"),
        )

    def traceroute(self, message_id: int | None, target: str, channel: int) -> int:
        if not self.connected:
            raise HTTPException(503, "Not connected to the Meshtastic node")
        tr_id = self.store.add_traceroute(message_id, target)
        try:
            self.iface.sendData(
                mesh_pb2.RouteDiscovery(), destinationId=target,
                portNum=portnums_pb2.PortNum.TRACEROUTE_APP, wantResponse=True,
                onResponse=lambda p: self._on_traceroute(tr_id, p), channelIndex=channel)
        except Exception as e:
            self.store.update_traceroute(tr_id, status="failed", error=str(e))
        return tr_id

    def _hop(self, num: int, snr: int | None) -> dict:
        node_id = f"!{num:08x}"
        return {"id": node_id, "name": self.node_name(node_id),
                "snr": snr / 4 if snr is not None and snr != UNKNOWN_SNR else None}

    def _on_traceroute(self, tr_id: int, packet: dict):
        decoded = packet["decoded"]
        if decoded.get("portnum") == "ROUTING_APP":  # NAK, e.g. NO_RESPONSE
            self.store.update_traceroute(
                tr_id, status="failed", error=decoded.get("routing", {}).get("errorReason"))
            return
        rd = mesh_pb2.RouteDiscovery()
        rd.ParseFromString(decoded["payload"])
        # snr lists hold one entry per hop plus one for the final receiver
        snr_t = list(rd.snr_towards) if len(rd.snr_towards) == len(rd.route) + 1 else []
        nodes_t = [packet["to"], *rd.route, packet["from"]]  # us, relays, target
        towards = [self._hop(n, (None, *snr_t)[i] if snr_t else None) for i, n in enumerate(nodes_t)]
        back = None
        if "hopStart" in packet and len(rd.snr_back) == len(rd.route_back) + 1:
            nodes_b = [packet["from"], *rd.route_back, packet["to"]]
            back = [self._hop(n, (None, *rd.snr_back)[i]) for i, n in enumerate(nodes_b)]
        self.store.update_traceroute(tr_id, status="done", towards=towards, back=back)


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


class TracerouteRequest(BaseModel):
    to: str | None = None  # node to trace to; defaults to the message's destination


@app.get("/api/messages/{msg_id}/route")
def message_route(msg_id: int):
    msg = store.get(msg_id)
    if not msg:
        raise HTTPException(404, "No such message")
    relay = msg.get("ack_relay")
    return {
        "message": msg,
        "relay_candidates": radio.relay_candidates(relay) if relay is not None else [],
        "ack_from_name": radio.node_name(msg["ack_from"]) if msg.get("ack_from") else None,
        "traceroutes": store.traceroutes(msg_id),
    }


@app.post("/api/messages/{msg_id}/traceroute", status_code=202)
def start_traceroute(msg_id: int, req: TracerouteRequest):
    msg = store.get(msg_id)
    if not msg:
        raise HTTPException(404, "No such message")
    target = req.to or msg["to_id"]
    if target in ("^all", "!ffffffff"):
        raise HTTPException(422, "Pick a node to trace to; a broadcast has no single destination")
    return {"id": radio.traceroute(msg_id, target, msg["channel"] or 0)}


@app.get("/api/nodes")
def list_nodes():
    return radio.nodes()


@app.get("/api/packets")
def list_packets(since_id: int = 0, before_id: int | None = None, limit: int = 100,
                 portnum: str | None = None, from_id: str | None = None):
    rows = store.packets(since_id, before_id, min(max(limit, 1), 500), portnum, from_id)
    for r in rows:  # names may have arrived after the packet did
        r["from_name"] = r["from_name"] or radio.node_name(r["from_id"])
        r["to_name"] = radio.node_name(r["to_id"])
    return rows


@app.get("/api/packets/types")
def packet_types():
    return store.packet_types()


@app.post("/api/nodes/{node_id}/traceroute", status_code=202)
def trace_node(node_id: str):
    if node_id in ("^all", "!ffffffff"):
        raise HTTPException(422, "Pick a node to trace to")
    return {"id": radio.traceroute(None, node_id, 0)}


NEIGHBOR_MAX_AGE = 48 * 3600  # neighbour reports older than this are left off the map


@app.get("/api/map")
def map_data():
    """Everything the map needs: nodes (with positions where known), links
    between nodes, and the latest traceroute to each traced node."""
    nodes = radio.nodes()
    me = next((n["id"] for n in nodes if n["is_me"]), None)
    links = {}
    # Nodes our radio hears directly; SNR is how well we hear them.
    for n in nodes:
        if me and not n["is_me"] and n["hops_away"] == 0:
            links[frozenset((me, n["id"]))] = {"a": me, "b": n["id"], "snr": n["snr"], "source": "direct"}
    for nb in store.neighbors(NEIGHBOR_MAX_AGE):
        key = frozenset((nb["node_id"], nb["neighbor_id"]))
        links.setdefault(key, {"a": nb["node_id"], "b": nb["neighbor_id"], "snr": nb["snr"],
                               "source": "neighborinfo", "updated_at": nb["updated_at"]})
    return {"me": me, "nodes": nodes, "links": list(links.values()),
            "traceroutes": store.latest_traceroutes()}


@app.get("/")
def index():
    return FileResponse(Path(__file__).parent / "static" / "index.html")
