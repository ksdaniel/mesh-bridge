# Mesh Bridge

Connects to a Meshtastic node over WiFi (TCP port 4403), stores every text
message it receives in SQLite, and serves an HTTP API plus a small web UI for
reading and sending messages.

## Run

```sh
docker compose up -d --build
```

Open http://localhost:8000. Set `MESHTASTIC_HOST` in `docker-compose.yml` if
the node's IP changes. Messages are kept in `./data/messages.db`.

> The node accepts only **one** TCP client at a time, and each new connection
> drops the previous one. While the bridge runs, don't point the phone app or
> `meshtastic --host` at the same node, or they will keep kicking each other off.

## API

| Method | Path            | Description |
|--------|-----------------|-------------|
| GET    | `/api/status`   | Connection state and node identity |
| GET    | `/api/messages` | Stored messages, oldest first. `?since_id=N` returns only newer ones, `?limit=` caps the count (default 200) |
| POST   | `/api/messages` | Send a message: `{"text": "hi", "to": "^all", "channel": 0}`. `to` is `^all` or a node id like `!1d69cba4` |
| GET    | `/api/nodes`    | Nodes the radio knows about |
| GET    | `/api/messages/{id}/route` | Delivery info for a sent message (ACK status, hop count, last relay) and its traceroutes |
| POST   | `/api/messages/{id}/traceroute` | Start a traceroute: `{"to": "!1d69cba4"}`. `to` defaults to the message's destination and is required for broadcasts |

Interactive docs: http://localhost:8000/docs

```sh
curl -X POST localhost:8000/api/messages -H 'content-type: application/json' \
     -d '{"text": "hello mesh"}'
curl localhost:8000/api/messages
```

## Run without Docker

```sh
pip install -r requirements.txt
DB_PATH=./data/messages.db uvicorn app.main:app --port 8000
```
# mesh-bridge
