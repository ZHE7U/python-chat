"""终极局域网聊天室 —— 后端

设计要点（对应需求规格书的十个模块）：
  * messages 用自增 seq 作为全局顺序号，一切补发、分页、未读计算都基于 seq
  * 客户端生成 id 作为幂等键，重试不会产生重复消息
  * 一个昵称可以有多条连接（多个窗口），在线人数按不同昵称统计
  * 私聊被建模成"双人房间"，复用同一套消息收发与历史逻辑
"""

import asyncio
import json
import re
import sqlite3
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "chat.db"
STATIC_DIR = BASE_DIR / "static"

MAX_TEXT_LEN = 500
MAX_NAME_LEN = 20
RECALL_WINDOW = timedelta(minutes=2)
EDIT_WINDOW = timedelta(minutes=10)
RATE_LIMIT_COUNT = 10
RATE_LIMIT_WINDOW = 5.0
CATCHUP_LIMIT = 200
HISTORY_PAGE = 30
PONG_TIMEOUT = 45.0
REAP_INTERVAL = 15.0

LOBBY = "lobby"
NAME_RE = re.compile(r"^[^\s:|]{1,%d}$" % MAX_NAME_LEN)

app = FastAPI()


# --------------------------------------------------------------------------
# 数据库
# --------------------------------------------------------------------------

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS rooms (
           id TEXT PRIMARY KEY,
           name TEXT NOT NULL,
           kind TEXT NOT NULL,
           created_by TEXT,
           created_at TEXT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS messages (
           seq INTEGER PRIMARY KEY AUTOINCREMENT,
           id TEXT NOT NULL UNIQUE,
           room_id TEXT NOT NULL,
           sender TEXT NOT NULL,
           content TEXT NOT NULL,
           created_at TEXT NOT NULL,
           edited_at TEXT,
           is_recalled INTEGER NOT NULL DEFAULT 0)""",
    "CREATE INDEX IF NOT EXISTS idx_messages_room_seq ON messages(room_id, seq)",
    """CREATE TABLE IF NOT EXISTS message_reads (
           seq INTEGER NOT NULL,
           username TEXT NOT NULL,
           read_at TEXT NOT NULL,
           PRIMARY KEY (seq, username))""",
    """CREATE TABLE IF NOT EXISTS room_reads (
           username TEXT NOT NULL,
           room_id TEXT NOT NULL,
           last_read_seq INTEGER NOT NULL DEFAULT 0,
           PRIMARY KEY (username, room_id))""",
    """CREATE TABLE IF NOT EXISTS dm_members (
           room_id TEXT NOT NULL,
           username TEXT NOT NULL,
           PRIMARY KEY (room_id, username))""",
]


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def init_db() -> None:
    conn = db()
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
        if cols and "room_id" not in cols:
            _migrate_v1_to_v2(conn)
        for stmt in SCHEMA:
            conn.execute(stmt)
        conn.execute(
            "INSERT OR IGNORE INTO rooms (id, name, kind, created_by, created_at)"
            " VALUES (?, ?, 'public', NULL, ?)",
            (LOBBY, "大厅", now_iso()),
        )
        conn.commit()
    finally:
        conn.close()


def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
    """旧库（没有 room_id、read_by 是逗号串）平滑升级，消息一条不丢。"""
    rows = conn.execute(
        "SELECT id, sender, content, timestamp, is_recalled, is_edited, read_by"
        " FROM messages ORDER BY timestamp ASC"
    ).fetchall()
    conn.execute("ALTER TABLE messages RENAME TO messages_v1")
    for stmt in SCHEMA:
        conn.execute(stmt)
    for r in rows:
        cur = conn.execute(
            "INSERT INTO messages (id, room_id, sender, content, created_at, is_recalled)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (r["id"], LOBBY, r["sender"], r["content"], r["timestamp"], r["is_recalled"] or 0),
        )
        seq = cur.lastrowid
        if r["is_edited"]:
            conn.execute("UPDATE messages SET edited_at=? WHERE seq=?", (r["timestamp"], seq))
        for name in (r["read_by"] or "").split(","):
            name = name.strip()
            if name:
                conn.execute(
                    "INSERT OR IGNORE INTO message_reads (seq, username, read_at)"
                    " VALUES (?, ?, ?)", (seq, name, r["timestamp"]))
    conn.execute("DROP TABLE messages_v1")
    conn.commit()


# --------------------------------------------------------------------------
# 数据访问
# --------------------------------------------------------------------------

def get_room(conn, room_id: str):
    return conn.execute("SELECT * FROM rooms WHERE id=?", (room_id,)).fetchone()


def dm_room_id(a: str, b: str) -> str:
    # 排序保证 A→B 和 B→A 落在同一个房间
    return "dm:" + "|".join(sorted((a, b)))


def room_allows(conn, room_id: str, username: str) -> bool:
    room = get_room(conn, room_id)
    if room is None:
        return False
    if room["kind"] == "public":
        return True
    return conn.execute(
        "SELECT 1 FROM dm_members WHERE room_id=? AND username=?",
        (room_id, username)).fetchone() is not None


def room_member_names(conn, room_id: str):
    """公开房间返回 None（表示所有人可见），私聊返回两个昵称。"""
    room = get_room(conn, room_id)
    if room is None:
        return set()
    if room["kind"] == "public":
        return None
    return {r["username"] for r in conn.execute(
        "SELECT username FROM dm_members WHERE room_id=?", (room_id,))}


def ensure_dm(conn, a: str, b: str) -> str:
    rid = dm_room_id(a, b)
    conn.execute(
        "INSERT OR IGNORE INTO rooms (id, name, kind, created_by, created_at)"
        " VALUES (?, ?, 'dm', ?, ?)", (rid, "私聊", a, now_iso()))
    conn.execute("INSERT OR IGNORE INTO dm_members (room_id, username) VALUES (?, ?)", (rid, a))
    conn.execute("INSERT OR IGNORE INTO dm_members (room_id, username) VALUES (?, ?)", (rid, b))
    conn.commit()
    return rid


def unread_count(conn, username: str, room_id: str) -> int:
    last = read_state(conn, username, room_id)
    return conn.execute(
        "SELECT count(*) FROM messages WHERE room_id=? AND seq>? AND sender<>?",
        (room_id, last, username)).fetchone()[0]


def read_state(conn, username: str, room_id: str) -> int:
    row = conn.execute(
        "SELECT last_read_seq FROM room_reads WHERE username=? AND room_id=?",
        (username, room_id)).fetchone()
    return row["last_read_seq"] if row else 0


def message_payload(row, read_by=None) -> dict:
    recalled = bool(row["is_recalled"])
    return {
        "type": "msg",
        "id": row["id"],
        "seq": row["seq"],
        "room": row["room_id"],
        "from": row["sender"],
        "text": "" if recalled else row["content"],
        "ts": row["created_at"],
        "is_recalled": recalled,
        "is_edited": row["edited_at"] is not None,
        "read_by": read_by or [],
    }


def fetch_messages(conn, room_id: str, after_seq=None, before_seq=None, limit=HISTORY_PAGE):
    if after_seq is not None:
        rows = conn.execute(
            "SELECT * FROM messages WHERE room_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
            (room_id, after_seq, limit)).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM messages WHERE room_id=? AND seq<? ORDER BY seq DESC LIMIT ?",
            (room_id, before_seq or 10 ** 18, limit)).fetchall()
        rows = list(reversed(rows))
    return rows


def read_by_map(conn, seqs):
    if not seqs:
        return {}
    marks = ",".join("?" * len(seqs))
    out = defaultdict(list)
    for r in conn.execute(
            "SELECT seq, username FROM message_reads WHERE seq IN (%s) ORDER BY read_at ASC" % marks,
            tuple(seqs)):
        out[r["seq"]].append(r["username"])
    return out


def display_name(conn, room_row, me: str) -> str:
    if room_row["kind"] == "public":
        return room_row["name"]
    return next((n for n in (room_member_names(conn, room_row["id"]) or set())
                 if n != me), "对方")


# --------------------------------------------------------------------------
# 连接管理
# --------------------------------------------------------------------------

class Client:
    def __init__(self, ws: WebSocket, username: str, ip: str = ""):
        self.ws = ws
        self.username = username
        self.ip = ip
        self.lock = asyncio.Lock()
        self.last_seen = time.monotonic()

    async def send(self, payload: dict) -> bool:
        try:
            async with self.lock:
                await self.ws.send_json(payload)
            return True
        except Exception:
            return False


class Hub:
    def __init__(self):
        self.clients = defaultdict(set)

    def add(self, c: Client):
        self.clients[c.username].add(c)

    def remove(self, c: Client):
        group = self.clients.get(c.username)
        if group is None:
            return
        group.discard(c)
        if not group:
            self.clients.pop(c.username, None)

    def users(self):
        return sorted(self.clients.keys())

    async def send_user(self, username: str, payload: dict):
        dead = [c for c in list(self.clients.get(username, ())) if not await c.send(payload)]
        for c in dead:
            self.remove(c)

    async def send_many(self, usernames, payload: dict):
        for name in list(usernames):
            await self.send_user(name, payload)

    async def broadcast(self, payload: dict):
        await self.send_many(list(self.clients.keys()), payload)

    async def send_room(self, room_id: str, payload: dict, members):
        target = self.clients.keys() if members is None else members
        await self.send_many(target, payload)

    async def presence(self):
        users = self.users()
        await self.broadcast({"type": "presence", "users": users, "count": len(users)})


hub = Hub()


class IPSessions:
    """一个 IP 只对应一个账号；同一账号可以开多个窗口（多标签页）。

    ip -> {username: 该账号在这个 IP 上的连接数}
    最后一条连接断开后就释放这个 IP 的绑定。
    """

    def __init__(self):
        self.claims = {}

    def holder(self, ip: str):
        group = self.claims.get(ip)
        return next(iter(group)) if group else None

    def allow(self, ip: str, username: str) -> bool:
        holder = self.holder(ip)
        return holder is None or holder == username

    def add(self, ip: str, username: str) -> None:
        group = self.claims.setdefault(ip, {})
        group[username] = group.get(username, 0) + 1

    def remove(self, ip: str, username: str) -> None:
        group = self.claims.get(ip)
        if not group:
            return
        left = group.get(username, 0) - 1
        if left > 0:
            group[username] = left
            return
        group.pop(username, None)
        if not group:
            self.claims.pop(ip, None)


ip_sessions = IPSessions()


# --------------------------------------------------------------------------
# 校验与限流
# --------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, count, window):
        self.count = count
        self.window = window
        self.hits = defaultdict(deque)

    def allow(self, username: str) -> bool:
        q = self.hits[username]
        now = time.monotonic()
        while q and now - q[0] > self.window:
            q.popleft()
        if len(q) >= self.count:
            return False
        q.append(now)
        return True


limiter = RateLimiter(RATE_LIMIT_COUNT, RATE_LIMIT_WINDOW)


def valid_name(name: str) -> bool:
    return bool(name) and bool(NAME_RE.match(name))


class Reject(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message


def clean_text(raw) -> str:
    if not isinstance(raw, str):
        raise Reject("bad_type", "消息格式不对")
    text = raw.strip()
    if not text:
        raise Reject("empty", "消息不能为空")
    if len(text) > MAX_TEXT_LEN:
        raise Reject("too_long", "消息最多 %d 字" % MAX_TEXT_LEN)
    return text


# --------------------------------------------------------------------------
# 业务处理
# --------------------------------------------------------------------------

async def push_rooms(client: Client):
    conn = db()
    try:
        rooms = []
        for row in conn.execute("SELECT * FROM rooms ORDER BY (kind='dm'), created_at ASC"):
            if row["kind"] == "dm" and not room_allows(conn, row["id"], client.username):
                continue
            rooms.append({
                "id": row["id"],
                "name": display_name(conn, row, client.username),
                "kind": row["kind"],
                "unread": unread_count(conn, client.username, row["id"]),
            })
    finally:
        conn.close()
    await client.send({"type": "rooms", "rooms": rooms})


async def handle_send(client: Client, data: dict):
    conn = db()
    try:
        room_id = data.get("room") or LOBBY
        if not room_allows(conn, room_id, client.username):
            raise Reject("no_room", "房间不存在或你不在这个房间里")
        text = clean_text(data.get("text"))
        msg_id = str(data.get("client_id") or uuid.uuid4())

        existing = conn.execute("SELECT * FROM messages WHERE id=?", (msg_id,)).fetchone()
        if existing is not None:
            # 重试命中同一条：先于限流判断，保证重试不会被"发送太快"挡掉
            await client.send({
                "type": "ack", "client_id": msg_id, "id": existing["id"],
                "seq": existing["seq"], "room": existing["room_id"], "ts": existing["created_at"],
            })
            return

        if not limiter.allow(client.username):
            raise Reject("too_fast", "发送太快了，缓一缓")

        cur = conn.execute(
            "INSERT INTO messages (id, room_id, sender, content, created_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (msg_id, room_id, client.username, text, now_iso()))
        conn.commit()
        row = conn.execute("SELECT * FROM messages WHERE seq=?", (cur.lastrowid,)).fetchone()
        payload = message_payload(row)
        members = room_member_names(conn, room_id)
    finally:
        conn.close()

    await client.send({"type": "ack", "client_id": msg_id, "id": payload["id"],
                       "seq": payload["seq"], "room": payload["room"], "ts": payload["ts"]})
    await hub.send_room(room_id, payload, members)


async def handle_recall(client: Client, data: dict):
    conn = db()
    try:
        msg_id = data.get("id")
        row = conn.execute("SELECT * FROM messages WHERE id=?", (msg_id,)).fetchone()
        if row is None or not room_allows(conn, row["room_id"], client.username):
            raise Reject("not_found", "找不到这条消息")
        if row["sender"] != client.username:
            raise Reject("not_owner", "只能撤回自己发的消息")
        if row["is_recalled"]:
            return
        if datetime.now() - datetime.fromisoformat(row["created_at"]) > RECALL_WINDOW:
            raise Reject("too_late", "超过 2 分钟，无法撤回")
        conn.execute("UPDATE messages SET is_recalled=1 WHERE id=?", (msg_id,))
        conn.commit()
        room_id, seq = row["room_id"], row["seq"]
        members = room_member_names(conn, room_id)
    finally:
        conn.close()

    await hub.send_room(room_id, {"type": "recall", "room": room_id, "id": msg_id, "seq": seq}, members)


async def handle_edit(client: Client, data: dict):
    conn = db()
    try:
        msg_id = data.get("id")
        row = conn.execute("SELECT * FROM messages WHERE id=?", (msg_id,)).fetchone()
        if row is None or not room_allows(conn, row["room_id"], client.username):
            raise Reject("not_found", "找不到这条消息")
        if row["sender"] != client.username:
            raise Reject("not_owner", "只能编辑自己发的消息")
        if row["is_recalled"]:
            raise Reject("recalled", "该消息已撤回，不能编辑")
        if datetime.now() - datetime.fromisoformat(row["created_at"]) > EDIT_WINDOW:
            raise Reject("too_late", "超过 10 分钟，无法编辑")
        text = clean_text(data.get("text"))
        ts = now_iso()
        conn.execute("UPDATE messages SET content=?, edited_at=? WHERE id=?", (text, ts, msg_id))
        conn.commit()
        room_id = row["room_id"]
        members = room_member_names(conn, room_id)
    finally:
        conn.close()

    await hub.send_room(room_id, {
        "type": "edit", "room": room_id, "id": msg_id, "text": text, "edited_at": ts}, members)


async def handle_read(client: Client, data: dict):
    conn = db()
    seq = 0
    prev = 0
    try:
        room_id = data.get("room") or LOBBY
        if not room_allows(conn, room_id, client.username):
            raise Reject("no_room", "房间不存在或你不在这个房间里")
        try:
            seq = int(data.get("seq") or 0)
        except (TypeError, ValueError):
            raise Reject("bad_seq", "已读位置无效")
        if seq <= 0:
            return
        prev = read_state(conn, client.username, room_id)
        advanced = seq > prev
        if advanced:
            conn.execute(
                "INSERT INTO room_reads (username, room_id, last_read_seq) VALUES (?, ?, ?)"
                " ON CONFLICT(username, room_id) DO UPDATE SET last_read_seq=excluded.last_read_seq",
                (client.username, room_id, seq))
            conn.execute(
                "INSERT OR IGNORE INTO message_reads (seq, username, read_at)"
                " SELECT seq, ?, ? FROM messages WHERE room_id=? AND seq<=? AND sender<>?",
                (client.username, now_iso(), room_id, seq, client.username))
            conn.commit()
        members = room_member_names(conn, room_id)
    finally:
        conn.close()

    if not advanced:
        # 位置没有前进就不必再广播，避免重复回执刷屏
        return
    await hub.send_room(room_id, {
        "type": "read", "room": room_id, "user": client.username, "seq": seq}, members)


async def handle_open_room(client: Client, data: dict):
    conn = db()
    try:
        room_id = data.get("room") or LOBBY
        if not room_allows(conn, room_id, client.username):
            raise Reject("no_room", "房间不存在或你不在这个房间里")
        room = get_room(conn, room_id)
        try:
            after = int(data.get("after_seq") or 0)
        except (TypeError, ValueError):
            after = 0
        rows = fetch_messages(conn, room_id, after_seq=after, limit=CATCHUP_LIMIT + 1)
        truncated = len(rows) > CATCHUP_LIMIT
        rows = rows[:CATCHUP_LIMIT]
        oldest = conn.execute(
            "SELECT min(seq) AS m FROM messages WHERE room_id=?", (room_id,)).fetchone()["m"] or 0
        rb = read_by_map(conn, [r["seq"] for r in rows])
        msgs = [message_payload(r, rb.get(r["seq"], [])) for r in rows]
        info = {
            "id": room["id"],
            "name": display_name(conn, room, client.username),
            "kind": room["kind"],
            "last_read_seq": read_state(conn, client.username, room_id),
        }
    finally:
        conn.close()

    await client.send({
        "type": "room_state", "room": info,
        "messages": msgs,
        "truncated": truncated,
        # after 比服务器现存最早一条还旧，说明中间那段已经拿不到了
        "gap": bool(after and oldest and after < oldest - 1),
    })


async def handle_history(client: Client, data: dict):
    conn = db()
    try:
        room_id = data.get("room") or LOBBY
        if not room_allows(conn, room_id, client.username):
            raise Reject("no_room", "房间不存在或你不在这个房间里")
        try:
            before = int(data.get("before_seq") or 0) or None
        except (TypeError, ValueError):
            before = None
        rows = fetch_messages(conn, room_id, before_seq=before, limit=HISTORY_PAGE + 1)
        has_more = len(rows) > HISTORY_PAGE
        rows = rows[-HISTORY_PAGE:] if has_more else rows
        rb = read_by_map(conn, [r["seq"] for r in rows])
        msgs = [message_payload(r, rb.get(r["seq"], [])) for r in rows]
    finally:
        conn.close()

    await client.send({"type": "history", "room": room_id, "messages": msgs, "has_more": has_more})


async def handle_create_room(client: Client, data: dict):
    name = (data.get("name") or "").strip()
    if not name:
        raise Reject("empty_name", "房间名不能为空")
    if len(name) > MAX_NAME_LEN:
        raise Reject("name_too_long", "房间名最多 %d 个字" % MAX_NAME_LEN)
    if ":" in name or "|" in name:
        raise Reject("bad_name", "房间名不能包含 : 或 |")
    room_id = "pub:" + name
    conn = db()
    try:
        if get_room(conn, room_id) is not None:
            raise Reject("exists", "已经有同名房间了")
        conn.execute(
            "INSERT INTO rooms (id, name, kind, created_by, created_at)"
            " VALUES (?, ?, 'public', ?, ?)", (room_id, name, client.username, now_iso()))
        conn.commit()
    finally:
        conn.close()

    await hub.broadcast({"type": "room_created", "room": {"id": room_id, "name": name, "kind": "public"}})
    for name_ in hub.users():
        for c in list(hub.clients[name_]):
            await push_rooms(c)


async def handle_open_dm(client: Client, data: dict):
    peer = (data.get("peer") or "").strip()
    if not valid_name(peer):
        raise Reject("bad_peer", "请选择要私聊的人")
    if peer == client.username:
        raise Reject("self_dm", "不能和自己私聊")
    conn = db()
    try:
        rid = ensure_dm(conn, client.username, peer)
    finally:
        conn.close()
    info = {"id": rid, "name": peer, "kind": "dm"}

    await client.send({"type": "dm_opened", "room": info})
    await push_rooms(client)
    if peer in hub.clients:
        for c in list(hub.clients[peer]):
            await push_rooms(c)
            await c.send({"type": "dm_invite",
                          "room": {"id": rid, "name": client.username, "kind": "dm"}})


async def handle_ping(client: Client, data: dict):
    await client.send({"type": "pong", "ts": time.time()})


async def handle_rooms(client: Client, data: dict):
    await push_rooms(client)


HANDLERS = {
    "send": handle_send,
    "recall": handle_recall,
    "edit": handle_edit,
    "read": handle_read,
    "open_room": handle_open_room,
    "history": handle_history,
    "create_room": handle_create_room,
    "open_dm": handle_open_dm,
    "rooms": handle_rooms,
    "ping": handle_ping,
}


# --------------------------------------------------------------------------
# WebSocket 入口
# --------------------------------------------------------------------------

async def reap_loop():
    """心跳超时的连接要清掉，否则在线列表里会留下幽灵用户。"""
    while True:
        await asyncio.sleep(REAP_INTERVAL)
        now = time.monotonic()
        for name, group in list(hub.clients.items()):
            for c in list(group):
                if now - c.last_seen > PONG_TIMEOUT:
                    hub.remove(c)
                    try:
                        await c.ws.close()
                    except Exception:
                        pass
        await hub.presence()


@app.on_event("startup")
async def on_startup():
    init_db()
    asyncio.create_task(reap_loop())


@app.websocket("/ws/{username}")
async def websocket_endpoint(websocket: WebSocket, username: str):
    if not valid_name(username):
        await websocket.close(code=1008)
        return

    ip = websocket.client.host if websocket.client else ""
    holder = ip_sessions.holder(ip)
    if holder is not None and holder != username:
        # 先完成握手再回一条可读的原因，前端收到后直接退回登录页
        await websocket.accept()
        await websocket.send_json({
            "type": "reject", "code": "ip_taken",
            "message": "这台设备（%s）已经登录了「%s」，一个 IP 只能对应一个账号" % (ip, holder),
        })
        await websocket.close(code=1008)
        return

    ip_sessions.add(ip, username)
    client = None
    try:
        await websocket.accept()
        client = Client(websocket, username, ip)
        hub.add(client)
        try:
            conn = db()
            try:
                rooms = [dict(r) for r in conn.execute("SELECT * FROM rooms ORDER BY created_at")]
            finally:
                conn.close()
            await client.send({"type": "hello", "you": username, "rooms": rooms})
            await push_rooms(client)
            await hub.presence()

            while True:
                raw = await websocket.receive_text()
                client.last_seen = time.monotonic()
                try:
                    data = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    await client.send({"type": "error", "code": "bad_json",
                                       "message": "服务器看不懂这条数据"})
                    continue
                if not isinstance(data, dict):
                    await client.send({"type": "error", "code": "bad_json",
                                       "message": "服务器看不懂这条数据"})
                    continue
                handler = HANDLERS.get(data.get("type"))
                if handler is None:
                    await client.send({"type": "error", "code": "unknown_type",
                                       "message": "不支持的操作"})
                    continue
                try:
                    await handler(client, data)
                except Reject as e:
                    await client.send({"type": "reject", "code": e.code, "message": e.message,
                                       "client_id": data.get("client_id")})
                except Exception as e:  # 单个请求出错不能把整条连接带下去
                    await client.send({"type": "reject", "code": "server_error",
                                       "message": "服务器处理失败：%s" % type(e).__name__,
                                       "client_id": data.get("client_id")})
        except WebSocketDisconnect:
            pass
    finally:
        # 无论怎么退出都要摘掉这一条；同名的其他窗口不受影响
        if client is not None:
            hub.remove(client)
            await hub.presence()
        ip_sessions.remove(ip, username)


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3000)
