"""按需求规格书的"验收例子"逐条验证。（测试副本专用）"""
import asyncio
import json
import sqlite3
import sys
import time
import os
import websockets

BASE = os.environ.get("PYCHAT_WS", "ws://127.0.0.1:3002/ws/")
DB = os.environ.get("PYCHAT_DB", "chat.db")

passed = failed = 0

def ok(name, cond, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print("  [PASS] %s" % name)
    else:
        failed += 1
        print("  [FAIL] %s %s" % (name, detail))

class C:
    def __init__(self, ws, name):
        self.ws = ws
        self.name = name
        self.buf = []
        self.outbox = asyncio.Queue()
        self.task = asyncio.create_task(self._pump())
        self.wtask = asyncio.create_task(self._writer())

    async def _writer(self):
        # 自动限速，避免测试自己触发服务端的"发送太快"
        times = []
        try:
            while True:
                raw, paced = await self.outbox.get()
                if paced:
                    now = time.monotonic()
                    times = [t for t in times if now - t < 5.0]
                    if len(times) >= 7:
                        await asyncio.sleep(5.0 - (now - times[0]) + 0.4)
                        times = []
                    times.append(time.monotonic())
                await self.ws.send(raw)
        except Exception:
            pass

    async def _pump(self):
        try:
            async for raw in self.ws:
                self.buf.append(json.loads(raw))
        except Exception:
            pass

    async def wait(self, typ, pred=None, timeout=12.0):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            for i, m in enumerate(self.buf):
                if m.get("type") == typ and (pred is None or pred(m)):
                    return self.buf.pop(i)
            await asyncio.sleep(0.02)
        return None

    async def drain(self, t=0.15):
        await asyncio.sleep(t)
        self.buf.clear()

    def send(self, **kw):
        self.outbox.put_nowait((json.dumps(kw), True))

    def flood(self, **kw):
        self.outbox.put_nowait((json.dumps(kw), False))

    async def close(self):
        self.task.cancel()
        self.wtask.cancel()
        await self.ws.close()


# 测试里用不同的回环地址模拟"不同设备"，因为一个 IP 只允许一个账号
IP = {"甲": "127.0.0.1", "乙": "127.0.0.2", "丙": "127.0.0.3"}


async def conn(name):
    ws = await websockets.connect(BASE + name, local_addr=(IP.get(name, "127.0.0.1"), 0))
    c = C(ws, name)
    await c.wait("hello")
    return c


async def main():
    print("=" * 60)
    print("模块五：在线成员与同名用户")
    print("=" * 60)
    a = await conn("甲")
    a2 = await conn("甲")
    b = await conn("乙")
    p = await a.wait("presence", lambda x: sorted(x["users"]) == ["乙", "甲"])
    ok("同名两个窗口只算一个人在线", p and p["count"] == 2 and sorted(p["users"]) == ["乙", "甲"],
       detail=str(p))
    await a2.close()
    await asyncio.sleep(0.4)
    a.buf.clear()
    a.send(type="ping")
    ok("关掉一个窗口后自己仍在线", (await a.wait("pong")) is not None)
    b.buf.clear()
    b.send(type="send", room="lobby", text="窗口还在吗", client_id="k1")
    got = await a.wait("msg", lambda m: m.get("text") == "窗口还在吗")
    ok("关掉一个窗口后仍能收到消息", got is not None)

    print()
    print("=" * 60)
    print("模块一：已读与未读 / 模块八：未读数量")
    print("=" * 60)
    a.send(type="send", room="lobby", text="已读测试", client_id="r1")
    m = await b.wait("msg", lambda x: x.get("text") == "已读测试")
    ok("B 收到消息", m is not None)
    b.send(type="read", room="lobby", seq=m["seq"])
    upd = await a.wait("read", lambda x: x.get("user") == "乙")
    ok("A 收到别人的已读回执", upd is not None and upd["seq"] == m["seq"])
    b.send(type="read", room="lobby", seq=m["seq"])
    await asyncio.sleep(0.3)
    dups = [x for x in a.buf if x.get("type") == "read" and x.get("user") == "乙"]
    ok("重复回执不会重复计数", len(dups) == 0, detail=str(dups))
    b.send(type="rooms")
    rooms = await b.wait("rooms")
    lobby = next(r for r in rooms["rooms"] if r["id"] == "lobby")
    ok("已读后未读数归零", lobby["unread"] == 0, detail=str(lobby))

    print()
    print("=" * 60)
    print("模块二：消息撤回")
    print("=" * 60)
    a.send(type="send", room="lobby", text="要撤回的", client_id="rc1")
    m2 = await a.wait("ack", lambda x: x.get("client_id") == "rc1")
    a.send(type="recall", room="lobby", id=m2["id"])
    rc = await b.wait("recall", lambda x: x.get("id") == m2["id"])
    ok("撤回后其他人收到通知", rc is not None)
    b.send(type="open_room", room="lobby", after_seq=0)
    st = await b.wait("room_state")
    target = [x for x in st["messages"] if x["id"] == m2["id"]][0]
    ok("历史里撤回的消息不带原文", target["is_recalled"] and target["text"] == "")
    b.send(type="recall", room="lobby", id=m2["id"])
    rj = await b.wait("reject")
    ok("不能撤回别人的消息", rj and rj["code"] == "not_owner", detail=str(rj))

    a.send(type="send", room="lobby", text="超时撤回", client_id="rc2")
    m3 = await a.wait("ack", lambda x: x.get("client_id") == "rc2")
    conn_db = sqlite3.connect(DB)
    conn_db.execute("UPDATE messages SET created_at=? WHERE id=?",
                    ("2020-01-01T00:00:00.000", m3["id"]))
    conn_db.commit(); conn_db.close()
    a.send(type="recall", room="lobby", id=m3["id"])
    rj2 = await a.wait("reject")
    ok("超过时限无法撤回", rj2 and rj2["code"] == "too_late", detail=str(rj2))

    print()
    print("=" * 60)
    print("模块三：离线后补消息")
    print("=" * 60)
    b.send(type="rooms")
    last = 0
    st2 = await b.wait("rooms")
    b.send(type="open_room", room="lobby", after_seq=0)
    rs = await b.wait("room_state")
    last = max(m["seq"] for m in rs["messages"])
    await b.close()
    for i in range(3):
        a.send(type="send", room="lobby", text="离线期间%d" % i, client_id="off%d" % i)
        await a.wait("ack", lambda x, i=i: x.get("client_id") == "off%d" % i)
    b = await conn("乙")
    b.send(type="open_room", room="lobby", after_seq=last)
    rs2 = await b.wait("room_state")
    texts = [m["text"] for m in rs2["messages"]]
    ok("补发正好 3 条且不重复", texts == ["离线期间0", "离线期间1", "离线期间2"], detail=str(texts))

    print()
    print("=" * 60)
    print("模块四：发送失败与重试（幂等）")
    print("=" * 60)
    conn_db = sqlite3.connect(DB)
    before = conn_db.execute("SELECT count(*) FROM messages").fetchone()[0]
    conn_db.close()
    a.send(type="send", room="lobby", text="重试测试", client_id="dup1")
    k1 = await a.wait("ack", lambda x: x.get("client_id") == "dup1")
    a.send(type="send", room="lobby", text="重试测试", client_id="dup1")
    k2 = await a.wait("ack", lambda x: x.get("client_id") == "dup1")
    conn_db = sqlite3.connect(DB)
    after = conn_db.execute("SELECT count(*) FROM messages").fetchone()[0]
    conn_db.close()
    ok("同一 client_id 只入库一条", after - before == 1, detail="%d -> %d" % (before, after))
    ok("两次 ack 指向同一条", k1 and k2 and k1["seq"] == k2["seq"])

    print()
    print("=" * 60)
    print("模块七：消息编辑")
    print("=" * 60)
    a.send(type="send", room="lobby", text="原文", client_id="e1")
    m4 = await a.wait("ack", lambda x: x.get("client_id") == "e1")
    a.send(type="edit", room="lobby", id=m4["id"], text="改后的内容")
    ed = await b.wait("edit", lambda x: x.get("id") == m4["id"])
    ok("其他人收到编辑通知", ed and ed["text"] == "改后的内容")
    b.send(type="edit", room="lobby", id=m4["id"], text="我来改")
    rj3 = await b.wait("reject")
    ok("不能编辑别人的消息", rj3 and rj3["code"] == "not_owner", detail=str(rj3))
    c2 = await conn("丙")
    c2.send(type="open_room", room="lobby", after_seq=0)
    st3 = await c2.wait("room_state")
    got = [x for x in st3["messages"] if x["id"] == m4["id"]][0]
    ok("后来加入的人看到最新版本", got["text"] == "改后的内容" and got["is_edited"])
    a.send(type="recall", room="lobby", id=m4["id"])
    await a.wait("recall")
    a.send(type="edit", room="lobby", id=m4["id"], text="再改")
    rj4 = await a.wait("reject", lambda x: x.get("code") == "recalled")
    ok("撤回后不能编辑", rj4 is not None, detail=str(rj4))
    await c2.close()

    print()
    print("=" * 60)
    print("模块九：输入校验与拒绝提示")
    print("=" * 60)
    a.send(type="send", room="lobby", text="   ", client_id="v1")
    r1 = await a.wait("reject", lambda x: x.get("client_id") == "v1")
    ok("空白消息被拒", r1 and r1["code"] == "empty", detail=str(r1))
    a.send(type="send", room="lobby", text="长" * 501, client_id="v2")
    r2 = await a.wait("reject", lambda x: x.get("client_id") == "v2")
    ok("超长消息被拒且说明原因", r2 and r2["code"] == "too_long" and "500" in r2["message"],
       detail=str(r2))
    await asyncio.sleep(3.2)
    codes = []
    for i in range(12):
        a.flood(type="send", room="lobby", text="刷屏%d" % i, client_id="sp%d" % i)
        await asyncio.sleep(0.05)
    await asyncio.sleep(0.6)
    rej = [x for x in a.buf if x.get("type") == "reject" and str(x.get("client_id", "")).startswith("sp")]
    ok("连续发送过快被拦", len(rej) > 0, detail="被拦 %d 条" % len(rej))
    a.buf.clear()
    await asyncio.sleep(5.2)   # 等限流窗口过去，别影响后面的用例

    print()
    print("=" * 60)
    print("模块十：历史分页")
    print("=" * 60)
    a.send(type="create_room", name="分页测试")
    await asyncio.sleep(0.4)
    rid = "pub:分页测试"
    for i in range(35):
        a.send(type="send", room=rid, text="第%d条" % i, client_id="pg%d" % i)
        await a.wait("ack", lambda x, i=i: x.get("client_id") == "pg%d" % i)
    a.send(type="open_room", room=rid, after_seq=0)
    st4 = await a.wait("room_state")
    ok("新房间首屏拿到全部 35 条（未超上限）", len(st4["messages"]) == 35, detail=str(len(st4["messages"])))
    first_seq = st4["messages"][0]["seq"]
    a.send(type="history", room=rid, before_seq=first_seq)
    h1 = await a.wait("history")
    ok("没有更早的消息时 has_more 为假", h1 and h1["has_more"] is False and h1["messages"] == [],
       detail=str(h1 and len(h1["messages"])))

    print()
    print("=" * 60)
    print("聊天室与私聊")
    print("=" * 60)
    a.send(type="create_room", name="分页测试")
    dup = await a.wait("reject", lambda x: x.get("code") == "exists")
    ok("重名房间被拒", dup is not None, detail=str(dup))
    a.send(type="open_dm", peer="乙")
    dm1 = await a.wait("dm_opened")
    b.send(type="open_dm", peer="甲")
    dm2 = await b.wait("dm_opened")
    ok("A找B和B找A是同一个私聊房间", dm1 and dm2 and dm1["room"]["id"] == dm2["room"]["id"],
       detail="%s vs %s" % (dm1 and dm1["room"]["id"], dm2 and dm2["room"]["id"]))
    dm_id = dm1["room"]["id"]
    c2 = await conn("丙")
    c2.send(type="open_room", room=dm_id, after_seq=0)
    rj5 = await c2.wait("reject")
    ok("第三方进不去私聊房间", rj5 and rj5["code"] == "no_room", detail=str(rj5))
    b.buf.clear(); c2.buf.clear()
    a.send(type="send", room=dm_id, text="悄悄话", client_id="dm1c")
    bgot = await b.wait("msg", lambda x: x.get("text") == "悄悄话")
    cgot = await c2.wait("msg", lambda x: x.get("text") == "悄悄话", timeout=0.8)
    ok("私聊消息只发给对方", bgot is not None and cgot is None)
    await c2.close()

    print()
    print("=" * 60)
    print("模块六：断线重连（客户端行为，服务端侧验证）")
    print("=" * 60)
    await a.close()
    await asyncio.sleep(0.4)
    p2 = await b.wait("presence", lambda x: "甲" not in x["users"])
    ok("断开后在线列表立即更新", p2 is not None, detail=str(p2))

    a = await conn("甲")
    a.send(type="open_room", room="lobby", after_seq=0)
    st5 = await a.wait("room_state")
    ok("重连后能补回历史", st5 is not None and len(st5["messages"]) > 0)

    print()
    print("=" * 60)
    print("一个 IP 只能对应一个账号")
    print("=" * 60)
    # 乙 正用 127.0.0.2 在线，同一 IP 换个昵称「丁」应被拒绝
    ws2 = await websockets.connect(BASE + "丁", local_addr=("127.0.0.2", 0))
    raw = json.loads(await asyncio.wait_for(ws2.recv(), 5))
    ok("同 IP 换账号登录被拒", raw.get("type") == "reject" and raw.get("code") == "ip_taken",
       detail=str(raw))
    try:
        await asyncio.wait_for(ws2.recv(), 3)
        closed = False
    except Exception:
        closed = True
    ok("被拒后连接随即断开", closed)

    # 同 IP 同账号再开一个窗口仍允许（需求五：同名多窗口算一个人）
    ws3 = await websockets.connect(BASE + "乙", local_addr=("127.0.0.2", 0))
    hello = json.loads(await asyncio.wait_for(ws3.recv(), 5))
    ok("同 IP 同账号开第二个窗口仍允许", hello.get("type") == "hello", detail=str(hello))
    pong = None
    await ws3.send(json.dumps({"type": "ping"}))
    while pong is None:
        m = json.loads(await asyncio.wait_for(ws3.recv(), 5))
        if m.get("type") == "pong":
            pong = m
    await ws3.close()

    for x in (a, b):
        await x.close()

    print()
    print("=" * 60)
    print("结果：通过 %d，失败 %d" % (passed, failed))
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)


asyncio.run(main())
