from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn
import sqlite3
import json
import uuid
from datetime import datetime, timedelta
import asyncio

app = FastAPI()

# 初始化数据库（新增了 room_id 字段）
def init_db():
    conn = sqlite3.connect('chat.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS messages
                 (id TEXT PRIMARY KEY, room_id TEXT, sender TEXT, content TEXT, timestamp TEXT, 
                  is_recalled INTEGER DEFAULT 0, is_edited INTEGER DEFAULT 0, read_by TEXT DEFAULT '')''')
    conn.commit()
    conn.close()

init_db()

html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>终极多房间聊天室</title>
    <style>
        body { font-family: sans-serif; max-width: 1000px; margin: 20px auto; padding: 10px; display: flex; height: 90vh; }
        #sidebar { width: 220px; border-right: 1px solid #ccc; padding-right: 10px; margin-right: 15px; display: flex; flex-direction: column; }
        #main { flex: 1; display: flex; flex-direction: column; position: relative; }
        
        /* 聊天框布局，实现左右对齐 */
        #chat-box { flex: 1; border: 1px solid #ccc; overflow-y: scroll; padding: 15px; margin-bottom: 10px; background: #f9f9f9; border-radius: 5px; display: flex; flex-direction: column; }
        .msg-row { margin-bottom: 15px; display: flex; flex-direction: column; max-width: 75%; }
        
        /* 自己的消息：靠右 */
        .my-msg { align-self: flex-end; align-items: flex-end; }
        .my-msg .msg-bubble { background: #95ec69; color: black; border-radius: 10px 0px 10px 10px; }
        
        /* 别人的消息：靠左 */
        .other-msg { align-self: flex-start; align-items: flex-start; }
        .other-msg .msg-bubble { background: #ffffff; color: black; border-radius: 0px 10px 10px 10px; border: 1px solid #e0e0e0; }

        .msg-bubble { padding: 10px 15px; word-wrap: break-word; font-size: 15px; position: relative; }
        .sender-name { font-size: 12px; color: #888; margin-bottom: 3px; }
        
        .system { color: gray; font-size: 12px; font-style: italic; text-align: center; margin: 10px 0; align-self: center; }
        .read-status { font-size: 11px; color: #999; margin-top: 4px; }
        .action-btn { font-size: 12px; color: #007bff; cursor: pointer; margin-top: 4px; }
        .edited-tag { font-size: 11px; color: #999; }
        .failed-msg { color: #ff4d4f; font-size: 12px; cursor: pointer; font-weight: bold; margin-top: 4px;}
        
        #input-area { display: flex; }
        #message-input { flex: 1; padding: 12px; font-size: 15px; border-radius: 5px; border: 1px solid #ccc; outline: none;}
        button { padding: 10px 20px; font-size: 15px; cursor: pointer; margin-left: 10px; border-radius: 5px; border: none; background: #007bff; color: white;}
        button:disabled { background: #ccc; }
        
        #login-box { text-align: center; margin-top: 100px; width: 100%;}
        .top-bar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; }
        .status-bar { font-size: 14px; color: #666; font-weight: bold; }
        #room-title { font-size: 18px; color: #333; font-weight: bold; }
        
        ul { list-style: none; padding: 0; margin: 0; }
        li { padding: 10px; background: #eee; margin-bottom: 5px; border-radius: 5px; cursor: pointer; transition: 0.2s;}
        li:hover { background: #d1e7ff; }
        
        #unread-badge { display: none; background: red; color: white; border-radius: 10px; padding: 3px 10px; font-size: 12px; position: absolute; right: 20px; bottom: 70px; cursor: pointer; z-index: 10;}
        #load-more-btn { display: block; margin: 0 auto 10px auto; font-size: 12px; padding: 5px 10px; background: #eee; color: #333; border: 1px solid #ccc;}
        #back-btn { display: none; padding: 5px 10px; font-size: 12px; background: #ff9800; margin-left: 10px; }
    </style>
</head>
<body>
    <div id="login-box">
        <h2>🐍 多房间私聊版聊天室</h2>
        <input type="text" id="username-input" placeholder="请输入你的昵称" style="padding:10px; font-size:16px;">
        <button onclick="login()">进入公共大厅</button>
    </div>

    <div id="chat-app" style="display: none; width: 100%; display: flex;">
        <div id="sidebar">
            <h3>在线成员 (点击私聊)</h3>
            <ul id="online-list"></ul>
        </div>
        <div id="main">
            <div class="top-bar">
                <div>
                    <span id="room-title">🌍 公共大厅</span>
                    <button id="back-btn" onclick="switchRoom('public')">返回大厅</button>
                </div>
                <div class="status-bar" id="status">正在连接...</div>
            </div>
            
            <button id="load-more-btn" onclick="loadMore()">加载更早的历史记录</button>
            <div id="chat-box"></div>
            <div id="unread-badge" onclick="scrollToBottom()">新消息 ↓</div>
            <div id="input-area">
                <input type="text" id="message-input" placeholder="输入消息..." autocomplete="off" disabled maxlength="500" />
                <button id="send-btn" disabled>发送</button>
            </div>
        </div>
    </div>

    <script>
        let ws;
        let currentUser = "";
        let currentRoomId = "public"; 
        let reconnectTimer;
        let lastSentTime = 0;
        let currentOldestId = "";

        function login() {
            const name = document.getElementById("username-input").value.trim();
            if (!name) return alert("请输入昵称");
            currentUser = name;
            document.getElementById("login-box").style.display = "none";
            document.getElementById("chat-app").style.display = "flex";
            switchRoom("public");
        }

        function switchRoom(roomId) {
            currentRoomId = roomId;
            document.getElementById("chat-box").innerHTML = ""; 
            document.getElementById("room-title").innerText = roomId === "public" ? "🌍 公共大厅" : "🔒 私聊: " + roomId.replace("private_", "").split("_").filter(n => n !== currentUser).join("");
            document.getElementById("back-btn").style.display = roomId === "public" ? "none" : "inline-block";
            document.getElementById("load-more-btn").style.display = "block";
            document.getElementById("load-more-btn").disabled = false;
            document.getElementById("load-more-btn").innerText = "加载更早的历史记录";

            if (ws) { ws.onclose = null; ws.close(); }
            connectWebSocket();
        }

        function connectWebSocket() {
            const status = document.getElementById("status");
            status.innerText = "正在连接...";
            status.style.color = "orange";
            
            // URL 中带上房间号
            ws = new WebSocket(`ws://${window.location.host}/ws/${currentUser}/${currentRoomId}`);

            ws.onopen = () => {
                status.innerText = "🟢 已连接";
                status.style.color = "green";
                document.getElementById("message-input").disabled = false;
                document.getElementById("send-btn").disabled = false;
                if (reconnectTimer) clearTimeout(reconnectTimer);
            };

            ws.onclose = () => {
                status.innerText = "🔴 断线重连...";
                status.style.color = "red";
                reconnectTimer = setTimeout(connectWebSocket, 3000);
            };

            ws.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (data.type === "system" && data.online_users) {
                    renderOnlineUsers(data.online_users);
                    return;
                }
                if (data.type === "error") { alert("系统提示：" + data.message); return; }
                if (data.type === "history_chunk") {
                    const chatBox = document.getElementById("chat-box");
                    const oldHeight = chatBox.scrollHeight;
                    data.messages.forEach(msg => renderMessage(msg, true));
                    chatBox.scrollTop = chatBox.scrollHeight - oldHeight;
                    if (data.messages.length < 20) {
                        document.getElementById("load-more-btn").innerText = "没有更早的记录了";
                        document.getElementById("load-more-btn").disabled = true;
                    }
                    return;
                }
                if (data.type === "recall") {
                    const msgRow = document.getElementById(`msg-${data.id}`);
                    if (msgRow) {
                        msgRow.innerHTML = `<span class="recalled">[该消息已被撤回]</span>`;
                    }
                    return;
                }
                if (data.type === "read_update") {
                    const readStatus = document.getElementById(`read-${data.id}`);
                    if (readStatus) readStatus.innerText = `已读: ${data.read_by.join(', ')}`;
                    return;
                }
                if (data.type === "edit_update") {
                    const msgRow = document.getElementById(`msg-${data.id}`);
                    if (msgRow) {
                        const bubble = msgRow.querySelector(".msg-bubble");
                        if (bubble) bubble.innerHTML = `${data.text} <span class="edited-tag">(已编辑)</span>`;
                    }
                    return;
                }
                if (data.type === "message") {
                    const chatBox = document.getElementById("chat-box");
                    const isAtBottom = chatBox.scrollHeight - chatBox.scrollTop - chatBox.clientHeight < 50;
                    renderMessage(data);
                    if (data.from !== currentUser && !data.is_recalled) {
                        ws.send(JSON.stringify({ type: "read", id: data.id }));
                        if (!isAtBottom) {
                            document.getElementById("unread-badge").style.display = "block";
                        }
                    }
                }
            };
        }

        function renderOnlineUsers(users) {
            const list = document.getElementById("online-list");
            list.innerHTML = "";
            users.forEach(u => {
                if (u === currentUser) {
                    list.innerHTML += `<li style="cursor:default;background:#d4edda;">我 (${u})</li>`;
                } else {
                    // 点击别人进入私聊
                    list.innerHTML += `<li onclick="startPrivateChat('${u}')">👤 ${u}</li>`;
                }
            });
        }

        function startPrivateChat(targetUser) {
            // 私聊房间ID：两人的名字排序后拼接，保证唯一
            const roomId = "private_" + [currentUser, targetUser].sort().join("_");
            switchRoom(roomId);
        }

        function renderMessage(data, prepend = false) {
            if (document.getElementById(`msg-${data.id}`)) return; 
            const chatBox = document.getElementById("chat-box");
            const row = document.createElement("div");
            row.id = `msg-${data.id}`;
            
            // 左右布局核心逻辑
            const isMe = data.from === currentUser;
            row.className = `msg-row ${isMe ? 'my-msg' : 'other-msg'}`;

            const senderName = !isMe ? `<div class="sender-name">${data.from}</div>` : '';
            const bubble = document.createElement("span");
            bubble.className = "msg-bubble";
            
            if (data.is_recalled) {
                bubble.outerHTML = `<span class="recalled" style="align-self:center;color:gray;">[该消息已被撤回]</span>`;
            } else {
                bubble.innerHTML = `${data.text} ${data.is_edited ? '<span class="edited-tag">(已编辑)</span>' : ''}`;
            }

            const readStatus = document.createElement("div");
            readStatus.id = `read-${data.id}`;
            readStatus.className = "read-status";
            if (data.read_by && data.read_by.length > 0 && isMe) {
                readStatus.innerText = `已读: ${data.read_by.join(', ')}`;
            }

            let actions = "";
            if (isMe && !data.is_recalled) {
                actions = `<div><span class="action-btn" onclick="recallMsg('${data.id}')">撤回</span>
                           <span class="action-btn" onclick="editMsg('${data.id}', '${data.text}')">编辑</span></div>`;
            }

            row.innerHTML = `${senderName}${bubble.outerHTML}${actions}`;
            row.appendChild(readStatus);
            
            if (prepend) {
                chatBox.insertBefore(row, chatBox.firstChild);
            } else {
                chatBox.appendChild(row);
                if (!prepend) chatBox.scrollTop = chatBox.scrollHeight;
            }
        }

        function loadMore() {
            const firstMsg = document.querySelector("#chat-box .msg-row");
            if (firstMsg) currentOldestId = firstMsg.id.replace("msg-", "");
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify({ type: "load_more", oldest_id: currentOldestId }));
            }
        }

        function scrollToBottom() {
            const chatBox = document.getElementById("chat-box");
            chatBox.scrollTop = chatBox.scrollHeight;
            document.getElementById("unread-badge").style.display = "none";
        }

        function recallMsg(id) {
            if(confirm("确定要撤回这条消息吗？")) ws.send(JSON.stringify({ type: "recall", id: id }));
        }

        function editMsg(id, oldText) {
            const newText = prompt("修改你的消息：", oldText);
            if (newText && newText !== oldText) ws.send(JSON.stringify({ type: "edit", id: id, text: newText }));
        }

        function send() {
            const input = document.getElementById("message-input");
            const msg = input.value.trim();
            if (!msg) return;
            if (msg.length > 500) return alert("消息太长啦，最多500字！");
            const now = Date.now();
            if (now - lastSentTime < 500) return alert("发送太快了，歇一下吧！");
            if (ws && ws.readyState === WebSocket.OPEN) {
                lastSentTime = now;
                const clientId = Math.random().toString(36).substring(2, 15);
                ws.send(JSON.stringify({ type: "chat", text: msg, client_id: clientId }));
                input.value = "";
            } else {
                alert("网络未连接，消息发送失败！");
            }
        }

        document.getElementById("send-btn").onclick = send;
        document.getElementById("message-input").addEventListener("keypress", (e) => { if (e.key === "Enter") send(); });
    </script>
</body>
</html>
"""

@app.get("/")
async def get():
    return HTMLResponse(html)

# ================= 房间管理器 =================
class RoomManager:
    def __init__(self):
        # 数据结构： { "room_id": { "username": websocket } }
        self.rooms = {}

    async def connect(self, room_id: str, username: str, websocket: WebSocket):
        await websocket.accept()
        if room_id not in self.rooms:
            self.rooms[room_id] = {}
        
        # 如果该用户在这个房间已有连接，先踢掉旧的
        if username in self.rooms[room_id]:
            try: await self.rooms[room_id][username].close()
            except: pass
            
        self.rooms[room_id][username] = websocket
        await self.broadcast_system_status(room_id)

    def disconnect(self, room_id: str, username: str):
        if room_id in self.rooms and username in self.rooms[room_id]:
            del self.rooms[room_id][username]
            if not self.rooms[room_id]:
                del self.rooms[room_id] # 房间空了就销毁
            else:
                asyncio.create_task(self.broadcast_system_status(room_id))

    async def broadcast(self, room_id: str, message: dict):
        if room_id in self.rooms:
            for user, conn in list(self.rooms[room_id].items()):
                try: await conn.send_json(message)
                except: pass

    async def broadcast_system_status(self, room_id: str):
        if room_id in self.rooms:
            users = list(self.rooms[room_id].keys())
            await self.broadcast(room_id, {"type": "system", "online_users": users})

manager = RoomManager()

# ================= WebSocket 路由 (带房间号) =================
@app.websocket("/ws/{username}/{room_id}")
async def websocket_endpoint(websocket: WebSocket, username: str, room_id: str):
    await manager.connect(room_id, username, websocket)
    
    conn = sqlite3.connect('chat.db')
    c = conn.cursor()
    # 只查询当前房间的最近 20 条消息
    c.execute("SELECT id, sender, content, timestamp, is_recalled, is_edited, read_by FROM messages WHERE room_id=? ORDER BY timestamp DESC LIMIT 20", (room_id,))
    history = c.fetchall()
    conn.close()
    
    history.reverse()

    for msg in history:
        await websocket.send_json({
            "type": "message", "id": msg[0], "from": msg[1], "text": msg[2],
            "is_recalled": bool(msg[4]), "is_edited": bool(msg[5]),
            "read_by": msg[6].split(",") if msg[6] else []
        })

    try:
        while True:
            data = await websocket.receive_text()
            msg_data = json.loads(data)
            msg_type = msg_data.get("type")

            if msg_type == "chat":
                text = msg_data.get("text", "").strip()
                if not text or len(text) > 500: continue 
                
                client_id = msg_data.get("client_id", str(uuid.uuid4()))
                conn = sqlite3.connect('chat.db')
                c = conn.cursor()
                c.execute("SELECT id FROM messages WHERE id=?", (client_id,))
                if c.fetchone():
                    conn.close()
                    continue 
                
                # 插入消息时带上 room_id
                c.execute("INSERT INTO messages (id, room_id, sender, content, timestamp) VALUES (?, ?, ?, ?, ?)", 
                          (client_id, room_id, username, text, datetime.now().isoformat()))
                conn.commit()
                conn.close()
                
                await manager.broadcast(room_id, {"type": "message", "id": client_id, "from": username, "text": text, "is_recalled": False, "is_edited": False, "read_by": []})

            elif msg_type == "recall":
                msg_id = msg_data.get("id")
                conn = sqlite3.connect('chat.db')
                c = conn.cursor()
                c.execute("SELECT sender, timestamp FROM messages WHERE id=?", (msg_id,))
                row = c.fetchone()
                if row and row[0] == username:
                    msg_time = datetime.fromisoformat(row[1])
                    if datetime.now() - msg_time > timedelta(minutes=2):
                        await websocket.send_json({"type": "error", "message": "超过2分钟，无法撤回！"})
                    else:
                        c.execute("UPDATE messages SET is_recalled=1 WHERE id=?", (msg_id,))
                        conn.commit()
                        await manager.broadcast(room_id, {"type": "recall", "id": msg_id})
                conn.close()

            elif msg_type == "edit":
                msg_id = msg_data.get("id")
                new_text = msg_data.get("text", "").strip()
                if not new_text or len(new_text) > 500: continue
                conn = sqlite3.connect('chat.db')
                c = conn.cursor()
                c.execute("SELECT sender FROM messages WHERE id=?", (msg_id,))
                row = c.fetchone()
                if row and row[0] == username:
                    c.execute("UPDATE messages SET content=?, is_edited=1 WHERE id=?", (new_text, msg_id))
                    conn.commit()
                    await manager.broadcast(room_id, {"type": "edit_update", "id": msg_id, "from": username, "text": new_text})
                conn.close()

            elif msg_type == "read":
                msg_id = msg_data.get("id")
                conn = sqlite3.connect('chat.db')
                c = conn.cursor()
                c.execute("SELECT read_by FROM messages WHERE id=?", (msg_id,))
                row = c.fetchone()
                if row:
                    read_by = row[0].split(",") if row[0] else []
                    if username not in read_by:
                        read_by.append(username)
                        c.execute("UPDATE messages SET read_by=? WHERE id=?", (",".join(read_by), msg_id))
                        conn.commit()
                        await manager.broadcast(room_id, {"type": "read_update", "id": msg_id, "read_by": read_by})
                conn.close()
                
            elif msg_type == "load_more":
                oldest_id = msg_data.get("oldest_id")
                conn = sqlite3.connect('chat.db')
                c = conn.cursor()
                if oldest_id:
                    c.execute("SELECT timestamp FROM messages WHERE id=?", (oldest_id,))
                    row = c.fetchone()
                    if row:
                        # 加上 room_id 过滤，只在当前房间往前翻
                        c.execute("SELECT id, sender, content, timestamp, is_recalled, is_edited, read_by FROM messages WHERE room_id=? AND timestamp < ? ORDER BY timestamp DESC LIMIT 20", (room_id, row[0]))
                        older_history = c.fetchall()
                        older_history.reverse()
                        msgs = []
                        for m in older_history:
                            read_by = m[6].split(",") if m[6] else []
                            msg_item = {"type": "message", "id": m[0], "from": m[1], "text": m[2], "is_recalled": bool(m[4]), "read_by": read_by}
                            msgs.append(msg_item)
                        await websocket.send_json({"type": "history_chunk", "messages": msgs})
                conn.close()

    except WebSocketDisconnect:
        manager.disconnect(room_id, username)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3000)