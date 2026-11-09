from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
import uvicorn

app = FastAPI()

# 前端网页代码，直接内嵌在这里
html = """
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Python 局域网聊天室</title>
    <style>
        body { font-family: sans-serif; max-width: 600px; margin: 20px auto; padding: 10px; }
        #chat-box { border: 1px solid #ccc; height: 350px; overflow-y: scroll; padding: 10px; margin-bottom: 10px; background: #f9f9f9; border-radius: 5px; }
        .system { color: gray; font-style: italic; font-size: 0.9em; }
        .my-msg { color: blue; font-weight: bold; margin-top: 5px; }
        .other-msg { color: black; margin-top: 5px; }
        #input-area { display: flex; }
        #message-input { flex: 1; padding: 10px; font-size: 16px; }
        button { padding: 10px 20px; font-size: 16px; cursor: pointer; }
    </style>
</head>
<body>
    <h2>🐍 Python 局域网聊天室</h2>
    <div id="chat-box"></div>
    <div id="input-area">
        <input type="text" id="message-input" placeholder="输入消息..." autocomplete="off" />
        <button id="send-btn">发送</button>
    </div>

    <script>
        // 自动识别当前网页的 IP 和端口
        const ws = new WebSocket("ws://" + window.location.host + "/ws");
        const chatBox = document.getElementById("chat-box");
        const input = document.getElementById("message-input");
        const btn = document.getElementById("send-btn");

        ws.onmessage = (event) => {
            const div = document.createElement("div");
            const data = JSON.parse(event.data);
            
            if (data.type === "system") {
                div.className = "system";
                div.textContent = "[系统] " + data.message;
            } else {
                div.className = data.isMe ? "my-msg" : "other-msg";
                div.textContent = data.from + ": " + data.text;
            }
            chatBox.appendChild(div);
            chatBox.scrollTop = chatBox.scrollHeight;
        };

        function send() {
            const msg = input.value.trim();
            if (msg) {
                ws.send(msg);
                input.value = "";
            }
        }
        btn.onclick = send;
        input.addEventListener("keypress", (e) => { if (e.key === "Enter") send(); });
    </script>
</body>
</html>
"""

# 当浏览器访问首页时，返回上面的网页
@app.get("/")
async def get():
    return HTMLResponse(html)

# 连接管理器，用来存所有正在聊天的人
class ConnectionManager:
    def __init__(self):
        self.active_connections = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict, sender: WebSocket = None):
        for connection in self.active_connections:
            if connection is not sender:
                await connection.send_json(message)

manager = ConnectionManager()

# WebSocket 聊天逻辑
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    client_ip = websocket.client.host
    
    # 广播新人加入
    await manager.broadcast({"type": "system", "message": f"新用户 {client_ip} 加入聊天室"})
    
    try:
        while True:
            data = await websocket.receive_text()
            # 发给自己
            await websocket.send_json({"type": "message", "from": "我", "text": data, "isMe": True})
            # 广播给别人
            await manager.broadcast({"type": "message", "from": client_ip, "text": data, "isMe": False}, sender=websocket)
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        await manager.broadcast({"type": "system", "message": f"用户 {client_ip} 离开了聊天室"})

if __name__ == "__main__":
    # 启动服务器，监听 3000 端口
    uvicorn.run(app, host="0.0.0.0", port=3000)