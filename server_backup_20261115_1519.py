from  fastapi  import  FastAPI,  WebSocket,  WebSocketDisconnect
from  fastapi.responses  import  HTMLResponse
import  uvicorn
import  sqlite3
import  json
import  uuid
from  datetime  import  datetime,  timedelta
import  asyncio

app  =  FastAPI()

def  init_db():
        conn  =  sqlite3.connect('chat.db')
        c  =  conn.cursor()
        c.execute('''CREATE  TABLE  IF  NOT  EXISTS  messages
                                  (id  TEXT  PRIMARY  KEY,  sender  TEXT,  content  TEXT,  timestamp  TEXT,  
                                    is_recalled  INTEGER  DEFAULT  0,  is_edited  INTEGER  DEFAULT  0,  read_by  TEXT  DEFAULT  '')''')
        conn.commit()
        conn.close()

init_db()

html  =  """
<!DOCTYPE  html>
<html>
<head>
        <meta  charset="UTF-8">
        <title>终极局域网聊天室</title>
        <style>
                body  {  font-family:  sans-serif;  max-width:  900px;  margin:  20px  auto;  padding:  10px;  display:  flex;  }
                #sidebar  {  width:  200px;  border-right:  1px  solid  #ccc;  padding-right:  10px;  margin-right:  15px;  }
                #main  {  flex:  1;  position:  relative;  }
                #chat-box  {  border:  1px  solid  #ccc;  height:  450px;  overflow-y:  scroll;  padding:  10px;  margin-bottom:  10px;  background:  #f9f9f9;  border-radius:  5px;}
                .msg-row  {  margin-bottom:  12px;  }
                .system  {  color:  gray;  font-size:  0.85em;  font-style:  italic;  text-align:  center;  margin:  10px  0;}
                .my-msg  {  text-align:  right;  color:  blue;  }
                .other-msg  {  text-align:  left;  color:  black;  }
                .msg-bubble  {  display:  inline-block;  padding:  8px  12px;  border-radius:  8px;  background:  #e0e0e0;  max-width:  70%;  text-align:  left;  word-wrap:  break-word;  }
                .my-msg  .msg-bubble  {  background:  #d1e7ff;  }
                .recalled  {  color:  gray;  font-style:  italic;  font-size:  0.9em;  }
                .read-status  {  font-size:  0.75em;  color:  gray;  margin-top:  2px;  }
                .action-btn  {  font-size:  0.75em;  color:  #666;  cursor:  pointer;  margin-left:  8px;  text-decoration:  underline;  }
                .my-msg:hover  .action-btn  {  display:  inline-block;  }
                .action-btn  {  display:  none;  }
                .edited-tag  {  font-size:  0.75em;  color:  #999;  margin-left:  4px;  }
                .failed-msg  {  color:  red;  font-size:  0.8em;  cursor:  pointer;  font-weight:  bold;}
                #input-area  {  display:  flex;  }
                #message-input  {  flex:  1;  padding:  10px;  font-size:  16px;  border-radius:  5px;  border:  1px  solid  #ccc;}
                button  {  padding:  10px  20px;  font-size:  16px;  cursor:  pointer;  margin-left:  10px;  border-radius:  5px;  border:  none;  background:  #007bff;  color:  white;}
                button:disabled  {  background:  #ccc;  }
                #login-box  {  text-align:  center;  margin-top:  50px;  }
                .status-bar  {  font-size:  0.85em;  color:  #666;  margin-bottom:  10px;  font-weight:  bold;}
                #unread-badge  {  display:  none;  background:  red;  color:  white;  border-radius:  50%;  padding:  2px  8px;  font-size:  12px;  position:  absolute;  right:  20px;  bottom:  60px;  cursor:  pointer;  z-index:  10;}
                #load-more-btn  {  display:  block;  margin:  0  auto  10px  auto;  font-size:  12px;  padding:  5px  10px;  background:  #eee;  color:  #333;  border:  1px  solid  #ccc;}
        </style>
</head>
<body>
        <div  id="login-box">
                <h2>🐍  终极版局域网聊天室</h2>
                <input  type="text"  id="username-input"  placeholder="请输入你的昵称"  style="padding:10px;  font-size:16px;">
                <button  onclick="login()">进入聊天室</button>
        </div>

        <div  id="chat-app"  style="display:  none;  width:  100%;  display:  flex;">
                <div  id="sidebar">
                        <h3>在线成员</h3>
                        <ul  id="online-list"  style="padding-left:  20px;  color:  #333;"></ul>
                </div>
                <div  id="main">
                        <div  class="status-bar"  id="status">正在连接...</div>
                        <button  id="load-more-btn"  onclick="loadMore()">加载更早的历史记录</button>
                        <div  id="chat-box"></div>
                        <div  id="unread-badge"  onclick="scrollToBottom()">新消息  ↓</div>
                        <div  id="input-area">
                                <input  type="text"  id="message-input"  placeholder="输入消息..."  autocomplete="off"  disabled  maxlength="500"  />
                                <button  id="send-btn"  disabled>发送</button>
                        </div>
                </div>
        </div>

        <script>
                let  ws;
                let  currentUser  =  "";
                let  reconnectTimer;
                let  lastSentTime  =  0;
                let  currentOldestId  =  "";

                function  login()  {
                        const  name  =  document.getElementById("username-input").value.trim();
                        if  (!name)  return  alert("请输入昵称");
                        currentUser  =  name;
                        localStorage.setItem("myUsername",  name);
                        if  (ws)  {  ws.onclose  =  null;  ws.close();  }
                        document.getElementById("login-box").style.display  =  "none";
                        document.getElementById("chat-app").style.display  =  "flex";
                        connectWebSocket();
                }

                function  connectWebSocket()  {
                        const  status  =  document.getElementById("status");
                        status.innerText  =  "正在连接...";
                        status.style.color  =  "orange";
                        const  lastReadId  =  localStorage.getItem("lastReadId")  ||  "";
                        ws  =  new  WebSocket(`ws://${window.location.host}/ws/${currentUser}?last_id=${lastReadId}`);

                        ws.onopen  =  ()  =>  {
                                status.innerText  =  "🟢  已连接";
                                status.style.color  =  "green";
                                document.getElementById("message-input").disabled  =  false;
                                document.getElementById("send-btn").disabled  =  false;
                                if  (reconnectTimer)  clearTimeout(reconnectTimer);
                        };

                        ws.onclose  =  ()  =>  {
                                status.innerText  =  "🔴  断线，尝试重连...";
                                status.style.color  =  "red";
                                reconnectTimer  =  setTimeout(connectWebSocket,  3000);
                        };

                        ws.onmessage  =  (event)  =>  {
                                const  data  =  JSON.parse(event.data);
                                if  (data.type  ===  "system"  &&  data.online_users)  {
                                        document.getElementById("online-list").innerHTML  =  data.online_users.map(u  =>  `<li>${u}</li>`).join("");
                                        return;
                                }
                                if  (data.type  ===  "error")  {
                                        alert("系统提示："  +  data.message);
                                        return;
                                }
                                if  (data.type  ===  "history_chunk")  {
                                        const  chatBox  =  document.getElementById("chat-box");
                                        const  oldHeight  =  chatBox.scrollHeight;
                                        data.messages.forEach(msg  =>  renderMessage(msg,  true));
                                        chatBox.scrollTop  =  chatBox.scrollHeight  -  oldHeight;
                                        if  (data.messages.length  <  20)  {
                                                document.getElementById("load-more-btn").innerText  =  "没有更早的记录了";
                                                document.getElementById("load-more-btn").disabled  =  true;
                                        }
                                        return;
                                }
                                if  (data.type  ===  "recall")  {
                                        const  msgRow  =  document.getElementById(`msg-${data.id}`);
                                        if  (msgRow)  {
                                                msgRow.querySelector(".msg-bubble").outerHTML  =  `<span  class="recalled">[该消息已被撤回]</span>`;
                                                msgRow.querySelector(".action-btn")?.remove();
                                        }
                                        return;
                                }
                                if  (data.type  ===  "read_update")  {
                                        const  readStatus  =  document.getElementById(`read-${data.id}`);
                                        if  (readStatus)  readStatus.innerText  =  `已读:  ${data.read_by.join(',  ')}`;
                                        return;
                                }
                                if  (data.type  ===  "edit_update")  {
                                        const  msgRow  =  document.getElementById(`msg-${data.id}`);
                                        if  (msgRow)  {
                                                const  bubble  =  msgRow.querySelector(".msg-bubble");
                                                if  (bubble)  bubble.innerHTML  =  `${data.from}:  ${data.text}  <span  class="edited-tag">(已编辑)</span>`;
                                        }
                                        return;
                                }
                                if  (data.type  ===  "message")  {
                                        const  chatBox  =  document.getElementById("chat-box");
                                        const  isAtBottom  =  chatBox.scrollHeight  -  chatBox.scrollTop  -  chatBox.clientHeight  <  50;
                                        renderMessage(data);
                                        if  (data.from  !==  currentUser  &&  !data.is_recalled)  {
                                                ws.send(JSON.stringify({  type:  "read",  id:  data.id  }));
                                                if  (!isAtBottom)  {
                                                        const  badge  =  document.getElementById("unread-badge");
                                                        badge.style.display  =  "block";
                                                }
                                        }
                                        localStorage.setItem("lastReadId",  data.id);
                                }
                        };
                }

                function  renderMessage(data,  prepend  =  false)  {
                        if  (document.getElementById(`msg-${data.id}`))  return;  
                        const  chatBox  =  document.getElementById("chat-box");
                        const  row  =  document.createElement("div");
                        row.id  =  `msg-${data.id}`;
                        row.className  =  `msg-row  ${data.from  ===  currentUser  ?  'my-msg'  :  'other-msg'}`;
                        const  bubble  =  document.createElement("span");
                        bubble.className  =  "msg-bubble";
                        if  (data.is_recalled)  {
                                bubble.outerHTML  =  `<span  class="recalled">[该消息已被撤回]</span>`;
                        }  else  {
                                bubble.innerHTML  =  `${data.from}:  ${data.text}  ${data.is_edited  ?  '<span  class="edited-tag">(已编辑)</span>'  :  ''}`;
                        }
                        const  readStatus  =  document.createElement("div");
                        readStatus.id  =  `read-${data.id}`;
                        readStatus.className  =  "read-status";
                        if  (data.read_by  &&  data.read_by.length  >  0)  {
                                readStatus.innerText  =  `已读:  ${data.read_by.join(',  ')}`;
                        }
                        let  actions  =  "";
                        if  (data.from  ===  currentUser  &&  !data.is_recalled)  {
                                actions  =  `<span  class="action-btn"  onclick="recallMsg('${data.id}')">撤回</span>
                                                      <span  class="action-btn"  onclick="editMsg('${data.id}',  '${data.text}')">编辑</span>`;
                        }
                        row.innerHTML  =  `<div>${bubble.outerHTML}${actions}</div>`;
                        row.appendChild(readStatus);
                        if  (prepend)  {
                                chatBox.insertBefore(row,  chatBox.firstChild);
                        }  else  {
                                chatBox.appendChild(row);
                                if  (!prepend)  chatBox.scrollTop  =  chatBox.scrollHeight;
                        }
                }

                function  loadMore()  {
                        const  firstMsg  =  document.querySelector("#chat-box  .msg-row");
                        if  (firstMsg)  currentOldestId  =  firstMsg.id.replace("msg-",  "");
                        if  (ws  &&  ws.readyState  ===  WebSocket.OPEN)  {
                                ws.send(JSON.stringify({  type:  "load_more",  oldest_id:  currentOldestId  }));
                        }
                }

                function  scrollToBottom()  {
                        const  chatBox  =  document.getElementById("chat-box");
                        chatBox.scrollTop  =  chatBox.scrollHeight;
                        document.getElementById("unread-badge").style.display  =  "none";
                }

                function  recallMsg(id)  {
                        if(confirm("确定要撤回这条消息吗？"))  ws.send(JSON.stringify({  type:  "recall",  id:  id  }));
                }

                function  editMsg(id,  oldText)  {
                        const  newText  =  prompt("修改你的消息：",  oldText);
                        if  (newText  &&  newText  !==  oldText)  ws.send(JSON.stringify({  type:  "edit",  id:  id,  text:  newText  }));
                }

                function  send()  {
                        const  input  =  document.getElementById("message-input");
                        const  msg  =  input.value.trim();
                        if  (!msg)  return;
                        if  (msg.length  >  500)  return  alert("消息太长啦，最多500字！");
                        const  now  =  Date.now();
                        if  (now  -  lastSentTime  <  500)  return  alert("发送太快了，歇一下吧！");
                        if  (ws  &&  ws.readyState  ===  WebSocket.OPEN)  {
                                lastSentTime  =  now;
                                const  clientId  =  Math.random().toString(36).substring(2,  15);
                                ws.send(JSON.stringify({  type:  "chat",  text:  msg,  client_id:  clientId  }));
                                input.value  =  "";
                        }  else  {
                                const  chatBox  =  document.getElementById("chat-box");
                                const  row  =  document.createElement("div");
                                row.className  =  "msg-row  my-msg";
                                row.innerHTML  =  `<span  class="msg-bubble"  style="background:#ffcccc">${currentUser}:  ${msg}</span>  <span  class="failed-msg"  onclick="this.parentElement.remove()">[发送失败,  点击重试]</span>`;
                                row.querySelector(".failed-msg").onclick  =  ()  =>  {  
                                        row.remove();  input.value  =  msg;  send();  
                                };
                                chatBox.appendChild(row);
                                chatBox.scrollTop  =  chatBox.scrollHeight;
                                input.value  =  "";
                        }
                }

                document.getElementById("send-btn").onclick  =  send;
                document.getElementById("message-input").addEventListener("keypress",  (e)  =>  {  if  (e.key  ===  "Enter")  send();  });
        </script>
</body>
</html>
"""

@app.get("/")
async  def  get():
        return  HTMLResponse(html)

class  ConnectionManager:
        def  __init__(self):
                self.active_connections  =  {}

        async  def  connect(self,  username:  str,  websocket:  WebSocket):
                await  websocket.accept()
                if  username  in  self.active_connections:
                        try:  await  self.active_connections[username].close()
                        except:  pass
                self.active_connections[username]  =  websocket
                await  self.broadcast_system_status()

        def  disconnect(self,  username:  str):
                if  username  in  self.active_connections:
                        del  self.active_connections[username]
                asyncio.create_task(self.broadcast_system_status())

        async  def  broadcast(self,  message:  dict):
                for  user,  conn  in  list(self.active_connections.items()):
                        try:  await  conn.send_json(message)
                        except:  pass

        async  def  broadcast_system_status(self):
                await  self.broadcast({"type":  "system",  "online_users":  list(self.active_connections.keys())})

manager  =  ConnectionManager()

@app.websocket("/ws/{username}")
async  def  websocket_endpoint(websocket:  WebSocket,  username:  str):
        await  manager.connect(username,  websocket)
        last_id  =  websocket.query_params.get("last_id")
        conn  =  sqlite3.connect('chat.db')
        c  =  conn.cursor()
        
        if  last_id:
                c.execute("SELECT  timestamp  FROM  messages  WHERE  id=?",  (last_id,))
                row  =  c.fetchone()
                if  row:
                        c.execute("SELECT  id,  sender,  content,  timestamp,  is_recalled,  is_edited,  read_by  FROM  messages  WHERE  timestamp  >  ?  ORDER  BY  timestamp  ASC",  (row[0],))
                else:
                        c.execute("SELECT  id,  sender,  content,  timestamp,  is_recalled,  is_edited,  read_by  FROM  messages  ORDER  BY  timestamp  DESC  LIMIT  20")
        else:
                c.execute("SELECT  id,  sender,  content,  timestamp,  is_recalled,  is_edited,  read_by  FROM  messages  ORDER  BY  timestamp  DESC  LIMIT  20")

        history  =  c.fetchall()
        conn.close()
        if  not  last_id:  history.reverse()

        for  msg  in  history:
                await  websocket.send_json({
                        "type":  "message",  "id":  msg[0],  "from":  msg[1],  "text":  msg[2],
                        "is_recalled":  bool(msg[4]),  "is_edited":  bool(msg[5]),
                        "read_by":  msg[6].split(",")  if  msg[6]  else  []
                })

        try:
                while  True:
                        data  =  await  websocket.receive_text()
                        msg_data  =  json.loads(data)
                        msg_type  =  msg_data.get("type")

                        if  msg_type  ==  "chat":
                                text  =  msg_data.get("text",  "").strip()
                                if  not  text  or  len(text)  >  500:  continue  
                                client_id  =  msg_data.get("client_id",  str(uuid.uuid4()))
                                conn  =  sqlite3.connect('chat.db')
                                c  =  conn.cursor()
                                c.execute("SELECT  id  FROM  messages  WHERE  id=?",  (client_id,))
                                if  c.fetchone():
                                        conn.close()
                                        continue  
                                c.execute("INSERT  INTO  messages  (id,  sender,  content,  timestamp)  VALUES  (?,  ?,  ?,  ?)",  (client_id,  username,  text,  datetime.now().isoformat()))
                                conn.commit()
                                conn.close()
                                await  manager.broadcast({"type":  "message",  "id":  client_id,  "from":  username,  "text":  text,  "is_recalled":  False,  "is_edited":  False,  "read_by":  []})

                        elif  msg_type  ==  "recall":
                                msg_id  =  msg_data.get("id")
                                conn  =  sqlite3.connect('chat.db')
                                c  =  conn.cursor()
                                c.execute("SELECT  sender,  timestamp  FROM  messages  WHERE  id=?",  (msg_id,))
                                row  =  c.fetchone()
                                if  row  and  row[0]  ==  username:
                                        msg_time  =  datetime.fromisoformat(row[1])
                                        if  datetime.now()  -  msg_time  >  timedelta(minutes=2):
                                                await  websocket.send_json({"type":  "error",  "message":  "超过2分钟，无法撤回！"})
                                        else:
                                                c.execute("UPDATE  messages  SET  is_recalled=1  WHERE  id=?",  (msg_id,))
                                                conn.commit()
                                                await  manager.broadcast({"type":  "recall",  "id":  msg_id})
                                conn.close()

                        elif  msg_type  ==  "edit":
                                msg_id  =  msg_data.get("id")
                                new_text  =  msg_data.get("text",  "").strip()
                                if  not  new_text  or  len(new_text)  >  500:  continue
                                conn  =  sqlite3.connect('chat.db')
                                c  =  conn.cursor()
                                c.execute("SELECT  sender  FROM  messages  WHERE  id=?",  (msg_id,))
                                row  =  c.fetchone()
                                if  row  and  row[0]  ==  username:
                                        c.execute("UPDATE  messages  SET  content=?,  is_edited=1  WHERE  id=?",  (new_text,  msg_id))
                                        conn.commit()
                                        await  manager.broadcast({"type":  "edit_update",  "id":  msg_id,  "from":  username,  "text":  new_text})
                                conn.close()

                        elif  msg_type  ==  "read":
                                msg_id  =  msg_data.get("id")
                                conn  =  sqlite3.connect('chat.db')
                                c  =  conn.cursor()
                                c.execute("SELECT  read_by  FROM  messages  WHERE  id=?",  (msg_id,))
                                row  =  c.fetchone()
                                if  row:
                                        read_by  =  row[0].split(",")  if  row[0]  else  []
                                        if  username  not  in  read_by:
                                                read_by.append(username)
                                                c.execute("UPDATE  messages  SET  read_by=?  WHERE  id=?",  (",".join(read_by),  msg_id))
                                                conn.commit()
                                                await  manager.broadcast({"type":  "read_update",  "id":  msg_id,  "read_by":  read_by})
                                conn.close()
                                
                        elif msg_type == "load_more":
                            oldest_id = msg_data.get("oldest_id")
                            conn = sqlite3.connect('chat.db')
                            c = conn.cursor()
                            if oldest_id:
                                c.execute("SELECT timestamp FROM messages WHERE id=?", (oldest_id,))
                                row = c.fetchone()
                                if row:
                                    c.execute("SELECT id, sender, content, timestamp, is_recalled, is_edited, read_by FROM messages WHERE timestamp < ? ORDER BY timestamp DESC LIMIT 20", (row[0],))
                                    older_history = c.fetchall()
                                    older_history.reverse()
                                    msgs = []
                                    for m in older_history:
                                        read_by = m[6].split(",") if m[6] else []
                                        msg_item = {
                                            "type": "message",
                                            "id": m[0],
                                            "from": m[1],
                                            "text": m[2],
                                            "is_recalled": bool(m[4]),
                                            "read_by": read_by
                                        }
                                        msgs.append(msg_item)
                                    await websocket.send_json({"type": "history_chunk", "messages": msgs})
                            conn.close()

        except  WebSocketDisconnect:
                manager.disconnect(username)

if  __name__  ==  "__main__":
        uvicorn.run(app,  host="0.0.0.0",  port=3000)