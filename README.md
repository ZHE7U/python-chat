# 天蓝聊天室 (Sky Blue Chat)

一个基于 Python 和 WebSocket 构建的轻量级、实时的在线聊天室项目。

## ✨ 功能特性

- **实时通信**：使用 WebSocket 技术，实现消息的即时收发。
- **历史记录**：基于 SQLite 数据库存储聊天记录，支持加载历史消息。
- **多房间支持**：支持根据 `room_id` 划分不同的聊天室。
- **现代化前端**：基于原生 HTML/CSS/JS 构建，采用响应式设计，界面清爽。

## 🛠️ 技术栈

- **后端**: Python 3, FastAPI, Uvicorn, WebSockets, SQLite3
- **前端**: HTML5, CSS3, JavaScript (原生)
- **包管理**: uv / pip

## 📁 项目结构

```text
python-chat/
├── src/python_chat/    # 后端核心业务逻辑 (如数据库连接等)
├── static/             # 前端静态资源 (HTML, CSS, JS)
│   └── index.html      # 聊天室主页面
├── tests/              # 测试脚本
├── pyproject.toml      # 项目依赖及配置文件
├── server.py           # FastAPI/Uvicorn 服务启动入口
└── README.md           # 项目说明文档
