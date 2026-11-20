"""一键验收：在临时副本里起一个服务，跑完全部用例，再清理。

用法（在项目根目录）：
    .venv\\Scripts\\python.exe tests\\run_spec.py

不需要手动起服务，也不会动你真实的 chat.db。
"""

import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVER = ROOT / "server.py"
STATIC = ROOT / "static"
DB = ROOT / "chat.db"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="pychat-spec-"))
    try:
        shutil.copy(SERVER, workdir / "server.py")
        shutil.copytree(STATIC, workdir / "static")
        if DB.exists():
            # 用真实库的副本，顺带验证旧库能不能平滑升级
            shutil.copy(DB, workdir / "chat.db")
            print("已复制现有 chat.db（会一并验证迁移）")
        else:
            print("没有找到 chat.db，将测试全新初始化")

        port = free_port()
        env = dict(os.environ)
        env["PYCHAT_WS"] = "ws://127.0.0.1:%d/ws/" % port
        env["PYCHAT_DB"] = str(workdir / "chat.db")

        log = open(workdir / "server.log", "w", encoding="utf-8")
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "server:app",
             "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(workdir), stdout=log, stderr=subprocess.STDOUT, env=env,
        )
        try:
            for _ in range(50):
                time.sleep(0.3)
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                        break
                except OSError:
                    continue
            else:
                print("服务没有起来，日志：")
                print((workdir / "server.log").read_text(encoding="utf-8", errors="replace"))
                return 2

            test = subprocess.run(
                [sys.executable, str(ROOT / "tests" / "check_spec.py")],
                env=env, cwd=str(workdir),
            )
            code = test.returncode
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            log.close()

        logtxt = (workdir / "server.log").read_text(encoding="utf-8", errors="replace")
        if "Traceback" in logtxt:
            print("\n!! 服务端出现了异常，日志片段：")
            idx = logtxt.find("Traceback")
            print(logtxt[idx:idx + 1500])
            return 3

        print("\n服务端无异常。")
        return code
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
