"""本地 HTTP 服务入口（可选）。

仅绑定回环地址，不依赖外部中间件或网络。运行：

    uv run python main.py

核心业务逻辑不依赖该进程，直接使用 iot_twin 包或运行 pytest 即可验证。
"""
import uvicorn

from iot_twin.app import app


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8080)
