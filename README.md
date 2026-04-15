# goodsmall

`goodsmall` 是一个基于 Python 的多服务系统，包含：

- `backend`：商城后端与后台管理 API（FastAPI + PostgreSQL）
- `bot_buyer`：买家 Telegram 机器人（商品浏览、下单、支付、订单查询）
- `bot_session`：会话/客服 Telegram 机器人（聚合会话、未读推送、快捷回复）
- `bot_shipping`：供应链/发货 Telegram 机器人（待发货统计、导出、模板样例）

项目通过 `docker-compose` 统一编排运行。

## 目录结构

```text
goodsmall/
├─ backend/
├─ bot_buyer/
├─ bot_session/
├─ bot_shipping/
├─ docker-compose.yml
└─ .env.example
```

## 技术栈

- Python 3.11+
- FastAPI / Uvicorn
- SQLAlchemy
- PostgreSQL
- Aiogram（Telegram Bot）
- httpx
- openpyxl（Excel 导出相关）

## 快速开始（Docker Compose）

1. 复制环境变量模板：

```bash
cp .env.example .env
```

2. 按需修改 `.env` 中的关键配置（尤其是密码、Token）。

3. 启动服务：

```bash
docker compose up -d --build
```

4. 查看服务状态：

```bash
docker compose ps
```

5. 后端健康检查：

- `http://127.0.0.1:${BACKEND_PORT:-8002}/health`

## 服务说明

### backend

- 容器名：`goodsmall-backend`
- 端口映射：`127.0.0.1:${BACKEND_PORT}:8000`
- 负责：
  - 公共接口（商品、下单、地址、订单等）
  - 后台接口（机器人配置、会话、供应链、物流、支付）
  - 定时/轮询任务（USDT 监听、物流同步等）

### bot_buyer

- 容器名：`goodsmall-bot-buyer`
- 负责：面向用户的商城购买流程。

### bot_session

- 容器名：`goodsmall-bot-session`
- 负责：客服会话聚合与回复。

### bot_shipping

- 容器名：`goodsmall-bot-shipping`
- 负责：供应链发货视角的统计与导出操作。

## 环境变量

请参考根目录 `.env.example`。最低建议先配置：

- `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD`
- `DATABASE_URL`
- `ADMIN_PASSWORD`
- `ADMIN_SESSION_SECRET`
- `INTERNAL_API_TOKEN`
- `BUYER_BOT_TOKEN` / `SESSION_BOT_TOKEN` / `SHIPPING_BOT_TOKEN`
- `BACKEND_PUBLIC_URL`

## 常见命令

```bash
# 查看日志
docker compose logs -f backend
docker compose logs -f bot_buyer
docker compose logs -f bot_session
docker compose logs -f bot_shipping

# 停止服务
docker compose down

# 停止并清理数据卷（危险操作）
docker compose down -v
```

## 安全建议

- 不要把真实 `.env` 提交到仓库。
- 所有密码、Token、密钥都使用强随机值。
- 生产环境建议仅内网暴露服务，并配合反向代理和访问控制。
