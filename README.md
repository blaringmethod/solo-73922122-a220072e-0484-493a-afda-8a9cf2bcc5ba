# 预测区间回测台

用 **FastAPI + SQLite + 原生前端** 实现的预测区间（prediction interval）回测工作台。
数据工程师上传预测 CSV 与实际值 CSV，指定 UTC 评估截止时间，系统选取截止前最新一版
预测，按实际 lead time（issue → target）分组统计覆盖率与平均区间宽度，并列出区间外样本。
整批校验通过后才写入数据库，可随时下载结果。

## 预测解析索引（重复上传不再重复解析）

同一份预测 CSV 配合不同实际值或不同 cutoff 反复回测时，预测文件的解析结果与 cutoff
无关、且对确定的字节内容是确定性的，因此服务会把解析结果缓存为可复用的 SQLite 索引：

* **键**：预测文件**完整字节**的 SHA-256（不是文件名），内容相差一个字节就得到不同键，
  绝不会误命中；相同字节永远命中同一份索引。
* **复用**：后续上传相同内容时直接从索引重建解析结果（响应中
  `prediction_index.reused=true`），仍然按**本次 cutoff** 选取截止前最新预测，并执行
  原有整批校验与统计，计算语义与旧版本逐字节一致。
* **安全发布**：一份索引在单个事务内构建，只有全部行解析并写入完成才把状态从
  `building` 翻成 `ready`；构建失败（编码/表头/缺列等结构性错误）整体回滚，不留任何
  记录，下次上传干净重试。任何不完整或残留的 `building` 标记都不会参与回测。
* **进程重启**：索引是共享 SQLite 文件中的普通行，重启后继续可用；崩溃时未提交的
  `building` 事务由 SQLite 自动回滚，不会出现残留可用索引。
* **并发单建**：两个服务进程同时首次上传同一文件时，靠进程内摘要锁 + SQLite
  `BEGIN IMMEDIATE` 写锁串行化，失败者等待后复用获胜者的成果，**只产生一份可用索引**。
* 实际值 CSV 每次回测都不同，不进入索引；历史回测记录及其全部下载接口保持不变。

索引保存在独立的可丢弃缓存库（默认 `data/prediction_index.db`），与回测审计库分离；
删除该文件只会让下次上传重新构建，不影响任何已保存的回测。

## 快速启动（Docker Compose）

```bash
docker compose up --build
```

打开 <http://localhost:8000>。数据库保存在命名卷 `backtest-data` 中。

端口与上传大小可覆盖：

```bash
BACKTEST_PORT=8080 BACKTEST_MAX_UPLOAD_BYTES=52428800 docker compose up --build
```

本机直接运行（不使用 Docker）：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 输入格式

预测 CSV（表头必须齐全，顺序不限）：

| 列 | 说明 |
| --- | --- |
| `series_id` | 序列标识 |
| `issue_time` | 预测发布时间，ISO-8601 |
| `target_time` | 被预测的目标时刻，ISO-8601 |
| `lower` / `upper` | 区间下界 / 上界 |

实际值 CSV：

| 列 | 说明 |
| --- | --- |
| `series_id` | 序列标识 |
| `target_time` | 目标时刻 |
| `actual` | 实际值 |

时间字段统一按 **UTC** 处理：

* 带 `Z`、`+08:00` 等偏移的值会被换算到 UTC；
* 不带时区的值**按 UTC 解释**（不会套用服务器本地时区）；
* 界面上 `datetime-local` 输入框填写的墙上时间同样视为 UTC。

`samples/` 下有一组可直接上传的示例（`predictions.csv`、`actuals.csv`，
以及用于验证拒绝路径的 `predictions-invalid.csv`），截止时间填 `2026-11-20 12:00`。
页面上的「填充示例数据」按钮也能一键载入。

## 计算规则

1. **选取预测**：对每个 `(series_id, target_time)`，在所有 `issue_time <= cutoff`
   的预测中取 `issue_time` 最新的一条。`issue_time == cutoff` 视为可用；晚于截止的预测
   被忽略，若某目标时刻只剩截止后的预测，则该批立即被拒。
2. **分组**：horizon 由 `target_time - issue_time` 的**实际时长**决定，而不是文件名或额外列。
   时长相同的样本归为同一组，标签如 `30m`、`1h`、`1h 30m`、`1d 2h`。
3. **统计**：每组输出样本数、覆盖数、覆盖率（`covered / samples`）、平均区间宽度
   （`upper - lower` 的均值）。覆盖判定为 `lower <= actual <= upper`。
4. **区间外样本**：单独列出，并给出越界幅度。
5. **整批校验**：任一行解析失败、重复预测（同 series/issue/target）、冲突重复
   （同键不同上下界）、上下界反转（`lower > upper`）、时间反转
   （`issue_time > target_time`）、重复实际值、或任一目标时刻缺少可用预测 / 实际值，
   都会 **拒绝整批**，不保存任何记录，并在页面列出全部错误。

## 输出下载

校验通过并保存后，页面与 API 提供：

* `GET /api/backtests/{id}/download/samples.csv` — 每个被评估样本一行
  （含 horizon、lead_seconds、区间、实际值、是否覆盖）
* `GET /api/backtests/{id}/download/summary.csv` — 各 horizon 的覆盖率与平均宽度
* `GET /api/backtests/{id}/download/input/prediction.csv` — 原始预测文件
* `GET /api/backtests/{id}/download/input/actual.csv` — 原始实际值文件

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET` | `/` | 工作台页面 |
| `GET` | `/docs` | Swagger UI，可直接上传文件演示复用与并发场景 |
| `GET` | `/healthz` | 健康检查（容器 HEALTHCHECK 使用） |
| `POST` | `/api/backtests` | multipart：`prediction_csv`、`actual_csv`、`cutoff`（UTC）；成功 `201`，校验失败 `422` 且 `errors` 列出原因。响应新增 `prediction_index`：`content_sha256`、`byte_size`、`reused`（本次是否命中缓存索引） |
| `GET` | `/api/backtests?limit=20` | 最近的回测（不含逐样本明细） |
| `GET` | `/api/backtests/{id}` | 单次回测的完整明细 |
| `GET` | `/api/prediction-index?limit=50` | 列出已建立的预测解析索引（诊断：SHA-256、状态、行数、错误数、时间） |
| `GET` | `/api/prediction-index/{sha256}` | 查询单份索引状态，不存在返回 404 |

请求示例：

```bash
curl -X POST http://localhost:8000/api/backtests \
  -F prediction_csv=@samples/predictions.csv \
  -F actual_csv=@samples/actuals.csv \
  -F cutoff=2026-11-20T12:00:00Z
```

### 在 /docs 演示重复上传与并发

启动后打开 <http://localhost:8000/docs>：

1. 展开 `POST /api/backtests` → **Try it out**，选择 `samples/predictions.csv` 与
   `samples/actuals.csv`，cutoff 填 `2026-11-20T12:00:00Z`，执行后响应中
   `prediction_index.reused` 为 `false`（首次构建）。
2. 原样再执行一次（也可换 cutoff 或实际值文件），这次 `reused=true` 且
   `content_sha256` 完全相同；页面结果卡片顶部会显示「♻ 复用解析索引」。
3. 调用 `GET /api/prediction-index` 可看到该 SHA-256 只有一份 `ready` 索引。
4. 并发首传（同一文件、十个并行请求，全部成功但索引只构建一次）：

   ```bash
   for i in $(seq 1 10); do
     curl -s -o /tmp/conc-$i.json -X POST http://localhost:8000/api/backtests \
       -F prediction_csv=@samples/predictions.csv \
       -F actual_csv=@samples/actuals.csv \
       -F cutoff=2026-11-20T12:00:00Z &
   done; wait
   python3 -c "import json,glob; r=[json.load(open(f))['prediction_index']['reused'] for f in glob.glob('/tmp/conc-*.json')]; print('builds =', r.count(False), 'reuses =', r.count(True))"
   ```

5. 改动预测文件任意字节后再上传，得到新的 SHA-256 与新索引（`reused=false`），
   不同内容互不干扰。

### 环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BACKTEST_DB_PATH` | `data/backtest.db` | 回测审计库（历史记录与原始上传字节） |
| `BACKTEST_INDEX_DB_PATH` | `data/prediction_index.db` | 预测解析索引库（可删除，删除后自动重建） |
| `BACKTEST_INDEX_BUSY_TIMEOUT` | `60` | 并发首传时等待 SQLite 写锁的秒数 |
| `BACKTEST_MAX_UPLOAD_BYTES` | `20971520` | 单文件上传大小上限 |

## 结构

```
app/
  main.py             FastAPI 路由、上传、下载、索引诊断接口
  backtest.py         纯计算：CSV 解析（可缓存）+ cutoff 选版、整批校验、分组统计
  prediction_index.py SHA-256 键的 SQLite 解析索引（WAL、跨进程单建、失败回滚）
  storage.py          SQLite 持久化与 CSV 渲染
  static/             index.html / style.css / app.js（无框架）
tests/
  test_backtest.py          21 个用例：校验规则、分组统计、API 往返
  test_prediction_index.py  14 个用例：复用/隔离、失败回滚、重启持久化、
                            线程与多进程并发单建、与旧路径结果等价
```

跑测试：

```bash
pip install pytest httpx
python -m pytest -q
```
