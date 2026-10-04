# 基因组数据访问治理

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8304`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8304
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `dataset`：受控数据集（`registered → restricted ↔ published`）。
- `application`：访问申请；`withdraw` 可在批准前后撤回，`amend` 可修改用途（改用途会重新校验所有在途导出）。
- `grant`：限时数据使用凭证，`data.quota_limit` 可选，表示配额上限。
- `export`：导出任务，取数走两阶段 saga：`queued → pulling → receipted`，旁支为 `rejected`（越权/失效）和 `cancelled`。

## 一致性账

导出放行不单独看凭证，而是同时校验数据集、访问申请和授权凭证：

- 数据集处于 `restricted`：取数请求被**暂时阻断**（HTTP 423），任务保持 `queued`，不扣配额、不发回执。
- 凭证被撤回/过期、申请被撤回或申请用途与排队时不一致：取数请求被**终局拒绝**，导出落为 `rejected`，拒绝原因落审计。
- 已发出的回执（`receipts`）和已记账的配额扣减（`quota_ledger`）在限制/撤回后**保留**，供对账使用。
- 数据集恢复 `publish` 时，对每个排队导出按**原凭证的剩余期限**重新确认；凭证已过期或用途已变化的申请不会被重新放行。
- 配额扣减以 `(export_id, step)` 为主键，取数/发卡崩溃后重试只续做未确认步骤：重复请求不会多扣配额，也不会产生第二份回执。
- `continue` 与 `cancel` 并发提交时由乐观锁仲裁，只有一方生效，另一方收到冲突和最新版本号。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。导出创建字段：`dataset_id`、`grant_id`、可选`amount`。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`；动作支持`Idempotency-Key`请求头。
- `GET /api/exports/receipts/<id>`：读取某导出已发出的回执（任务被拒则404）。
- `GET /api/receipts`：回执列表，可用`?dataset_id=`过滤。
- `GET /api/ledger`：配额扣减账与回执的对账视图。
- `GET /api/audit`：读取审计记录（含 `pull`、`reject`、`reconfirm` 等导出事件）。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

数据目录和授权凭证是治理流程演示，不包含真实数据下载、加密或机构身份联邦。
