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

- `dataset`：受控数据集；`application`：访问申请；`grant`：限时数据使用凭证；`export_task`：导出任务；`receipt`：放行回执。

## 导出一致性账

导出放行（`POST /api/export_tasks/<id>/actions`，`{"action":"release"}`）不再只看凭证状态，而是把数据集、访问申请、授权凭证和导出任务接成一致性账：

- **限制或撤回即拒绝**：数据集被临时限制（`restrict`）或凭证被撤回/过期（`revoke`/`expire`）后，排队中的导出任务会被置为 `blocked`；放行时按完整链路重新校验，越权或已失效的取数请求一律拒绝，不扣配额、不发回执。
- **回执保留对账**：回执（`receipt`）一经发出即不可变，数据集恢复发布或凭证撤回都不会删除历史回执，供对账使用。
- **恢复发布重新确认**：数据集恢复发布（`publish`）时，对 `blocked` 任务按凭证剩余期限重新确认——凭证仍在有效期内、申请未过期且用途未变化的任务回到 `queued` 可继续放行；凭证过期或用途变化的申请直接 `rejected`，不再放行。
- **崩溃恢复只续未确认步骤**：放行拆成链路校验、配额扣减、回执签发三个持久化步骤，每步落库后才进入下一步。进程崩溃后 `resume` 只续做未确认步骤；重复放行幂等，不会多扣配额或留下第二份回执。
- **并发只让一方生效**：同一任务同时提交 `release`（继续）和 `cancel`（取消）时，先到的一方生效，另一方因版本冲突收到 409 并在响应的 `current` 中看到最新版本。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

数据目录和授权凭证是治理流程演示，不包含真实数据下载、加密或机构身份联邦。
