# Scope Recall

本仓库是 [`410979729/scope-recall-hermes`](https://github.com/410979729/scope-recall-hermes) 的 fork，仓库名 `849506054/scope-recall-hermes`，当前版本 `3.11.2.4`，跟版基线上游 `v3.11.2`。每次发版的说明在本仓库的 [CHANGELOG.md](CHANGELOG.md) 里，每个版本对应一个 fork tag。

这个 fork 只为一类环境准备：**机器跑不了 LanceDB 的本地向量检索，但能连到一台 Qdrant 和外部 embedding 服务**。典型是 N5100 / N5105 这类没有 AVX2 的 NAS、软路由主板 —— `import lancedb` 直接 SIGILL（退出码 132），上游自带的 `sqlite-bruteforce` 后端是纯 Python 逐条扫描，我们在 N5105 上量到 2.7 万条 2048 维向量单次查询 17–22 秒，召回窗口装不下。

换到远端 Qdrant 之后，同一批数据单次查询中位约 8 ms、p90 约 10 ms；Qdrant 进程常驻约 217 MB 内存、数据约 277 MB。SQLite 仍是事实权威，store schema 与上游一致；要换回上游包，把 `vector` 段改回上游支持的后端就行。

## 适用前提

三条都满足才用得上这个 fork：

1. **本机跑不了 LanceDB** —— CPU 无 AVX2 时原生库加载即崩，崩在 `import` 期，会连带宿主进程 core dump。
2. **有一台自己管的 Qdrant** —— 内网可达即可，不要暴露到公网。
3. **有能调的 embedding 服务** —— 向量由服务算好再写进 Qdrant，fork 不做本地推理。

## 与上游的差异

改动分三类：绕开本地 ANN 的硬件死路、修上游缺陷、宿主与打包适配。上游的契约与 SQLite schema 没有分叉。

完整账本以 git 为准，别照着文档手抄：

```bash
git diff --stat upstream/v3.11.2 HEAD
```

### 新增模块

| 模块 | 作用 |
|---|---|
| `vector/qdrant_store.py` | 远端 Store 实现：集合身份绑定、fenced 写入、检索只取命中字段（不把 2048 维向量拖回来） |
| `vector/qdrant_config.py` | `vector.qdrant` 的严格校验（只认四个键，多一个就拒） |
| `vector/qdrant_http.py`、`vector/_qdrant_http_worker.py` | HTTP 传输层与独立写入 helper 子进程：来源只解析一次、按字节判 body 上限 |
| `vector/qdrant_mutation.py` | 未决写标记：超时不算未写入，未确认的删除不算删除 |
| `maintenance/vector_migration.py` | 本地 → 远端伴随迁移：逐页问目标缺哪些、逐页校验、可断点续跑、不重新 embedding |

### 新增配置

| 键 | 默认 | 说明 |
|---|---|---|
| `vector.backend` | `"lancedb"` | 新增取值 `"qdrant"`。选它必须同时给 `vector.qdrant`，否则配置报 `qdrant` 拒收 |
| `vector.qdrant.url` | 必填 | 只收 `http(s)://host[:port]` 形式，不能带路径、不能夹凭据 |
| `vector.qdrant.api_key_env` | `SCOPE_RECALL_QDRANT_API_KEY` | 存放 key 的环境变量名；key 本身不进配置文件 |
| `vector.qdrant.collection_prefix` | `"scope-recall"` | 实际集合名是 `<prefix>-<安装身份、嵌入空间与表形状派生的摘要>`，不同安装、不同嵌入空间不会共用集合 |
| `vector.qdrant.timeout_seconds` | `2.0` | 召回级预算；批量迁移另给 45 s。生产建议 `10.0`，2 s 在 Qdrant 抖动时会留下未决标记 |
| `vector.tool_output_embedding` | `true` | 设 `false` 时工具输出照常入库、进词法索引，但不做 embedding（`tool_output_retention_days` 继续回收已有向量） |
| `auxiliary.embedding.proxy_url` | 无 | 让这条嵌入路由走自己的出网代理；代理只进承载该路由的 helper 子进程，宿主其余进程不受影响 |

整合路由与嵌入路由的明文 `http://` 许可（`allow_insecure_endpoint`）统一走上游的 `core/endpoint_policy.py`，fork 直接复用。

### 行为修补

- **Voyage 用量记账**：`usage` 里只有 `total_tokens` 时按它记账（`runtime/embedding_models.py::_embedding_usage`）。
- **查询嵌入连接池**：单条常驻连接改为三条（`QUERY_CHANNEL_POOL`）。本机实测四条并发从 2.7 s 降到 1.3 s；借不到通道按截止时间抛 `AuxiliaryModelError("timeout")`。
- **语义检索与本地通道并行**：语义检索在召回开始时就与 SQLite 各通道同时启动，两边共用同一个截止时间（`runtime/instance.py::_QuerySearch`）。
- **宿主与打包**：`runtime/instance.py`、`maintenance/doctor.py`（报告远端集合）、`maintenance/install.py`、`packaging/v11-module-allowlist.json`、`pyproject.toml`、`scripts/check.py`、两个宿主 manifest。
- **维护工具**：`maintenance/cli.py` 加了下面维护命令表里的几条。

## 部署

以「Hermes 宿主 + 自建 Qdrant」为准，路径按自己的实例替换。

### 1. 起 Qdrant

Qdrant 只给宿主和内网用，不暴露到公网；带 key、带持久卷、镜像用 digest 固定：

```yaml
services:
  qdrant:
    image: qdrant/qdrant:<tag>@sha256:<digest>
    restart: unless-stopped
    environment:
      QDRANT__SERVICE__API_KEY: ${SCOPE_RECALL_QDRANT_API_KEY}
    volumes:
      - ./qdrant/storage:/qdrant/storage
      - ./qdrant/snapshots:/qdrant/snapshots
    networks: [memory]

networks:
  memory:
    internal: true
```

宿主和 Qdrant 放进同一个 docker 网络，配置里写服务名（例如 `http://qdrant:6333`）；从宿主 shell 里调就用发布端口或 `docker exec`，两边的地址不是一回事。

`internal: true` 只让这个网络不出外网，宿主容器同时挂在自己的出网网络上，embedding 请求照常出去。

给 payload 里高频过滤的字段建 keyword 索引。没建索引时，带 filter 的 scroll 实测从毫秒级涨到十几秒，表现为 worker 报 `vector_unavailable` 而 Qdrant 看起来健康。

### 2. 装包

这个 fork 不往 PyPI 发包，也不往 GitHub Release 挂附件；一个版本就是一个 tag，包从 tag 自己构建：

```bash
git clone https://github.com/849506054/scope-recall-hermes.git
cd scope-recall-hermes
git checkout v3.11.2.4
uv build --wheel      # 产物落在 dist/
uv pip install dist/hermes_scope_recall-3.11.2.4-py3-none-any.whl
```

宿主用独立目录加载（`PYTHONPATH` 或 `.pth` 注入）时，装进目标目录，别装进解释器 site-packages：

```bash
uv pip install --target /path/to/scope-recall-pkg dist/hermes_scope_recall-3.11.2.4-py3-none-any.whl
```

走 `qdrant` 后端不会导入 `lancedb`，不需要 `lancedb` extra。装完先确认导入落在安装包、不在源码 checkout：

```bash
env -u PYTHONPATH python3 -c "import scope_recall, scope_recall._version as v; print(scope_recall.__file__, v.__version__)"
```

### 3. 把已有的本地向量迁过来

全新的实例跳过这一节，直接看下一节写配置。已有本地向量库的，在维护窗口里做，**这一步配置里的 `vector.backend` 仍然是原来的本地后端**（迁移的源就是它，目标地址由命令行给）：

```bash
# 1) 停掉所有写这个 store 的进程：宿主 gateway、worker、watchdog
# 2) 复制 + 校验：目标集合不存在时由它创建，幂等，可续跑
scope-recall run-vector-migration --host hermes --instance-root <instance-root> \
  --qdrant-url http://qdrant:6333

# 3) 想复核剩余差量（只读估算），目标集合已在上一跑建好
scope-recall plan-vector-migration --host hermes --instance-root <instance-root> \
  --qdrant-url http://qdrant:6333
```

输出里 `verify.ok` 必须是 `true` 才能进下一节；不为真就停在窗口里查，别切配置。

配置里改了 `collection_prefix` 的话，这两条命令也要带上同样的 `--qdrant-collection-prefix`，否则它按默认前缀算集合名，复制的不是运行时读的那个集合。

迁移只搬已经算好的向量，不重新调用 embedding。必须先停写入者：源库被打开过会带 WAL 侧车，只读打开会被 SQLite 守卫拒绝，这正是这一步要进窗口的原因。

校验没过就翻配置，语义召回会缺一段而且不报错，所以这一步不将就。

### 4. 写配置

实例的 `runtime-config.json`，`vector` 段切到远端后端，`auxiliary.embedding` 指向自己的 embedding 服务。上一节做了迁移的，等 `verify.ok` 为真再落这份配置：

```json
{
  "vector": {
    "backend": "qdrant",
    "storage_dir": "<data_directory>/vectors/<embedding space id>",
    "table_name": "vectors",
    "dimensions": 2048,
    "metric": "cosine",
    "tool_output_retention_days": 180,
    "tool_output_embedding": true,
    "qdrant": {
      "url": "http://qdrant:6333",
      "api_key_env": "SCOPE_RECALL_QDRANT_API_KEY",
      "collection_prefix": "scope-recall",
      "timeout_seconds": 10.0
    }
  },
  "auxiliary": {
    "embedding": {
      "kind": "openai",
      "endpoint": "https://api.voyageai.com/v1/embeddings",
      "model": "voyage-4-large",
      "dimensions": 2048,
      "dimensions_field": "output_dimension",
      "dialect": "openai",
      "credential_env": "VOYAGE_API_KEY"
    }
  }
}
```

几条硬约束，写错会在加载期直接拒收：

- `dimensions` 必须等于嵌入路由的宽度，否则 `VECTOR_DIMENSIONS_MISMATCH`。
- `storage_dir` 必须正好是 `<data_directory>/vectors/<embedding space id>`，否则 `VECTOR_STORAGE_OUTSIDE_BINDING`。远端后端下向量写在服务端，这个目录只留本地遗留物。
- key 只放环境变量，配置文件里只有变量名。
- embedding 服务在内网、只有明文 `http://` 时，要在该路由上写 `"allow_insecure_endpoint": true`。回环默认放行；跨机必须显式写，且明文连接会剥掉凭据头，需要 key 的服务只能走 HTTPS。

### 5. 装到宿主

```bash
INSTANCE=/path/to/hermes-home
PLUGIN=/path/to/wrappers/scope-recall
PYTHON=/path/to/python      # hermes 命令本身用的那个解释器
AGENT=default               # 必须等于宿主 initialize 时送的 agent_identity

scope-recall plan-install --host hermes \
  --target-plugin-dir "$PLUGIN" --instance-root "$INSTANCE" \
  --agent-id "$AGENT" --python "$PYTHON"
scope-recall apply-install --host hermes \
  --target-plugin-dir "$PLUGIN" --instance-root "$INSTANCE" \
  --agent-id "$AGENT" --python "$PYTHON"
# 重启宿主 gateway
scope-recall doctor --host hermes --instance-root "$INSTANCE"
```

`plan-install` 只打印 JSON，`conflicts` 非空时退出码是 1，先解冲突再 apply。`--agent-id` 和宿主送的 `agent_identity` 不一致时，适配器会抛 `agent_identity conflict`。其余标志（`--project-root`、`--local-platform`、`--owner-login`）见 [docs/install.md](docs/install.md#4-install-for-a-hermes-host) 第 4 节。

`doctor` 要看 `package_health.version_mismatch.versions` 四方一致：`distribution` / `imported` / `receipt` / `running`。`running` 是列表，只在进程构造运行时写出 —— 重启后还没有会话活动时它会是空的，这时要么起一个探针进程走 worker 的加载路径产生一次记录，要么接受「三版一致 + 网关心跳新鲜」放行，等下个会话再复读。

新版本要读的配置键在换包**之前**就写进配置文件，并在旧包上确认它被忽略、在新包上确认它生效。窗口里就只剩换目录与重启。

### 6. 验收

- `doctor`：版本四方一致；`coverage_delta` 为 0，或者只差 ±1（分页竞态，隔一会儿复读再看）；集合状态 green，点数与 SQLite 期望一致。
- 真实链路：发一条消息确认捕获入库，再问一句只有语义命中才答得上的问题，看召回包里有没有 `vector_unavailable`。
- 缺口写成 `vector_unavailable:QdrantHTTPError:timeout` 时，先查 Qdrant 自己的访问日志：同一条 `/points/query` 返回 200 就是请求侧预算问题，不是服务坏了。

### 7. 回滚

- 换包前留三样：旧包目录、旧 wrapper、一份 store 快照（`scope-recall backup --database <store>/memory.sqlite3 --output <new file>`）。本 fork 各版本之间 store schema 没变，回滚到上一版包能直接打开库。
- 远端数据另有服务端快照：`snapshot-remote` 打快照，`recover-remote --apply` 才真正替换集合，不加 `--apply` 只出预演。
- 恢复旧包装机时，先恢复旧 wrapper，再删掉 receipt 拥有的版本戳文件（`plugin.yaml`），否则 `apply-install` 会因摘要守卫报 `edited prior file`。

## fork 维护命令

| 命令 | 用途 |
|---|---|
| `snapshot-remote --host … --instance-root … [--output …]` | 在写边界内让 Qdrant 打一份服务端快照 |
| `recover-remote --host … --instance-root … --snapshot <ref> [--apply]` | 预演 / 执行从快照恢复集合 |
| `plan-vector-migration` / `run-vector-migration` | 本地 → 远端伴随迁移，参数见第 3 节 |
| `clear-pending-remote --host … --instance-root … --qdrant-url … --confirm <collection> [--after-recopied]` | 受控窗口内、服务端读回一致后清未决写标记；集合里已有数据必须带 `--after-recopied` |
| `retry-failures --config <file> [--limit N] [--apply]` | 给一批失败工作项重开机会；不加 `--apply` 是只读预览 |

## 已知边界

- **远端写入超时 → 留未决标记，写入侧 fail-closed**。`upsert` 类标记在下次打开时读回服务端即自愈；`delete` 类不会自愈，要人工 `clear-pending-remote`，动手前先备份标记文件。
- **集合绑定安装身份、嵌入空间与表形状**。换安装、换嵌入模型都要新建集合；换模型本身走 `respace-embeddings`，不是改 `dimensions`。
- **Qdrant 只有 key 鉴权**。别把 6333 暴露到不可信网络，明文 HTTP 上的 key 就是明文凭据。
- **fork 的扩展上游没有**。远端后端、`tool_output_embedding`、嵌入代理这三类能力属于 fork 侧，跟版时逐项核对上游有没有吸收。

## 发版与跟版

- **版本号 = 上游版本号加一位**：上游 `v3.11.2` → fork `3.11.2.1`、`3.11.2.2`、`3.11.2.4`、`3.11.2.4`。裸 tag 空间只属于 fork，上游 tag 收在 `refs/tags/upstream/*`。
- **只跟稳定 tag，不跟 rc**。小版本前移必跟；同一小版本内的补丁版按「问题有多严重、修复有多急」决定，其余攒到下一个要跟的版本一起做。
- **跟版即销项**：上游已经修了同一处，就删掉我们的改动，不留双份。
- 发版前跑完整门禁（unit / contract / packaging / host / migration），tag 只打在门禁验过的提交上；wheel 从该提交构建、与提交逐字节核对后再打 tag。

---

# 上游原版 README

下面是上游 `410979729/scope-recall-hermes` 的 README 原文，只把一级标题降到这里，其余保持原样；上游文档里的 `pip install -U`、PyPI 发包、GitHub Release 和 `lancedb` 等说法针对上游自己的仓库与后端，本 fork 不发布包，只出 tag。

Scope Recall v3 is a bounded local memory core with SQLite as the authority and rebuildable vector companions. It provides host adapters for Hermes, and for Codex, Claude Code, WorkBuddy and DeepSeek Harness (dsh), which share one adapter of hooks and an MCP server (the MCP tools need the optional `codex` extra; dsh runs the hooks through a plugin). The public package is `hermes-scope-recall`; the Python import is `scope_recall`; the host wrapper identity remains `scope-recall`.

This checkout is `3.17.0.1`. Hermes, Codex, Claude Code, WorkBuddy and dsh can keep one memory: each attaches to a shared
store as an entry, what the owner tells one of them another can recall, and each memory says
which agent it came in through ([docs/shared-store.md](docs/shared-store.md)). An agent that is not
attached keeps its own store. A tool's output is kept and found, but not turned into facts. Each release's notes
are a line or two in [CHANGELOG.md](CHANGELOG.md) and in full on its
[GitHub release](https://github.com/410979729/scope-recall-hermes/releases). Upgrading from `3.2.x` or later to `3.17.0` is
`pip install -U`, `apply-install` and a host restart; the first store open adds the local management audit and private-interval tables. Back up
before upgrading and follow the [management migration and rollback guide](docs/manage.md#migration-and-rollback).
On a shared store in
the shipped embedding space, set `vector_threshold` from 0.653 to 0.70 by hand
([docs/configuration.md](docs/configuration.md#vector_threshold)). Older supported stores migrate in order to schema 1112;
an earlier release cannot open the upgraded store. Section 9 of the
[3.1.0 release notes](https://github.com/410979729/scope-recall-hermes/releases/tag/v3.1.0) is the
migration procedure for a 2.0.1 memory database.
Facts are drawn from accumulated evidence rather than judged one sentence on
sight; only the owner's explicit correction or confirmation changes a fact as it
is captured. SQLite is the only fact authority; host adapters share the same
contracts.

Automatic recall injects zero to six items that clear a lexical coverage or
vector similarity floor. Background preferences and tasks accompany passing
query evidence; a session's first resume request can recover its current task.
Acknowledgments, approvals, praise, bare continuation and session status prompts
within an existing session start no search or query embedding. Explicit recall
keeps its admission behavior. The thresholds and diagnostic counts are described
in [the recall configuration](docs/configuration.md#vector_threshold).

**Recall-quality checks.** The [synthetic recall benchmark](CONTRIBUTING.md#the-recall-benchmark)
provides reproducible development and held-out splits for retrieval, answerability,
scope isolation, packet size and latency. It uses proposed claims and a word-hashing
embedder without a model or network, so it does not establish consolidation quality,
semantic embedding quality or performance on private conversations. The test tiers
also check contracts, storage and host integration.

## Manage your memories

Use `scope-recall ui` with the same store selection to open the [local management page](docs/ui.md).
It provides Chinese and English browsing, reviewed actions, private rules, export and backup status in your browser.

Use `scope-recall memory` to browse, search, inspect history, correct a fact, mute it or delete a conversation.
It runs as the local store owner and makes no model request. Deletion shows the dependent sources and facts before
`--yes` applies it. `scope-recall memory export --out <new directory or .zip>` writes the visible store as a portable
package with original sources, facts, history, episodes, readable Markdown and optional retained attachments.
`scope-recall memory secrets` lists stored credential matches by reference, provenance, time and pattern names.
After upgrading, review its findings, remove them with `scope-recall memory forget <refs>` and change any credential found.
[The management guide](docs/manage.md) covers store selection, filters, version checks and receipts; the
[export guide](docs/export.md) covers package layout, hashes and filtering.

Use `scope-recall private on --session <id> | --project <path> | --all` to stop keeping a session,
directory or the whole store. `--mode full` refuses capture, recall and background model work;
`--mode read-only` refuses capture while recall continues; `--all --for 2h` sets a duration and `off`
closes a rule. Rules are stored as metadata only, overlap takes the stricter mode, and an event is
refused by its own occurrence time even when a later retry or transcript read arrives after the
rule ends. Undated events are refused when earlier private history could cover them. Directory
identity follows junctions and symbolic links on the owning machine; recalls and outbound
requests check again before release. [The private-mode guide](docs/private-mode.md) lists every host path, injection notice,
migration and what is stored or sent in each mode.

## For agents: install or upgrade on the user's behalf

Users only need to ask "install Scope Recall" or "帮我升级一下 scoperecall".
Start with `scope-recall setup --host <hermes-or-codex> --home <actual-instance-home>`.
New users go directly to installation; only detected legacy databases go through
[the agent migration workflow](maintenance/AGENT_WORKFLOW.md). The installed
`scope-recall-setup` skill makes this routing discoverable in Hermes and Codex.
A second installed skill, `scope-recall-memory`, which Claude Code gets too, is for the owner's everyday questions:
what is remembered about me, did I say it or was it worked out, does it still hold, and
what correcting, muting or deleting one memory does before it is done. Claude Code, WorkBuddy and dsh only read
memory through their tools; the change is made in Codex or a Hermes chat on the same store.
Perform path discovery, backup, audience binding, migration, indexing and host
checks yourself; do not ask the user to execute commands or govern old memories.

## Install

Step-by-step Hermes and Codex instructions: [docs/install.md](docs/install.md). Claude Code
installs only as an entry of a shared store, and Codex can join one too:
[docs/shared-store.md](docs/shared-store.md). WorkBuddy runs the same hooks and MCP server, also
only as an entry; its installer adds them to WorkBuddy's own `settings.json` and `mcp.json`
([docs/install.md](docs/install.md), section 12). dsh (DeepSeek Harness) runs them through a plugin
the installer writes and names in dsh's home patch, also only as an entry, with dsh's upload of its
session logs switched off ([docs/install.md](docs/install.md), section 13).

The package is `hermes-scope-recall` on PyPI. Install or upgrade it, with its vector extra, into
the same isolated Python environment the host uses. The default vector store is LanceDB: without
`[lancedb]` it cannot open, and recall finds memories by their words alone (the
`sqlite-bruteforce` backend needs no extra):

```text
python -m pip install -U "hermes-scope-recall[lancedb]"
python -m pip install -U "hermes-scope-recall[lancedb,codex]"
```

The second line is for Codex, Claude Code, WorkBuddy and dsh, whose MCP server needs the `codex`
extra. `-U` moves an older install to the current release; write `==<version>` instead to pin
one. A Hermes that builds its own plugin environment
installs the core from the declaration the installer writes instead (below). `doctor` names an
environment that lacks the vector extra while a LanceDB store is configured
(`vector_dependencies_missing`).

The same wheel and sdist are attached to each
[GitHub release](https://github.com/410979729/scope-recall-hermes/releases/latest)
alongside `SHA256SUMS` and `RELEASE-PROVENANCE.json`, for an offline install
(`python -m pip install "<path-to-wheel>[lancedb]"`). To build it yourself from this checkout instead:

```text
python -m build --wheel
```

Upgrading from 2.0.x is not an in-place upgrade. Read section 9 of the
[3.1.0 release notes](https://github.com/410979729/scope-recall-hermes/releases/tag/v3.1.0) before you start; your old database needs a
one-time offline migration and there are two errors people commonly hit.

The two console names `scope-recall` and `hermes-scope-recall` invoke the same v3 maintenance CLI. They are aliases for the current CLI only; neither is a compatibility promise for an older command set.

Use explicit absolute paths for installation planning. Hermes `--agent-id` must match the host active profile (`get_active_profile_name()`, commonly `default` on an isolated home). Hermes default `--agent-workspace` is `hermes` to match the host memory-provider init contract; pass the same value on plan and apply if you override it. Codex does not accept `--agent-workspace`.

If you talk to Hermes through the Desktop app or `hermes --tui` rather than the CLI, add `--local-platform desktop` (or `tui`) to both commands. Those surfaces name no user unless a dashboard login exists, and a session that names no user is refused everywhere but the CLI until the installer approves the surface; where Hermes's gateway runs those conversations over its local session transport, it names the account the gateway runs as, which binds as the owner on an approved surface and binds nothing on another. [docs/install.md](docs/install.md) says what the approval does and does not cover. If you reach the host through a dashboard login instead (`hermes serve` with `dashboard.basic_auth`, the Desktop app on another machine), the session is that login: approve it as yours with `--owner-login desktop=basic:<name>`. A Hermes with its package
manager (Hermes Desktop, and gateways on a Hermes that has one) builds the Python environment it runs plugins in, and
builds it again on updates: pass it `--target-plugin-dir <home>\plugins\scope-recall`, where Hermes finds the plugin
and the core it declares, vector dependencies included, then let Hermes sync that environment and restart it
(sections 1 and 4 of [docs/install.md](docs/install.md)).

```text
scope-recall plan-install --host hermes --target-plugin-dir <absolute-plugin-dir> --instance-root <absolute-instance-root> --project-root <absolute-project-root> --agent-id <agent-id> --python <absolute-python>
scope-recall apply-install --host hermes --target-plugin-dir <absolute-plugin-dir> --instance-root <absolute-instance-root> --project-root <absolute-project-root> --agent-id <agent-id> --python <absolute-python>
scope-recall doctor --host hermes --instance-root <absolute-instance-root> --python <absolute-python>
```

Codex uses the same commands with `--host codex`, plus `--env-file <absolute-file>` so the Codex-launched MCP server and hooks can read the credential names the runtime config declares (Codex does not pass them in the environment). The installer writes only its own wrapper and installation records and preserves foreign host files. Its receipt records host registration and applicable hook trust as pending at installation time; use doctor and actual host loading to verify the current state. Uninstall keeps Core data by default; explicit purge is bounded to verified installation-owned data and refuses uncertain ownership or active writers.

## Uninstall (default: retain memory)

Uninstall is receipt-driven. By default it removes only plugin wrapper files and retains Core data. Inspect the plan before applying:

```text
scope-recall plan-uninstall --instance-root <absolute-instance-root>
scope-recall apply-uninstall --instance-root <absolute-instance-root>
```

`--target-plugin-dir` may be omitted when the install receipt records it. Purge of installation-owned Core data requires a separate `plan-uninstall --purge` inspection and matching `apply-uninstall --purge`; see [docs/install.md](docs/install.md). Do not treat purge as a normal uninstall step.

## Data and host boundaries

SQLite truth lives in the verified Core data directory. Vector indexes are companions and may be rebuilt only through an explicit configured space. The Hermes and Codex adapters bind host identity, installation identity, scopes, and project roots before opening Core. After that binding is verified, an omitted runtime-config argument checks only `<data_directory>/runtime-config.json`; a missing file stays basic plus an explicit capability gap. The file is never generated or discovered from the current directory, parent directories, or credential locations. Optional host wiring must report a capability gap instead of creating or repairing a database.

The `codex` extra adds the MCP SDK. Without that extra, the Core and Hermes paths remain importable. The `lancedb` extra adds the LanceDB vector companion, the default `vector.backend`; without it, configure `sqlite-bruteforce` or recall stays lexical. PostgreSQL/pgvector is outside this v3 distribution; an explicit configuration error directs operators to retain the old installation and use the migration guide.

On Windows, choose a short data directory such as `C:\ScopeRecall\my-agent`. LanceDB appends index, table, and temporary file names to that path. If the resulting native path is too long, the worker reports `native_vector_path_too_long` before starting LanceDB or calling the embedding API. Use a short target directory for a new installation or an offline migration.

## Profile and entity read views

Two shared read-only Core methods, `profile` and `entity`, are exposed as Hermes tools and on Codex MCP. They return a deterministic categorized current-fact view, or an exact one-hop statement view, over admitted consolidated claims only. Raw chat is never silently turned into a profile. Incoming relations match the full scalar `value_text` only and keep recorded conditions and validity. Explicit project-name aliases may resolve when they are already admitted and still live; person aliases are not generalized. A `budget_tokens` value too small for even the minimal truthful envelope is a validation error, not an oversized view. See [docs/profile-entity.zh-CN.md](docs/profile-entity.zh-CN.md).

## Bounded multi-hop evidence paths

Use `trace` when a question needs two or three recorded relationships joined
together. It is read-only, shared by Hermes and Codex, and reuses existing SQLite
fact/evidence/visibility checks. It does not invoke another model or persist
inferred facts. Cross-scope names and conditional relations are not silently
joined. Node, path, time and explicit byte budgets bound its cost. See
[the trace contract and boundaries](docs/trace.zh-CN.md).

## One store for several agents

Several agents can share one store instead of each keeping its own: each attaches as an
entry, every memory is marked with the agent it came in through, and a deletion through
any of them applies to all. Moving the memory to another machine is copying one directory.
Hermes homes attach as entries, and so do Codex and Claude Code
(`attach --host codex|claude-code`). See [docs/shared-store.md](docs/shared-store.md).
WorkBuddy and dsh attach the same way (`attach --host workbuddy|dsh`). Full backup bundles
and disaster recovery restore to fresh destinations are in [docs/backup-restore.md](docs/backup-restore.md).

## Agent-operated migration

Users ask their agent to upgrade. The bundled `scope-recall-setup` skill routes
fresh installs directly to installation and existing Core databases to ordinary
updates. Only legacy SQLite uses a durable migration job. The agent reads
`scope-recall setup --workflow` and performs discovery, verified audience mapping,
backup, conversion, indexing and actual host checks. See
[the agent upgrade guide](docs/upgrade-guide.zh-CN.md).

Migration composes the existing converter, backup helper and worker. It preserves
original source/history/deletion semantics and never re-extracts the whole old
journal. Unsupported formats or unresolved permissions block cutover and preserve
the old installation. Index scheduling and actual live readiness remain separate.

## Tree layout

The package root holds only the entry (`__init__.py`), the version and the protocol contracts. `core/` is the host-independent memory core over SQLite truth; `vector/` the rebuildable vector companions; `adapters/` the Hermes plugin, the clients that use hooks and an MCP server (Codex, Claude Code, WorkBuddy, dsh; `adapters/clients`, with the entry module names installed configurations run kept in `adapters/codex`) and model transport; `runtime/` the background worker, budgets and scheduling; `maintenance/` install, doctor, upgrade and migration behind the operator CLI. Every shipped module is reachable by import from an entry point named in `packaging_hooks/module_inventory.py`; the wheel allowlist is derived from that, not typed.

## Development checks

The clean wheel must be tested outside the source checkout. At minimum, verify both CLI aliases, a read-only doctor result, Core capture and recall against a temporary installation, and the stdlib HTTP helper's bounded invalid-input response. Host registration, real gateway lifecycle, and production data are separate acceptance boundaries.
