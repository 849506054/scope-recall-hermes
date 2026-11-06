# scope-recall-hermes（个人 fork）

本仓库是 [`410979729/scope-recall-hermes`](https://github.com/410979729/scope-recall-hermes) 的 fork，仓库名 `849506054/scope-recall-hermes`，当前版本 `4.0.0.4`，跟版基线上游 `v4.0.0`。每次发版的说明在本仓库的 [CHANGELOG.md](CHANGELOG.md) 里，每个版本对应一个 fork tag。

fork 只保留上游没有、而这套部署又必须有的改动：**嵌入路由的出网代理**。远端 embedding 服务要经本机自己的代理出去，上游的传输层不认代理设置；其余一切按上游走，包括向量存储。

上游 4.0 把向量从 LanceDB 搬进了 SQLite，`import lancedb` 不再出现在运行路径上。这个 fork 曾经为此自带一套远端 Qdrant 后端（N5100 / N5105 这类没有 AVX2 的机器加载 LanceDB 会 SIGILL，上游的纯 Python 扫描后端在本机量到 2.7 万条 2048 维向量单次查询 17–22 秒）。4.0 之后那条死路不存在了，Qdrant 后端随之退役，向量改用上游的本地 SQLite 存储。

## 与上游的差异

完整账本以 git 为准，别照着文档手抄：

```bash
git diff --stat upstream/v4.0.0 HEAD
```

**新增配置**

| 键 | 默认 | 说明 |
|---|---|---|
| `auxiliary.embedding.proxy_url` | 无 | 让这条嵌入路由走自己的出网代理；代理只进承载该路由的 helper 子进程，宿主其余进程不受影响 |
| `vector.tool_output_embedding` | `true` | 设 `false` 时工具输出照常入库、进词法索引，但不做 embedding |

**行为修补**

- **Voyage 用量记账**：`usage` 里只有 `total_tokens` 时按它记账（`runtime/embedding_models.py::_embedding_usage`）。
- **doctor 的解释器探针**：用 `-P` 而不是 `-I`，让装在常驻目录、靠 `PYTHONPATH` 供货的安装也能被量到（`maintenance/doctor.py::_PROBE_FLAGS`）。

整合路由与嵌入路由的明文 `http://` 许可（`allow_insecure_endpoint`）走上游的 `core/endpoint_policy.py`，fork 直接复用。

## 部署

以「Hermes 宿主 + 远端 embedding」为准，路径按自己的实例替换。

1. 装 wheel：宿主那个 venv 里 `pip install -U hermes_scope_recall-4.0.0.4-py3-none-any.whl`。
2. 写 `runtime-config.json`：`vector.backend` 用上游的 `"sqlite"`（省略也是它），`vector.storage_dir` 指到实例自己的向量目录；嵌入路由给 `endpoint`、`model`、`dimensions`、`dimensions_field`、`dialect`，以及 `proxy_url`。
3. 起 worker 前先 `scope-recall upgrade-store --backup-dir <目录>`：4.0 的 store 结构 1112 → 1134，首开自动迁移，升上去之后老版本打不开。
4. `scope-recall doctor` 看四路诊断，再起 worker。

## 发版与跟版

- **版本号 = 上游版本号加一位**：上游 `v4.0.0` → fork `4.0.0.1`。裸 tag 空间只属于 fork，上游 tag 收在 `refs/tags/upstream/*`。
- **只跟稳定 tag，不跟 rc**。小版本前移必跟；同一小版本内的补丁版按「问题有多严重、修复有多急」决定，其余攒到下一个要跟的版本一起做。
- **跟版即销项**：上游已经修了同一处，就删掉我们的改动，不留双份。
- 发版前跑完整门禁（unit / contract / packaging / host / migration），tag 只打在门禁验过的提交上；wheel 从该提交构建、与提交逐字节核对后再打 tag。

---

# 上游原版 README

下面是上游 `410979729/scope-recall-hermes` 的 README 原文，只把一级标题降到这里，其余保持原样；上游文档里的 `pip install -U`、PyPI 发包、GitHub Release 等说法针对上游自己的仓库，本 fork 不发布包，只出 tag。

This checkout is `4.0.0.4`. Local, source-backed memory shared by Claude Code, Codex, Hermes, WorkBuddy and DeepSeek Harness.
SQLite keeps the original conversations; model-derived facts and vectors remain traceable and rebuildable.

## Quick start

1. In the Python environment you want the hooks to use, run `python -m pip install -U "hermes-scope-recall[codex]"`.
2. Run `scope-recall setup`. Review the detected hosts, store, scopes, zero model budget and worker plan, then confirm.
3. Start a new host session. Approve the plugin hooks or MCP server when the host asks.

For this development checkout, install from the repository root with `python -m pip install -U ".[codex]"`;
the package-index command above selects a published release. An unpublished wheel can be installed directly with its `codex` extra.

| Host | Setup and host-specific approval |
|---|---|
| Claude Code | [Setup](docs/install/claude-code.md) |
| Codex | [Setup and hook review](docs/install/codex.md) |
| Hermes | [Setup and local identity](docs/install/hermes.md) |
| WorkBuddy | [Setup and MCP approval](docs/install/workbuddy.md) |
| DeepSeek Harness | [Setup and plugin check](docs/install/dsh.md) |

Without model routes or budget, setup reports **原文检索可用** (original-text search available).
Use `scope-recall setup --configure-models` for guided routes and budget configuration, or
`--models <route-file.json>` for automation. Successful model checks report **完整记忆功能可用**
(full memory features available). Model keys are environment references; setup never asks for a key value.
[Installation](docs/install.md) covers shared stores, reruns, worker commands and verification boundaries.
For a shared store on a Linux Docker host, see [Docker deployment](docs/docker.md).
Agents can [hand off a task](docs/host-integration.md#task-handoffs) across entries, with one receiving entry and a bounded session-start report.

Automatic recall injects zero to six items that clear a lexical coverage or
vector similarity floor. Background preferences and tasks accompany passing
query evidence; a session's first resume request can recover its current task.
Acknowledgments, approvals, praise, bare continuation and session status prompts
within an existing session start no search or query embedding. Explicit recall
keeps its admission behavior. Retrieval reserves time for compilation and fresh
SQLite release checks, so slow optional channels can leave a partial packet with
their cutoff reason. Unverified items never ship. The thresholds and diagnostic counts are described
in [the recall configuration](docs/configuration.md#vector_threshold).

Current and automatic recall preserve the ordinary complete items and can
append a neutral list when remaining matching records span at least three
sessions. Each session contributes its highest-ranked remaining visible
record. The list does not assert that records describe the same event or
count how often something occurred. Every row labels its excerpt, full text
length, source time and versioned reference; use inspect to expand the source.
At most twelve rows share at most 1024 budget units and one quarter of the
packet budget, using only space left after the complete items. Rows that do
not fit are omitted without displacing full evidence. Visibility, privacy,
suppression, current versions and retelling checks apply before publication.
Historical reads keep their ordinary items. Existing packet fields and the
shared host renderer carry the list; no migration or configuration change is
required.

**Recall-quality checks.** The [synthetic recall benchmark](CONTRIBUTING.md#the-recall-benchmark)
provides reproducible development and held-out splits for retrieval, answerability,
scope isolation, packet size and latency. It uses proposed claims and a word-hashing
embedder without a model or network, so it does not establish consolidation quality,
semantic embedding quality or performance on private conversations. The test tiers
also check contracts, storage and host integration.

Extraction can normalize fact wording while keeping exact source character intervals. A separate model judgement handles support that literal checks cannot establish; unavailable or uncertain support remains a labelled candidate. [Claim contracts](docs/claim-mutation-contract.md) describe promotion, merging and source authority.

## Manage your memories

Use `scope-recall ui` with the same store selection to open the [local management page](docs/ui.md).
It provides Chinese and English browsing, reviewed actions, held-content review, private rules, export and backup status in your browser.
Capture screens text for embedded instructions, exfiltration links and hidden Unicode before indexing. Held sources
are excluded from recall and processing; explicit inspection returns a reason without their words. The owner can
review and release them on the [local management page](docs/ui.md). External and unattributed sources await an explicit review result.

Use `scope-recall memory` to browse, search, inspect history, correct a fact, mute it or delete a conversation.
It runs as the local store owner and makes no model request. Deletion shows the dependent sources and facts before
`--yes` applies it. `scope-recall memory export --out <new directory or .zip>` writes the visible store as a portable
package with original sources, facts, history, episodes, readable Markdown and optional retained attachments.
`scope-recall memory import --source <kind> --from <path> --source-id <stable account>` previews external
memory files and conversation exports without opening the store. Review the original actors, projects and time range,
then repeat with the same store selection, `--yes --preview-id <shown hash>`. Approval permits import; it does not
confirm the content as fact. [Importing memories](docs/import.md) describes supported formats and retry behavior.
`scope-recall memory secrets` lists stored credential matches by reference, provenance, time and pattern names.
After upgrading, review its findings, remove them with `scope-recall memory forget <refs>` and change any credential found.
[The management guide](docs/manage.md) covers store selection, filters, version checks and receipts; the
[export guide](docs/export.md) covers package layout, hashes and filtering.

Use `scope-recall private on` with one of `--session <id>`, `--project <path>` or `--all` to stop keeping a session,
directory or the whole store. `--mode full` refuses capture, recall and background model work;
`--mode read-only` refuses capture while recall continues; `--all --for 2h` sets a duration and `off`
closes a rule. Rules are stored as metadata only, overlap takes the stricter mode, and an event is
refused by its own occurrence time even when a later retry or transcript read arrives after the
rule ends. Undated events are refused when earlier private history could cover them. Directory
identity follows junctions and symbolic links on the owning machine; recalls and outbound
requests check again before release. [The private-mode guide](docs/private-mode.md) lists every host path, injection notice,
migration and what is stored or sent in each mode.

## Install or upgrade with an assistant

Run `scope-recall setup --dry-run --plan-json` to inspect the complete plan, then
`scope-recall setup --yes` after the owner has approved it. Existing installations reuse their store and grants.
Legacy conversion and live process coordination use the [migration workflow](maintenance/AGENT_WORKFLOW.md).

## Uninstall (default: retain memory)

Uninstall is receipt-driven. By default it removes only plugin wrapper files and retains Core data. Inspect the plan before applying:

```text
scope-recall plan-uninstall --instance-root <absolute-instance-root>
scope-recall apply-uninstall --instance-root <absolute-instance-root>
```

`--target-plugin-dir` may be omitted when the install receipt records it. Purge of installation-owned Core data requires a separate `plan-uninstall --purge` inspection and matching `apply-uninstall --purge`; see [docs/install.md](docs/install.md). Do not treat purge as a normal uninstall step.

## Data and host boundaries

SQLite truth lives in the verified Core data directory. Vector indexes are companions and may be rebuilt only through an explicit configured space. The Hermes and Codex adapters bind host identity, installation identity, scopes, and project roots before opening Core. After that binding is verified, an omitted runtime-config argument checks only `<data_directory>/runtime-config.json`; a missing file stays basic plus an explicit capability gap. The file is never generated or discovered from the current directory, parent directories, or credential locations. Optional host wiring must report a capability gap instead of creating or repairing a database.

The `codex` extra adds the MCP SDK. NumPy provides mapped retrieval and exact full-vector
scoring without SQLite extensions. The `lancedb` extra is only a reader for existing
LanceDB embeddings. Legacy `vector.backend` values map to the SQLite companion automatically.
The worker imports existing vectors without embedding calls and retains the original files.
Until all visible identities are verified, recall uses lexical channels and reports an import gap.
Missing or unreadable input remains a reported gap. Changing the embedding model still requires
an explicit re-embedding run; opening a store never authorizes paid reconstruction.

## Profile and entity read views

Two shared read-only Core methods, `profile` and `entity`, are exposed as Hermes tools and on Codex MCP. They return a deterministic categorized current-fact view, or an exact one-hop statement view, over admitted consolidated claims only. Raw chat is never silently turned into a profile. Incoming relations match the full scalar `value_text` only and keep recorded conditions and validity. Admitted aliases can identify people, objects, places, organizations and projects. A worker compares bounded spelling/vector candidates with source evidence before recording a reversible identity relation. Different scopes, projects and branches remain separate; ambiguous names stay unresolved. Entity, trace and recall recheck these relations when reading. A `budget_tokens` value too small for even the minimal truthful envelope is a validation error, not an oversized view. See [docs/profile-entity.zh-CN.md](docs/profile-entity.zh-CN.md).

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
updates. Only legacy SQLite uses a durable migration job. The operator follows the [migration workflow](maintenance/AGENT_WORKFLOW.md) for verified audience mapping,
backup, conversion, indexing and actual host checks. See
[the agent upgrade guide](docs/upgrade-guide.zh-CN.md).

Migration composes the existing converter, backup helper and worker. It preserves
original source/history/deletion semantics and never re-extracts the whole old
journal. Unsupported formats or unresolved permissions block cutover and preserve
the old installation. Index scheduling and actual live readiness remain separate.

## Tree layout

The package root holds only the entry (`__init__.py`), the version and the protocol contracts. `core/` is the host-independent memory core over SQLite truth; `vector/` the rebuildable vector companions; `adapters/` the Hermes plugin, the clients that use hooks and an MCP server (Codex, Claude Code, WorkBuddy, dsh; `adapters/clients`, with the entry module names installed configurations run kept in `adapters/codex`) and model transport; `runtime/` the background worker, budgets and scheduling; `maintenance/` install, doctor, upgrade and migration behind the operator CLI. Every shipped module is reachable by import from an entry point named in `packaging_hooks/module_inventory.py`; the wheel allowlist is derived from that, not typed.

## Development checks

[Contributor checks](CONTRIBUTING.md) cover clean-wheel installation, both CLI aliases, read-only diagnostics and
capture/recall in temporary stores. These checks do not establish that a running host loaded the package.

Queue states, retry limits and recovery actions are described in [Background work](docs/background-work.md).
