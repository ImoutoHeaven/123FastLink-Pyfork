# 123Pan JSON 转存 CLI

## 必需环境变量

```bash
export PAN_AUTH_TOKEN='...'
export PAN_LOGIN_UUID='...'
export PAN_COOKIE='...'
```

可选环境变量：

```bash
export PAN_HOST='https://www.123pan.com'
```

不设置 `PAN_HOST` 时，默认使用 `https://www.123pan.com`。

## 命令概览

- `export-json`：在线递归扫描一个 123Pan 目录，生成可直接被 `import-json` 使用的 JSON 导出文件。
- `import-json`：读取 JSON 导出文件，使用 SQLite 状态数据库规划、续跑并执行导入。
- `batch-import-json`：递归发现目录下的导出 JSON，为每个 JSON 维护独立 SQLite 状态数据库，并批量调度导入。
- `batch-check-json`：递归发现导出 JSON，对比远端目录树，按需写出 `.delta.export.json` 差量文件。

## export-json

`export-json` 只接受一个源目录 id。命令会递归扫描整个目录树，保留源根目录名到导出 JSON 的 `commonPath`，并在成功时原子写出最终 JSON 文件。

```bash
python -m fastlink_transfer export-json \
  --source-parent-id 12345678 \
  --output-file movies.json \
  --state-file .state/movies.export.state.json \
  --workers 8 \
  --max-retries 5 \
  --flush-every 100
```

- `export-json` 没有 `--dry-run`。
- 生成的 `--output-file` 可以直接作为 `import-json --file` 的输入。
- `--output-file` 必须与导出内部文件路径不同。
- 如果递归扫描后没有任何文件可导出，命令失败，不会生成最终 JSON。

导出运行期间会维护续跑所需的内部文件：

- `--state-file`
- `<state-file>.records.jsonl`
- `<state-file>.finalize.sqlite3`
- `<state-file>.output.tmp.json`

某些成功后清理失败的恢复场景下，还可能短暂残留 `<state-file>.output.committed`；这是内部成功标记，不是用户导出文件。

中断后可复用同一个 `--state-file` 续跑；`export-json` 不需要 `--retry-failed`。成功完成后，最终只保留 `--output-file`。如果清理内部文件时失败，命令仍返回 `0`，但会打印 cleanup warning。

## import-json Dry Run

`--dry-run` 会校验凭证、解析 JSON 导出文件、加载或校验状态文件，并输出执行摘要；不会创建目录，也不会调用秒传 API。

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.import-state.sqlite3 \
  --workers 8 \
  --dry-run
```

## import-json 正式执行

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.import-state.sqlite3 \
  --workers 8 \
  --max-retries 5 \
  --flush-every 100
```

## import-json 重试 failed 和 not_reusable

使用同一个状态文件，并加上 `--retry-failed`，即可重新排队状态文件里已经落入 `failed` 或 `not_reusable` 桶的记录。

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.import-state.sqlite3 \
  --workers 8 \
  --retry-failed
```

## import-json 状态数据库说明

- `--state-file` 指向一个 SQLite 数据库。
- 同一个 `--state-file` 可用于中断续跑和 `--retry-failed` 重试。
- 续跑时，当前 JSON 文件计算出的 `source_sha256`、`--target-parent-id` 和 `commonPath` 必须与状态数据库中的范围一致。
- 已存在但不是 SQLite 的 `--state-file` 会直接失败，并报 `unsupported state-file format`。
- 如果状态数据库里的 planning 未完成，会丢弃未完成规划并从源导出 JSON 重新构建。
- 如果运行结束后仍有 `failed` 或 `not_reusable` 记录，会按 `Path.with_suffix(".retry.export.json")` 规则写出 retry export，例如 `.state/h-suki.import-state.sqlite3` 会生成 `.state/h-suki.import-state.retry.export.json`，并且这个文件可再次用于 `import-json` 或 `batch-import-json`。

## batch-import-json

```bash
python -m fastlink_transfer batch-import-json \
  --input-dir exports \
  --target-parent-id 12345678 \
  --state-dir .state/batch \
  --workers 8 \
  --json-parallelism 2
```

- 递归发现 `--input-dir` 下的 `*.json`。
- 每个发现到的 JSON 都会在 `--state-dir` 下获得一个独立的 SQLite 状态数据库，路径后缀为 `.import-state.sqlite3`。
- `--json-parallelism` 控制同一时间运行的子导入任务数，默认值是 `2`；`--workers` 仍然按单个子任务生效。
- 子任务本地失败不会阻塞无关的其他子任务；credential fatal 会停止继续启动新的子任务。
- 每个 JSON 的 retry export 都会写到 `--state-dir` 下，并保持源 JSON 的相对目录结构。
- batch 运行会先完成所有 child job 的 planning 和目标路径碰撞检查，发现冲突时会在任何远端变更前失败。

## batch-check-json

```bash
python -m fastlink_transfer batch-check-json \
  --input-dir exports \
  --target-parent-id 12345678 \
  --state-dir .state/check \
  --output-dir delta-exports \
  --workers 8 \
  --json-parallelism 2 \
  --with-checksum
```

- 递归发现 `--input-dir` 下的 `*.json`，并保持源 JSON 的相对目录结构。
- 每个 JSON 都会在 `--state-dir` 下获得独立的 SQLite check state，文件后缀为 `.check-state.sqlite3`。
- 每个 JSON 的差量输出会写到 `--output-dir` 下，文件后缀为 `.delta.export.json`。
- 默认使用 `--exist-only`，只比较远端是否存在同路径文件；`--with-checksum` 会继续比较 checksum 和 size。
- 如果某个 JSON 的 file delta 为空，不会保留 `.delta.export.json`；如果存在旧的 stale delta 文件，会在 finalize 时删除。
- 目录差异会体现在 summary 的 `missing_dirs` 统计里；只有缺失或 checksum/size 不一致的文件才会进入 delta。
- 同一个 job 在 scope 不变时可复用已持久化的远端索引；从 `--exist-only` 切换到 `--with-checksum` 时会复用 remote scan 结果，只重跑 delta finalize。
- 如果 `commonPath` 对应的整棵远端子树不存在，该 job 仍会成功完成，并把预期文件全部写入 delta。

## 退出行为

- `export-json` 在成功生成最终 JSON 时返回 `0`。
- `export-json` 如果最终 JSON 已经写出，但清理内部文件失败，仍返回 `0` 并打印 cleanup warning。
- `export-json` 在启动校验失败、凭证失效、状态/sidecar 损坏或 scope 不匹配、源目录递归扫描后无文件，或运行中遇到 fatal 错误时返回非 `0`。
- `--dry-run` 成功完成规划时返回 `0`，不受现有 `failed` 或 `not_reusable` 状态记录影响。
- 非 `--dry-run` 运行在全部完成，或最终只剩 `not_reusable` 项时返回 `0`。
- `import-json` 在启动校验失败、凭证失效、状态数据库损坏、状态数据库与当前运行范围不匹配，或非 `--dry-run` 运行最终仍存在 `failed` 项时返回 `1`。
- `batch-import-json` 在所有子任务都完成，或最终只剩 `not_reusable` 项时返回 `0`。
- `batch-import-json` 在 discovery 为空、preflight collision 检查失败、任一子任务失败，或出现 batch-global credential fatal 时返回 `1`。
- `batch-check-json` 在所有子任务都成功完成检查时返回 `0`；发现 delta 或目录差异本身不会导致失败。
- `batch-check-json` 在 discovery 为空、启动校验失败、state/output 根目录创建失败、任一子任务失败，或出现 batch-global credential fatal 时返回 `1`。
