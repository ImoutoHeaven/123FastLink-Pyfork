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
- `import-json`：读取 JSON 导出文件，在目标目录下创建目录并发起秒传。

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

中断后可复用同一个 `--state-file` 续跑；成功完成后，最终只保留 `--output-file`。如果清理内部文件时失败，命令仍返回 `0`，但会打印 cleanup warning。

## import-json Dry Run

`--dry-run` 会校验凭证、解析 JSON 导出文件、加载或校验状态文件，并输出执行摘要；不会创建目录，也不会调用秒传 API。

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.state.json \
  --workers 8 \
  --dry-run
```

## import-json 正式执行

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.state.json \
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
  --state-file .state/h-suki.state.json \
  --workers 8 \
  --retry-failed
```

## import-json 状态文件说明

- 续跑时，当前 JSON 文件计算出的 `source_sha256` 和 `--target-parent-id` 必须与状态文件中的范围一致。
- 运行过程中，状态文件应位于同一文件系统内，便于原子替换写入。
- 中断续跑和 `--retry-failed` 都应复用同一个状态文件路径。

## 退出行为

- `export-json` 在成功生成最终 JSON 时返回 `0`。
- `export-json` 如果最终 JSON 已经写出，但清理内部文件失败，仍返回 `0` 并打印 cleanup warning。
- `export-json` 在启动校验失败、凭证失效、状态/sidecar 损坏或 scope 不匹配、源目录递归扫描后无文件，或运行中遇到 fatal 错误时返回非 `0`。
- `--dry-run` 成功完成规划时返回 `0`，不受现有 `failed` 或 `not_reusable` 状态记录影响。
- 非 `--dry-run` 运行在全部完成，或最终只剩 `not_reusable` 项时返回 `0`。
- 启动校验失败、凭证失效、状态文件与当前运行范围不匹配，或非 `--dry-run` 运行最终仍存在 `failed` 项时返回 `1`。
