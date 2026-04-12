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

## Dry Run

`--dry-run` 会校验凭证、解析 JSON 导出文件、加载或校验状态文件，并输出执行摘要；不会创建目录，也不会调用秒传 API。

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.state.json \
  --workers 8 \
  --dry-run
```

## 正式执行

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.state.json \
  --workers 8 \
  --max-retries 5 \
  --flush-every 100
```

## 重试 failed 和 not_reusable

使用同一个状态文件，并加上 `--retry-failed`，即可重新排队状态文件里已经落入 `failed` 或 `not_reusable` 桶的记录。

```bash
python -m fastlink_transfer import-json \
  --file h-suki.json \
  --target-parent-id 12345678 \
  --state-file .state/h-suki.state.json \
  --workers 8 \
  --retry-failed
```

## 状态文件说明

- 续跑时，当前 JSON 文件计算出的 `source_sha256` 和 `--target-parent-id` 必须与状态文件中的范围一致。
- 运行过程中，状态文件应位于同一文件系统内，便于原子替换写入。
- 中断续跑和 `--retry-failed` 都应复用同一个状态文件路径。

## 退出行为

- `--dry-run` 成功完成规划时返回 `0`，不受现有 `failed` 或 `not_reusable` 状态记录影响。
- 非 `--dry-run` 运行在全部完成，或最终只剩 `not_reusable` 项时返回 `0`。
- 启动校验失败、凭证失效、状态文件与当前运行范围不匹配，或非 `--dry-run` 运行最终仍存在 `failed` 项时返回 `1`。
