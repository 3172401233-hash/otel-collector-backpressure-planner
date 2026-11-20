# otel-collector-backpressure-planner
规划 Collector 队列、丢弃和导出退避的背压策略

> 注意：本策略只作用于离线指标快照（offline metric snapshots），
> 不直接连接或控制运行中的 Collector。输入为快照数据，输出为规划建议，
> 供 SRE 评审后人工或外部系统执行。

## 功能

`backpressure_planner.py` 根据每轮输入的 pipeline 快照规划背压动作：

- 输入：队列水位、导出延迟、重试预算、内存限制、trace 完整性、
  各信号（logs / metrics / traces）优先级与保底份额。
- 动作：`retain`（保留）、`delay`（延迟）、`downsample`（降采样）、
  `drop`（丢弃）。
- 输出：每轮各信号的接纳量、导出速率分配和丢弃原因
  （`queue_critical`、`memory_limit`、`retry_budget_exhausted`、
  `low_priority`、`incomplete_trace`）。

## 策略要点

- 导出延迟超过 SLO 时按 `0.5 ** 连续背压轮数` 指数退避导出速率，
  每轮消耗 1 点重试预算；恢复正常后速率与预算立即重置。
- 队列压力超过高水位时对 traces 降采样（不低于 `downsample_floor`），
  其他信号延迟；超过临界水位才允许丢弃。
- 日志和指标通过 `min_guaranteed_share` 获得保底导出份额，
  不会因 trace 积压被一起丢弃。
- 被尾部采样保留的 trace 受 `min_retention` 最低保留约束：
  队列降至保留线时停止接纳新数据，避免挤出受保护数据。

## 运行测试

```bash
python -m unittest
```

测试覆盖：导出恢复、队列接近上限（含最低保留约束与降采样）、
信号竞争、重复背压（指数退避与预算耗尽后恢复）。
