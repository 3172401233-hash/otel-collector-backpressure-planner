"""OpenTelemetry Collector 背压策略规划器。

根据离线指标快照（pipeline 配置、队列水位、导出延迟、重试预算、
内存限制、trace 完整性、信号优先级），为每一轮规划各信号的
保留(retain)、延迟(delay)、降采样(downsample)和丢弃(drop)动作。

设计约束：
- 日志和指标拥有容量保障份额，不会因为 trace 积压而一起丢失。
- 被尾部采样保留的 trace 受最低保留量(min_retention)约束，不得丢弃。
- 导出端超时时按指数退避降低导出速率并消耗重试预算；
  预算耗尽且队列逼近上限时才允许丢弃。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class Action(str, Enum):
    RETAIN = "retain"
    DELAY = "delay"
    DOWNSAMPLE = "downsample"
    DROP = "drop"


# 丢弃原因
REASON_QUEUE_CRITICAL = "queue_critical"
REASON_RETRY_BUDGET_EXHAUSTED = "retry_budget_exhausted"
REASON_MEMORY_LIMIT = "memory_limit"
REASON_LOW_PRIORITY = "low_priority"
REASON_INCOMPLETE_TRACE = "incomplete_trace"


@dataclass(frozen=True)
class SignalPolicy:
    """单个信号的静态策略。"""

    name: str
    priority: int  # 数值越大越重要
    min_guaranteed_share: float = 0.0  # 占总导出容量的保底份额 [0,1]
    min_retention: int = 0  # 尾部采样保留数据的最少保留条数
    downsample_floor: float = 1.0  # 降采样允许的最低采样比 [0,1]
    completeness_threshold: float = 0.0  # 低于该完整性的 trace 可优先降采样


@dataclass(frozen=True)
class SignalState:
    """单个信号本轮的观测快照。"""

    name: str
    queue_depth: int  # 队列中积压条数
    arrival_rate: float  # 到达速率 条/轮
    export_latency_ms: float  # 导出端观测延迟
    trace_completeness: float = 1.0  # trace 完整性 [0,1]，非 trace 信号忽略


@dataclass(frozen=True)
class PlannerConfig:
    memory_limit_bytes: int
    base_export_rate: float  # 健康状态下总导出速率 条/轮
    retry_budget: int  # 允许连续背压重试的轮数
    latency_slo_ms: float = 500.0
    high_watermark: float = 0.8  # 超过则开始延迟/降采样
    critical_watermark: float = 0.95  # 超过则允许丢弃
    item_size_bytes: int = 512
    min_export_rate: float = 1.0


@dataclass
class SignalPlan:
    name: str
    action: Action
    admitted: float  # 本轮接纳进入队列的条数
    export_rate: float  # 本轮分配的导出速率 条/轮
    sampling_ratio: float  # 实际采样比（1.0 表示未降采样）
    drop_reasons: list[str] = field(default_factory=list)


@dataclass
class RoundPlan:
    round_index: int
    queue_bytes: int
    memory_pressure: float
    total_admitted: float
    total_export_rate: float
    signals: dict[str, SignalPlan]


class BackpressurePlanner:
    """有状态的逐轮规划器，跟踪连续背压轮数与剩余重试预算。"""

    def __init__(self, config: PlannerConfig, policies: list[SignalPolicy]):
        self.config = config
        self.policies = {p.name: p for p in policies}
        self._consecutive_backpressure = 0
        self._retry_budget_remaining = config.retry_budget
        self._round_index = 0

    @property
    def retry_budget_remaining(self) -> int:
        return self._retry_budget_remaining

    @property
    def consecutive_backpressure(self) -> int:
        return self._consecutive_backpressure

    def plan_round(self, states: list[SignalState]) -> RoundPlan:
        cfg = self.config
        queue_bytes = sum(s.queue_depth for s in states) * cfg.item_size_bytes
        pressure = queue_bytes / cfg.memory_limit_bytes

        degraded = any(s.export_latency_ms > cfg.latency_slo_ms for s in states)
        if degraded:
            self._consecutive_backpressure += 1
            self._retry_budget_remaining = max(0, self._retry_budget_remaining - 1)
        else:
            self._consecutive_backpressure = 0
            self._retry_budget_remaining = cfg.retry_budget

        backoff = 0.5 ** self._consecutive_backpressure
        total_export_rate = max(cfg.base_export_rate * backoff, cfg.min_export_rate)

        plans: dict[str, SignalPlan] = {}
        ordered = sorted(
            states, key=lambda s: self.policies[s.name].priority, reverse=True
        )

        # 先划出各信号的保底份额，保证日志/指标不被 trace 积压饿死。
        reserved: dict[str, float] = {}
        for s in ordered:
            reserved[s.name] = (
                total_export_rate * self.policies[s.name].min_guaranteed_share
            )
        pool = max(total_export_rate - sum(reserved.values()), 0.0)

        # 剩余容量按优先级加权分配，高优先级没用完的让给低优先级。
        weights = {s.name: max(self.policies[s.name].priority, 1) for s in ordered}
        weight_total = sum(weights.values())
        export_alloc: dict[str, float] = {}
        for s in ordered:
            want = s.queue_depth + s.arrival_rate
            fair = pool * weights[s.name] / weight_total
            export_alloc[s.name] = reserved[s.name] + min(
                max(want - reserved[s.name], 0.0), fair
            )
        leftover = total_export_rate - sum(export_alloc.values())
        for s in reversed(ordered):
            if leftover <= 0:
                break
            want = s.queue_depth + s.arrival_rate - export_alloc[s.name]
            extra = min(max(want, 0.0), leftover)
            export_alloc[s.name] += extra
            leftover -= extra

        min_priority = min(p.priority for p in self.policies.values())
        for s in ordered:
            policy = self.policies[s.name]
            reasons: list[str] = []
            sampling_ratio = 1.0
            admitted = s.arrival_rate
            action = Action.RETAIN

            if pressure >= cfg.critical_watermark:
                action = Action.DROP
                reasons.append(REASON_QUEUE_CRITICAL)
                reasons.append(REASON_MEMORY_LIMIT)
                if self._retry_budget_remaining == 0:
                    reasons.append(REASON_RETRY_BUDGET_EXHAUSTED)
                if policy.priority == min_priority:
                    reasons.append(REASON_LOW_PRIORITY)
                if (
                    policy.completeness_threshold > 0
                    and s.trace_completeness < policy.completeness_threshold
                ):
                    reasons.append(REASON_INCOMPLETE_TRACE)
                # 尾部采样保留的数据受最低保留约束：队列中至少保留
                # min_retention 条，只在内存余量内接纳新数据。
                headroom = max(
                    (cfg.memory_limit_bytes - queue_bytes) // cfg.item_size_bytes, 0
                )
                admitted = min(s.arrival_rate, float(headroom))
                if policy.min_retention > 0 and s.queue_depth <= policy.min_retention:
                    # 已处于最低保留线，拒绝新接纳以免挤出受保护数据。
                    admitted = 0.0
                sampling_ratio = policy.downsample_floor
            elif pressure >= cfg.high_watermark:
                if policy.downsample_floor < 1.0:
                    action = Action.DOWNSAMPLE
                    # 压力越大采样比越低，但不低于策略下限。
                    span = cfg.critical_watermark - cfg.high_watermark
                    over = (pressure - cfg.high_watermark) / span
                    sampling_ratio = max(
                        1.0 - over * (1.0 - policy.downsample_floor),
                        policy.downsample_floor,
                    )
                    admitted = s.arrival_rate * sampling_ratio
                else:
                    action = Action.DELAY
                    admitted = 0.0
            elif degraded:
                action = Action.DELAY
                admitted = s.arrival_rate * backoff

            plans[s.name] = SignalPlan(
                name=s.name,
                action=action,
                admitted=admitted,
                export_rate=export_alloc[s.name],
                sampling_ratio=sampling_ratio,
                drop_reasons=reasons,
            )

        self._round_index += 1
        return RoundPlan(
            round_index=self._round_index,
            queue_bytes=queue_bytes,
            memory_pressure=pressure,
            total_admitted=sum(p.admitted for p in plans.values()),
            total_export_rate=total_export_rate,
            signals=plans,
        )
