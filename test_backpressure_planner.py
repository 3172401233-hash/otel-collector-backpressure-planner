import unittest

from backpressure_planner import (
    Action,
    BackpressurePlanner,
    PlannerConfig,
    REASON_LOW_PRIORITY,
    REASON_MEMORY_LIMIT,
    REASON_QUEUE_CRITICAL,
    REASON_RETRY_BUDGET_EXHAUSTED,
    SignalPolicy,
    SignalState,
)


def make_config(**overrides):
    base = dict(
        memory_limit_bytes=1_000_000,
        base_export_rate=1000.0,
        retry_budget=2,
        latency_slo_ms=500.0,
        high_watermark=0.8,
        critical_watermark=0.95,
        item_size_bytes=512,
    )
    base.update(overrides)
    return PlannerConfig(**base)


def make_policies():
    return [
        SignalPolicy(name="logs", priority=3, min_guaranteed_share=0.2),
        SignalPolicy(name="metrics", priority=2, min_guaranteed_share=0.1),
        SignalPolicy(
            name="traces",
            priority=1,
            min_retention=100,
            downsample_floor=0.1,
            completeness_threshold=0.9,
        ),
    ]


def state(name, queue, arrival, latency=100.0, completeness=1.0):
    return SignalState(
        name=name,
        queue_depth=queue,
        arrival_rate=arrival,
        export_latency_ms=latency,
        trace_completeness=completeness,
    )


class ExportRecoveryTest(unittest.TestCase):
    """导出端超时后退避，恢复后应回到全速并重置重试预算。"""

    def test_recovery_restores_full_export_rate(self):
        planner = BackpressurePlanner(make_config(), make_policies())

        degraded = planner.plan_round(
            [
                state("logs", 100, 100),
                state("metrics", 100, 100),
                state("traces", 100, 100, latency=900.0),
            ]
        )
        self.assertEqual(degraded.signals["traces"].action, Action.DELAY)
        self.assertAlmostEqual(degraded.total_export_rate, 500.0)
        self.assertEqual(planner.retry_budget_remaining, 1)
        self.assertAlmostEqual(degraded.signals["logs"].admitted, 50.0)

        recovered = planner.plan_round(
            [
                state("logs", 100, 100),
                state("metrics", 100, 100),
                state("traces", 100, 100, latency=120.0),
            ]
        )
        self.assertEqual(recovered.signals["traces"].action, Action.RETAIN)
        self.assertAlmostEqual(recovered.total_export_rate, 1000.0)
        self.assertEqual(planner.retry_budget_remaining, 2)
        self.assertEqual(planner.consecutive_backpressure, 0)
        self.assertAlmostEqual(recovered.signals["traces"].admitted, 100.0)


class QueueNearLimitTest(unittest.TestCase):
    """队列逼近内存上限时允许丢弃，并记录丢弃原因。"""

    def test_critical_watermark_triggers_drop_with_reasons(self):
        planner = BackpressurePlanner(make_config(), make_policies())
        # 总队列 1950 条 * 512B = 998400B，压力 0.9984 >= 0.95。
        plan = planner.plan_round(
            [
                state("logs", 950, 200),
                state("metrics", 950, 200),
                state("traces", 50, 200),
            ]
        )
        for name in ("logs", "metrics", "traces"):
            sig = plan.signals[name]
            self.assertEqual(sig.action, Action.DROP)
            self.assertIn(REASON_QUEUE_CRITICAL, sig.drop_reasons)
            self.assertIn(REASON_MEMORY_LIMIT, sig.drop_reasons)
        # 最低优先级信号标记 low_priority。
        self.assertIn(REASON_LOW_PRIORITY, plan.signals["traces"].drop_reasons)
        self.assertNotIn(REASON_LOW_PRIORITY, plan.signals["logs"].drop_reasons)
        # 延迟正常，重试预算未耗尽。
        self.assertNotIn(
            REASON_RETRY_BUDGET_EXHAUSTED,
            plan.signals["traces"].drop_reasons,
        )

    def test_tail_sampled_traces_respect_min_retention(self):
        planner = BackpressurePlanner(make_config(), make_policies())
        plan = planner.plan_round(
            [
                state("logs", 950, 200),
                state("metrics", 950, 200),
                # 队列仅剩 50 条，低于 min_retention=100 的保留线。
                state("traces", 50, 200),
            ]
        )
        traces = plan.signals["traces"]
        self.assertEqual(traces.action, Action.DROP)
        # 处于最低保留线时不再接纳，避免挤出受保护的尾部采样数据。
        self.assertEqual(traces.admitted, 0.0)

    def test_high_watermark_triggers_downsample_above_floor(self):
        planner = BackpressurePlanner(make_config(), make_policies())
        # 总队列 1700 条 -> 压力 0.8704，处于 high 与 critical 之间。
        plan = planner.plan_round(
            [
                state("logs", 800, 100),
                state("metrics", 800, 100),
                state("traces", 100, 100),
            ]
        )
        traces = plan.signals["traces"]
        self.assertEqual(traces.action, Action.DOWNSAMPLE)
        self.assertGreaterEqual(traces.sampling_ratio, 0.1)
        self.assertLess(traces.sampling_ratio, 1.0)
        # 日志/指标不可降采样，只能延迟。
        self.assertEqual(plan.signals["logs"].action, Action.DELAY)
        self.assertEqual(plan.signals["metrics"].action, Action.DELAY)


class SignalContentionTest(unittest.TestCase):
    """trace 积压时，日志和指标仍获得保底导出份额且被完整接纳。"""

    def test_logs_and_metrics_not_starved_by_trace_backlog(self):
        planner = BackpressurePlanner(make_config(), make_policies())
        plan = planner.plan_round(
            [
                state("logs", 100, 100),
                state("metrics", 100, 100),
                state("traces", 800, 800),
            ]
        )
        logs = plan.signals["logs"]
        metrics = plan.signals["metrics"]
        # 保底份额：logs >= 20%、metrics >= 10% 的总导出速率。
        self.assertGreaterEqual(logs.export_rate, 200.0)
        self.assertGreaterEqual(metrics.export_rate, 100.0)
        # 日志和指标全量接纳，不因 trace 积压被丢弃或延迟。
        self.assertEqual(logs.action, Action.RETAIN)
        self.assertEqual(metrics.action, Action.RETAIN)
        self.assertEqual(logs.admitted, 100.0)
        self.assertEqual(metrics.admitted, 100.0)
        self.assertEqual(logs.drop_reasons, [])
        self.assertEqual(metrics.drop_reasons, [])


class RepeatedBackpressureTest(unittest.TestCase):
    """连续背压轮次中导出速率指数退避，预算耗尽后升级动作。"""

    def test_exponential_backoff_and_budget_exhaustion(self):
        planner = BackpressurePlanner(make_config(), make_policies())

        rates = []
        for _ in range(3):
            plan = planner.plan_round(
                [
                    state("logs", 100, 100),
                    state("metrics", 100, 100),
                    state("traces", 100, 100, latency=2000.0),
                ]
            )
            rates.append(plan.total_export_rate)
        self.assertEqual(rates, [500.0, 250.0, 125.0])
        self.assertEqual(planner.consecutive_backpressure, 3)
        self.assertEqual(planner.retry_budget_remaining, 0)

        # 预算耗尽且队列越过上界：丢弃原因包含 retry_budget_exhausted。
        plan = planner.plan_round(
            [
                state("logs", 950, 200),
                state("metrics", 950, 200),
                state("traces", 50, 200, latency=2000.0),
            ]
        )
        traces = plan.signals["traces"]
        self.assertEqual(traces.action, Action.DROP)
        self.assertIn(REASON_RETRY_BUDGET_EXHAUSTED, traces.drop_reasons)

    def test_recovery_after_repeated_backpressure_resets_budget(self):
        planner = BackpressurePlanner(make_config(), make_policies())
        for _ in range(3):
            planner.plan_round(
                [
                    state("logs", 100, 100),
                    state("metrics", 100, 100),
                    state("traces", 100, 100, latency=2000.0),
                ]
            )
        plan = planner.plan_round(
            [
                state("logs", 100, 100),
                state("metrics", 100, 100),
                state("traces", 100, 100, latency=50.0),
            ]
        )
        self.assertAlmostEqual(plan.total_export_rate, 1000.0)
        self.assertEqual(planner.retry_budget_remaining, 2)
        self.assertEqual(plan.signals["traces"].action, Action.RETAIN)


if __name__ == "__main__":
    unittest.main()
