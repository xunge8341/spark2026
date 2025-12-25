# Backpressure·Budget·Shutdown 契约说明（占位稿）

> **状态**：占位草稿（P0-07）。后续迭代将补充示例、序列图与 TCK 映射。

## 范围
- 聚焦 [`spark_core::contract::BackpressureSignal`] 及其与预算、关闭语义的联动；
- 标准化 [`ShutdownGraceful`] 与 [`ShutdownImmediate`] 的传播路径；
- 规划 Pipeline / Transport / Service 在状态机层面的协同接口。

## TODO
1. 整理背压信号与 `ReadyState`/`PollReady` 的映射矩阵；
2. 给出 Budget 消耗/回滚与信号转换的参考实现；
3. 对齐 `spark-contract-tests` 的 TCK 场景，确保存在“预算耗尽”“优雅关闭”“强制关闭”三类测试用例；
4. 更新运行手册，指导运维如何读取统一信号并执行降级。

