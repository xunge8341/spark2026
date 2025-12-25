# spark-deprecation-lint

## 契约映射
- CI 工具，用于扫描工作区内的 `#[deprecated]` 属性，确保符合 `docs/governance/deprecation.md` 的策略。
- 通过 `collect_rust_files` 遍历源码，将发现的注解与策略契约（必须包含 `since`、`note` 中的 `removal`/`migration`/`tracking`）对齐。
- 与 `spark-core` 契约协作：当 API 标记弃用时，文档与契约测试需要同步更新，本工具负责在 CI 阶段守护该约束。

## 错误分类
- 工具内部定义 `ToolError::{Io, Policy}`：
  - `Io` 表示文件读取失败，映射到 `ErrorCategory::ImplementationError`（CI 环境问题）；
  - `Policy` 表示策略违规，会以非零退出码阻断流水线，提醒开发者补充信息。
- 输出中附带违规路径与缺失字段，便于快速修复。

## 背压语义
- 工具不参与数据面背压，但其输出会影响发布流程：若策略违规导致 CI 失败，相当于对发布过程施加“管理背压”。
- 为减少扫描开销，遍历过程中跳过 `target/` 与 `.git/`，避免对 CI 产生额外压力。

## TLS/QUIC 注意事项
- 与 TLS/QUIC 无直接交互，但在相关 crate 标记弃用（例如握手结构调整）时，本工具确保文档说明完整，避免安全相关契约缺失。

## 半关闭顺序
- 工具执行流程遵循：
  1. 收集文件列表；
  2. 逐个扫描并记录违规；
  3. 根据结果决定退出码。虽然无网络连接，但顺序与“先检查再退出”契约类似，确保在返回前释放文件句柄。

## ReadyState 映射表
| 执行阶段 | ReadyState | 说明 |
| --- | --- | --- |
| 开始扫描 | `Ready` | 可立即执行遍历 |
| 读取文件中 | `Pending` | 等待 IO 完成 |
| 检测到策略违规 | `Busy` | 阻断流程，等待开发者修复 |
| 违规项整理完毕 | `RetryAfter` | 需开发者修复后重新运行 |
| 所有检查通过 | `Ready` | 退出码为 0，CI 可继续 |
| IO 失败 | `Busy` + `CloseReason::ImplementationError` | 提醒修复环境问题 |

## 超时/取消来源（CallContext）
- 工具独立运行，不使用 `CallContext`。在 CI 流程中可由上层脚本提供超时或取消（例如 `timeout` 命令），确保流水线不会长时间阻塞。
