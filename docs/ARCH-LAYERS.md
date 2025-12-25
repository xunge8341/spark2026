# Spark 架构分层与契约发布矩阵

> **版本：** T-001A / 2025-03-01
>
> 本文档定义 `spark-core` 作为契约唯一来源时，各层组件的依赖矩阵与禁止项。

## 分层总览

| 层级 | 代表模块 | 可以直接依赖的契约 | 备注 |
| ---- | -------- | ------------------ | ---- |
| Core 契约层 | `crates/spark-core` | 全量 | 发布 `kernel::*`、`governance::*`、`data_plane::*`、`platform::*` 并提供 `prelude` 入口 |
| L2 路由引擎实现 | `crates/spark-router` | `spark_core::pipeline::*`、`spark_core::service::*`、`spark_core::observability::*` | 作为 L2 Router Handler 的标准实现，通过 `spark_router::pipeline::{ApplicationRouter, ApplicationRouterInitializer}` 提供默认实现，示例与教学均统一改走 `spark_router::pipeline` 入口 |
| 实现层 | `crates/spark-impl-*`、`transport/*` | 仅可通过 `spark_core::{...}` 或 `spark_core::prelude::*` 引入 | 禁止自定义同名契约；若需扩展必须回到 core 讨论 |
| 业务集成层 | `examples/`、`sdk/` | 推荐使用 `prelude`，如需细分模块需记录在本表 | 业务侧不得重新导出 core 内部模块 |
| 工具 & 测试 | `tools/`、`crates/spark-contract-tests` | 可读取所有契约用于断言 | 若发现未发布的契约需求，需提交 PR 更新本矩阵 |

### 依赖规则

1. **单向依赖**：`spark-core` 只能依赖 `alloc` 与语义上更低层的基础库（如 `spark-macros`）。其他所有 Crate 一律向下依赖 `spark-core`。
2. **公开入口**：所有外部依赖必须通过 `spark_core::{module::*, prelude::*}` 导入，禁止直接访问内部文件路径。
3. **测试例外**：契约测试可直接访问内部模块以验证不变式，但不得发布新的公共类型。

## 强制禁止清单

| 序号 | 禁止项 | 说明 |
| ---- | ------ | ---- |
| F-01 | 在非 `spark-core` Crate 中声明 `CoreError`、`RequestId`、`Budget` 等契约类型 | 违者会导致契约分裂，CI 将通过 `rg` 检查拒绝合并 |
| F-02 | 在业务代码中直接 `pub use spark_core::kernel::contract` 导出全部符号 | 请改用 `spark_core::prelude`，或按需重导出单个类型 |
| F-03 | 在传输实现中自定义消息/帧结构体 | 必须复用 `spark_core::protocol::{Message, Frame, Event}`，新增字段需通过 RFC |
| F-04 | 在配置文件中绕过 `Timeout::try_new` 自行解析超时 | 统一走 `spark_core::config`，保证软/硬超时校验一致 |
| F-05 | `docs/` 目录以外的文档引用旧的契约路径（如 `spark_core::contract::Budget`） | 请更新为 `spark_core::kernel::types::Budget` 或 `prelude`，保持文档一致性 |

> **提醒**：若需新增契约，请同步更新本文件及 `CONTRIBUTING.md`，并确保附带教案级注释。
