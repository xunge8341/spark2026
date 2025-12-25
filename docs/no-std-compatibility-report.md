# spark-core `no_std + alloc` 兼容性报告

> 状态：2025-02-15 构建验证

## 编译基线
- 构建命令：`cargo check -p spark-core --no-default-features`
- 目标：验证 `spark-core` 在顶层默认启用 `std` 的前提下，仍能在“核心最小化”轨道（禁用所有 Feature，但保留 `alloc` 分配能力）通过语义检查。
- 依赖策略：移除对 `std` 独占依赖的三方库或提供回退实现，确保核心契约在嵌入式/受限环境可用；CI 现已新增 `core-no-std-min` 组合，持续追踪该约束。

## 已覆盖模块
- **契约与上下文层**：`contract`, `context`, `pipeline`, `service`, `router` 等模块均依赖 `alloc` 类型，现已在 `no_std` 编译下通过检查。
- **缓冲与编解码**：`buffer::*`, `codec::*` 使用 `alloc` 持有堆内存，相关 `format!`/`String` 等宏已统一切换为 `alloc` 版本。
- **重试与节律**：`retry::adaptive` 采用 `libm` 计算浮点幂，避免 `std::f64::powf` 依赖；`status::ready` 等节律结构在 `no_std` 环境维持 API 一致。
- **时间子系统**：`time::clock` 在 `std`/`no_std` 下均可编译：
  - `MockClock` 维持原有虚拟时间行为；
  - `SystemClock` 改用后台线程驱动 Future，避免强依赖 Tokio。

## 已知限制与注意事项
- **ArcSwap 回退实现**：当前无论是否启用 `std`，均使用 `spin::RwLock` 实现的 `ArcSwap`，缺失锁自由优化。如需恢复第三方实现，可后续提供新的 Cargo Feature。该回退在高并发场景会牺牲读取延迟。
- **SystemClock 性能**：线程驱动的 `Sleep` Future 会为每次等待启动辅助线程，适合管理面或低频节律逻辑；高频生产场景建议注入自定义 `Clock`（例如运行时原生实现）。
- **审计回放工具**：`bin/audit_replay` 现要求显式启用 `std_json` Feature 以链接 `serde_json`，默认 `no_std` 构建不会尝试编译该可执行文件。
- **第三方依赖审查**：仓库其他 Crate 仍可能依赖 `std`。仅 `spark-core` 已验证 `no_std + alloc`，上层调用方仍需确保自身依赖链无额外 `std` 假设。

## 后续跟进建议
1. 跟踪 `arc-swap` 的 `no_std` 支持计划，一旦稳定可提供可选 Feature 恢复锁自由实现。
2. 评估通过轻量定时轮或宿主运行时注入方式替换线程睡眠，以降低 `SystemClock::sleep` 的性能损耗。
3. 在 CI 中补充最小运行示例（integration test），确保未来改动不会回归 `std` 依赖。
