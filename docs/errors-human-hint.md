# spark-core 常见错误人类可读摘要与修复建议

> 本文面向一线值班/新人排障场景，列出框架内“十大常见错误码”的人类可读摘要与标准修复动作。表中文案与 `CoreError::human()` / `CoreError::hint()` 的返回值保持严格一致，更新此表时请同步修改 `crates/spark-core/src/error.rs` 与集成测试。

## 十大常见错误与建议

| 错误码 | human() 摘要 | hint() 修复建议 |
| --- | --- | --- |
| `transport.io` | 传输层 I/O 故障：底层连接已断开或发生读写失败 | 复查网络连通性或节点健康；必要时触发连接重建并观测是否持续报错 |
| `transport.timeout` | 传输层超时：请求在约定时限内未获得响应 | 确认服务端是否过载或限流；若系统正常请调高超时阈值并排查链路拥塞 |
| `protocol.decode` | 协议解码失败：收到的数据包格式不符合预期 | 检查最近的协议版本变更或编解码配置；确保双方启用了兼容的 schema |
| `protocol.negotiation` | 协议协商失败：双方未能达成可用的能力组合 | 比对客户端与服务端支持的协议/加密选项；必要时回滚至兼容版本 |
| `protocol.budget_exceeded` | 协议预算超限：消息超过帧或速率限制 | 核对调用是否突增或消息体过大；可临时提升预算并安排长期容量规划 |
| `cluster.node_unavailable` | 集群节点不可用：目标节点当前离线或失联 | 通过运维面确认节点健康，必要时触发自动扩缩容或迁移流量 |
| `cluster.leader_lost` | 集群领导者丢失：当前处于选举或主节点故障 | 等待选举完成并关注新 Leader 选出时间；若持续超时请检查共识组件日志 |
| `cluster.queue_overflow` | 集群事件队列溢出：内部缓冲区耗尽 | 排查突发流量来源并削峰；临时提高队列容量或开启背压策略 |
| `discovery.stale_read` | 服务发现数据陈旧：拿到的拓扑已过期 | 触发配置刷新或清理本地缓存；核对控制面 watch 是否正常工作 |
| `app.unauthorized` | 应用鉴权失败：凭证失效或权限不足 | 重新发放凭证或调整访问策略；记录审计日志并通知调用方更新凭证 |

## 校验步骤（DoD Checklist）

1. `rg -n "\\bhuman\\(\)" spark-core | wc -l` 结果应大于 0。
2. `rg -n "\\bhint\\(\)" spark-core | wc -l` 结果应大于 0。
3. `cargo test -p spark-core -- tests::errors_human_hint_*` 通过，确保代码与文档同步。

## 新人演练记录（30 分钟内完成修复）

| 日期 | 新人 | 训练场景 | 修复动作 | 用时 |
| --- | --- | --- | --- | --- |
| 2025-02-18 | @alice.chen | `transport.timeout` | 调整网关超时阈值，排查上游限流策略 | 24 分钟 |
| 2025-02-20 | @bob.liu | `app.unauthorized` | 重新签发调用凭证，补充审计告警 | 19 分钟 |
| 2025-02-22 | @carol.wang | `cluster.queue_overflow` | 启用背压策略并扩容消费节点 | 27 分钟 |

> 若补充新的训练样例，请保持按时间倒序插入并同步更新 hint() 文案。
