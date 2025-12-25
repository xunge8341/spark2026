mod tests {
    pub mod contracts {
        pub mod error_category_autoresponse {
            use std::{
                any::Any,
                collections::BTreeSet,
                sync::{Arc, Mutex},
                time::Duration,
            };

            use serde::Deserialize;
            use spark_core::async_trait;
            use spark_core::contract::{
                Budget, CallContext, CallContextBuilder, Cancellation, CloseReason,
            };
            use spark_core::error::{CoreError, category_matrix};
            use spark_core::observability::metrics::MetricsProvider;
            use spark_core::observability::{CoreUserEvent, Logger, TraceContext, TraceFlags};
            use spark_core::pipeline::controller::{Handler, PipelineHandleId};
            use spark_core::pipeline::handler::InboundHandler;
            use spark_core::pipeline::{
                Channel, Context, HandlerRegistry, Pipeline, WriteSignal,
                channel::ChannelState,
                default_handlers::{ExceptionAutoResponder, ReadyStateEvent},
            };
            use spark_core::status::ReadyState;
            use spark_core::{
                buffer::{PipelineMessage, PoolStats},
                contract::Deadline,
                future::BoxFuture,
                runtime::{
                    JoinHandle, MonotonicTimePoint, TaskCancellationStrategy, TaskError,
                    TaskExecutor, TaskHandle, TaskResult, TimeDriver,
                },
                test_stubs::observability::{NoopLogger, NoopMetricsProvider},
            };

            #[derive(Deserialize)]
            struct ErrorMatrixContract {
                rows: Vec<ErrorMatrixRow>,
            }

            #[derive(Deserialize)]
            struct ErrorMatrixRow {
                codes: Vec<String>,
            }

            #[test]
            fn category_matrix_and_autoresponder_are_consistent() {
                // 教案式说明（Why）：该测试验证 `spark_core::error::category_matrix` 的声明式矩阵
                // 是否与运行时自动响应处理器的实际行为保持一致，防止“文档/表格已更新，但代码路径仍旧”
                // 的回归问题。
                //
                // 教案式说明（How）：遍历矩阵中每一条记录，构造带有可选预算与取消令牌的调用上下文，
                // 触发 [`ExceptionAutoResponder::on_exception_caught`]，然后依据矩阵返回的
                // [`DefaultAutoResponse`] 变体逐项断言 ReadyState、关闭原因及取消标记。
                //
                // 教案式说明（What）：
                // - 输入：矩阵内所有稳定错误码；
                // - 前置条件：矩阵条目必须完整声明默认动作；
                // - 后置条件：每条条目都能驱动期望的 ReadyState/关闭/取消行为。
                for entry in category_matrix::entries() {
                    let controller = Arc::new(RecordingController::default());
                    let cancellation = Cancellation::new();
                    let mut builder =
                        CallContextBuilder::default().with_cancellation(cancellation.clone());

                    if let Some(budget) = entry.default_response().budget() {
                        let budget = Budget::new(budget.to_budget_kind(), 1);
                        let _ = budget.try_consume(1);
                        builder = builder.add_budget(budget);
                    }

                    let call_context = builder.build();
                    let ctx = RecordingContext::new(controller.clone(), call_context);
                    let error = CoreError::new(entry.code(), "matrix-autoresponse");

                    ExceptionAutoResponder::new().on_exception_caught(&ctx, error);

                    match entry.default_response() {
                        category_matrix::DefaultAutoResponse::RetryAfter {
                            wait_ms,
                            reason,
                            busy,
                        } => {
                            let states = controller.ready_states();
                            let expected_len = if busy.is_some() { 2 } else { 1 };
                            assert_eq!(
                                states.len(),
                                expected_len,
                                "{} 应产生 {} 个 ReadyState 事件",
                                entry.code(),
                                expected_len
                            );

                            let mut index = 0;
                            if let Some(disposition) = busy {
                                assert_eq!(
                                    states[index],
                                    ReadyState::Busy(disposition.to_busy_reason()),
                                    "{} 的 Busy 原因必须与矩阵声明一致",
                                    entry.code()
                                );
                                index += 1;
                            }

                            match &states[index] {
                                ReadyState::RetryAfter(advice) => {
                                    assert_eq!(advice.wait, Duration::from_millis(wait_ms));
                                    assert_eq!(advice.reason.as_deref(), Some(reason));
                                }
                                state => {
                                    panic!("{} 应触发 RetryAfter，但收到 {:?}", entry.code(), state)
                                }
                            }

                            assert!(ctx.channel.closed_reasons().is_empty());
                            assert!(
                                !cancellation.is_cancelled(),
                                "Retryable 分类不应触发取消标记：{}",
                                entry.code()
                            );
                        }
                        category_matrix::DefaultAutoResponse::BudgetExhausted { .. } => {
                            let states = controller.ready_states();
                            assert_eq!(
                                states.len(),
                                1,
                                "{} 应广播单个 BudgetExhausted",
                                entry.code()
                            );
                            match &states[0] {
                                ReadyState::BudgetExhausted(snapshot) => {
                                    assert_eq!(snapshot.limit, 1);
                                    assert_eq!(snapshot.remaining, 0);
                                }
                                state => panic!(
                                    "{} 应触发 BudgetExhausted，但收到 {:?}",
                                    entry.code(),
                                    state
                                ),
                            }
                            assert!(ctx.channel.closed_reasons().is_empty());
                            assert!(
                                !cancellation.is_cancelled(),
                                "BudgetExhausted 分类不应取消上下文：{}",
                                entry.code()
                            );
                        }
                        category_matrix::DefaultAutoResponse::Close {
                            reason_code,
                            message,
                        } => {
                            let reasons = ctx.channel.closed_reasons();
                            assert_eq!(reasons.len(), 1, "{} 应触发优雅关闭", entry.code());
                            assert_eq!(reasons[0].code(), reason_code);
                            assert_eq!(reasons[0].message(), message);
                            assert!(controller.ready_states().is_empty());
                            assert!(
                                !cancellation.is_cancelled(),
                                "关闭分支不应标记取消：{}",
                                entry.code()
                            );
                        }
                        category_matrix::DefaultAutoResponse::Cancel => {
                            assert!(controller.ready_states().is_empty());
                            assert!(ctx.channel.closed_reasons().is_empty());
                            assert!(
                                cancellation.is_cancelled(),
                                "{} 必须标记取消令牌",
                                entry.code()
                            );
                        }
                        category_matrix::DefaultAutoResponse::None => {
                            assert!(controller.ready_states().is_empty());
                            assert!(ctx.channel.closed_reasons().is_empty());
                            assert!(
                                !cancellation.is_cancelled(),
                                "NonRetryable 默认不应产生副作用：{}",
                                entry.code()
                            );
                        }
                    }
                }
            }

            #[test]
            fn category_matrix_documentation_is_in_sync() {
                // 教案式说明（Why）：确保文档《error-category-matrix》作为交付给运维/研发的参考资料，
                // 始终列出所有矩阵条目，避免出现实现新增但文档缺失的情况。
                //
                // 教案式说明（How）：通过 `include_str!` 读取 Markdown，并断言每个错误码都出现在文档中。
                //
                // 教案式说明（What）：
                // - 输入：矩阵条目。
                // - 前置条件：文档按约定格式记录错误码。
                // - 后置条件：若缺少某个错误码将触发断言失败，提示同步更新文档。
                let doc = include_str!("../../../../docs/error-category-matrix.md");
                for entry in category_matrix::entries() {
                    assert!(
                        doc.contains(entry.code()),
                        "文档缺少错误码 {}，请同步更新 error-category-matrix.md",
                        entry.code()
                    );
                }
            }

            #[test]
            fn category_matrix_contract_is_in_sync() {
                // 教案式说明（Why）：确保声明式契约 `contracts/error_matrix.toml` 与生成的
                // `category_matrix::entries()` 始终同步，避免单独更新任意一侧时导致调用方
                // 获得过时的默认自动响应信息。构建阶段若发生漂移，该测试能立即失败，提醒
                // 维护者补齐同步步骤。
                //
                // 教案式说明（How）：
                // 1. 通过 `include_str!` 读取 TOML 合约，并使用 `toml` crate 解析为结构化数据；
                // 2. 将合约中的错误码收集进 [`BTreeSet`]，同时检查是否存在重复声明；
                // 3. 读取生成矩阵的所有错误码，转换为同样的集合并与合约集合对比。
                //
                // 教案式说明（What）：
                // - 输入：`contracts/error_matrix.toml` 与 `category_matrix::entries()`；
                // - 前置条件：TOML 合约遵循测试内的结构定义；
                // - 后置条件：若集合不相等或合约存在重复项，断言失败并给出原因。
                let raw_contract = include_str!("../../../../contracts/error_matrix.toml");
                let contract: ErrorMatrixContract =
                    toml::from_str(raw_contract).expect("解析 contracts/error_matrix.toml");

                let mut contract_codes = BTreeSet::new();
                for row in contract.rows {
                    for code in row.codes {
                        let inserted = contract_codes.insert(code.clone());
                        assert!(
                            inserted,
                            "contracts/error_matrix.toml 存在重复错误码：{code}"
                        );
                    }
                }

                let matrix_codes = category_matrix::entries()
                    .iter()
                    .map(|entry| entry.code().to_string())
                    .collect::<BTreeSet<_>>();

                assert_eq!(
                    matrix_codes, contract_codes,
                    "错误分类矩阵与 contracts/error_matrix.toml 不一致"
                );
            }

            #[derive(Default)]
            struct RecordingController {
                ready_states: Mutex<Vec<ReadyState>>,
                registry: EmptyRegistry,
            }

            impl RecordingController {
                fn ready_states(&self) -> Vec<ReadyState> {
                    self.ready_states.lock().unwrap().clone()
                }
            }

            impl Pipeline for RecordingController {
                type HandleId = PipelineHandleId;

                fn register_inbound_handler(
                    &self,
                    _: &str,
                    _: Box<dyn spark_core::pipeline::InboundHandler>,
                ) {
                }

                fn register_outbound_handler(
                    &self,
                    _: &str,
                    _: Box<dyn spark_core::pipeline::OutboundHandler>,
                ) {
                }

                fn install_middleware(
                    &self,
                    _: &dyn spark_core::pipeline::PipelineInitializer,
                    _: &spark_core::runtime::CoreServices,
                ) -> spark_core::Result<(), CoreError> {
                    Ok(())
                }

                fn emit_channel_activated(&self) {}

                fn emit_read(&self, _msg: PipelineMessage) {}

                fn emit_read_completed(&self) {}

                fn emit_writability_changed(&self, _is_writable: bool) {}

                fn emit_user_event(&self, event: CoreUserEvent) {
                    if let Some(ready) = event.downcast_application_event::<ReadyStateEvent>() {
                        self.ready_states
                            .lock()
                            .unwrap()
                            .push(ready.state().clone());
                    }
                }

                fn emit_exception(&self, _error: CoreError) {}

                fn emit_channel_deactivated(&self) {}

                fn registry(&self) -> &dyn HandlerRegistry {
                    &self.registry
                }

                fn add_handler_after(
                    &self,
                    anchor: Self::HandleId,
                    _: &str,
                    _: Arc<dyn Handler>,
                ) -> Self::HandleId {
                    anchor
                }

                fn remove_handler(&self, _: Self::HandleId) -> bool {
                    false
                }

                fn replace_handler(&self, _: Self::HandleId, _: Arc<dyn Handler>) -> bool {
                    false
                }

                fn epoch(&self) -> u64 {
                    0
                }
            }

            #[derive(Default)]
            struct EmptyRegistry;

            impl HandlerRegistry for EmptyRegistry {
                fn snapshot(&self) -> Vec<spark_core::pipeline::controller::HandlerRegistration> {
                    Vec::new()
                }
            }

            #[derive(Default)]
            struct RecordingChannel {
                controller: Arc<RecordingController>,
                closed: Mutex<Vec<CloseReason>>,
                extensions: NoopExtensions,
            }

            impl RecordingChannel {
                fn new(controller: Arc<RecordingController>) -> Self {
                    Self {
                        controller,
                        closed: Mutex::new(Vec::new()),
                        extensions: NoopExtensions,
                    }
                }

                fn closed_reasons(&self) -> Vec<CloseReason> {
                    self.closed.lock().unwrap().clone()
                }
            }

            impl Channel for RecordingChannel {
                fn id(&self) -> &str {
                    "test-channel"
                }

                fn state(&self) -> ChannelState {
                    ChannelState::Active
                }

                fn is_writable(&self) -> bool {
                    true
                }

                fn controller(&self) -> &dyn Pipeline<HandleId = PipelineHandleId> {
                    &*self.controller
                }

                fn extensions(&self) -> &dyn spark_core::pipeline::ExtensionsMap {
                    &self.extensions
                }

                fn peer_addr(&self) -> Option<spark_core::transport::TransportSocketAddr> {
                    None
                }

                fn local_addr(&self) -> Option<spark_core::transport::TransportSocketAddr> {
                    None
                }

                fn close_graceful(&self, reason: CloseReason, _deadline: Option<Deadline>) {
                    self.closed.lock().unwrap().push(reason);
                }

                fn close(&self) {}

                fn closed(
                    &self,
                ) -> BoxFuture<'static, spark_core::Result<(), spark_core::SparkError>>
                {
                    Box::pin(async { Ok(()) })
                }

                fn write(
                    &self,
                    _msg: PipelineMessage,
                ) -> spark_core::Result<WriteSignal, CoreError> {
                    Ok(WriteSignal::Accepted)
                }

                fn flush(&self) {}
            }

            #[derive(Default)]
            struct NoopExtensions;

            impl spark_core::pipeline::ExtensionsMap for NoopExtensions {
                fn insert(&self, _: std::any::TypeId, _: Box<dyn std::any::Any + Send + Sync>) {}

                fn get<'a>(
                    &'a self,
                    _: &std::any::TypeId,
                ) -> Option<&'a (dyn std::any::Any + Send + Sync + 'static)> {
                    None
                }

                fn remove(
                    &self,
                    _: &std::any::TypeId,
                ) -> Option<Box<dyn std::any::Any + Send + Sync>> {
                    None
                }

                fn contains_key(&self, _: &std::any::TypeId) -> bool {
                    false
                }

                fn clear(&self) {}
            }

            struct RecordingContext {
                controller_view: Arc<dyn Pipeline<HandleId = PipelineHandleId>>,
                channel: RecordingChannel,
                buffer_pool: NoopBufferPool,
                logger: NoopLogger,
                metrics: NoopMetricsProvider,
                executor: NoopTaskExecutor,
                timer: NoopTimeDriver,
                call_context: CallContext,
                trace: TraceContext,
            }

            impl RecordingContext {
                fn new(controller: Arc<RecordingController>, call_context: CallContext) -> Self {
                    let trace = TraceContext::new(
                        [1; TraceContext::TRACE_ID_LENGTH],
                        [2; TraceContext::SPAN_ID_LENGTH],
                        TraceFlags::new(0),
                    );
                    let controller_view: Arc<dyn Pipeline<HandleId = PipelineHandleId>> =
                        controller.clone() as Arc<dyn Pipeline<HandleId = PipelineHandleId>>;
                    Self {
                        channel: RecordingChannel::new(controller.clone()),
                        controller_view,
                        buffer_pool: NoopBufferPool,
                        logger: NoopLogger,
                        metrics: NoopMetricsProvider,
                        executor: NoopTaskExecutor,
                        timer: NoopTimeDriver,
                        call_context,
                        trace,
                    }
                }
            }

            impl Context for RecordingContext {
                fn channel(&self) -> &dyn Channel {
                    &self.channel
                }

                fn pipeline(&self) -> &Arc<dyn Pipeline<HandleId = PipelineHandleId>> {
                    &self.controller_view
                }

                fn executor(&self) -> &dyn TaskExecutor {
                    &self.executor
                }

                fn timer(&self) -> &dyn TimeDriver {
                    &self.timer
                }

                fn buffer_pool(&self) -> &dyn spark_core::buffer::BufferPool {
                    &self.buffer_pool
                }

                fn trace_context(&self) -> &TraceContext {
                    &self.trace
                }

                fn metrics(&self) -> &dyn MetricsProvider {
                    &self.metrics
                }

                fn logger(&self) -> &dyn Logger {
                    &self.logger
                }

                fn membership(&self) -> Option<&dyn spark_core::cluster::ClusterMembership> {
                    None
                }

                fn discovery(&self) -> Option<&dyn spark_core::cluster::ServiceDiscovery> {
                    None
                }

                fn call_context(&self) -> &CallContext {
                    &self.call_context
                }

                fn forward_read(&self, _msg: PipelineMessage) {}

                fn write(
                    &self,
                    msg: PipelineMessage,
                ) -> spark_core::Result<WriteSignal, CoreError> {
                    self.channel.write(msg)
                }

                fn flush(&self) {
                    self.channel.flush();
                }

                fn close_graceful(&self, reason: CloseReason, deadline: Option<Deadline>) {
                    self.channel.close_graceful(reason, deadline);
                }

                fn closed(
                    &self,
                ) -> BoxFuture<'static, spark_core::Result<(), spark_core::SparkError>>
                {
                    self.channel.closed()
                }
            }

            struct NoopBufferPool;

            impl spark_core::buffer::BufferPool for NoopBufferPool {
                fn acquire(
                    &self,
                    _: usize,
                ) -> spark_core::Result<Box<dyn spark_core::buffer::WritableBuffer>, CoreError>
                {
                    Err(CoreError::new("buffer.disabled", "buffer pool disabled"))
                }

                fn shrink_to_fit(&self) -> spark_core::Result<usize, CoreError> {
                    Ok(0)
                }

                fn statistics(&self) -> spark_core::Result<PoolStats, CoreError> {
                    Ok(PoolStats {
                        allocated_bytes: 0,
                        resident_bytes: 0,
                        active_leases: 0,
                        available_bytes: 0,
                        pending_lease_requests: 0,
                        failed_acquisitions: 0,
                        custom_dimensions: Vec::new(),
                    })
                }
            }

            /// `NoopTaskExecutor` 作为占位执行器，为契约测试提供最小依赖。
            ///
            /// # 设计背景（Why）
            /// - 错误分类自动响应逻辑与任务调度解耦，测试中无需拉起完整运行时。
            /// - 维持对 `TaskExecutor` 契约的实现，确保接口变更能第一时间在测试中暴露。
            ///
            /// # 逻辑解析（How）
            /// - 所有任务提交入口统一转化为 `spawn_dyn`，并返回立即失败的句柄。
            /// - 忽略上下文与任务内容，避免引入线程或调度复杂度。
            ///
            /// # 契约说明（What）
            /// - **前置条件**：调用方不得依赖任务执行结果。
            /// - **后置条件**：所有提交都会返回 [`TaskError::ExecutorTerminated`]。
            ///
            /// # 风险提示（Trade-offs）
            /// - 无法检测真实运行时的上下文传播；如需覆盖该路径，请在集成测试中替换实现。
            struct NoopTaskExecutor;

            impl TaskExecutor for NoopTaskExecutor {
                fn spawn_dyn(
                    &self,
                    _ctx: &CallContext,
                    _fut: spark_core::BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
                ) -> JoinHandle<Box<dyn Any + Send>> {
                    // 教案级说明（Why）
                    // - 合同层单元测试仅关注状态转换，异步执行器只需提供占位实现即可。
                    // - 提前返回错误，避免误导调用方认为任务已执行。
                    //
                    // 教案级说明（How）
                    // - 丢弃 `CallContext` 与 Future，构造 `NoopTaskHandle`。
                    // - 借助 [`JoinHandle::from_task_handle`] 保持接口一致性。
                    //
                    // 教案级说明（What）
                    // - 输出：立即失败的 [`JoinHandle`]。
                    //
                    // 教案级说明（Trade-offs）
                    // - 不覆盖真正的任务调度路径，但换取了测试的可维护性与确定性。
                    JoinHandle::from_task_handle(Box::new(NoopTaskHandle))
                }
            }

            /// `NoopTaskHandle` 对应立即失败的任务句柄。
            ///
            /// # 设计背景（Why）
            /// - 搭配 `NoopTaskExecutor` 一起使用，确保所有运行时调用都能顺利编译而无需调度线程。
            ///
            /// # 逻辑解析（How）
            /// - 固定输出类型为 `Box<dyn Any + Send>`，满足对象安全要求。
            /// - `join` 直接返回 [`TaskError::ExecutorTerminated`]，其余查询均为恒值。
            ///
            /// # 契约说明（What）
            /// - **前置条件**：任务从未开始执行。
            /// - **后置条件**：消费句柄不会对外部状态造成影响。
            ///
            /// # 风险提示（Trade-offs）
            /// - 不适合需要校验任务完成路径的测试，应在相应场景替换为更真实的实现。
            #[derive(Default)]
            struct NoopTaskHandle;

            #[async_trait::async_trait]
            impl TaskHandle for NoopTaskHandle {
                type Output = Box<dyn Any + Send>;

                fn cancel(&self, _: TaskCancellationStrategy) {}

                fn is_finished(&self) -> bool {
                    true
                }

                fn is_cancelled(&self) -> bool {
                    false
                }

                fn id(&self) -> Option<&str> {
                    None
                }

                fn detach(self: Box<Self>) {}

                async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
                    Err(TaskError::ExecutorTerminated)
                }
            }

            struct NoopTimeDriver;

            #[async_trait]
            impl TimeDriver for NoopTimeDriver {
                fn now(&self) -> MonotonicTimePoint {
                    MonotonicTimePoint::from_offset(Duration::from_millis(0))
                }

                async fn sleep(&self, _: Duration) {}
            }
            /// 验证分类矩阵默认映射与 `CoreError::category` 一致，确保文档、代码与测试联动。
            #[test]
            fn default_category_matrix_matches_lookup_contract() {
                for entry in category_matrix::entries() {
                    let error = CoreError::new(entry.code(), "matrix-check");
                    assert_eq!(
                        error.category(),
                        entry.category(),
                        "默认分类矩阵应与 CoreError::category 返回值一致，错误码：{}",
                        entry.code()
                    );
                }
            }
        }
    }
}
