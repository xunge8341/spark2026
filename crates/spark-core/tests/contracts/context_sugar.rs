#[path = "support/async.rs"]
mod async_support;

mod tests {
    pub mod contracts {
        pub mod context_sugar {
            use std::{
                any::Any,
                sync::{
                    atomic::{AtomicBool, AtomicUsize, Ordering},
                    Arc, Mutex,
                },
            };

            use spark_core::{
                async_trait,
                contract::CallContext,
                runtime::{
                    self, spawn_in, JoinHandle, TaskCancellationStrategy, TaskError, TaskExecutor,
                    TaskHandle, TaskResult,
                },
                with_ctx,
            };

            use crate::async_support::block_on;

            /// 即时执行的运行时桩：提交任务时立即轮询 Future 并缓存结果。
            ///
            /// # 设计目标（Why）
            /// - 为 `spawn_in` 提供最小可用的执行环境，避免依赖真实异步运行时；
            /// - 通过原子计数记录最近一次提交时看到的上下文指针，便于断言语法糖是否正确传参。
            ///
            /// # 内部结构（How）
            /// - `last_ctx`：存储最近一次 `spawn` 调用时的 `CallContext` 指针，便于测试校验；
            /// - `cancelled`：记录是否调用过 `cancel`，演示控制面状态同步；
            /// - `finished`：由句柄在 `join` 后更新，满足 `TaskHandle::is_finished` 契约。
            #[derive(Default)]
            struct ImmediateExecutor {
                last_ctx: AtomicUsize,
            }

            impl ImmediateExecutor {
                fn last_ctx(&self) -> usize {
                    self.last_ctx.load(Ordering::SeqCst)
                }
            }

            impl TaskExecutor for ImmediateExecutor {
                fn spawn_dyn(
                    &self,
                    ctx: &CallContext,
                    fut: spark_core::BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
                ) -> JoinHandle<Box<dyn Any + Send>> {
                    self.last_ctx
                        .store(ctx as *const CallContext as usize, Ordering::SeqCst);
                    let result = block_on(fut);
                    JoinHandle::from_task_handle(Box::new(ImmediateTaskHandle::new(result)))
                }
            }

            /// 保存即时执行结果的 `TaskHandle` 实现。
            ///
            /// - 以 `Mutex<Option<...>>` 持有任务结果，保证 `join` 只消费一次；
            /// - 通过原子布尔量暴露 `is_finished` / `is_cancelled` 状态。
            struct ImmediateTaskHandle {
                result: Mutex<Option<TaskResult<Box<dyn Any + Send>>>>,
                finished: AtomicBool,
                cancelled: AtomicBool,
            }

            impl ImmediateTaskHandle {
                fn new(result: TaskResult<Box<dyn Any + Send>>) -> Self {
                    let cancelled = matches!(result, Err(TaskError::Cancelled));
                    Self {
                        result: Mutex::new(Some(result)),
                        finished: AtomicBool::new(true),
                        cancelled: AtomicBool::new(cancelled),
                    }
                }
            }

            #[async_trait]
            impl TaskHandle for ImmediateTaskHandle {
                type Output = Box<dyn Any + Send>;

                fn cancel(&self, _strategy: TaskCancellationStrategy) {
                    self.cancelled.store(true, Ordering::SeqCst);
                }

                fn is_finished(&self) -> bool {
                    self.finished.load(Ordering::SeqCst)
                }

                fn is_cancelled(&self) -> bool {
                    self.cancelled.load(Ordering::SeqCst)
                }

                fn id(&self) -> Option<&str> {
                    None
                }

                fn detach(self: Box<Self>) {}

                async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
                    let result = self
                        .result
                        .lock()
                        .expect("即时句柄锁应始终可用")
                        .take()
                        .ok_or_else(|| TaskError::ExecutorTerminated)?;
                    Ok(result?)
                }
            }

            /// 将最小字段集封装为 Pipeline `Context` 测试桩，仅实现语法糖所需方法。
            struct FakeContext<'a> {
                call_ctx: &'a CallContext,
                executor: &'a ImmediateExecutor,
                pipeline: Arc<dyn spark_core::pipeline::Pipeline<
                    HandleId = spark_core::pipeline::controller::PipelineHandleId,
                >>,
            }

            impl<'a> FakeContext<'a> {
                fn new(call_ctx: &'a CallContext, executor: &'a ImmediateExecutor) -> Self {
                    Self {
                        call_ctx,
                        executor,
                        pipeline: Arc::new(RecordingController::default()) as Arc<dyn spark_core::pipeline::Pipeline<HandleId = spark_core::pipeline::controller::PipelineHandleId>>,
                    }
                }
            }

            impl<'a> spark_core::pipeline::Context for FakeContext<'a> {
                fn channel(&self) -> &dyn spark_core::pipeline::Channel {
                    panic!("测试桩未实现 channel()：语法糖示例不应调用该方法");
                }

                fn pipeline(&self) -> &Arc<dyn spark_core::pipeline::Pipeline<HandleId = spark_core::pipeline::controller::PipelineHandleId>> {
                    &self.pipeline
                }

                fn executor(&self) -> &dyn TaskExecutor {
                    self.executor
                }

                fn timer(&self) -> &dyn spark_core::runtime::TimeDriver {
                    panic!("测试桩未实现 timer()：示例中无需计时能力");
                }

                fn buffer_pool(&self) -> &dyn spark_core::buffer::BufferPool {
                    panic!("测试桩未实现 buffer_pool()：示例不会租借缓冲");
                }

                fn trace_context(&self) -> &spark_core::observability::TraceContext {
                    panic!("测试桩未实现 trace_context()：语法糖示例不会访问追踪上下文");
                }

                fn metrics(&self) -> &dyn spark_core::observability::MetricsProvider {
                    panic!("测试桩未实现 metrics()：示例不会打指标");
                }

                fn logger(&self) -> &dyn spark_core::observability::Logger {
                    panic!("测试桩未实现 logger()：示例不会写日志");
                }

                fn membership(&self) -> Option<&dyn spark_core::cluster::ClusterMembership> {
                    None
                }

                fn discovery(&self) -> Option<&dyn spark_core::cluster::ServiceDiscovery> {
                    None
                }

                fn call_context(&self) -> &CallContext {
                    self.call_ctx
                }

                fn forward_read(&self, _msg: spark_core::buffer::PipelineMessage) {
                    panic!("测试桩未实现 forward_read()：示例不会转发事件");
                }

                fn write(&self, _msg: spark_core::buffer::PipelineMessage) -> spark_core::Result<spark_core::pipeline::WriteSignal, spark_core::CoreError> {
                    panic!("测试桩未实现 write()：语法糖示例不会写出站消息");
                }

                fn flush(&self) {
                    panic!("测试桩未实现 flush()：语法糖示例不会刷新缓冲");
                }

                fn close_graceful(
                    &self,
                    _reason: spark_core::contract::CloseReason,
                    _deadline: Option<spark_core::contract::Deadline>,
                ) {
                    panic!("测试桩未实现 close_graceful()：示例不会触发关闭");
                }

                fn closed(&self) -> spark_core::future::BoxFuture<'static, spark_core::Result<(), spark_core::SparkError>> {
                    panic!("测试桩未实现 closed()：语法糖示例不会等待关闭");
                }
            }

            /// 验证 `spawn_in` 能够正确转发 `CallContext` 引用并返回可等待的句柄。
            #[test]
            fn spawn_in_dispatches_future() {
                let executor = ImmediateExecutor::default();
                let call_ctx = CallContext::builder().build();
                let sugar_ctx = runtime::CallContext::borrowed(&call_ctx, &executor);

                let join = spawn_in(&sugar_ctx, async move { 42usize });
                let output = block_on(join.join()).expect("任务应成功完成");

                assert_eq!(output, 42, "Future 应返回原始数值");
                assert_eq!(
                    executor.last_ctx(),
                    &call_ctx as *const CallContext as usize,
                    "语法糖应向执行器传递原始上下文引用",
                );
            }

            /// 验证 `with_ctx!` 宏在 Pipeline `Context` 中展开后可以直接调用 `spawn_in` 并复用上下文。
            #[test]
            fn with_ctx_macro_rebinds_context() {
                let executor = ImmediateExecutor::default();
                let call_ctx = CallContext::builder().build();
                let expected_ptr = &call_ctx as *const CallContext as usize;
                let ctx = FakeContext::new(&call_ctx, &executor);

                let fut = with_ctx!(ctx, async move {
                    assert_eq!(
                        ctx.call() as *const CallContext as usize,
                        expected_ptr,
                        "宏展开后应仍然指向原始上下文",
                    );

                    let cloned = ctx.clone_call();
                    assert_eq!(
                        cloned.deadline(),
                        call_ctx.deadline(),
                        "clone_call 应复制截止时间等上下文字段",
                    );

                    let join = spawn_in(&ctx, async move { 7u8 });
                    join.join().await.expect("语法糖句柄应支持 await")
                });

                let value = block_on(fut);
                assert_eq!(value, 7u8);
                assert_eq!(
                    executor.last_ctx(),
                    expected_ptr,
                    "通过宏调用 spawn_in 也应记录相同上下文指针",
                );
            }
        }
    }
}
