#[path = "support/async.rs"]
mod async_support;

mod tests {
    pub mod contracts {
        pub mod context_propagation_spawn {
            use std::any::Any;
            use std::panic::{self, AssertUnwindSafe};
            use std::sync::{
                Arc, Mutex,
                atomic::{AtomicBool, Ordering},
            };
            use std::thread;
            use std::time::{Duration, Instant};

            use spark_core::BoxFuture;
            use spark_core::async_trait;
            use spark_core::contract::{CallContext, CallContextBuilder, Deadline};
            use spark_core::runtime::sugar::BorrowedRuntimeCaps;
            use spark_core::runtime::{
                self, JoinHandle, MonotonicTimePoint, TaskCancellationStrategy, TaskError,
                TaskExecutor, TaskHandle, TaskResult, spawn_in,
            };

            use crate::async_support::block_on;

            /// 验证父上下文触发取消后，跨线程 `spawn` 的任务能够观察到同一取消位。
            #[test]
            fn cancellation_reaches_spawned_task() {
                let executor = ThreadExecutor;
                let ctx = CallContextBuilder::default().build();
                let worker_ctx = ctx.clone();

                let sugar_ctx =
                    runtime::CallContext::new(&ctx, BorrowedRuntimeCaps::new(&executor));
                let handle = spawn_in(&sugar_ctx, async move {
                    loop {
                        if worker_ctx.cancellation().is_cancelled() {
                            break "cancelled";
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                });

                assert!(!handle.is_finished(), "任务应在取消前保持运行");
                ctx.cancellation().cancel();
                let result = block_on(handle.join()).expect("任务应在取消后成功结束");
                assert_eq!(result, "cancelled");
            }

            /// 验证子任务能够读取截止时间并在超时后主动触发取消信号。
            #[test]
            fn deadline_expiry_propagates_cancel() {
                let executor = ThreadExecutor;
                let base = MonotonicTimePoint::from_offset(Duration::from_millis(0));
                let deadline = Deadline::with_timeout(base, Duration::from_millis(40));
                let ctx = CallContextBuilder::default()
                    .with_deadline(deadline)
                    .build();
                let worker_ctx = ctx.clone();

                let sugar_ctx =
                    runtime::CallContext::new(&ctx, BorrowedRuntimeCaps::new(&executor));
                let handle = spawn_in(&sugar_ctx, async move {
                    let start = Instant::now();
                    loop {
                        let elapsed = start.elapsed();
                        let now_point = MonotonicTimePoint::from_offset(elapsed);
                        if worker_ctx.deadline().is_expired(now_point) {
                            worker_ctx.cancellation().cancel();
                            break elapsed;
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                });

                let elapsed = block_on(handle.join()).expect("任务应返回截止等待时长");
                assert!(
                    ctx.cancellation().is_cancelled(),
                    "截止时间到期后应触发取消",
                );
                assert!(elapsed >= Duration::from_millis(40));
            }

            /// 基于系统线程执行 Future 的最小实现，演示上下文传播契约。
            #[derive(Default)]
            struct ThreadExecutor;

            impl TaskExecutor for ThreadExecutor {
                fn spawn_dyn(
                    &self,
                    _ctx: &CallContext,
                    fut: BoxFuture<'static, TaskResult<Box<dyn Any + Send>>>,
                ) -> JoinHandle<Box<dyn Any + Send>> {
                    let state = Arc::new(ThreadTaskState::new());
                    let thread_state = Arc::clone(&state);
                    let join = thread::spawn(move || {
                        let outcome = panic::catch_unwind(AssertUnwindSafe(|| block_on(fut)));
                        let mut slot = thread_state.result.lock().unwrap();
                        *slot = Some(match outcome {
                            Ok(value) => value,
                            Err(_) => Err(TaskError::Panicked),
                        });
                        thread_state.finished.store(true, Ordering::Release);
                    });

                    JoinHandle::from_task_handle(Box::new(ThreadTaskHandle {
                        state,
                        join: Mutex::new(Some(join)),
                    }))
                }
            }

            /// 线程执行器内部共享的任务状态。
            ///
            /// # 设计动机（Why）
            /// - 在教学场景下复刻最小的跨线程 `JoinHandle` 行为，需要记录任务是否完成、是否被取消以及最终结果；
            /// - 通过 `Arc` 共享该结构，线程函数与句柄对象之间可安全传递结果。
            ///
            /// # 实现细节（How）
            /// - `result` 使用 `Mutex<Option<TaskResult<T>>>` 存储任务产出，确保线程写入后可由 `join` 消费；
            /// - `finished` / `cancelled` 为原子布尔量，分别表示执行完毕与取消状态；
            /// - 线程执行结束后将结果写入 `result` 并置 `finished = true`，确保句柄查询行为具备可见性。
            ///
            /// # 契约（What）
            /// - **前置条件**：句柄与线程函数必须遵循“结果只写一次”的约束，避免数据竞争；
            /// - **后置条件**：一旦 `finished` 置为 `true`，`result` 必定包含可消费的 `TaskResult`。
            ///
            /// # 风险提示（Trade-offs）
            /// - 该实现借助 `Mutex` 与 `AtomicBool`，在高并发下性能一般；其目标是演示契约而非生产级实现；
            /// - 若线程在写入前 panic，将保持 `None`，句柄需兜底返回 `ExecutorTerminated`。
            struct ThreadTaskState<T> {
                result: Mutex<Option<TaskResult<T>>>,
                finished: AtomicBool,
                cancelled: AtomicBool,
            }

            /// 面向测试的 `TaskHandle` 实现，将线程句柄与共享状态封装。
            ///
            /// # 设计动机（Why）
            /// - 契约测试需要与真实运行时类似的控制面：既能 `cancel` 又能 `join`；
            /// - 通过封装 `std::thread::JoinHandle`，模拟执行器返回的标准句柄。
            ///
            /// # 实现细节（How）
            /// - `state` 与线程共享状态，`join` 会从中取出结果；
            /// - `join` 内部在第一次调用时获取 `JoinHandle` 并等待线程结束；
            /// - 若线程 panic，返回 `TaskError::Panicked`，否则返回存储的 `TaskResult<T>`。
            ///
            /// # 契约（What）
            /// - **前置条件**：调用者需保证只调用一次 `join`，否则内部的 `Option` 会返回 `None`；
            /// - **后置条件**：`join` 成功后 `state.result` 被消费，后续调用返回 `ExecutorTerminated` 作为保护。
            ///
            /// # 风险提示（Trade-offs）
            /// - `cancel` 仅设置标记，不会真正终止线程，演示“协作取消”语义；
            /// - 实现未对 `JoinHandle::join` 的阻塞成本做处理，长任务可能导致测试耗时增长。
            struct ThreadTaskHandle<T> {
                state: Arc<ThreadTaskState<T>>,
                join: Mutex<Option<thread::JoinHandle<()>>>,
            }

            impl<T> ThreadTaskState<T> {
                fn new() -> Self {
                    Self {
                        result: Mutex::new(None),
                        finished: AtomicBool::new(false),
                        cancelled: AtomicBool::new(false),
                    }
                }
            }

            #[async_trait]
            impl<T> TaskHandle for ThreadTaskHandle<T>
            where
                T: Send + 'static,
            {
                type Output = T;

                fn cancel(&self, _strategy: TaskCancellationStrategy) {
                    self.state.cancelled.store(true, Ordering::SeqCst);
                }

                fn is_finished(&self) -> bool {
                    self.state.finished.load(Ordering::Acquire)
                }

                fn is_cancelled(&self) -> bool {
                    self.state.cancelled.load(Ordering::Acquire)
                }

                fn id(&self) -> Option<&str> {
                    None
                }

                fn detach(self: Box<Self>) {}

                async fn join(self: Box<Self>) -> TaskResult<Self::Output> {
                    if let Some(handle) = self.join.lock().unwrap().take()
                        && handle.join().is_err()
                    {
                        return Err(TaskError::Panicked);
                    }
                    self.state
                        .result
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap_or(Err(TaskError::ExecutorTerminated))
                }
            }
        }
    }
}
