//! 集成示例：演示第三方实现如何在“模拟传输层”环境下运行完整 TCK。
//!
//! # 使用说明
//! - 将 `spark-contract-tests` 作为 dev-dependency，引入 `#[spark_tck]` 宏即可生成全部测试；
//! - 下方示例假定被测实现使用仓库内的默认 `spark-core` 组件，模拟了“无真实网络”的传输层。

use spark_contract_tests::spark_tck;

#[spark_tck]
mod mock_transport_tck {}
