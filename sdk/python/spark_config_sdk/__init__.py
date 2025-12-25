"""Spark 配置事件 SDK 顶层包。

Why:
    - 暴露跨语言共享的事件数据模型、Schema 与错误分类映射，供 TCK 与业务代码复用。
    - 通过 `__all__` 限定公开接口，避免内部生成细节泄露至调用方。

What:
    - `events`: 自动生成的 dataclass 定义与事件元数据。
    - `errors`: 错误码到分类模板的映射。
    - `schemas`: JSON Schema 与 AsyncAPI 访问器。

How:
    - 由 `tools/gen_config_events_artifacts.rs` 根据 schemas/ 下的 JSON Schema 与 AsyncAPI SOT 生成，不应手工编辑。
"""

from .events import EVENT_PAYLOAD_TYPES, EVENT_DESCRIPTORS
from .errors import ERROR_MATRIX
from .schemas import load_json_schema, load_asyncapi

__all__ = [
    "EVENT_PAYLOAD_TYPES",
    "EVENT_DESCRIPTORS",
    "ERROR_MATRIX",
    "load_json_schema",
    "load_asyncapi",
]
