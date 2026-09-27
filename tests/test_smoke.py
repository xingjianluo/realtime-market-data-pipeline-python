"""冒烟测试骨架。"""


def test_skeleton_imports() -> None:
    import market_pipeline.api  # noqa: F401
    import market_pipeline.engine  # noqa: F401
    import market_pipeline.model  # noqa: F401
    import market_pipeline.schema  # noqa: F401
    import market_pipeline.storage  # noqa: F401
    import market_pipeline.windows  # noqa: F401
