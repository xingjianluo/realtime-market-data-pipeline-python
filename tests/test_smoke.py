"""骨架冒烟测试：验证包可导入。"""


def test_imports():
    import app.api  # noqa: F401
    import app.config  # noqa: F401
    import app.models  # noqa: F401
    import app.pipeline  # noqa: F401
    import app.storage  # noqa: F401
    import app.windows  # noqa: F401
