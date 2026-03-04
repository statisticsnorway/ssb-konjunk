try:
    from .page_aio import AnalyticsPageAIO  # type: ignore
    from .page_aio import Tables  # type: ignore
except ImportError:

    class Tables:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Tables requires 'dash'. Install it with: pip install ssb-konjunk['dash']"
            )

    class AnalyticsPageAIO:
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "AnalyticsPageAIO requires 'dash'. Install it with: pip install ssb-konjunk['dash']"
            )


__all__ = ["AnalyticsPageAIO", "Tables"]
