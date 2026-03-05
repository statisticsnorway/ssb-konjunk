"""Components to use in the dash app."""

from typing import Any

try:
    from .page_aio import AnalyticsPageAIO  # type: ignore
    from .page_aio import Tables  # type: ignore
except ImportError:

    class Tables:
        """Fake class that gives easy to read error."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            """Helper that gives easy to read error."""
            raise ImportError(
                "Tables requires 'dash'. Install it with: pip install ssb-konjunk['dash']"
            )

    class AnalyticsPageAIO:
        """Fake class that gives easy to read error."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            """Helper that gives easy to read error."""
            raise ImportError(
                "AnalyticsPageAIO requires 'dash'. Install it with: pip install ssb-konjunk['dash']"
            )


__all__ = ["AnalyticsPageAIO", "Tables"]
