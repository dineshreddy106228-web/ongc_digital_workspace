"""Cache extension instance for the application.

Falls back to a no-op cache when Flask-Caching is unavailable so local
development can still boot the app.
"""

try:
    from flask_caching import Cache
except ModuleNotFoundError:
    class Cache:  # type: ignore[no-redef]
        """Minimal no-op cache interface used by the app."""

        def init_app(self, app):
            app.logger.warning(
                "Flask-Caching is not installed; caching is disabled for this run."
            )

        def memoize(self, timeout=None):
            def decorator(func):
                return func

            return decorator

        def delete_memoized(self, *args, **kwargs):
            return None


cache = Cache()
