"""Public API for ntfy-dispatch."""

from .models import Message
from .registry import HandlerSpec, Registry, get_registry, handler, reset_registry
from .version import __version__
from .worker import HandlerContext

__all__ = [
    "HandlerContext",
    "HandlerSpec",
    "Message",
    "Registry",
    "__version__",
    "get_registry",
    "handler",
    "reset_registry",
]
