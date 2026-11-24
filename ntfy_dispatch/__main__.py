"""Allow ``python -m ntfy_dispatch``."""

from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
