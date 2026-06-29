#!/usr/bin/env python
"""Django management script for the Rakaia chat sample."""

import os
import sys


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "chat_project.settings")
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you in the project's venv? "
            "Install with: pip install 'rakaia[django]'"
        ) from exc
    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
