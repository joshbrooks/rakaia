#!/usr/bin/env python
"""
Django management script for testing django_rakaia.

Usage:
    python runserver.py runserver
    python runserver.py migrate
    python runserver.py createsuperuser
"""

import os
import sys

import django


def main():
    """Run administrative tasks."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tests.test_django_rakaia.settings")

    django.setup()

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
