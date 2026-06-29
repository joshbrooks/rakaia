#!/bin/bash
# Start the Django development server for django_rakaia
# 
# This script will:
# 1. Run database migrations
# 2. Create a superuser (admin/admin) if it doesn't exist
# 3. Start the development server

set -e

cd "$(dirname "$0")"

echo "=============================================="
echo "  Django Rakaia - Development Server"
echo "=============================================="
echo ""

echo "→ Running database migrations..."
uv run python manage.py migrate --no-input

echo ""
echo "→ Creating superuser (admin/admin)..."
uv run python manage.py shell -c "
from django.contrib.auth import get_user_model
User = get_user_model()
if not User.objects.filter(username='admin').exists():
    User.objects.create_superuser('admin', 'admin@example.com', 'admin')
    print('  ✓ Superuser created!')
else:
    print('  ✓ Superuser already exists.')
"

echo ""
echo "=============================================="
echo "  Server Starting..."
echo "=============================================="
echo ""
echo "  📊 Dashboard: http://localhost:8000/streams/"
echo "  🔐 Admin:     http://localhost:8000/admin/"
echo "  👤 Login:     admin / admin"
echo ""
echo "  Press Ctrl+C to stop the server."
echo ""
echo "=============================================="
echo ""

uv run python manage.py runserver 0.0.0.0:8000
