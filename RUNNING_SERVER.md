# Running the Django Rakaia Development Server

## Quick Start

### Option 1: Use the start script (recommended)

```bash
./start-server.sh
```

This will:
1. Run database migrations
2. Create a superuser (`admin` / `admin`)
3. Start the development server

### Option 2: Manual steps

```bash
# 1. Run migrations
uv run python manage.py migrate

# 2. Create a superuser
uv run python manage.py createsuperuser

# 3. Start the server
uv run python manage.py runserver 0.0.0.0:8000
```

## Access the Application

Once the server is running:

| Page | URL |
|------|-----|
| **Dashboard** | http://localhost:8000/streams/ |
| **Admin** | http://localhost:8000/admin/ |

**Default login:** `admin` / `admin`

## Test the Dashboard

Create some test data to see in the dashboard:

```bash
# Open a Django shell
uv run python manage.py shell
```

```python
from django.contrib.auth import get_user_model
from tests.test_django_rakaia.models import Area, Project

# Create a user (this will trigger stream events)
User = get_user_model()
user = User.objects.create_user(username='testuser', password='test')

# Create an area and projects
area = Area.objects.create(name='My Area')
Project.objects.create(name='Project Alpha', area=area)
Project.objects.create(name='Project Beta', area=area)

# Now visit http://localhost:8000/streams/ to see the events!
```

## Server Commands

```bash
# Run migrations
uv run python manage.py migrate

# Create a new superuser
uv run python manage.py createsuperuser

# Collect static files (for production)
uv run python manage.py collectstatic

# Run tests
uv run pytest tests/test_django_rakaia/ -v
```

## Troubleshooting

### Port already in use

If port 8000 is already in use, specify a different port:

```bash
uv run python manage.py runserver 0.0.0.0:8001
```

### Database issues

To start fresh, delete the database file and re-run migrations:

```bash
rm db.sqlite3
uv run python manage.py migrate
uv run python manage.py createsuperuser
```

### Import errors

Make sure you're in the project directory and using `uv run`:

```bash
cd /var/home/josh/github/joshbrooks/durable-streams
uv run python manage.py runserver
```
