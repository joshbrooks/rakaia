"""
Integration tests for django_rakaia @stream_model decorator with normalized streams.

Tests verify:
1. Creating/saving a decorated model emits stream events with monotonic offsets
2. Deleting a decorated model emits a delete event
3. Multi-stream events (single event appearing in multiple streams)
4. The canonical example (User subscribes to Projects in an Area)
"""

import pytest
from django.contrib.auth import get_user_model

from django_rakaia.models import Stream

User = get_user_model()


@pytest.mark.django_db
class TestStreamModelLifecycle:
    """Test the @stream_model decorator lifecycle with normalized streams."""

    def test_user_create_emits_event(self):
        """Creating a User emits a 'create' event with offset 1."""
        user = User.objects.create_user(username="josh", password="test")
        stream_id = f"user:{user.id}:projects"

        stream = Stream.objects.get(stream_id=stream_id)
        entries = stream.entries.order_by("offset")
        assert entries.count() == 1

        entry = entries.first()
        assert entry is not None
        assert entry.offset == 1
        assert entry.event.event_type == "create"
        assert entry.event.data["id"] == user.id
        assert entry.event.data["username"] == "josh"

    def test_user_update_emits_event(self):
        """Updating a User emits an 'update' event with monotonic offset."""
        user = User.objects.create_user(username="josh", password="test")
        stream_id = f"user:{user.id}:projects"

        # Update
        user.username = "joshua"
        user.save()

        stream = Stream.objects.get(stream_id=stream_id)
        entries = stream.entries.order_by("offset")
        assert entries.count() == 2

        entry2 = entries.last()
        assert entry2 is not None
        assert entry2.offset == 2
        assert entry2.event.event_type == "update"
        assert entry2.event.data["username"] == "joshua"

    def test_user_delete_emits_event(self):
        """Deleting a User emits a 'delete' event."""
        user = User.objects.create_user(username="josh", password="test")
        stream_id = f"user:{user.id}:projects"

        # Update first to have multiple events
        user.username = "joshua"
        user.save()

        # Delete
        user_id = user.id
        user.delete()

        stream = Stream.objects.get(stream_id=stream_id)
        entries = stream.entries.order_by("offset")
        assert entries.count() == 3

        entry3 = entries.last()
        assert entry3 is not None
        assert entry3.offset == 3
        assert entry3.event.event_type == "delete"
        assert entry3.event.data["id"] == user_id

    def test_monotonic_offsets_per_stream(self):
        """Offsets are monotonically increasing within each stream partition."""
        # Create two users - each should have their own stream
        user1 = User.objects.create_user(username="user1", password="test")
        user2 = User.objects.create_user(username="user2", password="test")

        stream1 = Stream.objects.get(stream_id=f"user:{user1.id}:projects")
        stream2 = Stream.objects.get(stream_id=f"user:{user2.id}:projects")

        entries1 = stream1.entries.order_by("offset")
        entries2 = stream2.entries.order_by("offset")

        # Each stream should have offset 1 for the create event
        assert entries1.first().offset == 1  # type: ignore[union-attr]
        assert entries2.first().offset == 1  # type: ignore[union-attr]

        # Update both users
        user1.username = "user1updated"
        user1.save()
        user2.username = "user2updated"
        user2.save()

        # Offsets should be 2 for each stream
        entries1 = stream1.entries.order_by("offset")
        entries2 = stream2.entries.order_by("offset")

        assert entries1.last().offset == 2  # type: ignore[union-attr]
        assert entries2.last().offset == 2  # type: ignore[union-attr]

    def test_data_factory_mapping(self):
        """Verify the to_dataclass correctly maps model to dataclass fields."""
        user = User.objects.create_user(username="testuser", password="test")
        stream_id = f"user:{user.id}:projects"

        stream = Stream.objects.get(stream_id=stream_id)
        entry = stream.entries.first()
        assert entry is not None

        # Verify dataclass fields are present in event data
        assert "id" in entry.event.data
        assert "username" in entry.event.data
        assert entry.event.data["id"] == user.id
        assert entry.event.data["username"] == "testuser"


@pytest.mark.django_db
class TestMultiStreamEvents:
    """Test multi-stream events (single event in multiple streams)."""

    def test_project_appears_in_two_streams(self):
        """Creating a Project creates an event in both user and area streams."""
        # Create user and area
        user = User.objects.create_user(username="creator", password="test")
        area = Area.objects.create(name="Test Area")

        # Create project (appears in both streams)
        project = Project.objects.create(
            name="Test Project",
            area=area,
            created_by=user,
        )

        # Check user stream
        user_stream = Stream.objects.get(stream_id=f"user:{user.id}:projects")
        user_entries = user_stream.entries.order_by("offset")
        assert user_entries.count() >= 1

        # Check area stream
        area_stream = Stream.objects.get(stream_id=f"area:{area.id}:projects")
        area_entries = area_stream.entries.order_by("offset")
        assert area_entries.count() >= 1

        # The most recent entry in each stream should reference the SAME event
        user_event = user_entries.last().event  # type: ignore[union-attr]
        area_event = area_entries.last().event  # type: ignore[union-attr]
        assert user_event.id == area_event.id

        # Verify event data
        assert user_event.data["id"] == project.id
        assert user_event.data["name"] == "Test Project"
        assert user_event.data["area_id"] == area.id
        assert user_event.data["created_by_id"] == user.id

    def test_multi_stream_event_shares_data(self):
        """Verify that multi-stream events share the same data (no duplication)."""
        user = User.objects.create_user(username="creator", password="test")
        area = Area.objects.create(name="Test Area")

        Project.objects.create(
            name="Shared Data Project",
            area=area,
            created_by=user,
        )

        # Get the event from both streams
        user_stream = Stream.objects.get(stream_id=f"user:{user.id}:projects")
        area_stream = Stream.objects.get(stream_id=f"area:{area.id}:projects")

        # Filter for the Project's create event specifically (by data content)
        user_entry = user_stream.entries.filter(
            event__event_type="create",
            event__data__name="Shared Data Project"
        ).first()
        area_entry = area_stream.entries.filter(
            event__event_type="create",
            event__data__name="Shared Data Project"
        ).first()

        assert user_entry is not None
        assert area_entry is not None

        # Both entries should reference the SAME event (same ID, same data)
        assert user_entry.event_id == area_entry.event_id
        assert user_entry.event.data == area_entry.event.data

        # Each stream has independent offsets (they may coincidentally be equal)
        # The important thing is that offsets are monotonic WITHIN each stream
        assert user_entry.offset > 0
        assert area_entry.offset > 0

    def test_multi_stream_offsets_independent(self):
        """Verify that offsets are independent per stream."""
        user = User.objects.create_user(username="creator", password="test")
        area1 = Area.objects.create(name="Area 1")
        area2 = Area.objects.create(name="Area 2")

        # Create projects in different areas
        Project.objects.create(
            name="Project 1",
            area=area1,
            created_by=user,
        )
        Project.objects.create(
            name="Project 2",
            area=area2,
            created_by=user,
        )

        # User stream should have 3 events (user create + 2 projects)
        user_stream = Stream.objects.get(stream_id=f"user:{user.id}:projects")
        user_entries = user_stream.entries.order_by("offset")
        assert user_entries.count() == 3
        assert [e.offset for e in user_entries] == [1, 2, 3]

        # Each area stream should have 2 events (area create + 1 project)
        area1_stream = Stream.objects.get(stream_id=f"area:{area1.id}:projects")
        area1_entries = area1_stream.entries.order_by("offset")
        assert area1_entries.count() == 2
        assert [e.offset for e in area1_entries] == [1, 2]

        area2_stream = Stream.objects.get(stream_id=f"area:{area2.id}:projects")
        area2_entries = area2_stream.entries.order_by("offset")
        assert area2_entries.count() == 2
        assert [e.offset for e in area2_entries] == [1, 2]


@pytest.mark.django_db
class TestCanonicalExample:
    """
    Test the canonical example: User subscribes to Projects in an Area.

    Users subscribe to a stream like "area:{area_id}:projects" to receive
    updates about all projects in that area.
    """

    def test_area_create_emits_event(self):
        """Creating an Area emits a 'create' event."""
        area = Area.objects.create(name="Test Area")
        stream_id = f"area:{area.id}:projects"

        stream = Stream.objects.get(stream_id=stream_id)
        entries = stream.entries.order_by("offset")
        assert entries.count() == 1

        entry = entries.first()
        assert entry is not None
        assert entry.event.event_type == "create"
        assert entry.event.data["id"] == area.id
        assert entry.event.data["name"] == "Test Area"

    def test_project_emits_to_area_stream(self):
        """Creating a Project emits an event to the area's stream."""
        user = User.objects.create_user(username="creator", password="test")
        area = Area.objects.create(name="Test Area")
        project = Project.objects.create(
            name="Test Project",
            area=area,
            created_by=user,
        )

        area_stream = Stream.objects.get(stream_id=f"area:{area.id}:projects")
        entries = area_stream.entries.order_by("offset")
        # Area create + Project create = 2 entries
        assert entries.count() == 2

        project_entry = entries.last()
        assert project_entry is not None
        assert project_entry.event.event_type == "create"
        assert project_entry.event.data["id"] == project.id
        assert project_entry.event.data["name"] == "Test Project"
        assert project_entry.event.data["area_id"] == area.id

    def test_multiple_projects_same_area(self):
        """Multiple projects in the same area share the stream with monotonic offsets."""
        user = User.objects.create_user(username="creator", password="test")
        area = Area.objects.create(name="Test Area")
        Project.objects.create(name="Project 1", area=area, created_by=user)
        Project.objects.create(name="Project 2", area=area, created_by=user)

        area_stream = Stream.objects.get(stream_id=f"area:{area.id}:projects")
        entries = area_stream.entries.order_by("offset")
        # Area create + 2 Project creates = 3 entries
        assert entries.count() == 3

        # Verify monotonic offsets
        offsets = [e.offset for e in entries]
        assert offsets == [1, 2, 3]

    def test_projects_different_areas_isolated(self):
        """Projects in different areas have isolated streams."""
        user = User.objects.create_user(username="creator", password="test")
        area1 = Area.objects.create(name="Area 1")
        area2 = Area.objects.create(name="Area 2")

        Project.objects.create(name="Project A1", area=area1, created_by=user)
        Project.objects.create(name="Project A2", area=area2, created_by=user)

        area1_stream = Stream.objects.get(stream_id=f"area:{area1.id}:projects")
        area2_stream = Stream.objects.get(stream_id=f"area:{area2.id}:projects")

        entries1 = area1_stream.entries.order_by("offset")
        entries2 = area2_stream.entries.order_by("offset")

        # Each area stream should have: area create + 1 project = 2 entries
        assert entries1.count() == 2
        assert entries2.count() == 2

        # Both should start at offset 1
        assert entries1.first().offset == 1  # type: ignore[union-attr]
        assert entries2.first().offset == 1  # type: ignore[union-attr]

    def test_user_subscribes_to_area_projects(self):
        """
        Verify the subscription mechanism: user stream is separate from area stream.

        A user interested in an area's projects would query the area's stream
        (area:{area_id}:projects), not their personal stream.
        """
        # Create user
        user = User.objects.create_user(username="subscriber", password="test")
        user_stream_id = f"user:{user.id}:projects"

        # Create area and projects
        area = Area.objects.create(name="Development")
        Project.objects.create(name="Project Alpha", area=area, created_by=user)
        Project.objects.create(name="Project Beta", area=area, created_by=user)
        area_stream_id = f"area:{area.id}:projects"

        # User stream should have user events + project events
        user_stream = Stream.objects.get(stream_id=user_stream_id)
        user_entries = user_stream.entries.order_by("offset")
        # User create + 2 projects = 3 entries
        assert user_entries.count() == 3

        # Area stream should have area + project events
        area_stream = Stream.objects.get(stream_id=area_stream_id)
        area_entries = area_stream.entries.order_by("offset")
        assert area_entries.count() == 3  # area create + 2 projects

        # Verify project data is in area stream
        project_names = [
            e.event.data["name"]
            for e in area_entries
            if e.event.event_type == "create"
        ]
        assert "Development" in project_names  # area name
        assert "Project Alpha" in project_names
        assert "Project Beta" in project_names


# Import models for tests
from tests.test_django_rakaia.models import Area, Project  # noqa: E402
