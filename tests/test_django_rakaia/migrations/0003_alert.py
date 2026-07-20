from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_django_rakaia", "0002_history"),
    ]

    operations = [
        migrations.CreateModel(
            name="Alert",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("stream_key", models.CharField(max_length=64)),
                ("alert_type", models.CharField(max_length=64)),
                ("field_key", models.CharField(default="", max_length=64)),
                ("severity", models.CharField(default="info", max_length=16)),
                ("message", models.TextField(default="")),
                (
                    "resolved_at",
                    models.CharField(default=None, max_length=32, null=True),
                ),
                (
                    "resolved_by",
                    models.CharField(default=None, max_length=64, null=True),
                ),
                ("created_at", models.CharField(default="", max_length=32)),
            ],
            options={
                "ordering": ["stream_key", "alert_type", "field_key"],
                "unique_together": {("stream_key", "alert_type", "field_key")},
            },
        ),
    ]
