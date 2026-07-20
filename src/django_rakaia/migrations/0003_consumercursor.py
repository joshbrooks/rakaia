from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_rakaia", "0002_streamevent_metadata"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConsumerCursor",
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
                ("consumer_id", models.CharField(max_length=128)),
                ("stream_path", models.CharField(max_length=255)),
                ("offset", models.CharField(max_length=64)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "db_table": "rakaia_consumercursor",
                "unique_together": {("consumer_id", "stream_path")},
            },
        ),
    ]
