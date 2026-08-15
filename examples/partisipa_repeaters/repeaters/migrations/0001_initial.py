from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Node",
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
                ("submission_id", models.CharField(max_length=64)),
                ("node_id", models.CharField(max_length=64)),
                ("parent_node_id", models.CharField(default="", max_length=64)),
                ("depth", models.IntegerField(default=0)),
                ("value", models.IntegerField(default=0)),
                ("is_leaf", models.BooleanField(default=False)),
            ],
            options={
                "ordering": ["submission_id", "node_id"],
                "unique_together": {("submission_id", "node_id")},
            },
        ),
        migrations.CreateModel(
            name="Total",
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
                ("submission_id", models.CharField(max_length=64, unique=True)),
                ("total", models.IntegerField(default=0)),
            ],
            options={"ordering": ["submission_id"]},
        ),
    ]
