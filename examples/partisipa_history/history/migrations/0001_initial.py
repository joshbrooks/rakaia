from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="PghEventGolden",
            fields=[
                ("pgh_id", models.AutoField(primary_key=True, serialize=False)),
                ("submission_id", models.CharField(max_length=64)),
                ("pgh_label", models.CharField(max_length=16)),
                ("pgh_context_user", models.CharField(default="", max_length=128)),
                ("pgh_created_at", models.CharField(default="", max_length=32)),
                ("fields", models.JSONField(default=dict)),
            ],
            options={"ordering": ["pgh_id"]},
        ),
        migrations.CreateModel(
            name="SubmissionRecord",
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
                ("fields", models.JSONField(default=dict)),
                ("actor", models.CharField(default="", max_length=128)),
                ("updated_at", models.CharField(default="", max_length=32)),
            ],
            options={"ordering": ["submission_id"]},
        ),
        migrations.CreateModel(
            name="SubmissionHistoryEntry",
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
                ("seq", models.IntegerField()),
                ("label", models.CharField(max_length=1)),
                ("actor", models.CharField(default="", max_length=128)),
                ("ts", models.CharField(default="", max_length=32)),
                ("fields", models.JSONField(default=dict)),
            ],
            options={
                "ordering": ["submission_id", "seq"],
                "unique_together": {("submission_id", "seq")},
            },
        ),
    ]
