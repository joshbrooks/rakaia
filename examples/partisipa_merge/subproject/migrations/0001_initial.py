from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Balance",
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
                ("suku", models.CharField(max_length=64, unique=True)),
                (
                    "operational",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
                (
                    "infrastructure",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
            ],
            options={"ordering": ["suku"]},
        ),
        migrations.CreateModel(
            name="Claim",
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
                ("slot", models.CharField(max_length=64, unique=True)),
                ("claimed_by", models.CharField(default="", max_length=64)),
                ("ts", models.CharField(default="", max_length=32)),
            ],
            options={"ordering": ["slot"]},
        ),
        migrations.CreateModel(
            name="FinanceLine",
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
                ("suku", models.CharField(max_length=64)),
                ("account", models.CharField(max_length=16)),
                (
                    "delta",
                    models.DecimalField(decimal_places=2, default=0, max_digits=12),
                ),
            ],
            options={"ordering": ["submission_id"]},
        ),
        migrations.CreateModel(
            name="Meeting",
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
                ("suku", models.CharField(max_length=64)),
                ("meeting_id", models.CharField(max_length=64)),
                ("verified", models.BooleanField(default=False)),
            ],
            options={
                "ordering": ["suku", "meeting_id"],
                "unique_together": {("suku", "meeting_id")},
            },
        ),
        migrations.CreateModel(
            name="Project",
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
                ("suku", models.CharField(max_length=64)),
                ("output", models.CharField(max_length=64)),
                ("percent", models.IntegerField(default=0)),
            ],
            options={
                "ordering": ["suku", "output"],
                "unique_together": {("suku", "output")},
            },
        ),
        migrations.CreateModel(
            name="Readiness",
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
                ("suku", models.CharField(max_length=64, unique=True)),
                ("ready", models.BooleanField(default=False)),
                ("reasons", models.JSONField(default=list)),
            ],
            options={"ordering": ["suku"]},
        ),
    ]
