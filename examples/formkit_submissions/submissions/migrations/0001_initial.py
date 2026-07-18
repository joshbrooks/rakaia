from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="MonitoringVisit",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("submission_id", models.CharField(max_length=64, unique=True)),
                ("form_type", models.CharField(default="", max_length=64)),
                ("project_code", models.CharField(default="", max_length=32)),
                ("suku", models.CharField(default="", max_length=64)),
                ("monitor", models.CharField(default="", max_length=64)),
                ("visit_date", models.CharField(default="", max_length=32)),
                ("total_budget", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("overall_progress", models.DecimalField(decimal_places=2, default=0, max_digits=5)),
                ("status", models.CharField(default="", max_length=16)),
            ],
            options={
                "ordering": ["submission_id"],
            },
        ),
        migrations.CreateModel(
            name="ActivityProgress",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("submission_id", models.CharField(max_length=64)),
                ("activity_index", models.IntegerField()),
                ("name", models.CharField(default="", max_length=128)),
                ("budget", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("progress_pct", models.IntegerField(default=0)),
            ],
            options={
                "ordering": ["submission_id", "activity_index"],
                "unique_together": {("submission_id", "activity_index")},
            },
        ),
    ]
