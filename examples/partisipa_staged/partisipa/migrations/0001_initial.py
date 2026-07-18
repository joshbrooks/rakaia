import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Project",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("suku", models.CharField(max_length=64)),
                ("output", models.CharField(max_length=64)),
                ("name", models.CharField(default="", max_length=128)),
            ],
            options={
                "ordering": ["suku", "output"],
                "unique_together": {("suku", "output")},
            },
        ),
        migrations.CreateModel(
            name="Sf12",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("submission_id", models.CharField(max_length=64, unique=True)),
                ("suku", models.CharField(default="", max_length=64)),
                ("output", models.CharField(default="", max_length=64)),
                ("cost", models.DecimalField(decimal_places=2, default=0, max_digits=12)),
                ("link_reason", models.CharField(default="", max_length=8)),
                ("project", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="sf12_forms", to="partisipa.project")),
            ],
            options={
                "ordering": ["submission_id"],
            },
        ),
    ]
