from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_django_rakaia", "0005_measure"),
    ]

    operations = [
        migrations.CreateModel(
            name="SukuProjection",
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
                    "status",
                    models.CharField(default=None, max_length=16, null=True),
                ),
                ("ksp_total", models.IntegerField(default=None, null=True)),
            ],
        ),
    ]
