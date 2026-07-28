from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_django_rakaia", "0004_alert_dismissed_version"),
    ]

    operations = [
        migrations.CreateModel(
            name="Measure",
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
                ("ref", models.UUIDField(unique=True)),
                (
                    "amount",
                    models.DecimalField(decimal_places=2, default=0, max_digits=10),
                ),
            ],
        ),
    ]
