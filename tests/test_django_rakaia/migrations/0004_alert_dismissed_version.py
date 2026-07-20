from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_django_rakaia", "0003_alert"),
    ]

    operations = [
        migrations.AddField(
            model_name="alert",
            name="dismissed_version",
            field=models.IntegerField(default=None, null=True),
        ),
    ]
