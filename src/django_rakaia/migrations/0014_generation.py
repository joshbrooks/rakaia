from django.db import migrations, models

import django_rakaia.models


class Migration(migrations.Migration):
    """A stamp on each stream and on each saved cursor (#350).

    A stream's stamp changes when it is deleted and recreated, so a consumer can
    tell a different stream from the same one with later offsets. The cursor's is
    nullable: a cursor saved before this adopts the stream's stamp on its next run.
    """

    dependencies = [
        ("django_rakaia", "0013_event_tags_and_correlation"),
    ]

    operations = [
        migrations.AddField(
            model_name="stream",
            name="generation",
            field=models.CharField(
                default=django_rakaia.models.new_generation, max_length=32
            ),
        ),
        migrations.AddField(
            model_name="consumercursor",
            name="generation",
            field=models.CharField(blank=True, max_length=32, null=True),
        ),
    ]
