from django.db import migrations, models


class Migration(migrations.Migration):
    """The last event time rakaia stamped on each stream path (#284).

    Kept on the watermark because it is advanced under the watermark's lock, in
    the same save as the high mark. Nullable, and not backfilled: a path with no
    recorded stamp takes the clock as it finds it, which is what every path did
    before this column existed.
    """

    dependencies = [
        ("django_rakaia", "0011_outcome_recorded_at_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="streamoffsetwatermark",
            name="last_event_ts",
            field=models.FloatField(blank=True, null=True),
        ),
    ]
