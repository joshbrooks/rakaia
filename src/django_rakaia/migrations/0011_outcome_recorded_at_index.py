from django.db import migrations, models


class Migration(migrations.Migration):
    """An index on the outcome timestamp, for the retention sweep that filters on it.

    The table shipped with one index, over the scope `latest` queries by, and
    deliberately no others — see the model for why an index nothing queries is a
    cost rather than a spare. `manage.py prune_outcomes` is the first query on
    ``recorded_at``, so the index lands with it.
    """

    dependencies = [
        ("django_rakaia", "0010_consumeroutcome"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="consumeroutcome",
            index=models.Index(fields=["recorded_at"], name="rakaia_outcome_age_idx"),
        ),
    ]
