from django.db import migrations, models


class Migration(migrations.Migration):
    """The table behind `django_rakaia.outcomes.DjangoOutcomeStore`.

    A record of what happened to an event a consumer could not apply, kept beside
    the cursor that says how far it got. Empty for a consumer that never fails —
    only exceptions are recorded — so the cost of having it is the table itself.
    """

    dependencies = [
        ("django_rakaia", "0009_alter_stream_last_seq"),
    ]

    operations = [
        migrations.CreateModel(
            name="ConsumerOutcome",
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
                ("consumer", models.CharField(max_length=128)),
                ("stream_path", models.CharField(max_length=255)),
                ("subject", models.CharField(max_length=255)),
                ("offset", models.CharField(blank=True, max_length=64, null=True)),
                ("attempt", models.PositiveIntegerField()),
                ("payload", models.TextField()),
                ("recorded_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "db_table": "rakaia_consumeroutcome",
                "indexes": [
                    models.Index(
                        fields=["consumer", "stream_path"],
                        name="rakaia_outcome_scope_idx",
                    )
                ],
            },
        ),
    ]
