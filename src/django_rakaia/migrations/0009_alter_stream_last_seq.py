from django.db import migrations, models


class Migration(migrations.Migration):
    """Store `Stream-Seq` as text, because the protocol says it is a string.

    `Stream-Seq` values are opaque strings compared byte-wise lexicographically
    (PROTOCOL.md), so an integer column could not hold a conforming value such
    as a ULID. The column is only written by the protocol server and only ever
    held decimal digits, which survive the widening unchanged.
    """

    dependencies = [
        ("django_rakaia", "0008_alter_translatable_unique_together_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="stream",
            name="last_seq",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
