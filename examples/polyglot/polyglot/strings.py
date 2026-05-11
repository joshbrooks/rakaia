"""Catalog of msgids the polyglot landing page renders.

Keeping this list explicit (rather than scraping templates) means the
seed step is trivial: for every (langcode, msgid) we ensure a
Translatable row exists, defaulting msgstr to a sensible starter.
"""

from __future__ import annotations

# (msgid, default_msgstr_per_langcode)
CATALOG: list[tuple[str, dict[str, str]]] = [
    (
        "nav.product",
        {"tet": "Produtu", "pt": "Produto", "id": "Produk"},
    ),
    (
        "nav.pricing",
        {"tet": "Folin", "pt": "Preços", "id": "Harga"},
    ),
    (
        "nav.docs",
        {"tet": "Dokumentus", "pt": "Documentação", "id": "Dokumentasi"},
    ),
    (
        "hero.title",
        {
            "tet": "Hadia tradusaun ne'ebé moris.",
            "pt": "Traduções que ganham vida.",
            "id": "Terjemahan yang hidup.",
        },
    ),
    (
        "hero.subtitle",
        {
            "tet": "Edita string ida — hetan iha browser hotu-hotu.",
            "pt": "Edite uma string — ela aparece em todos os browsers.",
            "id": "Edit satu string — muncul di semua browser.",
        },
    ),
    (
        "hero.cta",
        {"tet": "Komesa agora", "pt": "Começar agora", "id": "Mulai sekarang"},
    ),
    (
        "feature.realtime.title",
        {"tet": "Tempu reál", "pt": "Tempo real", "id": "Waktu nyata"},
    ),
    (
        "feature.realtime.body",
        {
            "tet": "Mudansa hotu-hotu sai iha browser hotu-hotu kedan.",
            "pt": "Cada alteração aparece imediatamente em todos os browsers.",
            "id": "Setiap perubahan langsung muncul di semua browser.",
        },
    ),
    (
        "feature.durable.title",
        {"tet": "Durável", "pt": "Durável", "id": "Tahan lama"},
    ),
    (
        "feature.durable.body",
        {
            "tet": "Refresh la lakon nada — eventu sira persiste no replay.",
            "pt": "Refresh não perde nada — eventos persistem e fazem replay.",
            "id": "Refresh tidak kehilangan apa pun — event tetap dan diulang.",
        },
    ),
    (
        "footer.tagline",
        {
            "tet": "Konstrui ho Rakaia + Durable Streams.",
            "pt": "Construído com Rakaia + Durable Streams.",
            "id": "Dibangun dengan Rakaia + Durable Streams.",
        },
    ),
]

LANGUAGES: list[tuple[str, str]] = [
    ("tet", "Tetun"),
    ("pt", "Português"),
    ("id", "Bahasa Indonesia"),
]

DEFAULT_LANG = "tet"
