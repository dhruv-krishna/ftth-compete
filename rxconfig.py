"""Reflex config for the ftth-compete web UI.

The Streamlit app at `src/ftth_compete/ui/app.py` is still the primary UI
during the migration. This Reflex app is the long-term replacement and
will be built up phase-by-phase (see `.claude/roadmap.md`).

Run with:
    uv run reflex run         # dev mode (hot reload, localhost:3000)
    uv run reflex run --env prod    # production build

Reflex expects `<app_name>/<app_name>.py` at the import root. To keep the
existing `src/ftth_compete/` package layout untouched, the Reflex app lives
in a sibling top-level package `ftth_compete_web/`. It imports the
pipeline / analysis layers from `ftth_compete` as a normal library.
"""

from __future__ import annotations

import reflex as rx
from reflex_base.plugins.sitemap import SitemapPlugin
from reflex_components_radix.plugin import RadixThemesPlugin

# Radix theme moved here in Reflex 0.9 — `App(theme=...)` is deprecated.
_theme = rx.theme(
    appearance="light",
    accent_color="blue",
    gray_color="slate",
    radius="medium",
    scaling="100%",
)

config = rx.Config(
    app_name="ftth_compete_web",
    # Don't set frontend_port / backend_port explicitly: Reflex Cloud runs
    # the backend in --backend-only mode and rejects --frontend-port,
    # crashing the container in a loop. The defaults (3000 / 8000) are
    # the same as what we'd hardcode and work for local dev too.
    # Allow WS connections from the HF Spaces public hostname (and any
    # other origin you'd deploy under). Wildcard is fine for personal
    # use on a public read-only demo.
    cors_allowed_origins=["*"],
    plugins=[
        SitemapPlugin(),
        RadixThemesPlugin(theme=_theme),
    ],
    # Tailwind plugin enabled for utility classes used inside the app.
    tailwind={
        "theme": {
            "extend": {
                "colors": {
                    # Brand accent — tweak once we have a logo / palette decision.
                    "brand": {
                        "DEFAULT": "#2563EB",
                        "muted": "#1E40AF",
                    },
                },
            },
        },
    },
)
