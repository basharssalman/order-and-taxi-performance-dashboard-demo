"""Default to an offline demo; production access requires an explicit opt-in."""
import os

_mode = os.getenv("APP_MODE", "demo").strip().lower()
if _mode not in {"demo", "production"}:
    raise ValueError("APP_MODE must be 'demo' or 'production'")
DEMO_MODE = _mode == "demo"


def require_production():
    if DEMO_MODE:
        raise RuntimeError(
            "Live integrations are disabled in demo mode. Run shiny run app.py "
            "for the synthetic demo. Set APP_MODE=production only for live use."
        )
