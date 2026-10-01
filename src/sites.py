"""The AirBreda measurement locations. Shared by ingestion, training and serving so the
site-id mapping exists exactly once."""
from __future__ import annotations

# Luchtmeetnet air-quality station monitoring the A27 interchange (Breda-Tilburgseweg).
STATION_ID = "NL10240"

# Label → NDW measurement-site id. All four sit at hectometre 63 of the A27, the interchange that
# station NL10240 monitors, so one real air-quality reading legitimately covers all four.
SITES: dict[str, str] = {
    "hrl": "RWS01_MONIBAS_0271hrl0063ra",  # mainline, direction 1
    "hrr": "RWS01_MONIBAS_0271hrr0063ra",  # mainline, direction 2
    "vwd": "RWS01_MONIBAS_0270vwd0063ra",  # entry slip road (traffic leaving Breda)
    "vwa": "RWS01_MONIBAS_0270vwa0063ra",  # exit slip road (traffic entering Breda)
}
NDW_ID_TO_LABEL: dict[str, str] = {v: k for k, v in SITES.items()}
MAINLINE_SITES = ("hrl", "hrr")
