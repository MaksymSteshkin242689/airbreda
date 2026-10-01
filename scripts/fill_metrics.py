#!/usr/bin/env python3
"""Copy the numbers from model/metrics.json into the ADR-006 table in docs/index.md.

    python scripts/fill_metrics.py            # replace METRICS_* placeholders (first fill)
    python scripts/fill_metrics.py --update   # re-fill after retraining (replaces the previous numbers)

Keeping this mechanical means the document always quotes the metrics of the model that was
actually trained and shipped — never numbers typed by hand.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

DOC = Path("docs/index.md")
METRICS = Path("model/metrics.json")
ROW_RE = re.compile(r"^\| (?:METRICS_ROWS|\d+) \| .+ \|$", re.M)   # the table's data row


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--update", action="store_true", help="replace an already-filled row")
    args = parser.parse_args(argv)

    m = json.loads(METRICS.read_text())
    coef = m["coefficients"]
    row = (f"| {m['rows_total']} | {m['evaluation']} | {m['r2']:.3f} | {m['mae_ug_m3']:.2f} "
           f"| {coef['total_intensity_veh_per_hr']:+.5f} | {coef['hour_of_day']:+.3f} | {m['intercept']:.2f} |")

    text = DOC.read_text()
    if "METRICS_ROWS" in text:
        text = ROW_RE.sub(row, text, count=1)
        text = text.replace("METRICS_ROWS", str(m["rows_total"]))  # the context sentence
    elif args.update:
        text = ROW_RE.sub(row, text, count=1)
        text = re.sub(r"had \d+ joined hourly rows", f"had {m['rows_total']} joined hourly rows", text)
    else:
        print("placeholders already filled; pass --update to overwrite", file=sys.stderr)
        return 1
    DOC.write_text(text)
    print(row)
    sign = coef["total_intensity_veh_per_hr"]
    if sign < 0:
        print("NOTE: negative traffic coefficient — add a sentence to §6 explaining it", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
