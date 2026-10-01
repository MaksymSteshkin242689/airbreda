#!/usr/bin/env bash
# Pre-submission check of the live system: the six graded endpoints, response shapes and types.
#   ./scripts/verify.sh                 # uses VM_IP from infra/resources.env
#   ./scripts/verify.sh http://host:8000
set -uo pipefail
cd "$(dirname "$0")/.."
BASE="${1:-}"
if [ -z "$BASE" ]; then . infra/resources.env; BASE="http://$VM_IP:8000"; fi
fail=0
check() { if [ "$2" = "ok" ]; then printf '  PASS  %s\n' "$1"; else printf '  FAIL  %s — %s\n' "$1" "$2"; fail=1; fi; }

echo "== $BASE =="
for site in hrl hrr vwd vwa; do
  body=$(curl -s -m 15 "$BASE/site/$site")
  res=$(printf '%s' "$body" | python3 -c '
import sys, json
try: d = json.load(sys.stdin)
except Exception as e: print(f"not JSON: {e}"); sys.exit()
req = ["site_id", "no2_ug_m3", "intensity_veh_per_hr", "no2_exceedance_risk", "timestamp"]
missing = [k for k in req if k not in d]
if missing: print("missing keys " + ",".join(missing)); sys.exit()
if d["site_id"] != sys.argv[1]: print("wrong site_id"); sys.exit()
if not isinstance(d["no2_ug_m3"], (int, float)): print("no2_ug_m3 not numeric"); sys.exit()
if not isinstance(d["intensity_veh_per_hr"], int): print("intensity not int"); sys.exit()
r = d["no2_exceedance_risk"]
if r is None: print("risk is null: " + str(d.get("prediction_error"))); sys.exit()
if not (0.0 <= r <= 1.0): print("risk out of [0,1]"); sys.exit()
print("ok")' "$site")
  check "GET /site/$site" "$res"
done

res=$(curl -s -m 15 "$BASE/health" | python3 -c '
import sys, json
d = json.load(sys.stdin)
for src in ("luchtmeetnet", "ndw"):
    if src not in d or "last_successful_fetch" not in d[src] or "bad_data_count" not in d[src]:
        print(f"{src} block incomplete"); sys.exit()
    if not d[src]["last_successful_fetch"]: print(f"{src} never fetched"); sys.exit()
print("ok" if d.get("status") in ("ok", "degraded") else "status=" + str(d.get("status")))
print("   status:", d.get("status"), "| luchtmeetnet:", d["luchtmeetnet"]["last_successful_fetch"], "| ndw:", d["ndw"]["last_successful_fetch"], file=sys.stderr)')
check "GET /health" "$res"

code=$(curl -s -m 15 -o /tmp/airbreda_index.html -w '%{http_code}' "$BASE/")
if [ "$code" = "200" ] && grep -q 'id="actual"' /tmp/airbreda_index.html && grep -q 'row-vwa' /tmp/airbreda_index.html && grep -q 'fetch(`/site/' /tmp/airbreda_index.html; then res=ok; else res="HTTP $code or page markup missing"; fi
check "GET / (dashboard page)" "$res"

code=$(curl -s -m 15 -o /dev/null -w '%{http_code}' "$BASE/site/bogus"); [ "$code" = "404" ] && res=ok || res="HTTP $code"
check "GET /site/bogus → 404" "$res"

[ "$fail" = 0 ] && echo "ALL CHECKS PASSED" || { echo "SOME CHECKS FAILED"; exit 1; }
