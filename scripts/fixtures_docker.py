#!/usr/bin/env python3
"""Run the image on every fixture inside one container, then diff on the host.
Usage: python scripts/fixtures_docker.py [IMAGE]"""
import os, shutil, subprocess, sys, tempfile
sys.path.insert(0, os.path.dirname(__file__))
from run_fixtures import SCHEMAS, load_expected, load_actual, rows

image = sys.argv[1] if len(sys.argv) > 1 else "shopping-sessions:latest"
fx = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "fixtures"))
out = tempfile.mkdtemp()
cases = sorted(d for d in os.listdir(fx) if os.path.isdir(os.path.join(fx, d)))
loop = "; ".join(f"mkdir -p /o/{c} && DATA_DIR=/fx/{c}/input OUTPUT_DIR=/o/{c} /app/sessions 2>/o/{c}.err || echo FAILRUN {c}" for c in cases)
p = subprocess.run(["docker", "run", "--rm", "--user", "0", "-v", f"{fx}:/fx:ro", "-v", f"{out}:/o", image, "sh", "-c", loop],
                   capture_output=True, text=True)
print(p.stdout, p.stderr[-3000:])
failed = []
for c in cases:
    ok = True
    for t, sch in SCHEMAS.items():
        exp = rows(load_expected(os.path.join(fx, c, "expected", f"{t}.csv"), sch))
        try:
            act = rows(load_actual(os.path.join(out, c, t), sch))
        except Exception as e:
            ok = False; print(f"FAIL {c} {t}: {e}"); print(open(os.path.join(out, c + '.err')).read()[-1500:]); continue
        if exp != act:
            ok = False; print(f"FAIL {c} {t}\n  expected {exp}\n  actual   {act}")
    print(("ok   " if ok else "FAIL ") + c) if not ok else None
    if not ok: failed.append(c)
print(f"{len(cases)-len(failed)}/{len(cases)} fixtures passed")
shutil.rmtree(out, ignore_errors=True)
sys.exit(1 if failed else 0)
