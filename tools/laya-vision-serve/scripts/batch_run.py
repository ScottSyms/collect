"""Run a folder of images through a running laya-vision-serve with a vessel-imagery question set.

    .venv/bin/python scripts/batch_run.py IMAGE_DIR \
        --questions ../../crates/vessel-imagery/config/questions/v1.toml \
        [--labels labels.csv] [--out results.jsonl]

labels.csv: image,question,expected, where image is the file stem ("bulk" for bulk.jpg).
Prints each image's gate result and top answers, then accuracy per question and overall
against the labels, plus what an always-pick-the-most-common-answer baseline would score.
"""

import argparse
import base64
import csv
import json
import os
import sys
import time
import tomllib
from collections import Counter, defaultdict
from pathlib import Path

import httpx

MEDIA = {".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png"}


def load_questions(path):
    cfg = tomllib.loads(Path(path).read_text())
    wire = [{"id": q["id"], "type": q["type"], "text": q["text"], "criteria": q.get("criteria", [])}
            for q in cfg["questions"]]
    gates = {q["id"]: (q["pass"], cfg.get("min_probability", 0.5))
             for q in cfg["questions"] if q.get("role") == "gate"}
    return wire, gates


def top_label(a):
    """choice, or the most probable level for a score answer."""
    return a["choice"] or max(a["probabilities"], key=a["probabilities"].get)


def gate_passes(answers, gates):
    for qid, (passing, min_p) in gates.items():
        a = answers[qid]
        if a["choice"] not in passing or a["probabilities"][a["choice"]] < min_p:
            return False
    return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("images")
    ap.add_argument("--questions", required=True)
    ap.add_argument("--labels")
    ap.add_argument("--out")
    ap.add_argument("--url", default=os.environ.get("LAYA_VISION_URL", "http://127.0.0.1:8765"))
    args = ap.parse_args()

    questions, gates = load_questions(args.questions)
    files = sorted(p for p in Path(args.images).iterdir() if p.suffix.lower() in MEDIA)
    results = {}
    out = open(args.out, "w") if args.out else None
    for f in files:
        req = {"model": "laya-vision", "state": "asset:img",
               "assets": {"img": {"media_type": MEDIA[f.suffix.lower()],
                                  "data": base64.b64encode(f.read_bytes()).decode()}},
               "questions": questions}
        t0 = time.perf_counter()
        r = httpx.post(f"{args.url.rstrip('/')}/v1/systemone", json=req, timeout=600)
        dt = time.perf_counter() - t0
        if r.status_code != 200:
            print(f"{f.stem}: HTTP {r.status_code} {r.text[:200]}")
            continue
        answers = r.json()["answers"]
        results[f.stem] = answers
        if out:
            out.write(json.dumps({"image": f.stem, "seconds": round(dt, 3), "answers": answers}) + "\n")
        top = lambda q: (f"{answers[q]['choice']} {answers[q]['probabilities'][answers[q]['choice']]:.2f}"
                         if answers[q]["choice"] else f"value {answers[q]['value']:.2f}")
        print(f"{f.stem:12s} {dt:5.2f}s gate={'PASS' if gate_passes(answers, gates) else 'fail'} | "
              f"kind={top('photo_kind')} | single={top('single_ship')} | type={top('ship_type')} | "
              f"hull={top('hull_colour')}")
    if out:
        out.close()

    if not args.labels:
        return
    per_q = defaultdict(lambda: [0, 0])
    picks = defaultdict(Counter)  # what the model chose per question, across all images
    for answers in results.values():
        for qid, a in answers.items():
            picks[qid][top_label(a)] += 1
    rows = list(csv.DictReader(open(args.labels)))
    misses = []
    for row in rows:
        a = results.get(row["image"], {}).get(row["question"])
        if a is None:
            continue
        got = top_label(a)
        ok = got == row["expected"]
        per_q[row["question"]][0] += ok
        per_q[row["question"]][1] += 1
        if not ok:
            p = a["probabilities"].get(row["expected"], 0.0)
            misses.append(f"  {row['image']:11s} {row['question']:24s} expected {row['expected']!r} "
                          f"(p={p:.2f}), got {got!r} ({a['probabilities'][got]:.2f})")
    print("\naccuracy per question (correct/labelled) | model's most frequent pick across all images")
    for qid, (c, n) in sorted(per_q.items()):
        common, k = picks[qid].most_common(1)[0]
        print(f"  {qid:24s} {c}/{n}  | {common!r} x{k}/{len(results)}")
    c = sum(v[0] for v in per_q.values())
    n = sum(v[1] for v in per_q.values())
    print(f"overall {c}/{n} = {c / n:.0%}")
    print("\nmisses:")
    print("\n".join(misses))


if __name__ == "__main__":
    sys.exit(main())
