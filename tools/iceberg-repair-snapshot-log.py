#!/usr/bin/env python3
"""Repair an Iceberg table on RustFS whose snapshot log runs backwards.

iceberg-rust (and Java) refuse to load table metadata in which a snapshot-log
entry is more than a minute older than the one before it ("Expected sorted
snapshot log entries"). Older iceberg-sync builds produced exactly that by
copying source snapshot timestamps into a table that also had local writers.

RustFS leaves one way out. It refuses to modify an existing snapshot (even
removed and re-added in one commit) and registers only metadata files under
its internal `.rustfs-table/` prefix, which refuses S3 writes, so neither
fixing the timestamps nor drop + register is possible. What it does accept
is `remove-snapshots`, which also trims the snapshot log to the entries after
the newest removed snapshot. So this removes every snapshot whose log entry
is older than the one before it, in one commit that asserts the table and
`main` as loaded:

  - the current snapshot and its manifest list are untouched, so every row
    stays readable; the removed snapshots' manifests are carried by every
    later manifest list (fast appends), and their data files remain;
  - only time travel to the removed snapshots, and the log entries before the
    last of them, are lost.

Stop every writer to the table first. Dry run unless --apply. Credentials
come from RUSTFS_ACCESS_KEY / RUSTFS_SECRET_KEY; requests are signed with
curl's --aws-sigv4.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path


class Rest:
    def __init__(self, endpoint, region, warehouse, ns, table):
        self.url = f"{endpoint.rstrip('/')}/iceberg/v1/{warehouse}/namespaces/{ns}/tables/{table}"
        self.user = f"{os.environ['RUSTFS_ACCESS_KEY']}:{os.environ['RUSTFS_SECRET_KEY']}"
        self.region = region

    def call(self, method, body=None):
        cmd = ["curl", "-sS", "-X", method, "--user", self.user,
               "--aws-sigv4", f"aws:amz:{self.region}:s3", "-w", "\n%{http_code}", self.url]
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            cmd += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        out = subprocess.run(cmd, input=data, capture_output=True, check=True).stdout
        payload, _, code = out.rpartition(b"\n")
        if int(code) != 200:
            raise RuntimeError(f"{method} {self.url} -> {int(code)}: {payload[:2000].decode(errors='replace')}")
        return json.loads(payload)


def out_of_order(md):
    """Snapshot ids whose log entry is not later than the entry before it."""
    log = md["snapshot-log"]
    return [log[i]["snapshot-id"] for i in range(1, len(log))
            if log[i]["timestamp-ms"] <= log[i - 1]["timestamp-ms"]]


def check_loadable(md):
    ts = [e["timestamp-ms"] for e in md["snapshot-log"]]
    assert all(b > a for a, b in zip(ts, ts[1:])), "snapshot log still unsorted"
    if ts:
        assert md["last-updated-ms"] >= ts[-1], "last-updated-ms before last snapshot"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--endpoint", default="http://127.0.0.1:9000")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--warehouse", default="maritimedata")
    ap.add_argument("--namespace", default="ais")
    ap.add_argument("--table", required=True)
    ap.add_argument("--backup-dir", default="iceberg-repair-backups")
    ap.add_argument("--apply", action="store_true", help="commit the removal")
    a = ap.parse_args()

    rest = Rest(a.endpoint, a.region, a.warehouse, a.namespace, a.table)
    loaded = rest.call("GET")
    md = loaded["metadata"]
    print(f"{a.namespace}.{a.table}: {loaded['metadata-location']}")
    print(f"  {len(md['snapshots'])} snapshots, {len(md['snapshot-log'])} log entries, "
          f"current {md.get('current-snapshot-id')}")
    bad = out_of_order(md)
    if not bad:
        print("  snapshot log is already sorted; nothing to do")
        return
    referenced = {r["snapshot-id"] for r in md.get("refs", {}).values()}
    if referenced & set(bad):
        sys.exit(f"  refusing: a ref points at {sorted(referenced & set(bad))}")
    snaps = {s["snapshot-id"]: s for s in md["snapshots"]}
    for sid in bad:
        s = snaps[sid]
        print(f"  remove {sid}: seq {s['sequence-number']}, {s['timestamp-ms']} "
              f"({time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(s['timestamp-ms'] / 1000))}), "
              f"{s.get('summary', {}).get('added-records', '?')} rows added")
    last = max(i for i, e in enumerate(md["snapshot-log"]) if e["snapshot-id"] in bad)
    print(f"  the log keeps its last {len(md['snapshot-log']) - last - 1} entries")
    if not a.apply:
        print("  dry run; pass --apply to commit the removal")
        return

    backup = Path(a.backup_dir) / f"{a.namespace}.{a.table}-{time.strftime('%Y%m%dT%H%M%S')}.json"
    backup.parent.mkdir(parents=True, exist_ok=True)
    backup.write_text(json.dumps(loaded))
    print(f"  backup: {backup}")

    current = md["current-snapshot-id"]
    rest.call("POST", {
        "requirements": [
            {"type": "assert-table-uuid", "uuid": md["table-uuid"]},
            {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": current},
        ],
        "updates": [{"action": "remove-snapshots", "snapshot-ids": bad}],
    })

    after = rest.call("GET")
    am = after["metadata"]
    check_loadable(am)
    assert am["table-uuid"] == md["table-uuid"], "table uuid changed"
    assert am["current-snapshot-id"] == current, "current snapshot changed"
    cur_after = {s["snapshot-id"]: s for s in am["snapshots"]}[current]
    assert cur_after["manifest-list"] == snaps[current]["manifest-list"], "current manifest list changed"
    assert len(am["snapshots"]) == len(md["snapshots"]) - len(bad), "unexpected snapshot count"
    print(f"  committed: {after['metadata-location']}")
    print(f"  {len(am['snapshots'])} snapshots, {len(am['snapshot-log'])} log entries; log sorted")


if __name__ == "__main__":
    main()
