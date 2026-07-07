#!/usr/bin/env python3
"""Monitor cgroup v2 CPU and memory for a Nomad exec task."""

import argparse, csv, glob, os, sys, time
from datetime import datetime, timezone

CGROUP_ROOT = "/sys/fs/cgroup"

def find_cgroup_path(task_name, alloc_hint=None):
    pattern = f"{CGROUP_ROOT}/nomad.slice/**/*.{task_name}.scope"
    matches = sorted(glob.glob(pattern, recursive=True))
    if not matches:
        return None
    if alloc_hint:
        for m in matches:
            if alloc_hint[:8] in m:
                return m
    return matches[-1]

def read_cpu(path):
    with open(os.path.join(path, "cpu.stat")) as f:
        for line in f:
            if line.startswith("usage_usec"):
                return int(line.split()[1])
    return 0

def read_mem(path):
    with open(os.path.join(path, "memory.current")) as f:
        return int(f.read().strip())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--task", default="collect-socket", help="Task name")
    p.add_argument("--interval", type=float, default=5.0, help="Sample interval (s)")
    p.add_argument("--duration", type=float, default=7200, help="Duration (s)")
    p.add_argument("--output", default="resource_usage.csv")
    args = p.parse_args()

    cg = find_cgroup_path(args.task)
    if not cg:
        print(f"Error: no cgroup found for task '{args.task}'", file=sys.stderr)
        sys.exit(1)
    print(f"Cgroup: {cg}", file=sys.stderr)
    print(f"Logging to {args.output} for {args.duration}s...", file=sys.stderr)

    samples = int(args.duration / args.interval)
    prev_cpu = None

    with open(args.output, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(["timestamp", "elapsed_s", "cpu_usec", "cpu_percent", "mem_bytes", "mem_mb"])
        for i in range(samples):
            elapsed = i * args.interval
            cpu = read_cpu(cg)
            mem = read_mem(cg)

            pct = 0.0
            if prev_cpu is not None:
                delta = cpu - prev_cpu
                pct = delta / (args.interval * 1_000_000) * 100
            prev_cpu = cpu

            w.writerow([datetime.now(timezone.utc).isoformat(),
                        f"{elapsed:.0f}", cpu, f"{pct:.2f}",
                        mem, f"{mem/1024/1024:.2f}"])
            f.flush()

            print(f"\r[{i+1}/{samples}] CPU: {pct:.1f}%  Mem: {mem/1024/1024:.1f} MB  ({elapsed:.0f}s/{args.duration:.0f}s)",
                  end="", file=sys.stderr, flush=True)

            if i < samples - 1:
                time.sleep(args.interval)

    print(file=sys.stderr)
    print(f"Done → {args.output}", file=sys.stderr)

if __name__ == "__main__":
    main()
