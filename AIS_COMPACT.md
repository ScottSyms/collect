# ais-compact — maintain Iceberg tables on RustFS

`ais-compact` (the `collect-maint` crate) keeps the Iceberg tables that the
collectors and parsers write healthy: it merges small files into sorted,
right-sized ones, drops old snapshots, and deletes objects nothing references
any more.

It exists because RustFS's built-in Iceberg catalog has no table
maintenance, while this project's ingest is append-only and produces a lot of
small files and snapshots (see [Why it exists](#why-it-exists)). It works with
any Iceberg REST catalog, not only RustFS's.

```
collect-* / *-parse (append)  →  ais-compact (rewrite, expire, clean)
   many small files                 few large sorted files
```

New to the project? [TUTORIAL.md](TUTORIAL.md) covers Iceberg output first;
this page is a reference for this one binary. Flags are also listed in
[CLI_REFERENCE.md](CLI_REFERENCE.md#ais-compact).

## Quick start

Point it at the same catalog and warehouse you write to, with the same S3
environment variables the other binaries use:

```bash
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=... S3_SECRET_KEY=... \
       S3_REGION=us-east-1 S3_PATH_STYLE=true

# 1. Look at what's there
ais-compact --iceberg-catalog-uri http://localhost:9000/iceberg \
  --iceberg-warehouse data --iceberg-sigv4 inspect

# 2. See what compaction would do (dry run: changes nothing)
ais-compact --iceberg-catalog-uri http://localhost:9000/iceberg \
  --iceberg-warehouse data --iceberg-sigv4 compact

# 3. Do it
ais-compact --iceberg-catalog-uri http://localhost:9000/iceberg \
  --iceberg-warehouse data --iceberg-sigv4 compact --apply
```

`--iceberg-*` flags come from the shared
[Iceberg flags](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries), so
`ICEBERG_CATALOG_URI`, `ICEBERG_WAREHOUSE` and friends work as environment
variables too. Use `--iceberg-table-prefix` and `--iceberg-namespace` if your
writers do.

**Every command that changes anything is a dry run unless you pass
`--apply`.**

## Why it exists

Ingest commits to Iceberg with append-only `fast_append`:

- the collectors register **one `raw` commit per uploaded bronze file**;
- live silver decoding makes **up to six commits per bronze batch**, one per
  table;
- the batch parsers write one file per table per partition.

So tables collect many small files and one snapshot per commit, and nothing
is sorted by anything but arrival order. Queries pay for it: too many files to
open, too many manifests to read, and no way to skip files when filtering on
a vessel (`mmsi`).

iceberg-rust 0.9.1 (the version this workspace uses) has no rewrite, overwrite
or delete action, and RustFS's catalog offers no maintenance service. So
`ais-compact` builds the rewrite itself: it writes the new manifests and
manifest list with the crate's public spec writers, then posts an
`add-snapshot` update straight to the REST catalog, through the same SigV4
proxy the other binaries use for RustFS.

## Commands

All commands accept `--table <name>` (repeatable, before or after the
subcommand): a table's base name without any prefix. The default is `raw
positions statics meteo binary atons other`. A table that can't be loaded is
skipped with a message.

### `inspect`

Read-only. Per table: data files, total size, row count, how many files are
under half the target size, how many partitions need compaction, snapshot
count, small manifests, and whether a sort order is set.

| Flag | Default | |
|---|---|---|
| `--target-file-mb` | `512` | Size the "under half" count is measured against |

### `compact`

Rewrites partitions into sorted, right-sized files.

| Flag | Default | |
|---|---|---|
| `--apply` | off | Actually rewrite and commit |
| `--min-age-hours` | `2` | Only touch partitions that ended at least this long ago |
| `--target-file-mb` | `512` | Size the output files roll at, and the threshold for "small" |
| `--max-partition-mb` | `1024` | Skip partitions whose compressed input is larger than this |
| `--sort-by a,b` | `mmsi,ts` | Sort columns, ascending, nulls last |
| `--consolidate-manifests <n>` | `20` | Merge small manifests once at least `n` exist; `0` turns it off |

Steps, per table:

1. **Register the sort order** on the table (`--apply` only) if it isn't
   already the default. Engines and later writers can see it in table
   metadata.
2. **Plan.** Group the live files by partition, then keep the partitions that
   are *closed* and *need work* (below).
3. **Rewrite each partition.** Read its files, sort all rows, and write new
   files with zstd compression, row groups of at most 128Ki rows, and bloom
   filters on `mmsi`, `imo_number`, `call_sign` and `name` (those the table
   has). Filters are sized per row group (32Ki distinct values; 1% false
   positives on `mmsi`, 5% elsewhere) rather than parquet's 1M-value default,
   which cost about 1 MB per column per row group. `station` and `source` get
   none: they are near-constant, so a filter only adds size.
4. **Commit** the swap as one Iceberg `replace` snapshot: old files out, new
   files in.
5. **Consolidate manifests** (optional): merge the many small data manifests
   into one per partition spec.

#### What counts as "closed"

A partition is closed once the end of its time range is at least
`--min-age-hours` in the past. The range comes from the table's partition
transform (`year`, `month`, `day` or `hour` on `ts`), so a `day` partition for
2026-09-27 is closed after 2026-09-28 02:00 UTC with the default. This keeps
compaction off partitions that ingest is still writing to.

#### What counts as "needs work"

A closed partition is rewritten if either:

- any of its files was **not** written by `ais-compact` (compacted files are
  named `compact-…`, so this means new ingest landed), or
- **two or more** of its files are smaller than half the target size.

A partition made only of compacted, full-size files is left alone, and a
single small tail file doesn't trigger another rewrite. That makes the
command **idempotent**: running it again does nothing until new data arrives.

#### Sorting

`--sort-by` defaults to `mmsi,ts`: files group a vessel's messages together
in time order, so a query for one vessel can skip most files and row groups
using their min/max statistics and the `mmsi` bloom filter. Columns the table
doesn't have are dropped from the default, so `raw` (which has no `mmsi`)
sorts by `ts`. An explicit `--sort-by` naming a missing column is an error.

For map-region queries you may prefer a spatial key, e.g.
`--sort-by hilbert,ts` on `positions`. Pick one order per table and keep it:
changing it later only affects partitions that are rewritten afterwards.

### `expire`

Drops old snapshots from the table metadata.

| Flag | Default | |
|---|---|---|
| `--apply` | off | Actually expire |
| `--older-than-days` | `7` | Expire snapshots older than this |
| `--retain-last` | `5` | Always keep this many of the newest snapshots |

The current snapshot and anything a branch or tag points at are never
expired. This changes metadata only; the files those snapshots referenced stay
in storage until you run `orphans`.

Expire matters here because one-commit-per-file ingest creates snapshots
quickly, and every snapshot keeps its manifests and replaced data files alive.

### `orphans`

Deletes objects under the table's location that no snapshot references.

| Flag | Default | |
|---|---|---|
| `--apply` | off | Actually delete |
| `--older-than-days` | `3` | Only delete unreferenced objects older than this |
| `--s3-endpoint`, `--s3-region`, `--s3-access-key`, `--s3-secret-key`, `--s3-disable-tls` | | S3 connection, as for the batch parsers |

An object counts as referenced if it is the current or a logged metadata
file, a manifest list, a manifest, or a data file (deleted entries included)
reachable from any snapshot. If no referenced objects can be found at all, it
refuses to list anything.

The age limit is what makes this safe alongside live ingest: writers upload a
file first and commit it afterwards, so a young unreferenced object may be an
in-flight write. Keep `--older-than-days` larger than the longest gap between
a writer uploading a file and committing it.

Run it after `expire` and `compact`: those free the references, `orphans`
frees the space.

### `completions`

`ais-compact completions <shell>` prints shell completions to stdout, as the
other binaries' `--completions` does.

## Running it routinely

A reasonable schedule is `compact --apply` shortly after each partition
closes, then `expire --apply` and `orphans --apply` daily. For example, from
cron or a Nomad periodic job:

```bash
ais-compact ... compact --apply --min-age-hours 2
ais-compact ... expire  --apply --older-than-days 7 --retain-last 5
ais-compact ... orphans --apply --older-than-days 3
```

Each run is independent and safe to repeat. Runs overlapping with ingest are
fine; see [Concurrent ingest](#concurrent-ingest).

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Did something, or ran `inspect` |
| `1` | Unclassified error (bad flags, cannot reach the catalog) |
| `2` | Nothing to do: every table was already in order |
| `5` | At least one table or partition failed; the rest were still processed |

Code `2` is normal for a scheduled run when nothing new has arrived.

## Safety

### Concurrent ingest

Ingest keeps appending while `ais-compact` works, so every commit is pinned to
the snapshot it was planned against. If ingest commits first, the catalog
answers `409 Conflict`; `ais-compact` deletes the files it just wrote,
reloads the table, re-plans that partition, and tries again, up to four
attempts. If a partition still can't commit it is reported as failed and the
run continues with the next one. Data appended while a partition is being
rewritten is never lost: the replace only removes the exact files it read.

A commit also fails, rather than corrupting anything, if any file it meant to
replace is no longer live (someone else already rewrote it).

### Dry run

Without `--apply`, nothing is written or deleted. Dry-run `compact` reports
the partitions it would rewrite and how many small manifests it would merge.

### What can't go wrong silently

- Row counts and rows are preserved; a rewrite reads exactly the files it
  replaces.
- The new files carry the partition value of the files they replace, not a
  value derived from the data, so a partition can't be mislabelled.
- Failed or conflicting rewrites clean up the files they wrote.

## Limits

- **Memory.** A partition is sorted in memory. `--max-partition-mb` bounds its
  *compressed* input; expect several times that in RAM. Larger partitions are
  skipped with a message (not failed), so lower the partition granularity
  (`--partition hour`) for very busy tables, or raise the flag on a big host.
- **Iceberg v2 only**, and tables **without delete files**. This project never
  writes deletes; a table that has them is refused.
- **Files don't record their sort order.** The table's default sort order is
  registered, but individual data files aren't tagged with it, so engines
  can't rely on it per file. Query speedups come from min/max statistics and
  bloom filters, which are written.
- **Partition spec changes.** Planning assumes the table's default partition
  spec covers its files.
- **Tables partitioned by year before the fix.** Older `--partition year`
  tables were written with the wrong partition value (the calendar year, not
  years since 1970). `ais-compact` reads those partitions as far in the
  future and never treats them as closed, so recreate such tables.

## RustFS notes

- The catalog must be SigV4-signed: pass `--iceberg-sigv4` and the S3
  environment variables, as for the other binaries.
- The warehouse bucket must be enabled as a table bucket, as for writing.
- RustFS validates a snapshot's operation against what it changes. A
  `replace` must delete data files, so compaction commits are `replace`; a
  manifest-only merge changes no data files and is published as an `append`
  that adds and deletes none.

## How the pieces fit

| Module | Role |
|---|---|
| `plan.rs` | Partition grouping, closed/needs-work rules, sort columns, partition time ranges. Pure functions with unit tests |
| `rewrite.rs` | List live files, read named files, sort, write right-sized files (no commit) |
| `commit.rs` | Hand-built `replace` snapshot, manifest consolidation, snapshot expiry, and the small REST client that posts commits |
| `orphans.rs` | Work out which objects are referenced, list and filter the rest |
| `compact.rs` | The per-table driver: sort-order registration, retries, reporting |
| `main.rs` | CLI |

## Testing

Unit tests run with `cargo test -p collect-maint`. The integration tests spawn
a real local RustFS and are `#[ignore]`d by default; run them with `rustfs`
on `PATH`:

```bash
cargo test -p collect-maint -- --ignored
```

They skip cleanly when `rustfs` isn't installed. `rustfs_replace.rs` checks that
the catalog accepts a hand-built replace commit and the table still reads;
`cli.rs` drives the real binary through dry-run, compact, an idempotent
re-run, expire and orphans, reading the table back afterwards.
