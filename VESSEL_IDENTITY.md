# vessel-identity

The **maritime vessel identity registry**. It collects publicly available
vessel identity data (national registers, classification societies, port
state systems) into Apache Iceberg, keeps every claim each source makes with
its provenance and history, and reconciles the claims into one record per
**physical vessel** with a permanent internal id.

By default it writes to namespace **`identity`** in warehouse
**`maritimedata`**. Every command that writes is a dry run unless `--apply`.

It runs independently of the AIS pipeline: nothing here reads positions,
and the AIS tables can join against it (see [Querying](#querying)).

## Contents

- [Principles](#principles)
- [Quick start](#quick-start)
- [Sources](#sources) · [Adding a source](#adding-a-source)
- [Pipeline](#pipeline) · [Ingest](#ingest) · [Reconcile](#reconcile) · [Manual decisions](#manual-decisions)
- [Tables](#tables)
- [Querying](#querying)
- [Operations](#operations)
- [Where this departs from the specification](#where-this-departs-from-the-specification)
- [Not built yet](#not-built-yet)

## Principles

- **A vessel is a hull, not an identifier.** Each vessel gets a random UUID.
  IMO numbers, MMSIs, call signs, names and registrations are identifiers
  the vessel has carried, kept with their validity and history.
- **Keep what the sources say.** Every claim is an *assertion* with its
  source, its record, and two kinds of time. Disagreements are kept and
  reported, never overwritten.
- **Precision over recall.** Records are joined only on independently
  supported, persistent identifiers. A wrongly merged pair of ships is worse
  than a duplicate, so anything weaker becomes a proposal for review.
- **Reversible.** Merges retire ids with a redirect; undoing a merge gives
  the old id back.
- **Collect only what is authorized.** Each source's licence and approval
  are in its configuration and are checked before every run.

## Quick start

Against the local RustFS (credentials from `.rustfs.env`, as for
`run_foundation_rustfs.sh`):

```bash
./run_identity_rustfs.sh init --apply
```

```bash
./run_identity_rustfs.sh ingest --source tc_large_vessels,tc_small_vessels --apply --reconcile
```

```bash
./run_identity_rustfs.sh lookup --name segwun
```

```bash
./run_identity_rustfs.sh status
```

For an unattended deployment, run `ingest --due --apply --reconcile` from
cron, Nomad or a container on a schedule (hourly is fine): it collects each
source only when its refresh policy says it is due.

## Sources

| Source id | Publisher / dataset | Access | Records | As shipped |
|---|---|---|---|---|
| `tc_large_vessels` | Transport Canada, Canadian Register of Large Vessels | XLSX download, daily | ~26,800 | **approved** (Open Government Licence – Canada) |
| `tc_small_vessels` | Transport Canada, Small Commercial Vessel Registry | XLSX download, weekly | ~19,200 | **approved** (OGL – Canada) |
| `iacs_vessels_in_class` | IACS, Vessels in Class (Equasis weekly CSV) | page → newest `EquasisToIACS_<date>_<n>.zip`, weekly | ~87,500 rows | **approved** for internal use (IACS website terms; no redistribution or commercial use) |
| `uscg_psix` | USCG PSIX web service (`getVesselSummaryXMLString`) | SOAP, sliced by build year, weekly | not measured | **pending**: terms for automated use not confirmed |
| `gfw_vessel_identity` | Global Fishing Watch Vessels API (`public-global-vessel-identity`) | API with a token, batched lookups of known IMOs/MMSIs, weekly | not measured | **approved** for noncommercial use (CC BY-NC 4.0 + GFW API terms; attribution required) |
| `uscg_misle_deficiencies` | USCG MISLE Deficiency Data Report (yearly workbooks, 2018 on) | saved by hand, `--source-dir` | ~27,300 identities of ~25,700 vessels | **approved** (US federal government work, public domain) |

A source is collected only when its configuration has `enabled = true`,
`access_status = "approved"`, and `permitted_uses` includes `access`,
`automated_download`, `retain_history` and `combine`. For files saved by
hand (`--source-dir`), `automated_download` isn't needed. A refused run is
still logged in `ingestion_runs` with the reason. Approving PSIX
means editing its TOML, or using `--config-dir` with your own copies.

Notes on each source:

- **Transport Canada.** One row per registered vessel, keyed by official
  number. That number is the national registration, issued in `CA`, so it
  is authoritative and makes the vessel `confirmed`. The files have no call
  signs or MMSIs, and the small register has no names. TC regenerates both
  files daily, so their HTTP date is recorded as the *version*, not the
  publication date. The newest registration date in the file is reported as
  a hint at the data's real age. A few official numbers appear on two rows
  with different particulars; each row is kept as its own record
  (`<number>#2`) of the one subject.
- **IACS.** One row per vessel per class-society record. The name carries
  the date the society reported it (`TRANQUILLITY(25/07/24)`); the suffix
  goes to the assertion's qualifier and the record's publication date.
  Records are keyed by IMO, society and that date.
- **Global Fishing Watch.** The Vessels API search, asked about vessels
  the registry already knows. GFW has no bulk export, and its search pages
  can't be followed reliably (the `since` cursor repeats and a repeated
  request comes back cached, so paging stalls after about two pages). So the
  collector takes every check-digit-valid IMO number and every MMSI in
  `vessel_identifiers` that another source supports (63,093 on
  2026-10-18), ORs them together `batch_size` (20) at a time, and sends each
  batch as one request for one page of up to 50 vessels. A batch whose
  matches overflow the page is split in half until they fit. GFW's own
  earlier answers are not looked up again, so the set doesn't grow by
  itself. `max_batches` limits a run, and `queries` adds raw `where`
  clauses (one page each). A full pass is about 3,200 requests. Each
  finished batch is kept in `--scratch` for `batch_max_age_hours`, so an
  interrupted run resumes. GFW's limits (50,000 requests a
  day and 1,500,000 a month per user, across all tokens) are enforced
  across runs: every request, retries included, is counted in a ledger at
  `~/.local/state/vessel-identity/gfw_vessel_identity_usage.json` (or
  `[options] usage_file`), raised to GFW's own count when its
  `x-ratelimit-*` headers report more, and nothing is sent once
  `daily_request_cap` (45,000) or `monthly_request_cap` (1,350,000) is
  reached, or for 24 hours (30 days at the monthly cap) after a 429. The
  caps can't be set above GFW's limits. `max_requests` (40,000) also caps a
  single run. A run that stops early is incomplete, and the next run
  resumes from the finished batches. Set the token in `GFW_API_TOKEN`;
  `run_identity_rustfs.sh` reads it from `.rustfs.env`. A result is one
  vessel as GFW resolved it, the subject
  being its registry vessel id (`registry:<id>`) or, with no registry
  records, its AIS vessel id (`ais:<id>`). Registry records assert IMO,
  call sign, name, flag, MMSI, gear type, length, gross tonnage, depth and
  build year, qualified by their registry source codes. AIS identities
  assert MMSI, call sign, name and flag, qualified `AIS self-reported`; the
  IMO a ship broadcasts asserts nothing. Both carry the period GFW saw them
  transmitted as `valid_from`/`valid_to`, with the newest open-ended, so
  AIS positions join to the vessel that used an MMSI at the time. Owners
  and authorizations are kept in the record but not yet mapped. Licence:
  CC BY-NC 4.0 and GFW's API Terms of Use (noncommercial only, "Powered by
  Global Fishing Watch" attribution). The terms forbid "derivative products
  from the API" and say nothing about local databases; the source was
  approved by the operator on 2026-10-18 for noncommercial use, including
  keeping history and combining.
- **USCG MISLE deficiencies.** The workbooks `collect-foundation evidence
  uscg` loads as inspection evidence, read here for who each inspected
  vessel was. The Coast Guard's site refuses non-browser downloads, so save
  the workbooks in one directory and run
  `ingest --source uscg_misle_deficiencies --source-dir <dir>`; the refresh
  policy is `manual`. Each row names the vessel by its primary VIN, which is
  the subject (`IMO:9194957`, `USON:1091257`, `STATE:FL6076PU`, ...). One
  record is kept per subject per name and flag it was seen under, with
  `First Observed`, `Last Observed` and the number of activities, so a
  renamed or reflagged ship has one record per identity and the newest
  observation is its publication date. These are observations, so
  assertions carry no validity dates; the observation window is the
  qualifier. VIN types map to `imo` (IMO number), `national_registry` in
  `US` (US official numbers, which the Coast Guard issues, so the vessel is
  `confirmed`), `national_registry` in the flag's jurisdiction (foreign
  official numbers, which link to that country's register but confirm
  nothing), `mmsi`, and `hull_number` (HIN, hull number). From 2025 the
  workbooks carry MISLE's vessel id, recorded as `psix_vessel_id` (PSIX
  uses the same numbers). State, Coast Guard, ABS and EPIRB numbers only
  key the subject. Removal is `incremental`: a vessel absent from newer
  workbooks just wasn't inspected.
- **USCG PSIX.** There is no bulk export, so the collector asks for one
  build year at a time (1850 to next year, plus "unknown"). It waits 3 s
  between requests and backs off on 503s, which the service returns when
  pressed. Each slice is kept in `--scratch`, so an interrupted fetch
  resumes. A run with a failed slice is *incomplete* and removes nothing.
  `Identification` is the IMO number for foreign vessels and the
  documentation number for US ones; it is typed accordingly.
  `Recreational` is excluded by default (`exclude_service_types`). The
  parser is verified on real responses; a full sliced fetch has not been
  run.

Other sources from the specification (WCPFC, NAFO, Paris MoU, FAO, ITU
MARS) are not built. ITU's restriction of bulk downloads to administrations
needs a decision before a collector is written.

### Adding a source

1. A TOML file in `crates/vessel-identity/config/sources/` (or your
   `--config-dir`), with licence, permitted uses, approval, refresh policy,
   priority, trust, the identifiers it issues, and removal semantics. Keys
   never go in the file: `authentication = "env:<VAR>"` names where one is.
2. An adapter in `crates/vessel-identity/src/sources/`, with `fetch` (bytes
   and version), `parse` (records with the source's own fields and ids) and
   `map` (fields → assertions, using the shared normalizers).
3. One line in `Adapter::from_name`.

The framework, tables and reconciliation don't change.

## Pipeline

```text
source TOML ──► fetch ──► hash ──► parse ──► diff vs bronze ──► source_records (append)
                  │                                      │
                  └──► raw archive (S3)                  └──► derive ──► identity_assertions
                                                                         (source partition)
identity_assertions + vessel_subjects + redirects + decisions ──► reconcile ──► vessels, vessel_*,
                                                                                conflicts, review queue
```

### Ingest

`vessel-identity ingest --source <id>[,<id>] | --all | --due [--apply]`

1. Check the source may be collected (above).
2. Fetch, hashing every object. If the content hash equals the last
   successful run's, stop (`unchanged`). `--force` overrides this.
3. Parse, and refuse a changed layout (missing columns, an unknown page).
   A layout failure fails the run; nothing is written.
4. Compare each record with its latest stored version: **NEW**,
   **MODIFIED**, unchanged (writes nothing), or **REMOVED_FROM_SOURCE**.
   Removal is recorded only for complete fetches of snapshot sources.
   Removal never means scrapped or deregistered.
5. If more than `max_removal_fraction` (default 20%) of the source would
   disappear at once, **quarantine**: nothing is written, the run is logged,
   and `--accept-removals` applies it when the removals are real.
6. Archive the objects (when `retain_raw`) under
   `s3://<bucket>/raw_archive/<namespace>/<source>/<date>/`. This is outside
   every table's prefix, so orphan cleanup never takes them. Override with
   `--raw-archive`.
7. Append the changed records to `source_records`.
8. Rebuild the source's partition of `identity_assertions` from its whole
   bronze history.
9. Log the run in `ingestion_runs` and refresh `source_catalog`.

**Idempotent and restartable.** Assertions are a pure function of bronze,
and a run is marked succeeded only after both commits. A run interrupted
between them is completed by running it again: bronze already holds the
records, so nothing is added and the assertions are rebuilt. One source
failing doesn't stop the others; the exit code is non-zero if any failed or
was quarantined.

**Assertions are bitemporal.** `valid_from`/`valid_to` say when the source
claims something held in the world; they are null when the source doesn't
say, and never invented. `recorded_at`/`superseded_at` say when the
registry learned it and when a later version of the same record replaced
it. A repeated claim keeps the time and provenance of the version that
first made it. A removed record's claims are `withdrawn`.

### Reconcile

`vessel-identity reconcile [--apply]` (or `ingest ... --reconcile`)

Reads every assertion from sources configured for combination, plus the
previous subject map, redirects, decisions, conflicts and proposals. It
rebuilds the gold tables and keeps ids stable. Rules:

- **Subjects** are what each source describes: a TC official number, an IACS
  IMO number, a PSIX vessel id.
- **Links** only on the latest state of each record:
  1. a check-digit-valid IMO number from a `registry`-trust source;
  2. a national registration within its jurisdiction (`CA:834567`);
  3. a manual `LINK`.
- **Refused links.** A link that would put two different valid IMO numbers
  on one vessel is refused. A `BLOCKED_MERGE` conflict is recorded on both
  sides, and both vessels become `disputed`. A link across a `KEEP_APART`
  decision is refused too. Names, call signs and MMSIs never link anything.
- **Ids.** A cluster keeps the id its subjects had. When it holds several,
  the one with the most subjects survives and the others are **retired**
  with a redirect (`MERGE`). When a vessel comes apart, the piece holding
  the subjects a retired id once had gets that id back (`RESTORE`); a piece
  with none gets a new id (`SPLIT`). The side with more persistent
  identifiers keeps the original. All three are logged in
  `identity_decisions`.
- **Preferred values** come from active claims first, then the
  higher-`priority` source, then the newer publication. Each one is
  traceable in `vessel_attributes`, with the alternatives the sources offer.
- **identity_status**:
  - `confirmed`: a source that issues national registrations reports one
    in its own jurisdiction, or two registry sources agree on its IMO;
  - `probable`: a valid IMO from one source;
  - `provisional`: neither;
  - `disputed`: a significant conflict.
- **Conflicts** (kept forever; `cleared` when the sources stop disagreeing):
  - `IMO`, `BLOCKED_MERGE`: significant;
  - `FLAG`, `YEAR_BUILT` (more than a year apart): moderate;
  - `NAME`, `DIMENSIONS` (length more than 10% apart): informational.
- **Proposals** (`identity_review_queue`): pairs of vessels sharing a call
  sign, an MMSI, or a name and build year, unless they have different
  IMOs. Buckets of more than 5 vessels are skipped as too ambiguous. A pair
  matching on two keys is `probable`, on one `possible`. Proposals never
  merge anything.

### Manual decisions

```bash
vessel-identity decide keep-apart tc_large_vessels/834567 iacs_vessels_in_class/9321483 --reason "different hulls" --apply
```

```bash
vessel-identity decide link tc_large_vessels/834567 uscg_psix/275677 --reason "same hull per survey" --apply
```

```bash
vessel-identity decide revoke <decision_id> --reason "entered in error" --apply
```

Subjects are `<source_id>/<subject_id>`, as in `vessel_subjects`. Decisions
are appended to `identity_decisions` with who made them and why, and take
effect at the next reconcile. Nothing an agent or a job does can delete an
identity.

## Tables

All in one namespace. The specification's bronze, silver and gold layers
are logical layers here, without prefixes on the table names.

| Layer | Table | Written | Partitioning | Holds |
|---|---|---|---|---|
| control | `source_catalog` | replaced by ingest | none | each source's configuration, licence, approval, last fetch/ingest/publication/hash |
| control | `ingestion_runs` | appended | none | one row per run: status, version, content hash, archived URIs, counts, snapshot ids, warnings |
| bronze | `source_records` | appended | `source_id`, month(`ingested_at`) | one row per record version: fields as published (`raw_content`, JSON), hash, change type, validation findings |
| bronze | `rejected_records` | appended | `source_id` | rows that couldn't be read, with reasons |
| silver | `identity_assertions` | source partition rebuilt | `source_id` | every claim: attribute, original and normalized value, qualifier, check status, valid and record times, status, provenance |
| resolution | `vessel_subjects` | replaced | none | source subject → vessel id, and what linked it |
| resolution | `vessel_id_redirects` | replaced, never shrinks | none | retired id → surviving id, the retired id's subjects, active/restored |
| resolution | `identity_decisions` | appended | none | manual KEEP_APART/LINK/REVOKE and automated MERGE/SPLIT/RESTORE |
| resolution | `identity_conflicts` | replaced, never shrinks | none | conflicting values with their assertions and sources, severity, status |
| resolution | `identity_review_queue` | replaced | none | proposed matches with evidence |
| gold | `vessels` | replaced | none | one row per physical vessel: preferred name, IMO, MMSI, call sign, flag (ISO alpha-3), type, build year, dimensions, tonnages, class, operational and identity status, sources |
| gold | `vessel_attributes` | replaced | none | which assertion each preferred value came from, and the alternatives |
| gold | `vessel_identifiers` | replaced | none | every identifier a vessel carried: type, value, authority, jurisdiction, validity, verified/reported/disputed, current or former |
| gold | `vessel_history` | replaced | none | every name, flag, port, type, registry and class status a vessel had |
| control | `reconciliation_runs` | appended | none | counts and the `vessels` snapshot each reconcile produced |

The exact columns are defined in `crates/vessel-identity/src/tables.rs`.
Each table's list drives both its Iceberg schema and its Arrow batches.
Bloom filters are on the lookup columns (`vessel_id`, `imo_number`,
`identifier_value`, `subject_id`, ...). The gold tables are small (tens of
thousands of rows today) and stay unpartitioned until query patterns call
for buckets.

Attributes in `identity_assertions.attribute` include `imo`, `mmsi`,
`callsign`, `national_registry`, `psix_vessel_id`, `hull_number`, `name`,
`flag_state`, `port_of_registry`, `registration_date`, `registry_status`,
`vessel_type`, `vessel_subtype`, `year_built`, `length_m`, `beam_m`,
`depth_m`, `gross_tonnage`, `net_tonnage`, `class_society`, `class_status`,
`class_status_reason`, `last_survey_date` and `next_survey_due` (full list:
`sources::attr`). `check_status` records validation: `valid`,
`bad_check_digit` (IMO), `unverifiable` (an 8-digit IMO, which the scheme
allows for but has no published check rule), `invalid` or `unrecognised`.

## Querying

Find a vessel: `vessel-identity lookup --imo 9321483` (or `--mmsi`,
`--callsign`, `--registration`, `--name`, or `--id` with a retired id).

Join AIS positions to the registry by MMSI at the time of the observation.
This uses the identifier's validity, so an MMSI reassigned later doesn't
re-label older positions:

```sql
SELECT p.mmsi, p.ts, v.vessel_id, v.preferred_name, v.vessel_type, v.flag_state, i.identity_status
FROM ais.positions p
JOIN identity.vessel_identifiers i
  ON i.identifier_type = 'MMSI' AND i.identifier_value = lpad(CAST(p.mmsi AS VARCHAR), 9, '0')
 AND (i.valid_from IS NULL OR p.ts >= i.valid_from)
 AND (i.valid_to IS NULL OR p.ts < i.valid_to)
JOIN identity.vessels v ON v.vessel_id = i.vessel_id;
```

Of today's sources only the MISLE workbooks carry MMSIs, for a few dozen
vessels, so that join stays nearly empty until GFW (approved, not yet
collected) or PSIX particulars are collected. Several vessels can
share an identifier; aggregate or pick before joining if one row per
position is required.

The evidence behind a vessel:

```sql
SELECT a.source_id, a.attribute, a.value_original, a.valid_from, a.recorded_at, a.assertion_status
FROM identity.vessel_subjects s
JOIN identity.identity_assertions a ON a.source_id = s.source_id AND a.subject_id = s.subject_id
WHERE s.vessel_id = '<uuid>'
ORDER BY a.attribute, a.recorded_at;
```

## Operations

- **Scheduling:** `ingest --due --apply --reconcile` on any schedule.
  Sources are checked against their `refresh_policy`: daily means 20 h,
  weekly 6 days, monthly 27 days, manual never.
- **One logical writer per table.** Sources write separate partitions, and
  reconcile is the only writer of the gold tables. Run one reconcile at a
  time. Commits retry on conflict.
- **Freshness:** `status`, or the `source_catalog` table.
- **Maintenance:** `ais-compact` works on these tables like any other.
  History lives in `source_records` and `identity_assertions` rows, not in
  Iceberg snapshots, so expiring snapshots loses no identity history.
- **Memory:** reconcile holds every assertion in memory. For the two TC
  registers (0.78 million assertions, 46,000 vessels) it peaks at about
  1 GB and takes 1.5 s. IACS adds about 0.53 million assertions, and PSIX
  more, so plan for 2 to 4 GB; streaming the read is a later step.

## Where this departs from the specification

- **Namespace:** `identity` (as asked), not `vessel_identity`.
- **Table names:** no `bronze_`/`silver_`/`gold_` prefixes, matching the
  other derived tables in this workspace.
- **Resolution lives outside the assertions.** `identity_assertions` has no
  `vessel_id`; the subject → vessel mapping is `vessel_subjects`. Merges and
  splits then never rewrite evidence, and undoing one is just a new mapping.
- **`gold_identity_resolution`** is `vessel_identifiers` (with validity),
  joined as in [Querying](#querying).
- **Reconciliation is plain Rust**, not DataFusion. It is a graph problem
  over well under a million subjects. DataFusion stays available for
  queries over the tables.
- **Mapping rules are code** (one `map` function per adapter) rather than
  configuration. Each one is short and tested, and a TOML mapping language
  would not save much.

## Not built yet

These are in the specification but outside this first version:

- The REST API and the MCP tools. `lookup` covers manual checks.
- AIS enrichment and discrepancy codes.
- The change feed.
- Probabilistic matching beyond the review-queue proposals.
- Ownership and relationship tables.
- More sources: WCPFC, NAFO, Paris MoU, and other national registers.
