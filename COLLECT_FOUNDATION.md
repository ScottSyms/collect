# collect-foundation

Loads **foundation data** into Apache Iceberg. This is reference data that
the AIS tables join against, and it changes slowly compared with the feeds.
There are two families:

- **Placenames.** The [GeoNames](https://www.geonames.org) gazetteer: about
  13 million named places worldwide (populated places, water bodies, coastal
  and undersea features, terrain, administrative areas), with lookup tables
  for its codes.
- **Vessel evidence.** Port state control inspections and detentions, and
  marine occurrences, from four publishers, normalized into two shared
  tables keyed by vessel identifiers (IMO, MMSI, call sign).

Column-by-column schemas are in [SCHEMAS.md](SCHEMAS.md#4-foundation-collect-foundation).
Every flag is in [CLI_REFERENCE.md](CLI_REFERENCE.md#collect-foundation).

## Contents

- [Configuration](#configuration)
- [Placenames](#placenames)
- [Vessel evidence](#vessel-evidence)
  - [Data model](#data-model) · [Sources](#sources) · [Running each source](#running-each-source) · [Querying](#querying-evidence) · [Caveats](#caveats)
- [Load semantics](#load-semantics)
- [Licences and attribution](#licences-and-attribution)

## Configuration

All tables go to one Iceberg namespace in one warehouse. By default that is
namespace **`foundation`** in warehouse **`maritimedata`**:

| Setting | Flag | Env | Default |
|---|---|---|---|
| Warehouse | `--iceberg-warehouse` | `ICEBERG_WAREHOUSE` | `maritimedata` |
| Namespace | `--iceberg-namespace` | `ICEBERG_NAMESPACE` | `foundation` |
| Table prefix | `--iceberg-table-prefix` | `ICEBERG_TABLE_PREFIX` | none |

With RustFS's built-in catalog, the warehouse is the bucket name. Every
other Iceberg flag is shared with the rest of the workspace:
`--iceberg-catalog-uri`, `--iceberg-token`, `--iceberg-sigv4`, and the
`S3_*` environment variables
([CLI_REFERENCE.md](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries)).
The Iceberg flags come **before** the subcommand.

Every command is a **dry run unless `--apply` is given**. A dry run
downloads and parses the data and prints what it found. It needs no
catalog, so you can try a source before you have one. With `--apply`, the
catalog connection is opened first, so a bad catalog fails before anything
is downloaded.

```bash
# RustFS on localhost, used by every --apply example below.
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=rustfsadmin S3_SECRET_KEY=rustfsadmin \
       S3_REGION=us-east-1 S3_PATH_STYLE=true
CAT="--iceberg-catalog-uri http://localhost:9000/iceberg --iceberg-sigv4"
```

Downloads go to `--scratch`, by default `$TMPDIR/collect-foundation`. The
directory is removed after a run unless you pass `--keep-scratch`. A kept
scratch directory has the same layout `--source-dir` expects, so it can be
reloaded later without downloading again.

## Placenames

```bash
# Dry run for one country.
collect-foundation placenames load --country NO

# Water and undersea features worldwide, with alternate names.
collect-foundation $CAT placenames load --feature-class H,U --alternate-names --apply

# Everything, from a local copy of the dump directory.
collect-foundation $CAT placenames load --source-dir ./geonames-dump --alternate-names --apply
```

| Flag | Default | Notes |
|---|---|---|
| `--source-url` (`GEONAMES_URL`) | `https://download.geonames.org/export/dump/` | Dump directory to download from |
| `--source-dir <dir>` | — | Read from a local copy of the dump directory instead: the same file names, with per-country alternate names under `alternatenames/` |
| `--country <CC,...>` | whole world | ISO alpha-2 codes, case-insensitive. Loads `CC.zip` for each country instead of `allCountries.zip` |
| `--feature-class <X,...>` | all | `A` admin, `H` water, `L` area, `P` populated place, `R` road/rail, `S` spot/building, `T` terrain, `U` undersea, `V` vegetation |
| `--alternate-names` | off | Also load `placename_alternate_names` |
| `--scratch`, `--keep-scratch`, `--apply` | | See [Configuration](#configuration) |

Each load writes these tables:

| Table | From | One row per |
|---|---|---|
| `placenames` | `allCountries.zip` or `CC.zip` | Place (`geonameid`) |
| `placename_alternate_names` | `alternateNamesV2.zip` or `alternatenames/CC.zip` (only with `--alternate-names`) | Alternate name: other languages, historic and colloquial names, airport codes, links |
| `placename_feature_codes` | `featureCodes_en.txt` | Feature class and code, with its English name and description |
| `placename_countries` | `countryInfo.txt` | Country: ISO codes, capital, area, population, currency, languages, neighbours |
| `placename_admin1_codes` | `admin1CodesASCII.txt` | First-level division. `code` is `CC.A1`, e.g. `CA.08` |
| `placename_admin2_codes` | `admin2Codes.txt` | Second-level division. `code` is `CC.A1.A2` |

- **The lookup tables are always loaded whole.** They are a few MB in total,
  and `--country` and `--feature-class` don't narrow them.
- **Alternate names follow the places loaded.** With `--country` or
  `--feature-class`, only names for places in `placenames` are kept, so a
  narrowed load doesn't carry orphan names.
- **Parsing.** The files are tab-separated and unquoted, and names can
  contain `"`. A line with no `geonameid` (or another required value) is
  rejected. An optional value that doesn't parse, such as text in
  `elevation`, is stored as null. Both are counted in the output.
- **Size.** `placenames` is unpartitioned, with bloom filters on `geonameid`
  and `country_code`. Each table is assembled in memory before it is
  written, so a full-world load peaks at a few GB, and more with
  `--alternate-names`. `--country` and `--feature-class` reduce this.

A full-country dry run for Norway takes about 15 seconds: 608,564 places and
896,424 alternate names.

```sql
-- Named water features in Norway, with their kind and county.
SELECT p.name, f.name AS kind, a1.name AS county, p.latitude, p.longitude
FROM foundation.placenames p
JOIN foundation.placename_feature_codes f USING (feature_class, feature_code)
LEFT JOIN foundation.placename_admin1_codes a1
       ON a1.code = p.country_code || '.' || p.admin1_code
WHERE p.feature_class = 'H' AND p.country_code = 'NO';

-- A place's preferred English name.
SELECT p.geonameid, coalesce(a.alternate_name, p.name) AS name_en
FROM foundation.placenames p
LEFT JOIN foundation.placename_alternate_names a
       ON a.geonameid = p.geonameid AND a.isolanguage = 'en' AND a.is_preferred_name
WHERE p.geonameid = 6167865;
```

## Vessel evidence

```bash
collect-foundation evidence <source> [flags]     # alias: collect-foundation interdictions
```

### Data model

Every source is normalized into two shared tables:

- **`vessel_events`** has one row per event per vessel: an inspection, a
  detention, a refusal of access, or an accident a vessel was involved in.
  Each row records the vessel as the source identified it (IMO, MMSI, call
  sign, name, flag), plus the place, the authority and the outcome.
- **`vessel_event_findings`** has one row per thing an event found, mostly
  PSC deficiencies. It joins to `vessel_events` on `event_id`.

There are also a few supporting tables:

- **`evidence_sources`** has one row per source: publisher, URL, access
  terms, coverage and notes. Every evidence load rewrites it.
- **Source-specific tables** hold detail that doesn't fit the shared shape:
  `paris_mou_certificates`, `paris_mou_charterers`, and the six `marsis_*`
  tables, which hold the TSB files exactly as published.

Each row carries two classifiers, which are kept apart on purpose.

**`evidence_category`** says what kind of evidence a row is:

| Value | Meaning | Sources |
|---|---|---|
| `port_state_control` | A port state inspecting a foreign-flagged ship against SOLAS, MARPOL, MLC and other conventions | Paris MoU, Tokyo MoU, USCG (foreign vessels) |
| `flag_state_control` | A flag state inspecting its own ships | USCG (US-flagged vessels) |
| `marine_occurrence` | An accident or incident reported to a safety investigator | TSB |
| `enforcement` | IUU listings, boardings, seizures, sanctions | Reserved; no source loads it yet |

These describe different things about a vessel: regulatory compliance,
enforcement, and safety. **Don't sum them into one score.**

**`event_type`** says what happened, within a category:

| Value | Meaning |
|---|---|
| `inspection` | An inspection that ended without the ship being held |
| `detention` | An inspection that ended with the ship detained |
| `refusal_of_access` | Refused entry to port (USCG "denied entry") |
| `expulsion` | Ordered out of port |
| `accident` / `incident` | A TSB accident (loss of life, serious injury, loss of the vessel, collision, grounding and so on), or an incident (a dangerous situation without an accident's consequences) |
| `occurrence` | A TSB occurrence classed as neither |

The source's own, finer classification goes in `event_subtype`: the PSC
inspection type, or the TSB accident/incident type. Its severity grading,
such as the TSB's IMO casualty class, goes in `severity`.

`event_id` is `<source>:<the source's own id>` and is unique across the
table. Both shared tables are **partitioned by `source`**. A load replaces
only its own source's partition, so the sources load independently, in any
order, and a rerun is idempotent.

### Sources

| `source` | Publisher | Command | Coverage | Access |
|---|---|---|---|---|
| `paris_mou` | Paris MoU Secretariat / EMSA (THETIS) | `paris-mou` | Every PSC inspection by Paris MoU member states, as far back as the files you supply | Bulk XML for registered users ([data-sharing page](https://parismou.org/Inspection-Database/data-sharing)); you download the files yourself |
| `uscg_misle` | US Coast Guard (CG-CVC) | `uscg` | Closed deficiencies on foreign and US vessels, 2018 onward. This is the MISLE data PSIX shows per vessel | Public yearly Excel workbooks, refreshed monthly ([MISLE Deficiency Data Report](https://www.dco.uscg.mil/Our-Organization/Assistant-Commandant-for-Prevention-Policy-CG-5P/Inspections-Compliance-CG-5PC-/Commercial-Vessel-Compliance/MISLE-DEF-DATA-REPORT/)) |
| `tsb_marsis` | Transportation Safety Board of Canada | `tsb` | Marine accidents and incidents reported to the TSB, 1975 onward (complete from 1995) | Six CSV files, refreshed monthly around the 15th, under the Open Government Licence – Canada ([MARSIS data](https://www.tsb.gc.ca/eng/stats/marine/data-6.html)) |
| `tokyo_mou` | Tokyo MoU Secretariat (APCIS) | `tokyo-mou` | Detentions by Asia-Pacific member authorities, 2000 onward. **Detentions only**, not every inspection | Public monthly detention lists ([detention list](https://www.tokyo-mou.org/inspections-detentions/detention-list/)) |

`collect-foundation evidence sources` prints this list. With `--apply` it
rewrites only `evidence_sources`.

### Running each source

Every source command also takes `--scratch`, `--keep-scratch` and `--apply`.
**Each load replaces the source's entire partition**, so give it the whole
history every time, not just the newest file.

#### Paris MoU

```bash
collect-foundation $CAT evidence paris-mou --source-dir ./paris-mou --apply
```

Access needs an account, so there's no download step. Put the bulk files
from the data-sharing page in one directory and point `--source-dir` at it.
The files can be `.xml`, or `.zip` files holding `.xml`. They are THETIS
`InspectionResultsReceipt` documents with one `Inspection` element per
inspection. Every file in the directory is loaded, so **keep the whole
history there**: a load of only the newest file would drop older
inspections from the table.

- Each inspection is one `vessel_events` row. It is a `detention` when the
  inspection recorded one (with `detention_days`), and an `inspection`
  otherwise.
- `event_end_date` is the date of the final visit.
- Codes are kept as THETIS gives them: flag, authority and company country
  as ISO alpha-2, place as UN/LOCODE, ship type and RO as numeric codes.
- Deficiencies become findings, with `ground_for_detention`, the
  responsible RO, and `accidental_damage`.
- Certificates (class and statutory) go to `paris_mou_certificates`, and
  charterers to `paris_mou_charterers`. Both join on `event_id`.
  `classification_society` on the event is the class certificate's issuer.
- An `Inspection` with no `InspectionID` is skipped and counted.

#### USCG MISLE

```bash
# Download the default workbooks (2018 through November 2025).
collect-foundation $CAT evidence uscg --apply

# If the site refuses the download, save the workbooks in a browser first.
collect-foundation $CAT evidence uscg --source-dir ./uscg --apply
```

| Flag | Notes |
|---|---|
| `--source-dir <dir>` | Read every `.xlsx` file in the directory instead of downloading |
| `--file <name,...>` | Workbook names to download, URL-encoded as on the report page. Default: every workbook listed there in October 2026 |
| `--user-agent` (`COLLECT_USER_AGENT`) | User-Agent header for downloads |

The Coast Guard site often refuses downloads that don't come from a
browser. When it does, either set a browser `--user-agent`, or save the
workbooks from the report page into a directory and use `--source-dir`. The
error message names the page. When the Coast Guard publishes a new year's
workbook, pass its name with `--file` along with the earlier ones, or add it
to `--source-dir`.

- Each MISLE activity (an inspection or exam) is one event, joined across
  workbooks. US-flagged vessels are `flag_state_control`; all others are
  `port_state_control`.
- An activity takes the most severe required action among its deficiencies
  as its `event_type`. From least to most severe: `inspection`, `detention`
  (action 30), `refusal_of_access` (25), `expulsion` (20).
- Each deficiency is a finding, with its `action_code` and `action`.
- **Only activities that found something are in the report.** A clean
  inspection doesn't appear, so this source can't give inspection counts or
  detention rates. The report also excludes security, self-reported and
  worklist items.
- The workbooks have had three layouts: 2018–2020; 2021–2024, which added
  class and certificate issuers; and 2025 onward, which added deficiency
  ids and dates. Columns are matched by name. A row with no activity id is
  skipped, and so is a deficiency that appears twice. Both are counted.

#### TSB MARSIS

```bash
collect-foundation $CAT evidence tsb --user-agent "Mozilla/5.0" --apply
```

| Flag | Notes |
|---|---|
| `--source-url` (`TSB_MARSIS_URL`) | Directory the six CSV files are published in. Default: `https://www.tsb.gc.ca/sites/default/files/stats/` |
| `--source-dir <dir>` | Read the six files, with their published names, from a directory |
| `--user-agent` (`COLLECT_USER_AGENT`) | The TSB's firewall refuses clients that don't identify as a browser |
| `--no-raw` | Skip the `marsis_*` tables |

The six files load as published: every column is a string, named as in the
file.

| File | Table |
|---|---|
| `MARSISdb_MDOTW_VW_OCCURRENCE_PUBLIC.csv` | `marsis_occurrence` |
| `MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_PUBLIC.csv` | `marsis_occurrence_vessel` |
| `MARSISdb_MDOTW_VW_INJURIES_PUBLIC.csv` | `marsis_injuries` |
| `MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_LSA_EQUIPMENT_PUBLIC.csv` | `marsis_vessel_lsa_equipment` |
| `MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_NAV_EQUIPMENT_PUBLIC.csv` | `marsis_vessel_nav_equipment` |
| `MARSISdb_MDOTW_VW_OCCURRENCE_VESSEL_REC_EQUIPMENT_PUBLIC.csv` | `marsis_vessel_rec_equipment` |

- Each vessel involved in an occurrence is one `vessel_events` row, with
  `event_id` `tsb_marsis:<occurrence>:<vessel>`. An occurrence that lists no
  vessel gets one row with no vessel.
- `case_id` is the occurrence number, shared by every vessel involved.
  Group by it to count occurrences rather than vessels.
- `event_type` is `accident`, `incident` or `occurrence`. `severity` is the
  IMO casualty class.
- `vessel_id` is the Canadian official number, or the vessel registration
  number when there is none (`vessel_id_type` says which).
- `pollution`, `deaths`, `injuries` and `missing_persons` come from the
  occurrence and vessel records.
- The occurrence file repeats an occurrence once per notification, and the
  vessel file repeats a vessel once per cargo, damage and tow record. The
  first row stands for each (for vessels, the row with the primary accident
  type). The repeats stay in the `marsis_*` tables.
- The TSB has no findings, so the load leaves the `tsb_marsis` partition of
  `vessel_event_findings` alone.
- Some data is withheld from the public files for privacy.

#### Tokyo MoU

```bash
collect-foundation $CAT evidence tokyo-mou --keep-scratch --scratch ./tokyo-cache --apply
# Later, reload from the cache without downloading:
collect-foundation $CAT evidence tokyo-mou --source-dir ./tokyo-cache --apply
```

| Flag | Default | Notes |
|---|---|---|
| `--source-url` (`TOKYO_MOU_URL`) | `https://apcis.tmou.org/isss/public_apcis.php` | The APCIS public interface |
| `--source-dir <dir>` | — | Read `tokyo_mou/detlist-YYYY-MM.xls` files, as `--keep-scratch` leaves them |
| `--from YYYY-MM` | `2000-01` | First month |
| `--to YYYY-MM` | this month | Last month, inclusive |
| `--delay-ms` | `250` | Pause between monthly requests |

The Tokyo MoU has no public bulk feed of all inspections. This command
fetches the monthly detention list for each month, about 320 requests for
the full history. The list's "Excel" export is really an HTML table.

- Every row is a `detention`. `event_end_date` is the release date, and
  `detention_days` the difference. A detention not yet released has
  neither; the run prints how many there are.
- The findings are the grounds for detention. The list doesn't give the
  total deficiency count.
- The lists have no id. `event_id` is built from the IMO number, the
  detention date and the place. When rows share all three (mostly rows with
  no IMO number), the later ones get `#2`, `#3` and so on, in the order
  read. Nothing is dropped.
- **`--from` later than 2000-01 shrinks the table.** The load replaces every
  Tokyo MoU row, so the earlier months disappear. The run warns when this
  happens.

### Querying evidence

```sql
-- Everything known about one ship, newest first.
SELECT event_date, source, evidence_category, event_type, authority, place_name,
       deficiency_count, detention_days
FROM foundation.vessel_events
WHERE imo_number = 9321483
ORDER BY event_date DESC;

-- PSC detention record per ship over five years, by regime.
SELECT imo_number, regime,
       count(*) FILTER (WHERE event_type = 'detention') AS detentions,
       count(*) AS events
FROM foundation.vessel_events
WHERE evidence_category = 'port_state_control'
  AND event_date >= current_date - INTERVAL 5 YEAR
GROUP BY imo_number, regime;

-- The grounds behind a detention.
SELECT f.finding_code, f.finding_area, f.finding_item, f.ground_for_detention, f.responsible_ro
FROM foundation.vessel_event_findings f
JOIN foundation.vessel_events e USING (event_id)
WHERE e.event_type = 'detention' AND e.imo_number = 9321483;

-- Link evidence to AIS identity (ais-tracks `vessels`, in its own namespace).
SELECT v.mmsi, e.*
FROM ais.vessels v
JOIN foundation.vessel_events e ON e.imo_number = v.imo_number;
```

Filter on `source` where you can: it is the partition column, so it prunes
whole partitions. Bloom filters cover `event_id`, `imo_number`, `mmsi` and
`call_sign` on events, and `event_id` on findings.

### Caveats

- **Coverage isn't comparable across sources.** Paris MoU lists every
  inspection, USCG only inspections that found deficiencies, and Tokyo MoU
  only detentions. So a detention *rate* is only meaningful for
  `paris_mou`. Compare counts within a source, not across sources.
- **Identifiers are as reported.** Each source writes the IMO, MMSI, name
  and flag it recorded, and they are validated only for shape: IMO numbers
  must have 7 digits, MMSIs 9. Vessels with no IMO number (small craft,
  fishing vessels, most TSB occurrences) need `mmsi`, `call_sign` or
  `vessel_id` to link.
- **Flags differ by source.** `flag` is what the source wrote, a name or a
  code. `flag_code` is ISO alpha-2, filled in only when the source gives a
  code.
- **There's no deduplication across sources.** A ship inspected under both
  Paris MoU and USCG has two events, which is correct. The same inspection
  never appears in two sources.

## Load semantics

- **Replace, don't append.** Each `--apply` atomically replaces what it
  owns, in one Iceberg snapshot per table. For placenames and the
  source-specific tables, that is the whole table. For `vessel_events` and
  `vessel_event_findings`, it is that source's partition. Readers never see
  a partial load, and a rerun gives the same result.
- **History is kept by Iceberg.** Earlier loads stay reachable through
  snapshot history (time travel) until `ais-compact expire` drops them.
- **Empty loads are refused.** An empty result almost always means an
  unreadable or wrong input, so a table or partition is never replaced with
  zero rows.
- **Tables and the namespace are created on first write.**
- **Exit codes.** `0` success, `1` error.

## Licences and attribution

| Data | Terms |
|---|---|
| GeoNames | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Credit GeoNames (<https://www.geonames.org>) wherever the data is used or shown |
| TSB MARSIS | [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada). Attribute the Transportation Safety Board of Canada |
| USCG MISLE | US federal government data, published for public use |
| Tokyo MoU | Publicly published detention lists. Check the Tokyo MoU's terms before redistributing |
| Paris MoU | Supplied to registered users under the Paris MoU data-sharing terms. **Check those terms before sharing tables that contain this data** |

`evidence_sources.access` records each source's access terms alongside the
data.
