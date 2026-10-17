# collect-foundation

Loads foundation (reference) data that the maritime tables join against into
Apache Iceberg. Today that is the [GeoNames](https://www.geonames.org)
gazetteer: about 13 million named places worldwide (populated places, water
bodies, coastal and undersea features, terrain, administrative areas).

Tables go to namespace **`foundation`** in warehouse **`maritimedata`** unless
`--iceberg-namespace` / `ICEBERG_NAMESPACE` or `--iceberg-warehouse` /
`ICEBERG_WAREHOUSE` say otherwise. With RustFS's built-in catalog the
warehouse is the bucket name. All other Iceberg flags are the shared ones
([CLI_REFERENCE.md](CLI_REFERENCE.md#iceberg-icebergcliargs-all-binaries)).

## Usage

```bash
# Dry run: download and parse, report row counts, write nothing.
collect-foundation placenames load --country NO

# Load the whole world, with alternate names, into RustFS.
export S3_ENDPOINT=http://localhost:9000 S3_ACCESS_KEY=rustfsadmin S3_SECRET_KEY=rustfsadmin \
       S3_REGION=us-east-1 S3_PATH_STYLE=true
collect-foundation --iceberg-catalog-uri http://localhost:9000/iceberg --iceberg-sigv4 \
  placenames load --alternate-names --apply
```

| Flag | Default | Notes |
|---|---|---|
| `--source-url` (`GEONAMES_URL`) | `https://download.geonames.org/export/dump/` | Dump directory to download from |
| `--source-dir <dir>` | — | Read the files from a local copy of the dump directory instead (same names; per-country alternate names under `alternatenames/`) |
| `--country <CC,...>` | whole world | ISO alpha-2 codes; loads `CC.zip` per country instead of `allCountries.zip` |
| `--feature-class <X,...>` | all | `A` admin, `H` water, `L` area, `P` populated place, `R` road/rail, `S` spot/building, `T` terrain, `U` undersea, `V` vegetation |
| `--alternate-names` | off | Also load `placename_alternate_names` |
| `--scratch <dir>` | `$TMPDIR/collect-foundation` | Where downloads go; removed afterwards unless `--keep-scratch` |
| `--apply` | off | Write the tables; needs `--iceberg-catalog-uri` |

## Tables

Each load **replaces** every table it writes in one Iceberg snapshot, so
readers never see a partial load and earlier loads stay reachable through
snapshot history (until `ais-compact expire`). A table is never replaced with
zero rows. Every table ends with `loaded_at` (timestamptz) and `source_file`
(the GeoNames file the row came from).

| Table | Source file | Columns |
|---|---|---|
| `placenames` | `allCountries.zip` or `CC.zip` | `geonameid` (long), `name`, `asciiname`, `alternatenames` (comma-separated), `latitude`, `longitude` (WGS84 double), `feature_class`, `feature_code`, `country_code`, `cc2`, `admin1_code`..`admin4_code`, `population` (long), `elevation`, `dem` (int, metres), `timezone`, `modification_date` (date) |
| `placename_alternate_names` | `alternateNamesV2.zip` or `alternatenames/CC.zip` | `alternate_name_id`, `geonameid`, `isolanguage`, `alternate_name`, `is_preferred_name`, `is_short_name`, `is_colloquial`, `is_historic` (boolean), `from_period`, `to_period` |
| `placename_feature_codes` | `featureCodes_en.txt` | `feature_class`, `feature_code`, `name`, `description` |
| `placename_countries` | `countryInfo.txt` | `iso`, `iso3`, `iso_numeric`, `fips`, `country`, `capital`, `area_sq_km`, `population`, `continent`, `tld`, `currency_code`, `currency_name`, `phone`, `postal_code_format`, `postal_code_regex`, `languages`, `geonameid`, `neighbours`, `equivalent_fips_code` |
| `placename_admin1_codes` | `admin1CodesASCII.txt` | `code` (`CC.A1`), `name`, `asciiname`, `geonameid` |
| `placename_admin2_codes` | `admin2Codes.txt` | `code` (`CC.A1.A2`), `name`, `asciiname`, `geonameid` |

With `--country` or `--feature-class`, alternate names are kept only for the
places loaded. The lookup tables are always loaded whole (a few MB).

Parsing: the files are tab-separated and unquoted. A line missing a required
value (e.g. no `geonameid`) is rejected; an unparseable optional value is
stored as null. Both are counted in the output.

Example joins:

```sql
SELECT p.name, f.name AS kind, c.country, a1.name AS province
FROM foundation.placenames p
JOIN foundation.placename_feature_codes f USING (feature_class, feature_code)
JOIN foundation.placename_countries c ON c.iso = p.country_code
LEFT JOIN foundation.placename_admin1_codes a1 ON a1.code = p.country_code || '.' || p.admin1_code
WHERE p.feature_class = 'H' AND p.country_code = 'NO';
```

## Limits

- `placenames` is written unpartitioned, with bloom filters on `geonameid`
  and `country_code`.
- A table is assembled in memory before it is written: a full-world load
  peaks at a few GB (more with `--alternate-names`). `--country` and
  `--feature-class` reduce this.

## Licence

GeoNames data is licensed under
[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): credit GeoNames
(<https://www.geonames.org>) wherever the data is used or shown.
