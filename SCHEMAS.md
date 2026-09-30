# Table schemas

Every table the workspace's programs write, column by column. This page is the
reference for *what is in the data*; the per-program pages ([AIS_PARSE.md](AIS_PARSE.md),
[AISSTREAM_PARSE.md](AISSTREAM_PARSE.md), [AIS_TRACKS.md](AIS_TRACKS.md), …) cover *how to run
the programs*.

Schemas were taken from the source, not from the other docs, so where they
disagree this page reflects the code:

| Layer | Tables | Written by | Source of truth |
|-------|--------|------------|-----------------|
| [Bronze](#1-bronze-raw-feed) | `raw` (Parquet `(ts, payload)` files; Iceberg `raw`) | `collect-file`, `collect-socket`, `collect-kafka`, `collect-aisstream`, `collect-barentswatch` | `crates/collect-core/src/lib.rs` (`BatchBuf`), `iceberg/table_schemas.rs` |
| [Silver](#2-silver-decoded-ais) | `positions`, `statics`, `meteo`, `binary`, `atons`, `other` | `ais-parse`, `aisstream-parse`, and any collector run with `--parser` | `crates/ais-parse/src/output*.rs`, `crates/aisstream-parse/src/output*.rs`, `crates/collect-core/src/iceberg/table_schemas.rs` |
| [Derived](#3-derived-ais-tracks) | `track_points`, `tracks`, `stop_segments`, `stops`, `voyages`, `open_voyages`, `vessels`, `vessel_attributes`, `ref_ports`, plus daily and bookkeeping tables | `ais-tracks` | `crates/ais-tracks/src/*.rs` |

`ais-compact` ([AIS_COMPACT.md](AIS_COMPACT.md)) creates no tables of its own: it rewrites
the files of the tables above, sorted by `mmsi, ts` by default, without
changing their schema.

## Contents

- [Conventions](#conventions)
- [1. Bronze: raw feed](#1-bronze-raw-feed)
- [2. Silver: decoded AIS](#2-silver-decoded-ais)
  - [positions](#positions) · [statics](#statics) · [meteo](#meteo) · [binary](#binary) · [atons](#atons) · [other](#other)
  - [Differences between the two parsers](#differences-between-the-two-parsers)
- [3. Derived: ais-tracks](#3-derived-ais-tracks)
  - [track_points](#track_points) · [tracks](#tracks) · [stop_segments](#stop_segments) · [stops](#stops) · [voyages and open_voyages](#voyages-and-open_voyages)
  - [vessels](#vessels) · [vessel_attributes](#vessel_attributes) · [ref_ports](#ref_ports)
  - [Daily tables](#daily-tables): [vessel_daily](#vessel_daily) · [attribute_daily](#attribute_daily) · [static_daily](#static_daily) · [destination_daily](#destination_daily)
  - [Bookkeeping tables](#bookkeeping-tables): [vessel_state](#vessel_state) · [voyage_state](#voyage_state) · [build_log](#build_log)
- [Querying notes](#querying-notes)

## Conventions

**Two physical forms.** Silver tables exist either as Hive-partitioned Parquet
(`<out>/positions/year=YYYY/month=MM/day=DD/*.parquet`) or as Iceberg tables in a
REST catalog (`--iceberg-catalog-uri`). The column *names and meaning* are
identical; the *types* differ slightly because Iceberg has no unsigned
integers and the Iceberg schema is more permissive about nulls:

| Local Parquet | Iceberg | Notes |
|---------------|---------|-------|
| `uint8`, `uint16` | `int` (32-bit) | |
| `uint32` (`mmsi`, `imo_number`, `mothership_mmsi`, `payload_bits`) | `long` for `mmsi`; `int` for the rest | |
| `uint64` (`h3`, `hilbert`) | `long` (signed) | Values fit in `i64`, nothing is lost |
| `timestamp(ms, UTC)` | `timestamptz` (µs) | |
| `float64` | `double` | |
| `utf8` | `string` | |
| non-null columns such as `ais_class`, `nav_status`, `high_accuracy`, `raim`, `payload` | `optional` | In Iceberg only `ts`, `source`, `msg_type`, `mmsi` are `required` (`other`: `ts`, `source`, `msg_type`, `payload`) |

Iceberg field IDs are stable and shared by `ais-parse`, `aisstream-parse` and
the collectors' inline `--parser`, so they can all append to the same tables.
Silver tables are partitioned by `day(ts)` by default (the `--partition`
granularity: `year`, `month`, `day` or `hour`). The `ais-tracks` tables are
Iceberg only.

**Time.** Every `ts` is UTC. In bronze and silver it is the corrected capture
time of the message (from `$PGHP` / NMEA tag-block `c:` when
`--process-timestamps` is used, otherwise the collector's receive time), not
a time decoded from the AIS payload. Silver keeps millisecond precision
(Parquet) and `ais-tracks` tables use microseconds.

**Nulls.** A null means "not reported" or "not available". AIS transmits a
per-field sentinel for "not available" (heading 511, ROT −128, SOG 1023, lat 91°,
…); the decoders convert these to null rather than storing the sentinel.
Flag columns documented as non-null default to `false` when the source
does not supply them.

**Units.** Columns carry their unit in the name where one applies: `_knots` /
`_kn` (knots), `_nm` (nautical miles), `_m` (metres), `_s` (seconds), `_deg`
(degrees), `_hpa`, `_pct`, `_c` (°C). Latitude/longitude are WGS-84 decimal
degrees, `cog` and `heading_true` are degrees true.

**Identifiers.** `mmsi` is the 9-digit Maritime Mobile Service Identity
(stored as `uint32` / `long`). `source` is the origin feed label given to the
collector with `--source` (e.g. `norway`, `aisstream`, `barentswatch`); silver
is *not* partitioned by source, so this column is how you separate feeds.

---

## 1. Bronze: raw feed

Written by the collectors. Bronze keeps the feed untouched so that silver can be
rebuilt from it.

### Bronze Parquet files

Path: `source=<label>/year=YYYY/month=MM/day=DD/<file>.parquet`. The source is
carried by the **directory name**, not a column. Sorted by `ts` within a file.
Zstd-compressed.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts` | timestamp(ms, UTC) | no | When the collector received the line (or the capture time parsed from a tag block / `$PGHP` line when the collector is asked to correct timestamps). Determines the `year/month/day` partition. |
| `payload` | utf8 | no | The message exactly as received: one NMEA 0183 sentence for `collect-socket` / `collect-kafka` / `collect-file` (possibly with a NMEA 4.10 tag block such as `\s:station,c:1700000000*5E\!AIVDM,…`; multi-part messages are separate rows until consolidated with `--consolidate-ais`), or one JSON document for `collect-aisstream` and `collect-barentswatch`. |

### Iceberg `raw` table

Registered by a collector (`--iceberg-catalog-uri`) after each successful
upload; one commit per bronze file. The Iceberg table *adds* the source as a
real column. Partitioned by `day(ts)` (configurable).

| ID | Column | Iceberg type | Required | Description |
|----|--------|--------------|----------|-------------|
| 1 | `ts` | timestamptz | yes | As above. Must stay column 0 (the partition column). |
| 2 | `source` | string | yes | Origin feed label (the `source=` directory of the Parquet file). |
| 3 | `payload` | string | yes | The raw line or JSON document. |

---

## 2. Silver: decoded AIS

Six sibling tables. They are written by `ais-parse` (NMEA/AIVDM sentences),
by `aisstream-parse` (AISStream.io JSON envelopes; also used for BarentsWatch),
and by any collector started with `--parser ais|aisstream`. All three share
one set of tables when pointed at the same Iceberg namespace.

Which table a message lands in:

| Table | AIS message types | Row means |
|-------|-------------------|-----------|
| `positions` | 1, 2, 3 (Class A), 4 (base station), 9 (SAR aircraft), 18, 19 (Class B), 27 (long range) | one position report |
| `statics` | 5 (Class A static & voyage), 24 (Class B static, parts A and B) | one static / voyage report |
| `meteo` | 8 with DAC 1, FID 31 (IMO289) or FID 11 (IMO236) | one meteorological / hydrological report |
| `binary` | every other 8 (binary broadcast) | one binary broadcast, payload kept as hex |
| `atons` | 21 | one aid-to-navigation report |
| `other` | everything else that parses (6, 7, 10–17, 20, 22, 23, 25, 26, …) | one message kept undecoded |

Sentences that do not parse at all, and multi-part fragments whose partner never
arrives, produce **no row** (they are counted in the run summary as `unparsed` /
`incomplete`; the bronze row still exists).

### positions

One row per position report. Class A reports (types 1–3) and Class B reports
(18, 19) share the table; base stations (4) and SAR aircraft (9) also land here
with the fields they do not carry left null.

| Column | Type (Parquet / Iceberg) | Nullable | Description |
|--------|--------------------------|----------|-------------|
| `ts` | timestamp(ms, UTC) | no | Capture time of the message. |
| `source` | utf8 | no | Origin feed label. |
| `msg_type` | uint8 / int | no | AIS message type that produced the row (1, 2, 3, 4, 9, 18, 19, 27). For `aisstream-parse` this is the envelope's `MessageID`. |
| `mmsi` | uint32 / long | no | Transmitting station's MMSI. |
| `ais_class` | utf8 | no | `Class A`, `Class B`, `Base Station` (type 4), or, for SAR aircraft, `Class A`. |
| `latitude` | float64 | yes | WGS-84 degrees, −90…90. Null when the report says "not available" (91°). |
| `longitude` | float64 | yes | WGS-84 degrees, −180…180. Null when not available (181°). |
| `sog_knots` | float64 | yes | Speed over ground, knots. Null at the "not available" sentinel. AIS encodes 0.1-knot steps; values above 102.2 are not valid speeds. |
| `cog` | float64 | yes | Course over ground, degrees true (0–359.9). Null when not available (360). |
| `heading_true` | float64 | yes | True heading of the hull, whole degrees 0–359. Null when not available (511). |
| `rot` | float64 | yes | Rate of turn as the decoder reports it (AIS encodes −127…+127, sign = turning to port/starboard; −128 "no turn information" becomes null). Null for Class B, base stations and SAR. |
| `altitude_m` | float64 | yes | Altitude in metres. Populated only for SAR aircraft reports (type 9), else null. |
| `h3` | uint64 / long | yes | [H3](https://h3geo.org) cell id at **resolution 10** (edge ≈ 75 m) of the position. Null when there is no position. |
| `hilbert` | uint64 / long | yes | Hilbert-curve index of the position on a 31-bit-per-axis equirectangular grid (cell ≈ 2 cm in longitude at the equator). A spatial clustering / sort key, not an S2 cell id. Null when there is no position. |
| `nav_status` | utf8 | no | Navigational status as text: `under way using engine`, `at anchor`, `not under command`, `restricted manoeuverability`, `constrained by draught`, `moored`, `aground`, `engaged in fishing`, `under way sailing`, `(reserved9)`…`(reserved13)`, `ais sart is active`, `(notDefined)`. SAR aircraft (both parsers) and `aisstream-parse` Class B rows are written as `under way using engine` because they do not transmit a status; base stations use the empty string. `ais-parse` Class B rows carry whatever text `nmea-parser` produces for a missing status. |
| `high_accuracy` | boolean | no | Position accuracy flag: true = better than 10 m (DGNSS quality), false = worse. |
| `raim` | boolean | no | Receiver Autonomous Integrity Monitoring in use. |
| `special_manoeuvre` | boolean | yes | Class A special-manoeuvre indicator: null = not available, false = not engaged, true = engaged. |
| `station` | utf8 | yes | Receiving/base station from the NMEA tag-block `s:` field, when the feed supplies one. Always null from `aisstream-parse`. |
| `payload` | utf8 | no / yes | The original NMEA sentence (ais-parse) that produced the row. **Not present** in `aisstream-parse` local Parquet; present but nullable in Iceberg. |

Iceberg nullability: only `ts`, `source`, `msg_type` and `mmsi` are required.

### statics

One row per static/voyage report. Type 24 (Class B) arrives as **two separate
messages**: part A carries only the name; part B carries ship type, call sign,
dimensions and (for auxiliary craft) the mothership. They produce two rows
with complementary nulls. `ais-tracks` resolves each attribute independently so
the halves combine downstream.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts`, `source`, `msg_type`, `mmsi`, `station` | as in `positions` | | `msg_type` is 5 or 24. |
| `ais_class` | utf8 | no | `Class A` (type 5) or `Class B` (type 24). |
| `imo_number` | uint32 / int | yes | IMO ship identification number (7 digits). Zero is stored as null. Not checked for validity here (see `vessels.imo_valid`). Class B never reports one. |
| `call_sign` | utf8 | yes | Radio call sign, up to 7 characters; trailing `@` and spaces stripped, empty becomes null. |
| `name` | utf8 | yes | Vessel name, up to 20 characters; trailing `@` and spaces stripped. |
| `ship_type` | utf8 | no | Category derived from the AIS ship-type code: `fishing`, `tug`, `pilot`, `search and rescue`, `port tender`, `anti-pollution equipment`, `law enforcement`, `medical transport`, `noncombatant`, `passenger`, `cargo`, `tanker`, `high-speed craft`, `wing in ground`, `other`, `(local)`, `(not available)`. Empty string when a Class B part A message carried no type. The numeric code is not kept. |
| `dimension_to_bow` | uint16 / int | yes | Metres from the GNSS antenna to the bow (AIS "A"). |
| `dimension_to_stern` | uint16 / int | yes | Metres to the stern ("B"). Bow + stern = length overall. |
| `dimension_to_port` | uint16 / int | yes | Metres to the port side ("C"). |
| `dimension_to_starboard` | uint16 / int | yes | Metres to the starboard side ("D"). Port + starboard = beam. |
| `draught_m` | float64 | yes | Current maximum static draught in metres (AIS stores 0.1 m steps). Class A only. |
| `destination` | utf8 | yes | Declared destination as typed by the crew (free text, often a UN/LOCODE like `NOBGO` or `ROTTERDAM`, often stale); padding stripped. Class A only. |
| `eta` | timestamp(ms, UTC) / timestamptz | yes | Declared estimated time of arrival. AIS carries month/day/hour/minute **but no year**; the decoder supplies one, so treat the year as unreliable, especially when re-processing old data in a later year. Null when month or day is 0. |
| `mothership_mmsi` | uint32 / int | yes | For craft associated with a parent ship (type 24 part B, MMSI 98xxxxxxx): the parent's MMSI. |
| `payload` | utf8 | no / yes | Original NMEA sentence; same availability as `positions.payload`. |

### meteo

Meteorological and hydrological broadcasts (type 8, DAC 1). Two layouts are
decoded into the same columns: **FID 31** (IMO289, current, 360 bits) and
**FID 11** (IMO236, deprecated, 352 bits). Bit offsets, scales and sentinels
follow gpsd's reference decoder. Every measurement is nullable: the per-field
"not available" sentinel becomes null. Quantities are already scaled to natural
units.

*Header and position*

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts`, `source`, `station` | as above | | |
| `msg_type` | uint8 / int | no | Always 8. |
| `mmsi` | uint32 / long | no | Reporting station (usually a shore station or buoy). |
| `dac` | uint16 / int | no | Designated Area Code (1 = international). |
| `fid` | uint8 / int | no | Functional ID: 31 (current) or 11 (deprecated). Tells you which layout/scale a row was decoded with. |
| `latitude`, `longitude` | float64 | yes | Station position, degrees. Transmitted in 1/1000 arc-minute. |
| `hilbert` | uint64 / long | yes | Same Hilbert index as in `positions`. (No `h3` column in this table.) |
| `position_accuracy` | boolean | yes | High-accuracy position flag. Null for FID 11, which has no such field. |
| `day`, `hour`, `minute` | uint8 / int | yes | UTC day of month (1–31), hour (0–23) and minute (0–59) of the observation. Null when not available (day 0, hour 24, minute 60). |

*Wind and atmosphere*

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `wind_speed_kn` | uint16 / int | yes | Average wind speed, knots (whole). |
| `wind_gust_kn` | uint16 / int | yes | Wind gust speed, knots. |
| `wind_dir_deg` | uint16 / int | yes | Wind direction, degrees true, direction the wind blows **from**. |
| `wind_gust_dir_deg` | uint16 / int | yes | Gust direction, degrees. |
| `air_temp_c` | float64 | yes | Air temperature, °C, 0.1° resolution (FID 31 range −60…+60). |
| `humidity_pct` | uint8 / int | yes | Relative humidity, 0–100 %. |
| `dew_point_c` | float64 | yes | Dew point, °C, 0.1° resolution. |
| `pressure_hpa` | uint16 / int | yes | Air pressure, hPa, offset so that the 799/800–1310 hPa range maps to the raw field. |
| `pressure_tendency` | uint8 / int | yes | Code: 0 = steady, 1 = decreasing, 2 = increasing. |
| `visibility_nm` | float64 | yes | Horizontal visibility, nautical miles, 0.1 nm resolution. |
| `visibility_greater` | boolean | yes | FID 31 only: true when the true visibility is **greater than** `visibility_nm` (the gauge's upper limit). Null for FID 11. |

*Water level and currents*

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `water_level_m` | float64 | yes | Water level relative to the local datum, metres (negative below datum). |
| `water_level_trend` | uint8 / int | yes | Code: 0 = steady, 1 = decreasing, 2 = increasing. |
| `surface_current_speed_kn` | float64 | yes | Surface current speed, knots, 0.1 resolution. |
| `surface_current_dir_deg` | uint16 / int | yes | Surface current direction, degrees true (direction it flows **towards**). |
| `current2_speed_kn`, `current2_dir_deg`, `current2_depth_m` | float64 / uint16 / float64 | yes | A second current measurement: speed, direction, and the depth in metres (0.1 m steps in this decoder) at which it was taken. |
| `current3_speed_kn`, `current3_dir_deg`, `current3_depth_m` | float64 / uint16 / float64 | yes | A third current measurement, same fields. |

*Waves, sea and ice*

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `wave_height_m` | float64 | yes | Significant wave height, metres, 0.1 resolution. |
| `wave_period_s` | uint16 / int | yes | Wave period, seconds. |
| `wave_dir_deg` | uint16 / int | yes | Wave direction, degrees. |
| `swell_height_m`, `swell_period_s`, `swell_dir_deg` | float64 / uint16 / uint16 | yes | Swell height (m), period (s) and direction (degrees). |
| `sea_state` | uint8 / int | yes | Sea state on the Beaufort scale (0–12). |
| `water_temp_c` | float64 | yes | Water temperature, °C, 0.1 resolution. |
| `precipitation_type` | uint8 / int | yes | IMO289 precipitation code (rain, thunderstorm, freezing rain, mixed/ice, snow, …). |
| `salinity_pct` | float64 | yes | Salinity in percent (per the column name), 0.1 resolution. Raw values at/above the sentinel (510 or 511) are null. |
| `ice` | uint8 / int | yes | Ice flag: 0 = no ice, 1 = ice present (2 reserved). |

*Tail:* `station` (utf8, nullable) and `payload` (utf8, original sentence; same
availability as in `positions`).

### binary

Every type-8 message that is **not** a recognised meteo layout. The generic
header is decoded; the application-specific payload is kept raw so it can be
decoded later.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts`, `source`, `station` | as above | | |
| `msg_type` | uint8 / int | no | Always 8. |
| `mmsi` | uint32 / long | no | Sender. |
| `dac` | uint16 / int | no | Designated Area Code (e.g. 1 international, 366 USA, 316 Canada). |
| `fid` | uint8 / int | no | Functional ID within the DAC. `(dac, fid)` identifies the application (area notice, extended voyage data, regional data, …). |
| `payload_hex` | utf8 | no | The application payload (all bits after the 56-bit header: type, repeat, MMSI, spare, DAC, FID) as hexadecimal. |
| `payload_bits` | uint32 / int | no | Length of that payload in bits (message length − 56). Use it to ignore padding bits at the end of `payload_hex`. |
| `payload` | utf8 | no / yes | Original NMEA sentence. |

### atons

Type 21 reports from physical and virtual aids to navigation (buoys, lights,
beacons, light vessels).

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts`, `source`, `station` | as above | | |
| `msg_type` | uint8 / int | no | Always 21. |
| `mmsi` | uint32 / long | no | The aid's MMSI (normally 99MIDxxxx). |
| `ais_class` | utf8 | no | Always `AtoN`. |
| `aid_type` | utf8 | no | Type of aid as text, from the 32 AIS values: `reference point`, `RACON`, `FixedStructure`, `light without sectors`, `light with sectors`, `leading light front/rear`, `cardinal beacon/mark, north/east/south/west`, `lateral beacon, port/starboard side`, `isolated danger`, `safe water`, `special mark`, `light vessel`, `not specified`, … |
| `name` | utf8 | yes | Aid name, padding stripped. |
| `name_extension` | utf8 | yes | Extra name characters when the name is longer than 20. `aisstream-parse` fills it; `ais-parse` always leaves it null. |
| `latitude`, `longitude` | float64 | yes | Position, degrees. |
| `h3` | uint64 / long | yes | H3 res-10 cell. |
| `hilbert` | uint64 / long | yes | Hilbert index. |
| `dimension_to_bow` / `_stern` / `_port` / `_starboard` | uint16 / int | yes | Extent of the aid in metres (same convention as `statics`). |
| `off_position` | boolean | yes | The aid is reporting a position away from its charted one. Only meaningful for fixed, non-virtual aids. |
| `virtual_aid` | boolean | yes | True when the aid does not physically exist and is broadcast by a shore station. |
| `assigned_mode` | boolean | yes | Station operating in assigned (as opposed to autonomous) mode. |
| `high_accuracy` | boolean | no | Position accuracy flag. **`ais-parse` always writes false** for AtoNs. |
| `raim` | boolean | no | RAIM flag. |
| `payload` | utf8 | no / yes | Original sentence (ais-parse). |

### other

The catch-all: valid messages with no typed decoder. The original text is kept so
nothing is lost.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `ts` | timestamp | no | |
| `source` | utf8 | no | |
| `station` | utf8 | yes | Tag-block station (Parquet from `ais-parse` and Iceberg). **Absent** from `aisstream-parse` local Parquet; always null from that parser in Iceberg. |
| `msg_type` | **utf8** | no | A label, not a number. `ais-parse`: `Type<N>` (e.g. `Type6`, `Type12`, `Type25`) or `Unknown` when the type could not be determined. `aisstream-parse`: the AISStream message-type name, e.g. `SafetyBroadcastMessage`, `AddressedBinaryMessage`, `Interrogation`, `LongRangeAisBroadcastMessage`, `ChannelManagement`. |
| `payload` | utf8 | no | The undecoded original: an NMEA sentence (`ais-parse`) or the JSON document (`aisstream-parse`). |

### Differences between the two parsers

| Aspect | `ais-parse` (NMEA) | `aisstream-parse` (JSON) |
|--------|--------------------|--------------------------|
| `station` | from tag-block `s:` | always null |
| `payload` in local Parquet | yes, every table | no, except `other` |
| `payload` in Iceberg | original sentence | optional; the inline `--parser aisstream` fills it |
| `positions.msg_type` | 1, 2, 3, 4, 9, 18, 19, 27 | the envelope's `MessageID` |
| `atons.name_extension` | always null | filled |
| `atons.high_accuracy` | always false | from the report's accuracy flag |
| `other.msg_type` | `Type<N>` / `Unknown` | AISStream message-type name |
| `other.station` (local Parquet) | present | absent |
| `statics.draught_m` | from the 0.1 m field | from `MaximumStaticDraught` |
| `imo_number` of 0 | library-dependent | null |
| `ship_type` / `nav_status` wording | `nmea-parser` display strings | a fixed mapping written to match them |

---

## 3. Derived: ais-tracks

`ais-tracks` reads silver `positions` and `statics` from `--iceberg-namespace`
and writes the tables below to `--output-namespace` (optionally prefixed with
`--output-table-prefix`). All are Iceberg. "Partition" is the partition column;
days are UTC. Distances are in **nautical miles**, speeds in **knots**,
durations in **seconds**.

| Table | One row per | Partition |
|-------|-------------|-----------|
| [`track_points`](#track_points) | kept position report | day(`ts`) |
| [`tracks`](#tracks) | continuous movement segment, per UTC day | day(`ts`) |
| [`stop_segments`](#stop_segments) | stationary run, per UTC day | day(`ts`) |
| [`stops`](#stops) | stop (day pieces merged) | day(`depart_ts`) |
| [`voyages`](#voyages-and-open_voyages) | finished leg | day(`arrive_ts`) |
| [`open_voyages`](#voyages-and-open_voyages) | leg still under way | none (replaced each run) |
| [`vessels`](#vessels) | MMSI | none |
| [`vessel_attributes`](#vessel_attributes) | MMSI × attribute × distinct value | none |
| [`ref_ports`](#ref_ports) | port per World Port Index release | none |
| [`vessel_daily`](#vessel_daily) | vessel × day | day(`ts`) |
| [`attribute_daily`](#attribute_daily) | vessel × day × attribute × value | day(`ts`) |
| [`static_daily`](#static_daily) | vessel × day | day(`ts`) |
| [`destination_daily`](#destination_daily) | vessel × day × declared destination | day(`ts`) |
| [`vessel_state`](#vessel_state) | vessel × built day | day(`ts`) |
| [`voyage_state`](#voyage_state) | vessel | none |
| [`build_log`](#build_log) | step × day built | none |

A *day number* (`Int`) used in a few bookkeeping columns is **days since
1970-01-01**.

### track_points

The movement spine. Each vessel-day of silver `positions` is streamed once,
annotated, and optionally **thinned**: only rows that movement makes worth
keeping are written (`--no-thin` keeps all). Every report is still accounted
for: a kept row carries the counts and sums of the reports it stands for, and
`sum(n_raw)` over a day equals the reports read.

Coordinates, `sog_knots`, `cog` and `heading_true` are stored at reduced
precision (latitude/longitude to 1e-7°, speed/course/heading to 0.1) and are
otherwise as received. The table drops silver columns that do not help
tracking (`msg_type`, `ais_class`, `rot`, `altitude_m`, `h3`, `hilbert`,
`high_accuracy`, `raim`, `special_manoeuvre`, `payload`).

*The report*

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Report time. Partition column. |
| 2 | `mmsi` | long | yes | |
| 3 | `source` | string | yes | Feed that delivered this (kept) copy. |
| 4 | `station` | string | no | Receiving station of this copy. |
| 5 | `latitude` | double | no | Degrees. Null when the report had no usable position. |
| 6 | `longitude` | double | no | |
| 7 | `sog_knots` | double | no | Reported speed over ground. |
| 8 | `cog` | double | no | Reported course over ground, degrees. |
| 9 | `heading_true` | double | no | Reported heading, degrees. |
| 10 | `nav_status` | string | no | Declared navigational status text. |

*Classification*

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 11 | `has_position` | boolean | yes | Latitude and longitude present and in range. |
| 12 | `dup_rank` | int | yes | Rank among exact duplicates of this message (same mmsi, ts, position, speed, course, heading and status heard via different `source`/`station`). 1 = the first copy. |
| 13 | `n_dups` | int | yes | How many copies were heard. |
| 14 | `is_duplicate` | boolean | yes | This row is a repeat copy (rank > 1). With thinning on, duplicates are collapsed into the kept row and counted in `n_collapsed_dups`, so this is false on every kept row. |

*Movement since the previous **kept** row*

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 15 | `prev_ts` | timestamptz | no | Time of the previous kept row for this vessel. Null on the vessel's first row. |
| 16 | `dt_s` | double | no | Seconds since `prev_ts`. |
| 17 | `dist_nm` | double | no | **Summed** great-circle distance of every hop since the previous kept row, so distance totals are exact thinned or not. |
| 18 | `implied_speed_kn` | double | no | Speed of this row's own last hop (distance ÷ time), independent of reported speed. |

*Flags*

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 19 | `gap_before` | boolean | yes | First row of the vessel's stream, or `dt_s` exceeds `--gap-minutes` (default 30). Starts a new segment. |
| 20 | `is_speed_jump` | boolean | yes | Implied speed exceeds `--max-speed-kn` (default 60), or two positions more than 0.05 nm apart share the same second. |
| 21 | `is_spike` | boolean | yes | A jump **into** this row and a jump **out** of it: an isolated bad fix, the row to discard. (The return leg after a spike is `is_speed_jump` but not `is_spike`.) |
| 22 | `is_sog_invalid` | boolean | yes | Reported speed outside the valid AIS range (> 102.2 kn). |
| 23 | `is_cog_invalid` | boolean | yes | Reported course ≥ 360. |
| 24 | `is_heading_invalid` | boolean | yes | Reported heading > 359. |
| 25 | `is_outlier` | boolean | yes | A spike or any invalid value. |

*What a kept row stands for (thinning bookkeeping)*

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 26 | `n_raw` | int | yes | Reports this row stands for, itself included. |
| 27 | `n_collapsed_dups` | int | yes | Of those, exact duplicates of another report. |
| 28 | `n_no_position` | int | yes | Of those, reports without a usable position. |
| 29 | `n_outliers_raw` | int | yes | Of those, reports carrying a flag or invalid value. |
| 30 | `sum_speed` | double | yes | Sum of speeds (reported when valid, else implied) over the reports it stands for. `sum_speed / n_speed` is a count-weighted mean speed. |
| 31 | `n_speed` | int | yes | Number of speeds in `sum_speed`. |
| 32 | `max_dev_nm` | double | yes | Farthest any collapsed report was from this row; bounds the positional error thinning introduced. |
| 33 | `max_hop_speed_kn` | double | no | Fastest hop among the reports it stands for. |
| 34 | `keep_reason` | int | yes | Bitmask of why the row was kept: 1 first of vessel-day, 2 last, 4 gap before it, 8 flagged, 16 next to a flagged row, 32 distance from last kept ≥ `--keep-distance-nm`, 64 interval ≥ `--keep-interval-s`, 128 turn ≥ `--keep-turn-deg`, 256 speed change ≥ `--keep-speed-kn`, 512 nav status change, 1024 just before a gap. `0` only when thinning is off. |
| 35 | `sum_sog` | double | yes | Sum of **valid reported** speeds only. |
| 36 | `n_sog` | int | yes | Count in `sum_sog`. |
| 37 | `max_sog` | double | no | Largest valid reported speed. |

Only *stream* rows (positioned, first of their message) are ever kept.
Duplicates and position-less reports are counted on the next kept row, never
stored themselves. Reports with an implausible MMSI are set aside before this
table.

### tracks

`track_points` rolled up into continuous segments (runs with no `gap_before`).
Built one UTC day at a time, so a segment that crosses midnight is **one row
per day** sharing a `track_id`; `GROUP BY track_id` reassembles it.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of this day's piece. Partition column. |
| 2 | `mmsi` | long | yes | |
| 3 | `track_id` | string | yes | `<mmsi>-<start epoch ms>` of the segment's first piece; later day pieces inherit it. |
| 4 | `ts_end` | timestamptz | yes | End of this piece. |
| 5 | `duration_s` | double | yes | `ts_end − ts`. |
| 6 | `continues_previous` | boolean | yes | The vessel kept reporting through midnight and this piece inherited the previous day's `track_id`. |
| 7 | `chain_broken` | boolean | yes | The piece should have continued but the previous day's partition is missing, so it got an id of its own. |
| 8 | `n_rows` | int | yes | Reports in the piece (weighted by `n_raw`, so thinning does not change it). |
| 9 | `n_stream` | int | yes | Of those, positioned non-duplicate ones. |
| 10 | `n_duplicates` | int | yes | |
| 11 | `n_no_position` | int | yes | |
| 12 | `n_jumps` | int | yes | Rows flagged `is_speed_jump`. |
| 13 | `n_spikes` | int | yes | Rows flagged `is_spike`. |
| 14 | `n_outliers` | int | yes | Rows flagged `is_outlier`. |
| 15–16 | `start_lat`, `start_lon` | double | no | First positioned point. |
| 17–18 | `end_lat`, `end_lon` | double | no | Last positioned point. |
| 19 | `distance_nm_raw` | double | no | Sum of hops between positioned points, outliers included. |
| 20 | `distance_nm_clean` | double | no | The same leaving out every hop flagged `is_speed_jump` (a spike removes both its legs, so this slightly undercounts). |
| 21–24 | `min_lat`, `max_lat`, `min_lon`, `max_lon` | double | no | Bounding box over kept positioned points. |
| 25–28 | `clean_min_lat`, `clean_max_lat`, `clean_min_lon`, `clean_max_lon` | double | no | Bounding box leaving out `is_outlier` points. |
| 29 | `bbox_wraps` | boolean | yes | Longitude span exceeds 180°: the piece probably crosses the antimeridian and min/max longitude are not a usable box. |
| 30 | `mean_sog_knots` | double | no | Mean reported speed, invalid values excluded. |
| 31 | `max_sog_knots` | double | no | Max reported speed. |

A stretch with no positioned point of its own falls in no piece (the run prints
how many).

### stop_segments

Where vessels were stationary, one UTC day at a time, chained across midnight
like `tracks`. A point is stationary when its speed, averaged over a centred
window (`--smooth-minutes`, default 10), is under `--slow-kn` (default 0.5).
Stops come from movement only, never from declared nav status.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of the piece. Partition column. |
| 2 | `mmsi` | long | yes | |
| 3 | `stop_id` | string | yes | `<mmsi>-<start epoch ms>` of the stop's first piece; continuation pieces inherit it. |
| 4 | `ts_end` | timestamptz | yes | |
| 5 | `duration_s` | double | yes | |
| 6 | `continues_previous` | boolean | yes | Chained from the previous day. |
| 7 | `open_at_day_end` | boolean | yes | The run reaches the vessel's last point of the day and may continue tomorrow. Runs shorter than `--min-stop-minutes` (30) are kept when they touch midnight for this reason. |
| 8 | `n_points` | int | yes | Points in the piece. |
| 9 | `lat` | double | yes | Centroid latitude. |
| 10 | `lon` | double | yes | Centroid longitude (averaged circularly, so stops near the antimeridian are right). |
| 11 | `radius_nm` | double | yes | Largest distance of any point from the centroid; a large radius means a drifting vessel. |
| 12 | `n_moored` | int | yes | Points whose declared nav status is `moored`. |
| 13 | `n_anchored` | int | yes | Points whose nav status is `at anchor`. |
| 14 | `mean_speed_kn` | double | no | Mean speed over the piece. |

### stops

One row per stop: the `stop_segments` of a stop merged, then matched to the
nearest port in the latest `ref_ports` release within a radius by harbour size
(Large 15 nm, Medium 10, Small 6, Very Small / unknown 4). Partitioned by the
day the stop **departs** (`depart_ts`); a stop still in progress advances one
partition per day, so a finished stop never moves.

There is no "vessel is still here" column: it would go stale. Derive it from
the vessel's `last_seen` in `vessels`.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `stop_id` | string | yes | Same id as its `stop_segments` pieces. |
| 2 | `mmsi` | long | yes | |
| 3 | `arrive_ts` | timestamptz | yes | Start of the first piece. |
| 4 | `depart_ts` | timestamptz | yes | End of the last piece (latest data so far if the stop is ongoing). Partition column. |
| 5 | `duration_s` | double | yes | |
| 6 | `n_segments` | int | yes | Day pieces merged. |
| 7 | `n_points` | long | yes | |
| 8 | `lat` | double | yes | Centroid. |
| 9 | `lon` | double | yes | |
| 10 | `radius_nm` | double | yes | Largest piece radius. |
| 11 | `n_moored` | long | yes | |
| 12 | `n_anchored` | long | yes | Use with `n_moored` and `duration_s` to tell a berth from an anchorage. |
| 13 | `port_id` | long | no | World Port Index number of the nearest port in range. Null for an anchorage, offshore platform, or anywhere with no port in range. |
| 14 | `port_name` | string | no | |
| 15 | `port_unlocode` | string | no | UN/LOCODE, e.g. `NOBGO`. |
| 16 | `port_country` | string | no | |
| 17 | `port_distance_nm` | double | no | Distance from the stop centroid to the port. |
| 18 | `port2_id` | long | no | Runner-up port. |
| 19 | `port2_distance_nm` | double | no | |
| 20 | `wpi_release` | string | no | The `ref_ports` release matched against, so a match is reproducible. |
| 21 | `computed_at` | timestamptz | yes | When the row was (re)computed. |

The `port_*` columns describe *proximity*, not a confirmed port call.

### voyages and open_voyages

The legs between a vessel's consecutive stops. Every stretch of a vessel's
observed life belongs to exactly one leg: the leg before its first stop
(`origin_known` false), legs between stops, the leg after its last stop once it
has moved on, or, for a vessel that never stopped, one leg with neither end
known. **Both tables have the same columns.** `voyages` holds legs that have
ended (written once, partitioned by the day of `arrive_ts`, never changed);
`open_voyages` holds legs still under way and is replaced each run.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `voyage_id` | string | yes | `<mmsi>-<depart epoch ms>`. |
| 2 | `mmsi` | long | yes | |
| 3 | `depart_ts` | timestamptz | yes | When the vessel left the origin stop (or was first seen). |
| 4 | `arrive_ts` | timestamptz | no | Null on an open leg. |
| 5 | `duration_s` | double | no | Null on an open leg. |
| 6 | `origin_known` | boolean | yes | The leg starts at an observed stop. |
| 7 | `dest_known` | boolean | yes | The leg ends at an observed stop. |
| 8 | `is_open` | boolean | yes | Leg not yet ended. |
| 9 | `origin_stop_id` | string | no | Join to `stops.stop_id`. |
| 10 | `dest_stop_id` | string | no | |
| 11–12 | `origin_lat`, `origin_lon` | double | no | Origin stop centroid. |
| 13–17 | `origin_port_id`, `origin_port_name`, `origin_unlocode`, `origin_country`, `origin_port_distance_nm` | long, string, string, string, double | no | Origin stop's matched port. |
| 18–19 | `dest_lat`, `dest_lon` | double | no | Destination stop centroid **as it stood when the leg ended**. |
| 20–24 | `dest_port_id`, `dest_port_name`, `dest_unlocode`, `dest_country`, `dest_port_distance_nm` | long, string, string, string, double | no | Destination stop's matched port. |
| 25 | `distance_nm_raw` | double | no | Distance travelled along the leg, outliers included. |
| 26 | `distance_nm_clean` | double | no | Same, excluding speed-jump hops. |
| 27 | `avg_speed_kn` | double | no | Distance ÷ duration; null on open legs. |
| 28 | `max_sog_knots` | double | no | |
| 29 | `n_points` | long | yes | Track points along the leg. |
| 30 | `n_gaps` | long | yes | Reporting gaps along the leg. |
| 31 | `n_outliers` | long | yes | Outlier points along the leg. |
| 32 | `declared_destination` | string | no | The destination text the vessel reported most often while the leg lasted (closed legs only). |
| 33 | `n_declared_destinations` | long | no | How many different destinations it reported. |
| 34 | `declared_eta` | timestamptz | no | Latest ETA reported for it (year unreliable, see `statics.eta`). |
| 35 | `declared_matches_dest` | boolean | no | Text heuristic comparing `declared_destination` with the reached port's UN/LOCODE and name; null when either side is missing. A hint, not a verdict. |
| 36 | `computed_at` | timestamptz | yes | |

Because legs are folded a day at a time, `dest_lat`, `dest_lon` and
`dest_port_distance_nm` reflect the destination stop on the day the leg ended;
`dest_stop_id`/`dest_port_*` are exact. Join `stops` on `dest_stop_id` for the
final figures. Declared destinations are counted per day (departure and arrival
days in full), and `open_voyages` has none.

### vessels

One row per MMSI seen in either input, rebuilt incrementally from the daily
tables. Current identity values are the rank-1 rows of `vessel_attributes`.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `mmsi` | long | yes | |
| 2 | `mmsi_class` | string | yes | From the MMSI range: `ship` (200000000–799999999), `handheld` (8xxxxxxxx), `distress_beacon` (970–974xxxxxx), `craft_associated` (982–987xxxxxx), `aton` (992–997xxxxxx), `sar_aircraft` (1112xxxxx–1117xxxxx), `group` (020000000–079999999), `coast_station` (002000000–007999999), else `other`. |
| 3 | `mid` | int | no | Maritime Identification Digits: the 3-digit flag-state / country code embedded in the MMSI; null for `other`. |
| 4 | `vessel_key` | string | yes | `imo:<imo>` when the IMO number is valid, else `mmsi:<mmsi>`: a steadier identity across MMSI changes. |
| 5 | `imo_number` | int | no | Current IMO number. |
| 6 | `call_sign` | string | no | Current call sign. |
| 7 | `name` | string | no | Current name. |
| 8 | `ship_type` | string | no | Current ship-type category (as in `statics`). |
| 9 | `length_m` | int | no | bow + stern dimensions. |
| 10 | `beam_m` | int | no | port + starboard dimensions. |
| 11 | `ais_class` | string | no | `Class A` / `Class B` (from statics). |
| 12 | `first_seen` | timestamptz | no | First position report. |
| 13 | `last_seen` | timestamptz | no | Last position report. |
| 14 | `n_positions` | long | yes | Position reports (duplicates and position-less ones included). |
| 15 | `first_static_seen` | timestamptz | no | |
| 16 | `last_static_seen` | timestamptz | no | |
| 17 | `n_statics` | long | yes | |
| 18 | `mmsi_valid` | boolean | yes | `mmsi_class <> 'other'`. |
| 19 | `imo_valid` | boolean | yes | IMO passes its check digit (first six digits weighted 7…2, last digit of the sum equals the seventh digit). |
| 20 | `multiple_imos` | boolean | yes | More than one distinct IMO was reported; a sign of a spoofed or reused MMSI. |
| 21 | `multiple_names` | boolean | yes | |
| 22 | `multiple_call_signs` | boolean | yes | |
| 23 | `computed_at` | timestamptz | yes | |
| 24 | `folded_through` | int | yes | Day number of the last day included. |

### vessel_attributes

Every distinct value a vessel has ever reported for an identity attribute, so
nothing is decided away. `@` padding and blanks count as "not reported".

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `mmsi` | long | yes | |
| 2 | `attribute` | string | yes | One of `name`, `call_sign`, `imo`, `ship_type`, `length_m`, `beam_m`. |
| 3 | `value` | string | yes | The value, as text (numbers included). |
| 4 | `n_obs` | long | yes | How many times it was reported. |
| 5 | `first_seen` | timestamptz | yes | |
| 6 | `last_seen` | timestamptz | yes | |
| 7 | `rank` | int | yes | 1 = current. Order: for `imo`, a value passing its check digit beats one that does not; then most reports; then most recent; then alphabetical. |
| 8 | `is_current` | boolean | yes | `rank = 1`. |
| 9 | `folded_through` | int | yes | Day number of the last day included. |
| 10 | `computed_at` | timestamptz | yes | |

### ref_ports

NGA World Port Index (Publication 150), appended per release with
`ais-tracks ports load --release <label>`. Never overwritten; a release cannot
be loaded twice, and matching uses the greatest label (use sortable labels such
as dates). Every port in the file is kept, including those without coordinates.
`port_id` is not unique in the source (a duplicate spelling, two terminals).

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `wpi_release` | string | yes | Release label. |
| 2 | `loaded_at` | timestamptz | yes | |
| 3 | `port_id` | long | yes | World Port Index number. |
| 4 | `name` | string | no | Main port name. |
| 5 | `alt_name` | string | no | |
| 6 | `unlocode` | string | no | UN/LOCODE; blank in the source becomes null. |
| 7 | `country` | string | no | Country code. |
| 8 | `region` | string | no | |
| 9 | `harbor_size` | string | no | Large / Medium / Small / Very Small; drives the matching radius. |
| 10 | `harbor_type` | string | no | |
| 11 | `harbor_use` | string | no | |
| 12–13 | `latitude`, `longitude` | double | no | |
| 14 | `channel_depth_m` | double | no | |
| 15 | `max_vessel_draft_m` | double | no | |
| 16 | `tidal_range_m` | double | no | |

### Daily tables

Built by the reduce pass and by `statics-daily`; `vessels`, `vessel_attributes`
and `voyages` are folds of them. All are partitioned by day on `ts`, which is the
**start of the UTC day**.

#### vessel_daily

One row per vessel per day: identity counts plus the day's movement totals
(from `track_points`), so a leg lying wholly inside a day can be totalled without
reading points.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of day. |
| 2 | `mmsi` | long | yes | |
| 3 | `first_seen` | timestamptz | yes | First report that day, of any kind. |
| 4 | `last_seen` | timestamptz | yes | |
| 5 | `n_positions` | long | yes | Reports that day, duplicates and position-less included. |
| 6 | `first_stream_ts` | timestamptz | no | First positioned, non-duplicate row. |
| 7 | `last_stream_ts` | timestamptz | no | |
| 8 | `n_points` | long | yes | Stream points (weighted, thinning-independent). |
| 9 | `dist_nm_raw` | double | yes | Distance that day, outliers included. |
| 10 | `dist_nm_clean` | double | yes | Excluding speed-jump hops. |
| 11 | `n_gaps` | long | yes | |
| 12 | `n_outliers` | long | yes | |
| 13 | `max_sog_knots` | double | no | |

#### attribute_daily

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of day. |
| 2 | `mmsi` | long | yes | |
| 3 | `attribute` | string | yes | As in `vessel_attributes`. |
| 4 | `value` | string | yes | |
| 5 | `n_obs` | long | yes | Times reported that day. |
| 6 | `first_seen` | timestamptz | yes | |
| 7 | `last_seen` | timestamptz | yes | |

#### static_daily

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of day. |
| 2 | `mmsi` | long | yes | |
| 3 | `first_static_seen` | timestamptz | yes | |
| 4 | `last_static_seen` | timestamptz | yes | |
| 5 | `n_statics` | long | yes | Static reports that day. |
| 6 | `ais_class` | string | no | |

#### destination_daily

The destinations a vessel declared on a day; feeds `voyages.declared_*`.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of day. |
| 2 | `mmsi` | long | yes | |
| 3 | `destination` | string | yes | Declared text, trailing `@`/spaces removed, blanks skipped. |
| 4 | `n` | long | yes | Times declared that day. |
| 5 | `last_ts` | timestamptz | yes | When it was last declared that day. |
| 6 | `eta` | timestamptz | no | Latest ETA given with it (year unreliable). |

### Bookkeeping tables

Ordinary tables you rarely need to query; they make the daily flow incremental
and idempotent.

#### vessel_state

Each vessel's last positioned report as of the end of each built day. The next
day reads this one small partition instead of rescanning the previous output.
Partitioned by day on `ts`.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `ts` | timestamptz | yes | Start of the day the state is as of. |
| 2 | `mmsi` | long | yes | |
| 3 | `last_ts` | timestamptz | yes | Time of the last positioned report. |
| 4 | `lat` | double | yes | |
| 5 | `lon` | double | yes | |

#### voyage_state

One row per vessel: where its current leg began and its totals so far, so a day
only adds to them. Replaced each run. Internal.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `mmsi` | long | yes | |
| 2 | `first_ts` | timestamptz | yes | First positioned row ever seen. |
| 3 | `last_ts` | timestamptz | yes | Last positioned row seen. |
| 4 | `from_ts` | timestamptz | yes | Where the current leg began: the last stop's departure, or `first_ts`. |
| 5 | `origin_stop_id` | string | no | Last stop the vessel left. |
| 6–7 | `origin_lat`, `origin_lon` | double | no | |
| 8–12 | `origin_port_id`, `origin_port_name`, `origin_unlocode`, `origin_country`, `origin_port_distance_nm` | long, string, string, string, double | no | |
| 13 | `dist_nm_raw` | double | yes | Running totals for the leg. |
| 14 | `dist_nm_clean` | double | yes | |
| 15 | `max_sog_knots` | double | no | |
| 16 | `n_points` | long | yes | |
| 17 | `n_gaps` | long | yes | |
| 18 | `n_outliers` | long | yes | |
| 19 | `through` | int | yes | Day number of the last day folded in. |

#### build_log

One row appended per step and day **after** that day's data is committed, so a
day counts as built only once its row exists. The latest row for a
`(step, day)` wins.

| ID | Column | Type | Req. | Description |
|----|--------|------|------|-------------|
| 1 | `step` | string | yes | `track_points`, `tracks`, `stop_segments`, `vessel_daily`, `statics_daily`, `vessels`, `stops`, or `voyages`. |
| 2 | `day` | int | yes | Day number of the day built. |
| 3 | `input_token` | string | yes | Fingerprint of what the day was built from (silver row count + digest of starting vessel states for `track_points`; the upstream build for `tracks`/`stop_segments`/`stops`). A day is rebuilt when the token it would have now differs from the logged one. |
| 4 | `output_rows` | long | yes | Rows written. |
| 5 | `built_at` | timestamptz | yes | |

---

## Querying notes

- **Reassemble a multi-day track or stop:** `GROUP BY track_id` (or `stop_id`).
- **Separate feeds:** filter silver on `source`; it is not a partition column.
- **Best position table for analysis:** `track_points` if you want annotated,
  thinned data with duplicate and outlier flags; silver `positions` if you
  want every report.
- **Mean speed over a thinned point set:** `sum(sum_speed) / sum(n_speed)`, not
  `avg(implied_speed_kn)`.
- **Exact distance:** `sum(dist_nm)` over `track_points` is exact thinned or not.
- **Current vessel identity:** `vessels`, or `vessel_attributes WHERE is_current`.
- **`h3` and `hilbert` in Iceberg** are signed `long`. H3 ids never use the top
  bit, so they read back unchanged; a `hilbert` value is also within `i64` range.
