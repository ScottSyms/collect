# vessel-imagery

Ship photos for the vessels in the [identity registry](VESSEL_IDENTITY.md), and what each ship *looks like*: its type, colours, funnels, masts, cranes and other features you could use to recognize it. Photos come from Wikimedia Commons (free licences), found by IMO category or through Wikidata's IMO and MMSI links, and from collections you already hold (restricted licences). You can look a vessel up by IMO number, MMSI or registry id. A **System 1 model** reads each photo, and its answers are rolled up into one visual profile per vessel.

Tables go to namespace `imagery` in warehouse `maritimedata`. The registry is read from namespace `identity`. Every command that writes is a dry run unless you pass `--apply`.

## Principles

- **Photos are tied to vessels by persistent identifiers only.** That means IMO numbers on Commons, IMO numbers or MMSIs on Wikidata, and IMO, MMSI or the registry's `vessel_id` in a manifest. Names are never used: a ship name isn't an identity, and the registry has the same rule. An MMSI that more than one vessel has carried ties a photo to nothing.
- **Licence travels with every photo.** Each row in `vessel_images` records the licence, its class, whether attribution is required, the author or credit, and the page the photo came from. Restricted photos are stored under a separate prefix and flagged at every level, including in the profile (`restricted_only`).
- **The model answers questions; it doesn't describe.** System 1 models (Laya, Jev) are non-generative. They pick one label from a fixed set, with a probability. Every visual feature worth recording is therefore a typed question in a versioned question set. The model can't read the ship's name off the hull or describe a one-off feature in prose.
- **Answers are evidence, not facts, until the eval says otherwise.** Image support for Laya comes from the community `laya-vision` adapter, which was trained and measured only on classifying document types. On ship photos its accuracy is unmeasured and its probabilities aren't calibrated, so every observation is stored with `calibrated = false`. Run [the eval](#eval) before you rely on a question.

## Quick start

```bash
./run_imagery_rustfs.sh init --apply
./run_imagery_rustfs.sh fetch wikidata --contact you@example.org --limit 500 --apply
./run_imagery_rustfs.sh fetch commons --contact you@example.org --limit 100 --apply
./run_imagery_rustfs.sh import --manifest ~/held-photos/manifest.csv --apply
# start a System 1 server (see "The model") on :8765, then
./run_imagery_rustfs.sh describe --limit 50 --apply
./run_imagery_rustfs.sh profile --apply
./run_imagery_rustfs.sh lookup --imo 9186405
./run_imagery_rustfs.sh lookup --mmsi 316001325
```

`fetch`, `lookup` and the others that pick a vessel take one of `--imo`, `--mmsi` or `--vessel-id`. An MMSI that more than one registry vessel has carried is refused rather than guessed.

## Sources

### Wikimedia Commons (`fetch commons`)

Commons keeps a `Category:IMO <number>` for many ships. The ship's own category (for example `Category:A2B Independent (ship, 1998)`) usually sits inside it, and the photos are filed in that. `fetch commons` works through every vessel in the registry that has a valid IMO number. It also tries any former IMO numbers the vessel has carried. For each one it walks the IMO category and up to `--depth` levels of subcategories (default 2), but never steps into another IMO category. It stops after `--per-vessel` photos (default 10).

It only takes raster files (JPEG, PNG, TIFF, WebP). What it stores is a thumbnail `--thumb-width` pixels wide (default 1280), which is what the model takes anyway.

Licence and credit come from Commons' `extmetadata` fields:

| Class | From |
|---|---|
| `public_domain` | `Public domain`, `PD-*`, or `Copyrighted: False` |
| `cc0` | `CC0` |
| `cc_by` | `CC BY *` (but not NC or ND variants) |
| `cc_by_sa` | `CC BY-SA *` |
| `other_free` | GFDL, FAL, OGL, `Attribution`, `Copyrighted free use` |
| `unknown` | anything else, including NC and ND licences. These photos are kept but left out of profiles unless `--include-unknown-licence` |

Follow Wikimedia's API etiquette:

- A User-Agent with contact details. `--contact` or `VESSEL_IMAGERY_CONTACT` is required; give an email address or URL.
- One request at a time, with `--delay-ms` between requests (default 1000).
- `maxlag=5`, and honour `Retry-After` when told to wait.
- Stop at `--max-requests` per run (default 2000).

Vessels that already have Commons photos are skipped unless you pass `--refresh`, `--imo` or `--vessel-id`, so repeated runs work through the registry bit by bit. A full pass over the roughly 124,000 registry vessels takes 2–3 requests each, which is several days at one request a second. Run it in batches.

### Wikidata (`fetch wikidata`)

Wikidata has items for about 26,000 ships that carry both an image (P18) and an IMO number (P458) or MMSI (P587). `fetch wikidata` gets all of them from the Wikidata query service in a handful of paged SPARQL queries (5,000 rows each). That is much faster than walking Commons categories ship by ship, and it reaches ships that are known only by MMSI.

Each item is tied to one registry vessel, by a valid IMO number first and otherwise by an MMSI that only one vessel has carried. Items that match no vessel, or only a shared MMSI, are counted and left out. The image is always a Commons file, so licence, author and thumbnail come from Commons in batches of 50 titles. Licence classes are the same as for `fetch commons`, and so is `source` (`wikimedia_commons`); `match_basis` is `wikidata_imo` or `wikidata_mmsi`.

Files a vessel already has from either Commons route are skipped, so the two commands complement each other, and re-running `fetch wikidata` only picks up new links. The same etiquette applies: `--contact`, `--delay-ms` and `--max-requests` cover the query service and Commons together.

### Held photos (`import`)

A CSV manifest lists photos you already have: licensed sets, your own photography, or exports from restricted sites. Paths are relative to the manifest file.

| Column | Required | Meaning |
|---|---|---|
| `path` | yes | The image file (JPEG, PNG or WebP) |
| `imo` / `mmsi` / `vessel_id` | exactly one | Which vessel the photo shows |
| `source` | yes | Where it came from (for example `shipspotting_licensed`) |
| `licence` | yes | The licence or agreement, as text |
| `licence_url`, `attribution`, `author`, `source_url` | no | |
| `captured_at` | no | `YYYY-MM-DD` or RFC 3339 |
| `restricted` | no | Defaults to **true**. Set it to `false` only for a freely licensed photo; the licence is then classified as for Commons |

```csv
path,imo,source,licence,attribution,restricted
a2b/IMG_0412.jpg,9186405,own_photography,All rights reserved,A. Photographer,true
```

Any line that can't be tied to exactly one vessel, or whose file isn't a readable image, is reported and skipped.

## The model

`describe` sends each photo, with every question in the question set, to a Jev-style System 1 server:

```
POST <VESSEL_IMAGERY_SYSTEMONE_URL>/v1/systemone
TypeSafe-Danger-Zone-Images: true
{"model": "laya-vision",
 "assets": {"photo": {"media_type": "image/jpeg", "data": "<base64>"}},
 "state": "asset:photo",
 "questions": [{"id": "hull_colour", "type": "choice", "text": "...", "criteria": ["black", "red", ...]}, ...]}
→ {"answers": {"hull_colour": {"choice": "red", "probabilities": {...}, "calibrated": false}, ...}}
```

The server can be any of these:

- `tools/laya-vision-serve`, a small wrapper around the `laya-vision` Python SDK.
- `laya-serve`, if it accepts images.
- A hosted Jev endpoint, with `--systemone-token` and `--model`.

Before sending, `describe` checks the server's limits. Photos are sent as PNG or JPEG, 256–1568 px on each side and at most 16 MiB. Larger photos are shrunk, and photos too small to be useful are skipped. `--skip-restricted` keeps restricted photos away from a hosted model that their licence doesn't cover.

Photos already described by the same model with the same question-set version are skipped. Rows are written every 20 photos, so an interrupted run keeps most of its work.

## Question sets

The questions are in `crates/vessel-imagery/config/questions/<version>.toml`. `--config-dir <dir>` reads `<dir>/questions/<version>.toml` instead.

v1 asks 33 questions:

- **Gates:** whether the photo shows a ship's exterior, and whether one ship is the main subject. A photo that fails either gate counts toward nothing else.
- **Viewpoint.**
- **Type:** 18 classes.
- **Colours:** hull, boot topping, superstructure, funnel, and a contrasting funnel top.
- **Funnels:** a logo on the funnel, the number of funnels and how they're arranged.
- **Masts:** how many.
- **Layout:** where the superstructure sits, and whether the bridge wings are enclosed.
- **Cargo gear:** number and colour of cranes, derricks, containers on deck.
- **Fittings:** helideck, stern ramp, A-frame.
- **Hull form:** bulbous bow, ice bow, bow and stern shape.
- **Livery:** stripes, lettering, racing stripes, rust streaking.
- **Lifeboats.**
- **Deck clutter.**
- **Size estimate:** a score.

A question with `enabled = false` is still asked, but its answers stay out of the profile. Changing a question's wording or its options means making a new version file, so that earlier observations stay comparable.

## Profile

`profile` rebuilds `vessel_visual_profile` from one model's answers to one question-set version:

- Only photos that pass every gate are counted.
- For choice and yes/no questions, only answers with at least `min_probability` (0.5 in v1) are counted, and the most common answer wins.
- A row is marked `conflicting` when the runner-up has at least half the winner's support.
- `restricted_only` is set when every supporting photo is restricted.
- Score questions are averaged. Their `mean_probability` is 1 minus the standard deviation across photos.

## Eval

Hand-label photos in a CSV with the columns `image_id,question,expected`; `config/eval/v1.csv` is the template. Aim for about 100 photos across ship types. Then run:

```bash
./run_imagery_rustfs.sh eval --labels crates/vessel-imagery/config/eval/v1.csv --threshold 0.8
```

It prints the accuracy of each question and lists the questions that fall below the threshold. Set those to `enabled = false`, or make a new question-set version with better wording.

## Tables

All three tables are in namespace `imagery`. Photo bytes are stored at `s3://<bucket>/raw_archive/imagery/{free|restricted}/<aa>/<sha256>.<ext>`, outside any table's prefix so that table maintenance doesn't take them for orphans. `--archive-root` (`VESSEL_IMAGERY_ARCHIVE_ROOT`) moves them.

### vessel_images

One row per (photo, vessel). It is appended by `fetch` and `import`. It is unpartitioned, with bloom filters on `image_id`, `vessel_id` and `imo_number`.

| Column | Type | Req. | Description |
|---|---|---|---|
| `image_id` | string | ✓ | sha256 of the stored bytes |
| `vessel_id` | string | ✓ | The registry's vessel id |
| `imo_number`, `mmsi` | string | | The vessel's identifiers when recorded |
| `match_basis` | string | ✓ | `commons_imo_category`, `wikidata_imo`, `wikidata_mmsi`, `manifest_imo`, `manifest_mmsi`, `manifest_vessel_id` |
| `source` | string | ✓ | `wikimedia_commons` or the manifest's source |
| `source_page_url`, `original_url` | string | | The photo's page, and the full-size original |
| `object_uri` | string | ✓ | Where the bytes are stored |
| `mime`, `width`, `height` | string, int, int | | Of the stored bytes |
| `licence_short`, `licence_url` | string | | As the source gives them |
| `licence_class` | string | ✓ | See [Sources](#wikimedia-commons-fetch-commons); `restricted` for restricted imports |
| `restricted` | boolean | ✓ | |
| `attribution_required` | boolean | ✓ | |
| `author`, `credit` | string | | |
| `captured_at` | timestamptz | | When the photo was taken, if known |
| `retrieved_at` | timestamptz | ✓ | |

### image_observations

One model answer to one question about one photo. It is appended by `describe` and partitioned by `question_set_version`, with bloom filters on `image_id` and `vessel_id`.

| Column | Type | Req. | Description |
|---|---|---|---|
| `image_id`, `vessel_id` | string | ✓ | |
| `model_id` | string | ✓ | The `--model` asked |
| `question_set_version`, `question` | string | ✓ | |
| `answer` | string | ✓ | The label (`yes`/`no` for yes/no questions); the score as text for score questions |
| `probability` | double | | The label's probability; null for scores |
| `runner_up`, `runner_up_probability` | string, double | | Kept when the top label is below `runner_up_below` |
| `calibrated` | boolean | ✓ | As the server reports it |
| `observed_at` | timestamptz | ✓ | |

### vessel_visual_profile

One row per (vessel, attribute). It is replaced by `profile`. It is unpartitioned, with bloom filters on `vessel_id` and `imo_number`.

| Column | Type | Req. | Description |
|---|---|---|---|
| `vessel_id`, `imo_number` | string | ✓ / | |
| `attribute`, `value` | string | ✓ | The question id and the winning answer |
| `support_images`, `total_images` | int | ✓ | Photos that gave `value`, and photos that answered at all |
| `mean_probability` | double | ✓ | Over the supporting photos |
| `conflicting` | boolean | ✓ | The runner-up has at least half the support |
| `restricted_only` | boolean | ✓ | Only restricted photos support it |
| `model_id`, `question_set_version` | string | ✓ | |
| `updated_at` | timestamptz | ✓ | |

## Not built yet

- Other free sources: Openverse and Flickr's Creative Commons search (matched by `IMO <n>` in tags, so weaker), and national archives.
- Free-text descriptions, or reading the name and IMO number off the hull. These need a generative vision model; a System 1 model can't do either.
- Using the photos to check identity, for example flagging a vessel whose profile contradicts its registered type.
- Hand labels for the eval set.
