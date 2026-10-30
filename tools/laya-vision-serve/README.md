# laya-vision-serve

A small FastAPI service that puts the community
[laya-vision](https://huggingface.co/savita10/laya-vision) model behind a Jev-style
`POST /v1/systemone` endpoint that takes **images**. `vessel-imagery` calls it to pull
typed visual attributes (ship type, colours, funnel count, cranes, …) out of vessel photos.

## Why not `laya-serve`?

`laya-serve` 0.2.1 (PyPI, the official Jev-compatible server for Laya) is text-only: its
`SystemOneRequest` is `{state, model, questions}` with `extra="forbid"`, and `state` is
text/dict/list. It has no `assets` field and never calls the laya-vision SDK, so it cannot
answer questions about an image. Hence this wrapper.

## Run it

```bash
cd tools/laya-vision-serve
uv venv --python 3.12 .venv
git clone https://github.com/savita10/laya-vision .deps/laya-vision
uv pip install --python .venv/bin/python -r requirements.txt -e .deps/laya-vision
# ~1 GB of weights, pinned to the revision this was tested with
.venv/bin/python -c "from huggingface_hub import snapshot_download; snapshot_download('savita10/laya-vision', local_dir='release', revision='f73afb190e9888113fd6153a35079e4246973c68')"
.venv/bin/python app.py
```

Startup loads the model and runs one warm-up prediction, which on the first ever run downloads
the SigLIP encoder (`google/siglip-base-patch16-384`) into the Hugging Face cache (took ~4 min
here); after that it runs offline. `/healthz` reports `model_loaded: true` only once that is
done. Measured on an Apple-silicon laptop: ~0.3 s per request (one image, two questions) warm.

| env | default | |
|---|---|---|
| `LAYA_RELEASE_DIR` | `./release` | laya-vision release folder (or a Hub repo id) |
| `HOST` | `127.0.0.1` | bind address |
| `PORT` | `8765` | |
| `LAYA_DEVICE` | SDK's choice | `cpu`, `mps` or `cuda` |

## API

`GET /healthz` → `{"status": "ok", "model_loaded": true}`

`POST /v1/systemone`

```json
{
  "model": "laya-vision",
  "assets": {"ship": {"media_type": "image/jpeg", "data": "<base64>"}},
  "state": "asset:ship",
  "questions": [
    {"id": "ship_type", "type": "choice", "text": "What type of vessel is shown?",
     "criteria": ["container ship", "tanker", "bulk carrier"]},
    {"id": "has_cranes", "type": "yes_no", "text": "Does the vessel have deck cranes?", "criteria": []},
    {"id": "rust", "type": "score", "text": "How rusty is the hull?", "criteria": ["none", "some", "heavy"]}
  ]
}
```

```json
{
  "model": "laya-vision",
  "answers": {
    "ship_type":  {"choice": "container ship", "value": null, "probabilities": {"container ship": 0.41, "tanker": 0.3, "bulk carrier": 0.29}, "calibrated": false},
    "has_cranes": {"choice": "no", "value": null, "probabilities": {"yes": 0.48, "no": 0.52}, "calibrated": false},
    "rust":       {"choice": null, "value": 0.37, "probabilities": {"none": 0.4, "some": 0.45, "heavy": 0.15}, "calibrated": false}
  }
}
```

- `model` in the response echoes the request.
- `choice`: the SDK's choice question; `choice` is the argmax label, `value` is null.
- `yes_no`: sent to the SDK as a choice over `["yes", "no"]` (any `criteria` given are ignored);
  `choice` is `"yes"` or `"no"`, `value` is null.
- `score`: the SDK's native score question over the criteria as ordered levels. `value` is the
  probability-weighted level index divided by `len(criteria) - 1`, so 0..1; `probabilities` is
  keyed by level label; `choice` is null.
- `calibrated` passes through the SDK: true only for its trained RVL-CDIP `doc_type` question,
  so false for every vessel question.

400 for: a `state` that is not `asset:<id>` or names a missing asset; any asset the state does
not reference; a media type other than `image/jpeg`/`image/png`, or data that is not that
format; invalid base64; an image over 16 MiB or with either side outside 256–1568 px; an empty
choice, a score with fewer than two levels, duplicate criteria or question ids; and any schema
error. 413 for a body over 32 MiB. 503 until the model has loaded.

One request runs at a time (a lock around the forward pass).

## Caveats — read before trusting the answers

laya-vision was trained only on RVL-CDIP (scanned 1960s–90s business documents). Its card
says non-document images "yield meaningless but confident-looking answers" and that new
questions are uncalibrated with unmeasured accuracy. Ship photos are far out of distribution,
so treat these answers as experimental until they are evaluated against labelled vessel
photos. `calibrated: false` on every answer is the honest signal.

The smoke test shows this. On the Koper container-ship photo, the model ranked "passenger ship"
first (0.25) and "container ship" only fourth (0.17), and it answered "yes" to deck cranes with
0.98 when the ship has none. The pipe works, but the answers are not yet usable.

### Batch test (2026-10-18): the model can't see ships

I ran 14 Commons images through the full `vessel-imagery` v1 question set (33 questions):
11 ships of different types, plus 3 paintings or close-ups as gate negatives. I hand-labelled
the unambiguous answers (70 labels). Results:

- **14/70 correct (20%)**, and every hit comes from a constant answer: `view` was "broadside"
  12/14 times and `single_ship` was "yes" 14/14.
- `ship_type` was "other" on **all 14** images (0/11 correct). The right type never got more
  than 0.06.
- `photo_kind` was 0/14: ship photos came back as "no ship" or "a drawing, painting or model",
  so **the v1 gate rejected every image**.
- `containers_on_deck` was "yes" for 9/14 images, including the cruise ship, ferry and icebreaker,
  and "no" for the actual container ship.
- Requests took about 0.5 s each for 33 questions.

Calling `VisionAgent.predict` directly gives the same probabilities (max difference 0.0000),
so this service passes answers through unchanged; the limit is the model. This matches the
model card: SigLIP features go through a projector trained only on RVL-CDIP document scans.
It needs a vision head trained on vessel photos, or a different model, before `vessel-imagery`
can use it.

Rerun it with:

```bash
.venv/bin/python scripts/batch_run.py IMAGE_DIR \
  --questions ../../crates/vessel-imagery/config/questions/v1.toml \
  --labels labels.csv --out results.jsonl
```

`labels.csv` is `image,question,expected`, where `image` is the file stem.

## Tests

```bash
.venv/bin/python -m pytest tests/test_contract.py   # wire contract, fake model, no weights needed
.venv/bin/python tests/test_smoke.py                # against a running server: ship JPEG, 2 questions
```

`tests/fixtures/container_ship_koper.jpg`: "Container ship in Koper 2013" from Wikimedia
Commons, CC0, resized to 1024×682.
