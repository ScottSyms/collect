"""laya-vision-serve: a Jev-style POST /v1/systemone over the laya-vision SDK.

laya-serve (0.2.1) speaks only text state, so this wraps `laya_vision.VisionAgent` for images.

Request:  {"model", "assets": {id: {"media_type", "data"}}, "state": "asset:<id>",
           "questions": [{"id", "type": "choice"|"yes_no"|"score", "text", "criteria"}]}
Response: {"model", "answers": {id: {"choice", "value", "probabilities", "calibrated"}}}

Config (env): LAYA_RELEASE_DIR (default ./release), HOST (127.0.0.1), PORT (8765),
LAYA_DEVICE (optional: cpu / mps / cuda; the SDK picks one when unset).
"""

from __future__ import annotations

import base64
import binascii
import io
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from PIL import Image, UnidentifiedImageError
from pydantic import BaseModel, ConfigDict, Field

MAX_BODY_BYTES = 32 * 1024 * 1024
MAX_IMAGE_BYTES = 16 * 1024 * 1024
MIN_SIDE, MAX_SIDE = 256, 1568
MEDIA_TYPES = {"image/jpeg": "JPEG", "image/png": "PNG"}
STATE_PREFIX = "asset:"
YES_NO = ["yes", "no"]

# Pillow's bomb check would fire long after our own 1568 px limit; keep it as a backstop.
Image.MAX_IMAGE_PIXELS = 64 * 1024 * 1024


# --- schemas ------------------------------------------------------------------------


class Asset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    media_type: str
    data: str


class Question(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    type: Literal["choice", "yes_no", "score"]
    text: str = Field(min_length=1)
    criteria: list[str] = Field(default_factory=list)


class SystemOneRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model: str = Field(min_length=1)
    assets: dict[str, Asset]
    state: str
    questions: list[Question] = Field(min_length=1)


class Answer(BaseModel):
    choice: str | None
    value: float | None
    probabilities: dict[str, float]
    calibrated: bool


class SystemOneResponse(BaseModel):
    model: str
    answers: dict[str, Answer]


# --- validation ---------------------------------------------------------------------


def bad_request(msg: str) -> HTTPException:
    return HTTPException(status_code=400, detail=msg)


def resolve_image(req: SystemOneRequest) -> Image.Image:
    """The one image the state references, decoded and checked; 400 on anything else."""
    if not req.state.startswith(STATE_PREFIX) or len(req.state) == len(STATE_PREFIX):
        raise bad_request(f"state must be '{STATE_PREFIX}<asset_id>'")
    asset_id = req.state[len(STATE_PREFIX):]
    if asset_id not in req.assets:
        raise bad_request(f"state references missing asset {asset_id!r}")
    unreferenced = sorted(set(req.assets) - {asset_id})
    if unreferenced:
        raise bad_request(f"unreferenced assets: {unreferenced}")

    asset = req.assets[asset_id]
    fmt = MEDIA_TYPES.get(asset.media_type)
    if fmt is None:
        raise bad_request(f"asset {asset_id!r}: media_type must be one of {sorted(MEDIA_TYPES)}")
    # base64 inflates by 4/3; reject an oversized payload before decoding it.
    if len(asset.data) > (MAX_IMAGE_BYTES * 4) // 3 + 4:
        raise bad_request(f"asset {asset_id!r}: image exceeds {MAX_IMAGE_BYTES} bytes")
    try:
        raw = base64.b64decode(asset.data, validate=True)
    except (binascii.Error, ValueError):
        raise bad_request(f"asset {asset_id!r}: data is not valid base64") from None
    if len(raw) > MAX_IMAGE_BYTES:
        raise bad_request(f"asset {asset_id!r}: image exceeds {MAX_IMAGE_BYTES} bytes")
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except (UnidentifiedImageError, OSError, Image.DecompressionBombError) as e:
        raise bad_request(f"asset {asset_id!r}: cannot decode image ({e})") from None
    if img.format != fmt:
        raise bad_request(f"asset {asset_id!r}: declared {asset.media_type} but data is {img.format}")
    w, h = img.size
    if not (MIN_SIDE <= w <= MAX_SIDE and MIN_SIDE <= h <= MAX_SIDE):
        raise bad_request(f"asset {asset_id!r}: {w}x{h} px; each side must be {MIN_SIDE}-{MAX_SIDE} px")
    return img


def to_sdk_questions(questions: list[Question]) -> tuple[dict[str, dict[str, Any]], dict[str, list[str]]]:
    """Wire questions -> SDK question dict, plus each question's criteria labels in order."""
    sdk: dict[str, dict[str, Any]] = {}
    labels: dict[str, list[str]] = {}
    for q in questions:
        if q.id in sdk:
            raise bad_request(f"duplicate question id {q.id!r}")
        crit = YES_NO if q.type == "yes_no" else q.criteria
        if q.type == "choice" and not crit:
            raise bad_request(f"question {q.id!r}: a choice question needs at least one criterion")
        if q.type == "score" and len(crit) < 2:
            raise bad_request(f"question {q.id!r}: a score question needs at least two criteria")
        if len(set(crit)) != len(crit) or any(not c.strip() for c in crit):
            raise bad_request(f"question {q.id!r}: criteria must be distinct, non-empty labels")
        sdk_type = "score" if q.type == "score" else "choice"
        sdk[q.id] = {"type": sdk_type, "instructions": q.text, "criteria": list(crit)}
        labels[q.id] = list(crit)
    return sdk, labels


def to_wire_answer(qtype: str, ans: dict[str, Any], labels: list[str]) -> Answer:
    calibrated = bool(ans.get("calibrated", False))
    if qtype == "score":
        # SDK: probabilities keyed "0".."k-1", score = probability-weighted level index.
        probs = {labels[int(i)]: float(p) for i, p in ans["probabilities"].items()}
        value = float(ans["score"]) / (len(labels) - 1)
        return Answer(choice=None, value=min(1.0, max(0.0, value)), probabilities=probs,
                      calibrated=calibrated)
    return Answer(choice=ans["choice"], value=None,
                  probabilities={k: float(v) for k, v in ans["probabilities"].items()},
                  calibrated=calibrated)


# --- app ----------------------------------------------------------------------------


def load_agent():
    from laya_vision import VisionAgent

    release = os.environ.get("LAYA_RELEASE_DIR", str(Path(__file__).parent / "release"))
    agent = VisionAgent(release_dir=release, device=os.environ.get("LAYA_DEVICE") or None)
    # The SDK loads SigLIP lazily on the first image; do it now so /healthz means "ready".
    agent.predict(image=Image.new("RGB", (MIN_SIDE, MIN_SIDE), "white"),
                  questions={"warmup": {"type": "choice", "instructions": "warm up", "criteria": ["a", "b"]}})
    return agent


def create_app(agent_factory=load_agent) -> FastAPI:
    state: dict[str, Any] = {"agent": None}
    lock = threading.Lock()  # one forward at a time; the SDK's model is not re-entrant

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        state["agent"] = agent_factory()
        yield

    app = FastAPI(title="laya-vision-serve", lifespan=lifespan)

    @app.middleware("http")
    async def limit_body(request: Request, call_next):
        length = request.headers.get("content-length")
        if length is not None and length.isdigit() and int(length) > MAX_BODY_BYTES:
            return JSONResponse({"detail": f"body exceeds {MAX_BODY_BYTES} bytes"}, status_code=413)
        if request.method == "POST" and length is None:
            body = await request.body()  # chunked: measure what arrived (Starlette caches it)
            if len(body) > MAX_BODY_BYTES:
                return JSONResponse({"detail": f"body exceeds {MAX_BODY_BYTES} bytes"}, status_code=413)
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def validation_400(_request: Request, exc: RequestValidationError):
        return JSONResponse({"detail": exc.errors()}, status_code=400)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok" if state["agent"] is not None else "loading",
                "model_loaded": state["agent"] is not None}

    @app.post("/v1/systemone", response_model=SystemOneResponse)
    def systemone(req: SystemOneRequest) -> SystemOneResponse:
        agent = state["agent"]
        if agent is None:
            raise HTTPException(status_code=503, detail="model not loaded")
        img = resolve_image(req)
        sdk_questions, labels = to_sdk_questions(req.questions)
        try:
            with lock:
                result = agent.predict(image=img, questions=sdk_questions)
        except ValueError as e:  # the SDK names the question and what to fix
            raise bad_request(str(e)) from None
        types = {q.id: q.type for q in req.questions}
        answers = {qid: to_wire_answer(types[qid], ans, labels[qid])
                   for qid, ans in result["answers"].items()}
        return SystemOneResponse(model=req.model, answers=answers)

    return app


app = create_app()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", "8765")))
