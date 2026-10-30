"""Wire-contract tests against a fake agent: no model download needed."""

import base64
import io
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from PIL import Image

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import MAX_BODY_BYTES, create_app  # noqa: E402

FIXTURE = Path(__file__).parent / "fixtures" / "container_ship_koper.jpg"


class FakeAgent:
    """Returns SDK-shaped answers: uniform probabilities, choice = first option."""

    def __init__(self):
        self.calls = []

    def predict(self, image, questions):
        self.calls.append((image.size, questions))
        answers = {}
        for qid, q in questions.items():
            k = len(q["criteria"])
            if q["type"] == "score":
                answers[qid] = {"type": "score", "score": (k - 1) / 2,
                                "probabilities": {str(i): 1 / k for i in range(k)}, "calibrated": False}
            else:
                answers[qid] = {"type": "choice", "choice": q["criteria"][0],
                                "probabilities": {c: 1 / k for c in q["criteria"]}, "calibrated": False}
        return {"model": "laya-vision", "answers": answers}


@pytest.fixture
def client():
    agent = FakeAgent()
    with TestClient(create_app(lambda: agent)) as c:
        c.agent = agent
        yield c


def b64(data: bytes) -> str:
    return base64.b64encode(data).decode()


def image_bytes(size=(512, 384), fmt="JPEG") -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", size, (40, 80, 120)).save(buf, fmt)
    return buf.getvalue()


def body(**over):
    req = {
        "model": "laya-vision",
        "assets": {"img1": {"media_type": "image/jpeg", "data": b64(FIXTURE.read_bytes())}},
        "state": "asset:img1",
        "questions": [
            {"id": "ship_type", "type": "choice", "text": "What type of ship is this?",
             "criteria": ["container ship", "tanker", "bulk carrier"]},
            {"id": "has_cranes", "type": "yes_no", "text": "Does the ship have deck cranes?", "criteria": []},
            {"id": "rust", "type": "score", "text": "How rusty is the hull?",
             "criteria": ["none", "some", "heavy"]},
        ],
    }
    req.update(over)
    return req


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok", "model_loaded": True}


def test_valid_request(client):
    r = client.post("/v1/systemone", json=body())
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["model"] == "laya-vision"
    a = out["answers"]
    assert set(a) == {"ship_type", "has_cranes", "rust"}
    assert a["ship_type"]["choice"] == "container ship" and a["ship_type"]["value"] is None
    assert a["has_cranes"]["choice"] in ("yes", "no")
    assert set(a["has_cranes"]["probabilities"]) == {"yes", "no"}
    assert a["rust"]["choice"] is None and a["rust"]["value"] == pytest.approx(0.5)
    assert set(a["rust"]["probabilities"]) == {"none", "some", "heavy"}
    assert all(ans["calibrated"] is False for ans in a.values())
    # yes_no went to the SDK as a choice over ["yes", "no"]
    _, sent = client.agent.calls[0]
    assert sent["has_cranes"] == {"type": "choice", "instructions": "Does the ship have deck cranes?",
                                  "criteria": ["yes", "no"]}


def test_png_accepted(client):
    req = body(assets={"p": {"media_type": "image/png", "data": b64(image_bytes(fmt="PNG"))}}, state="asset:p")
    assert client.post("/v1/systemone", json=req).status_code == 200


@pytest.mark.parametrize("over, needle", [
    ({"state": "asset:nope"}, "missing asset"),
    ({"state": "img1"}, "state must be"),
    ({"assets": {"img1": {"media_type": "image/jpeg", "data": b64(image_bytes())},
                 "img2": {"media_type": "image/jpeg", "data": b64(image_bytes())}}}, "unreferenced"),
    ({"assets": {"img1": {"media_type": "image/gif", "data": b64(image_bytes())}}}, "media_type"),
    ({"assets": {"img1": {"media_type": "image/png", "data": b64(image_bytes())}}}, "declared image/png"),
    ({"assets": {"img1": {"media_type": "image/jpeg", "data": "!!notbase64"}}}, "base64"),
    ({"assets": {"img1": {"media_type": "image/jpeg", "data": b64(image_bytes((200, 400)))}}}, "each side"),
    ({"assets": {"img1": {"media_type": "image/jpeg", "data": b64(image_bytes((1600, 800)))}}}, "each side"),
    ({"questions": [{"id": "q", "type": "choice", "text": "x", "criteria": []}]}, "at least one"),
    ({"questions": [{"id": "q", "type": "score", "text": "x", "criteria": ["a"]}]}, "at least two"),
    ({"questions": [{"id": "q", "type": "choice", "text": "x", "criteria": ["a", "a"]}]}, "distinct"),
    ({"questions": [{"id": "q", "type": "choice", "text": "x", "criteria": ["a"]},
                    {"id": "q", "type": "choice", "text": "y", "criteria": ["b"]}]}, "duplicate"),
])
def test_rejects(client, over, needle):
    r = client.post("/v1/systemone", json=body(**over))
    assert r.status_code == 400
    assert needle in r.text


def test_schema_errors_are_400(client):
    assert client.post("/v1/systemone", json=body(questions=[])).status_code == 400
    bad_type = [{"id": "q", "type": "noul", "text": "x", "criteria": []}]
    assert client.post("/v1/systemone", json=body(questions=bad_type)).status_code == 400


def test_body_limit(client):
    r = client.post("/v1/systemone", content=b"x" * (MAX_BODY_BYTES + 1),
                    headers={"content-type": "application/json"})
    assert r.status_code == 413
