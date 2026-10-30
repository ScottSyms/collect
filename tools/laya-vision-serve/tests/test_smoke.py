"""Smoke test against a running server with the real model.

    python tests/test_smoke.py                     # prints the response
    pytest tests/test_smoke.py                     # skips if the server is not up

LAYA_VISION_URL overrides the base URL (default http://127.0.0.1:8765).
"""

import base64
import json
import os
import sys
from pathlib import Path

import httpx

BASE = os.environ.get("LAYA_VISION_URL", "http://127.0.0.1:8765").rstrip("/")
FIXTURE = Path(__file__).parent / "fixtures" / "container_ship_koper.jpg"

REQUEST = {
    "model": "laya-vision",
    "assets": {"ship": {"media_type": "image/jpeg", "data": base64.b64encode(FIXTURE.read_bytes()).decode()}},
    "state": "asset:ship",
    "questions": [
        {"id": "ship_type", "type": "choice", "text": "What type of vessel is shown in this photo?",
         "criteria": ["container ship", "tanker", "bulk carrier", "passenger ship", "fishing vessel", "tug"]},
        {"id": "has_cranes", "type": "yes_no", "text": "Does the vessel have deck cranes?", "criteria": []},
    ],
}


def run():
    health = httpx.get(f"{BASE}/healthz", timeout=5)
    health.raise_for_status()
    assert health.json()["model_loaded"] is True, health.json()

    r = httpx.post(f"{BASE}/v1/systemone", json=REQUEST, timeout=300)
    assert r.status_code == 200, r.text
    out = r.json()
    assert out["model"] == "laya-vision"
    assert set(out["answers"]) == {"ship_type", "has_cranes"}
    for qid, crit in (("ship_type", REQUEST["questions"][0]["criteria"]), ("has_cranes", ["yes", "no"])):
        a = out["answers"][qid]
        assert set(a) == {"choice", "value", "probabilities", "calibrated"}
        assert a["choice"] in crit and a["value"] is None
        assert set(a["probabilities"]) == set(crit)
        assert abs(sum(a["probabilities"].values()) - 1.0) < 1e-2
        assert a["calibrated"] is False
    return out


def test_smoke():
    import pytest

    try:
        httpx.get(f"{BASE}/healthz", timeout=2)
    except httpx.TransportError:
        pytest.skip(f"no server at {BASE}")
    run()


if __name__ == "__main__":
    try:
        print(json.dumps(run(), indent=2))
    except AssertionError as e:
        sys.exit(f"smoke test failed: {e}")
