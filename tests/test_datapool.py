import json
from contextlib import suppress
from typing import Any

import pytest

from peakina.datapool import DataPool


def templatize(d: dict[str, Any], real_ftp_path: str) -> dict[str, Any]:
    res = {
        "uri": d.pop("file"),
        "type": d.pop("type", None),
        "match": d.pop("match", None),
        "reader_kwargs": {**d},
    }
    with suppress(IndexError):
        res["uri"] = res["uri"].format(ftp_path=real_ftp_path)
    return res


@pytest.fixture
def config(path, ftp_path):
    with open(path("demo_config.json")) as f:
        config = json.load(f)
    return {k: templatize(v, ftp_path) for k, v in config.items()}


def test_datapool(config, path):
    fixtures_path = path("")
    pool = DataPool(config, fixtures_path)
    assert len(pool) == len(config)
    assert "0_0" in pool
    df = pool["0_0"]
    assert df.shape == (2, 2)
