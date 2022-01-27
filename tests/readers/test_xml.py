import pytest

from peakina.readers.xml import transform_with_jq

data = {
    "records": {
        "record": [
            {"@id": "1", "title": "Keep on dancin'"},
            {"@id": "2", "title": "Small Talk"},
        ]
    }
}


@pytest.mark.parametrize(
    "jq_filter,expected",
    [
        (".records", data["records"]),
        (".records[]", data["records"]["record"]),
        (".records[][0]", [{"@id": "1", "title": "Keep on dancin'"}]),
    ],
)
def test_transform_with_jq(jq_filter, expected):
    assert transform_with_jq(data, jq_filter) == expected
