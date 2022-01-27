from peakina.readers.json import transform_with_jq


def test_transform_with_jq():
    assert transform_with_jq("[1, 2, 3]", ".[] + 1") == "2\n3\n4"
