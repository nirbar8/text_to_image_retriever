import pytest
from pydantic import ValidationError

from retriever.core.schemas import IndexRequest


def test_index_request_validates() -> None:
    req = IndexRequest(
        image_id=123,
        image_path="/tmp/a.jpg",
        width=640,
        height=480,
        coco_file_name="a.jpg",
    )
    assert req.image_id == 123


def test_index_request_missing_required() -> None:
    with pytest.raises(ValidationError):
        IndexRequest(image_path="/tmp/a.jpg", width=10, height=10)
