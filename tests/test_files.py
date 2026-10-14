"""
File transports: jsonl and csv in, jsonl out, and a drop directory that
emits coordinates or, with read=True, the file's own bytes.
"""

import asyncio

from hyperwire import MemorySink, Pipeline
from hyperwire.files import CsvSource, DirectorySource, JsonlSink, JsonlSource


def test_jsonl_to_jsonl(tmp_path):
    source_path = tmp_path / "in.jsonl"
    source_path.write_text('{"a": 1}\n\n{"a": 2}\n')
    target = tmp_path / "out.jsonl"
    pipeline, source, sink = Pipeline(name="copy"), JsonlSource(str(source_path)), JsonlSink(str(target))
    source >> sink
    asyncio.run(pipeline.run(source))
    assert target.read_text().splitlines() == ['{"a": 1}', '{"a": 2}']


def test_csv_rows_are_dicts(tmp_path):
    path = tmp_path / "in.csv"
    path.write_text("id,name\n1,ada\n")
    pipeline, source, sink = Pipeline(name="csv"), CsvSource(str(path)), MemorySink()
    source >> sink
    asyncio.run(pipeline.run(source))
    assert [message.content for message in sink.messages] == [{"id": "1", "name": "ada"}]


def settle(source: DirectorySource) -> list:
    """Two polls of a directory, collecting what settles."""

    async def run():
        found = []
        async for message in source.events():
            found.append(message)
            if len(found) == 1:
                await source.close()
        return found

    return asyncio.run(asyncio.wait_for(run(), timeout=5))


def test_directory_emits_coordinates_by_default(tmp_path):
    (tmp_path / "blob.bin").write_bytes(b"\x00\x01")
    (message,) = settle(DirectorySource(str(tmp_path), interval=0.01))
    assert message.content["name"] == "blob.bin"
    assert message.content["size"] == 2


def test_directory_read_slurps_bytes(tmp_path):
    (tmp_path / "blob.bin").write_bytes(b"\x00\x01not json")
    (message,) = settle(DirectorySource(str(tmp_path), interval=0.01, read=True))
    assert message.content == b"\x00\x01not json"
    assert message.metadata["name"] == "blob.bin"


def test_file_nodes_name_the_paths_they_touch(tmp_path):
    target = tmp_path / "out.jsonl"
    assert JsonlSink(str(target)).describe()["datasets"] == [target.resolve().as_uri()]
    assert JsonlSource(str(target)).datasets() == JsonlSink(str(target)).datasets()
    assert CsvSource(str(tmp_path / "in.csv")).datasets()[0].startswith("file:///")
    assert DirectorySource(str(tmp_path)).datasets() == [tmp_path.resolve().as_uri()]
