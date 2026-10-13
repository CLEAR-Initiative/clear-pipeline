"""The gold catalog's S3 uploads and its checksum-mode guard
(iceberg_catalog._require_checksums_when_required)."""

import contextlib
import http.server
import os
import threading
from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_catalog


class _RecordingS3(http.server.BaseHTTPRequestHandler):
    """Accepts any upload and records each PUT's headers."""

    puts: list[dict[str, str]] = []

    def _drain_body(self):
        if length := int(self.headers.get("Content-Length") or 0):
            self.rfile.read(length)
        elif self.headers.get("Transfer-Encoding") == "chunked":
            while size := int(self.rfile.readline().split(b";")[0], 16):
                self.rfile.read(size + 2)
            self.rfile.readline()

    def _reply(self, body: bytes = b""):
        self.send_response(200)
        self.send_header("ETag", '"etag"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self):
        self.puts.append({k.lower(): v for k, v in self.headers.items()})
        self._drain_body()
        self._reply()

    def do_POST(self):
        self._drain_body()
        if "uploads" in self.path:
            self._reply(
                b"<InitiateMultipartUploadResult><UploadId>u</UploadId></InitiateMultipartUploadResult>"
            )
        else:
            self._reply(
                b'<CompleteMultipartUploadResult><ETag>"etag"</ETag></CompleteMultipartUploadResult>'
            )

    def do_HEAD(self):
        self.send_response(404)
        self.end_headers()

    def log_message(self, *args):
        pass


@pytest.fixture
def s3_endpoint():
    _RecordingS3.puts = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RecordingS3)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


@contextlib.contextmanager
def _s3_settings(s3_endpoint, tmp_path, **env_overrides):
    """Point the catalog at `s3_endpoint`, with the checksum env cleared then overridden."""
    settings = iceberg_catalog.settings
    env = {
        k: v for k, v in os.environ.items() if k not in iceberg_catalog._CHECKSUM_ENV
    }
    with contextlib.ExitStack() as stack:
        for ctx in (
            patch.dict(os.environ, env | env_overrides, clear=True),
            patch.object(settings, "iceberg_warehouse", "s3://bucket/gold-iceberg"),
            patch.object(
                settings, "iceberg_catalog_uri", f"sqlite:///{tmp_path / 'catalog.db'}"
            ),
            patch.object(settings, "s3_endpoint", s3_endpoint),
            patch.object(settings, "s3_region", "us-east-1"),
            patch.object(settings, "s3_access_key_id", "key"),
            patch.object(settings, "s3_secret_access_key", "secret"),
        ):
            stack.enter_context(ctx)
        yield


def test_catalog_uploads_without_aws_chunked(s3_endpoint, tmp_path):
    with _s3_settings(s3_endpoint, tmp_path):
        io = iceberg_catalog.catalog()._load_file_io(
            location="s3://bucket/gold-iceberg"
        )
        with io.new_output("s3://bucket/gold-iceberg/probe.json").create(
            overwrite=True
        ) as out:
            out.write(b"{}")

    assert _RecordingS3.puts, "no upload reached the endpoint"
    for headers in _RecordingS3.puts:
        assert "aws-chunked" not in headers.get("content-encoding", "")
        assert "x-amz-trailer" not in headers


@pytest.mark.parametrize("name", iceberg_catalog._CHECKSUM_ENV)
def test_catalog_refuses_a_conflicting_checksum_mode(s3_endpoint, tmp_path, name):
    with _s3_settings(s3_endpoint, tmp_path, **{name: "WHEN_SUPPORTED"}):
        with pytest.raises(RuntimeError, match=name):
            iceberg_catalog.catalog()


def test_catalog_accepts_an_explicit_when_required(s3_endpoint, tmp_path):
    with _s3_settings(
        s3_endpoint, tmp_path, AWS_REQUEST_CHECKSUM_CALCULATION="when_required"
    ):
        iceberg_catalog.catalog()
