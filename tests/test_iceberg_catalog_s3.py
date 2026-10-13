"""The gold Iceberg catalog must upload to S3 without aws-chunked encoding,
which the S3-compatible backend rejects with 501 NotImplemented."""

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


def test_catalog_uploads_without_aws_chunked(s3_endpoint, tmp_path):
    checksum_vars = {
        "AWS_REQUEST_CHECKSUM_CALCULATION",
        "AWS_RESPONSE_CHECKSUM_VALIDATION",
    }
    env = {k: v for k, v in os.environ.items() if k not in checksum_vars}
    settings = iceberg_catalog.settings
    with (
        patch.dict(os.environ, env, clear=True),
        patch.object(settings, "iceberg_warehouse", "s3://bucket/gold-iceberg"),
        patch.object(
            settings, "iceberg_catalog_uri", f"sqlite:///{tmp_path / 'catalog.db'}"
        ),
        patch.object(settings, "s3_endpoint", s3_endpoint),
        patch.object(settings, "s3_region", "us-east-1"),
        patch.object(settings, "s3_access_key_id", "key"),
        patch.object(settings, "s3_secret_access_key", "secret"),
    ):
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
