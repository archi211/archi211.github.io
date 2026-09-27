"""The local diagnostic checks the same history endpoint used by MT5."""

from contextlib import redirect_stdout
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pocketoption_bridge import server


class LocalCheckTests(unittest.TestCase):
    def run_check(self, payload=None, failure=None):
        secret = "offline-check-private-token"
        requests = []
        class Opener:
            def open(self, request, timeout):
                requests.append(request)
                self_outer.assertEqual(timeout, 5)
                self_outer.assertEqual(request.get_header("Authorization"), "Bearer " + secret)
                if failure:
                    raise failure
                parsed = urlparse(request.full_url)
                self_outer.assertEqual(parsed.netloc, "127.0.0.1:5001")
                if parsed.path == "/healthz":
                    return io.BytesIO(json.dumps({"status": "READY", "untrusted": secret}).encode())
                self_outer.assertEqual(parsed.path, "/v1/klines")
                self_outer.assertEqual(parse_qs(parsed.query), {"symbol": ["#TEST_otc"], "limit": ["1"]})
                return io.BytesIO(json.dumps(payload).encode())

        self_outer = self
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            (base / "bridge.json").write_text('{"port":5001}')
            (base / "api_token.txt").write_text(secret)
            (base / "pairs.json").write_text('[{"symbol":"#TEST_otc"}]')
            output = io.StringIO()
            with patch.object(server, "BASE", base), patch.object(server, "build_opener", return_value=Opener()) as build, redirect_stdout(output):
                result = server.check_local_api()
                self.assertEqual(build.call_args.args[0].proxies, {})
                self.assertIsNone(build.call_args.args[1].redirect_request(None, None, 302, "", {}, "https://example.invalid"))
        self.assertNotIn(secret, output.getvalue())
        return result, output.getvalue(), requests

    def test_reports_available_cached_bar_without_claiming_live(self):
        result, output, requests = self.run_check({"pair_status": "STALE", "data": [[1699999980000, 1.1, 1.3, 1.0, 1.2]]})
        self.assertEqual(result, 0)
        self.assertEqual(len(requests), 2)
        self.assertTrue(json.loads(output)["pairs"][0]["bar_available_over_http"])
        self.assertEqual(json.loads(output)["pairs"][0]["status"], "STALE")

    def test_reports_empty_history(self):
        result, output, _ = self.run_check({"pair_status": "LOADING", "data": []})
        self.assertEqual(result, 1)
        self.assertFalse(json.loads(output)["pairs"][0]["bar_available_over_http"])

    def test_http_error_does_not_print_response_body_or_credentials(self):
        result, output, _ = self.run_check(failure=HTTPError("http://localhost", 401, "offline-check-private-token", {}, None))
        self.assertEqual(result, 1)
        self.assertIn("HTTP 401", output)


if __name__ == "__main__":
    unittest.main(verbosity=2)
