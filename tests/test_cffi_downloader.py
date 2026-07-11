import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from bricks.downloader.cffi import Downloader
from bricks.lib.request import Request


class RedirectHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status, body=b"", **headers):
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        for key, value in headers.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/location-on-200":
            self._send(200, b"original", Location="/unexpected")
        elif self.path == "/final":
            self._send(200, b"GET")
        else:
            self._send(404, b"not found")

    def do_POST(self):
        if self.path == "/redirect-303":
            length = int(self.headers.get("Content-Length", 0))
            self.rfile.read(length)
            self._send(303, Location="/final")
        else:
            self._send(404, b"not found")

    def log_message(self, *args):
        pass


class CffiDownloaderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), RedirectHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_location_header_on_success_is_not_followed(self):
        downloader = Downloader()
        try:
            response = downloader.fetch(
                Request(f"{self.base_url}/location-on-200")
            )
        finally:
            downloader.clear_session()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"original")
        self.assertEqual(response.history, [])

    def test_client_handles_303_method_conversion(self):
        downloader = Downloader()
        try:
            response = downloader.fetch(
                Request(
                    f"{self.base_url}/redirect-303",
                    method="POST",
                    body="payload",
                )
            )
        finally:
            downloader.clear_session()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content, b"GET")
        self.assertEqual(response.url, f"{self.base_url}/final")


if __name__ == "__main__":
    unittest.main()
