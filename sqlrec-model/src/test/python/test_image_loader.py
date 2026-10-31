"""Image URL validation and download limits, without external network calls."""

import io
import unittest
from unittest.mock import patch

import httpx
from PIL import Image

from huggingface import image_loader


class ImageLoaderTest(unittest.TestCase):
    def setUp(self):
        self.loader = image_loader.ImageUrlLoader(
            "safe.example,private.example", timeout_ms=1000,
            max_bytes=1024, max_pixels=4,
        )

    def _fetch(self, url, handler):
        requests = []
        client_type = httpx.Client

        def route(request):
            requests.append(str(request.url))
            return handler(request)

        transport = httpx.MockTransport(route)
        with patch.object(image_loader.httpx, "Client",
                          side_effect=lambda **kwargs: client_type(transport=transport, **kwargs)):
            result = self.loader.load(url)
        return result, requests

    @staticmethod
    def _png(size=(1, 1)):
        stream = io.BytesIO()
        Image.new("RGBA", size, "red").save(stream, format="PNG")
        return stream.getvalue()

    def test_valid_image_is_decoded_to_rgb(self):
        payload = self._png()
        image, requests = self._fetch(
            "https://safe.example/image",
            lambda request: httpx.Response(
                200, headers={"content-type": "image/png"}, content=payload,
            ),
        )
        self.assertEqual(requests, ["https://safe.example/image"])
        self.assertEqual(image.mode, "RGB")
        self.assertEqual(image.size, (1, 1))

    def test_http_client_handles_dns_and_proxy_routing(self):
        # No preflight DNS resolution: a real HTTP transport or proxy owns it.
        with patch("socket.getaddrinfo", side_effect=AssertionError("unexpected DNS precheck")):
            image, requests = self._fetch(
                "https://safe.example/image",
                lambda request: httpx.Response(200, headers={"content-type": "image/png"},
                                               content=self._png()),
            )
        self.assertEqual(image.mode, "RGB")
        self.assertEqual(requests, ["https://safe.example/image"])

    def test_private_and_fake_ip_urls_are_supported_without_host_restrictions(self):
        self.loader = image_loader.ImageUrlLoader("", 1000, 1024, 4)
        for host in ("127.0.0.1", "192.168.0.1", "198.18.0.109", "[::1]"):
            url = f"http://{host}/image"
            with self.subTest(url=url):
                image, requests = self._fetch(
                    url, lambda request: httpx.Response(
                        200, headers={"content-type": "image/png"}, content=self._png()),
                )
                self.assertEqual(image.mode, "RGB")
                self.assertEqual(requests, [url])

    def test_optional_host_allowlist_is_enforced(self):
        with self.assertRaisesRegex(ValueError, "host is not allowed"):
            self._fetch("https://untrusted.example/image", lambda request: self.fail("request was sent"))

    def test_non_http_urls_and_missing_hosts_are_rejected(self):
        for url in ("file:///tmp/image.png", "ftp://safe.example/image", "https:///image"):
            with self.subTest(url=url), self.assertRaisesRegex(ValueError, "http or https"):
                self._fetch(url, lambda request: self.fail("request was sent"))

    def test_redirect_target_is_checked_against_host_allowlist(self):
        requests = []

        def redirect(request):
            requests.append(str(request.url))
            return httpx.Response(302, headers={"location": "https://untrusted.example/image"})

        with self.assertRaisesRegex(ValueError, "host is not allowed"):
            self._fetch("https://safe.example/image", redirect)
        self.assertEqual(requests, ["https://safe.example/image"])

    def test_declared_and_streamed_byte_limits(self):
        responses = (
            httpx.Response(200, headers={"content-type": "image/png", "content-length": "1025"},
                           content=b"x"),
            httpx.Response(200, headers={"content-type": "image/png"},
                           stream=httpx.ByteStream(b"x" * 1025)),
        )
        for response in responses:
            with self.subTest(response=response):
                with self.assertRaisesRegex(ValueError, "image_max_bytes"):
                    self._fetch("https://safe.example/image", lambda request: response)

    def test_pixel_limit(self):
        payload = self._png(size=(3, 3))
        with self.assertRaisesRegex(ValueError, "image_max_pixels"):
            self._fetch(
                "https://safe.example/image",
                lambda request: httpx.Response(
                    200, headers={"content-type": "image/png"}, content=payload,
                ),
            )


if __name__ == "__main__":
    unittest.main()
