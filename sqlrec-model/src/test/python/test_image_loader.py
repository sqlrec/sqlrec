"""Image URL validation and download limits, without external network calls."""

import io
import socket
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

    def _fetch(self, url, handler, addresses=None):
        requests = []
        client_type = httpx.Client

        def route(request):
            requests.append(str(request.url))
            return handler(request)

        def resolve(host, port):
            ip = (addresses or {}).get(host, "8.8.8.8")
            return [(socket.AF_INET, socket.SOCK_STREAM, 0, "", (ip, port))]

        transport = httpx.MockTransport(route)
        with (
            patch.object(image_loader.httpx, "Client",
                         side_effect=lambda **kwargs: client_type(transport=transport, **kwargs)),
            patch.object(image_loader.socket, "getaddrinfo", side_effect=resolve),
        ):
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

    def test_private_address_is_rejected_before_request(self):
        requests = []
        with self.assertRaisesRegex(ValueError, "non-public"):
            self._fetch(
                "http://private.example/image",
                lambda request: requests.append(request),
                addresses={"private.example": "127.0.0.1"},
            )
        self.assertEqual(requests, [])

    def test_redirect_target_is_validated_before_request(self):
        requests = []

        def redirect(request):
            requests.append(str(request.url))
            return httpx.Response(302, headers={"location": "http://private.example/image"})

        with self.assertRaisesRegex(ValueError, "non-public"):
            self._fetch(
                "https://safe.example/image", redirect,
                addresses={"private.example": "127.0.0.1"},
            )
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
