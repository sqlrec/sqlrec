"""Bounded, SSRF-resistant HTTP image loading."""
from __future__ import annotations

import io
import ipaddress
import socket
from urllib.parse import urljoin, urlparse

import httpx
from PIL import Image


class ImageUrlLoader:
    def __init__(self, allowed_hosts: str, timeout_ms: int, max_bytes: int, max_pixels: int):
        self.allowed_hosts = {
            host.strip().lower() for host in allowed_hosts.split(",") if host.strip()
        }
        self.timeout = timeout_ms / 1000.0
        self.max_bytes = max_bytes
        self.max_pixels = max_pixels

    def _validate_url(self, value: str) -> None:
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError("image URL must use http or https")
        host = parsed.hostname.lower()
        if self.allowed_hosts and host not in self.allowed_hosts:
            raise ValueError(f"image URL host is not allowed: {host}")
        try:
            addresses = socket.getaddrinfo(host, parsed.port or (443 if parsed.scheme == "https" else 80))
        except socket.gaierror as exc:
            raise ValueError(f"unable to resolve image URL host: {host}") from exc
        if not addresses:
            raise ValueError(f"unable to resolve image URL host: {host}")
        for address in addresses:
            ip = ipaddress.ip_address(address[4][0])
            if not ip.is_global:
                raise ValueError("image URL resolves to a non-public address")

    def load(self, url: str) -> Image.Image:
        current = url
        with httpx.Client(timeout=self.timeout, follow_redirects=False) as client:
            for _ in range(6):
                self._validate_url(current)
                with client.stream("GET", current, headers={"Accept": "image/*"}) as response:
                    if response.status_code in (301, 302, 303, 307, 308):
                        location = response.headers.get("location")
                        if not location:
                            raise ValueError("image URL redirect has no location")
                        current = urljoin(current, location)
                        continue
                    response.raise_for_status()
                    content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
                    if not content_type.startswith("image/"):
                        raise ValueError("image URL did not return an image content type")
                    declared = response.headers.get("content-length")
                    if declared and int(declared) > self.max_bytes:
                        raise ValueError("image exceeds image_max_bytes")
                    buffer = bytearray()
                    for chunk in response.iter_bytes():
                        buffer.extend(chunk)
                        if len(buffer) > self.max_bytes:
                            raise ValueError("image exceeds image_max_bytes")
                image = Image.open(io.BytesIO(buffer))
                if image.width * image.height > self.max_pixels:
                    raise ValueError("image exceeds image_max_pixels")
                image.load()
                return image.convert("RGB")
        raise ValueError("too many image URL redirects")
