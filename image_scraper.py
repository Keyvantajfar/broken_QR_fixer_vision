#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Image downloader for a search term.

Features
- Uses DuckDuckGo Images (no API key) to fetch many image URLs.
- Optional: scrapes <img> tags from top web pages for extra images (--also-scrape-pages).
- Async downloads with concurrency, timeouts, retries, polite jitter.
- Deduplicates by file content (SHA-256).
- Categorizes images into "high_quality" if resolution >= 1280x720.
- Creates folder named from search term (e.g., "brad_pitt").
- Preserves descriptive filenames from source URLs when possible.

Usage
  python image_scraper.py --search "brad pitt" -n 100
  python image_scraper.py --search "cute cats" -n 1000 --concurrency 20
  # scrape DuckDuckGo image results and also parse the top 40 web pages for inline images
  python image_scraper.py --search "brad pitt" -n 300 --also-scrape-pages --pages 40

Notes
- Respect site terms/robots and copyright. This is for personal/educational use.
- Google Images scraping is brittle and commonly against ToS; this script deliberately avoids it.
"""

import argparse
import asyncio
import hashlib
import io
import math
import mimetypes
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin, unquote

import aiohttp
from aiohttp import ClientSession
from PIL import Image

# Optional but recommended:
try:
    from bs4 import BeautifulSoup  # for --also-scrape-pages
    HAVE_BS4 = True
except Exception:
    HAVE_BS4 = False

try:
    # Preferred new package name
    from ddgs import DDGS
    HAVE_DDG = True
except Exception:
    try:
        # Fallback to the old package for compatibility
        from duckduckgo_search import DDGS  # type: ignore
        HAVE_DDG = True
    except Exception:
        HAVE_DDG = False


# ----------------------------- Helpers -----------------------------

SAFE_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

VALID_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".gif", ".tiff", ".jfif"}

def slugify(text: str) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s-]+", "_", text)
    return text[:100] if text else "query"

def safe_filename(name: str) -> str:
    """Sanitize a filename while preserving readability."""
    name = unquote(name)
    name = name.strip().replace("/", "_").replace("\\", "_")
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name[:120]

def guess_ext(url: str, content_type: Optional[str]) -> str:
    # Priority 1: content-type
    if content_type:
        ext = mimetypes.guess_extension(content_type.split(";")[0].strip())
        if ext in VALID_EXTS:
            return ext
        # Some servers return odd types; special cases:
        if "jpeg" in content_type:
            return ".jpg"
        if "png" in content_type:
            return ".png"
        if "webp" in content_type:
            return ".webp"
    # Priority 2: URL path
    p = urlparse(url).path
    _, _, suffix = p.rpartition(".")
    ext = f".{suffix.lower()}" if suffix else ""
    if ext in VALID_EXTS:
        return ext
    # Fallback
    return ".jpg"

def is_image_content_type(ct: Optional[str]) -> bool:
    return bool(ct and ct.lower().startswith("image/"))

def qualifies_hq(img: Image.Image, min_w: int = 1280, min_h: int = 720) -> bool:
    """Return True if image meets or exceeds the min resolution in any orientation."""
    try:
        w, h = img.size
        # Compare the smaller and larger dimension against the thresholds so portrait
        # images are treated the same as landscape ones.
        return min(w, h) >= min(min_w, min_h) and max(w, h) >= max(min_w, min_h)
    except Exception:
        return False

def unique(iterable: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for x in iterable:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def normalize_img_url(u: str) -> Optional[str]:
    if not u:
        return None
    if not u.lower().startswith(("http://", "https://")):
        return None
    # filtering obvious tracking/data URIs
    if u.lower().startswith("data:"):
        return None
    return u

# ---------------------------- URL Sources ----------------------------

async def ddg_image_urls(query: str, max_results: int, gif_only: bool = False) -> List[str]:
    if not HAVE_DDG:
        print("[warn] duckduckgo_search not installed. `pip install duckduckgo_search`", file=sys.stderr)
        return []
    urls: List[str] = []
    # DDGS().images returns a generator of dicts with key 'image'
    try:
        with DDGS() as ddgs:
            for r in ddgs.images(
                query,
                max_results=max_results,
                safesearch="Off",  # You can change to "Moderate"/"Strict"
                type_image="gif" if gif_only else None,
            ):
                u = normalize_img_url(r.get("image") or r.get("thumbnail") or "")
                if u:
                    urls.append(u)
    except Exception as e:
        print(f"[warn] DDG images failed: {e}", file=sys.stderr)
    return unique(urls)

async def ddg_top_pages(query: str, max_pages: int) -> List[str]:
    """Get top HTML page URLs for optional scraping."""
    if not HAVE_DDG:
        return []
    pages = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(
                query,
                max_results=max_pages,
                safesearch="Off",
            ):
                u = normalize_img_url(r.get("href") or r.get("url") or "")
                if u:
                    pages.append(u)
    except Exception as e:
        print(f"[warn] DDG text failed: {e}", file=sys.stderr)
    return unique(pages)

async def scrape_imgs_from_page(session: ClientSession, page_url: str, timeout: int = 15) -> List[str]:
    """Fetch a page and collect candidate <img src> links."""
    if not HAVE_BS4:
        return []
    try:
        async with session.get(page_url, timeout=timeout) as resp:
            if resp.status != 200:
                return []
            ct = resp.headers.get("content-type", "")
            if "text/html" not in ct.lower():
                return []
            html = await resp.text(errors="ignore")
    except Exception:
        return []
    soup = BeautifulSoup(html, "lxml")
    candidates = []

    # Prefer large-ish images (heuristic via attributes)
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not src:
            continue
        src = urljoin(page_url, src)
        src = normalize_img_url(src)
        if not src:
            continue

        # Heuristic filters: skip tiny icons/sprites
        w = img.get("width")
        h = img.get("height")
        try:
            w = int(w) if w is not None else None
            h = int(h) if h is not None else None
        except Exception:
            w = h = None
        if w and h and (w < 64 or h < 64):
            continue

        # Skip obvious tracker pixels
        if any(tok in src.lower() for tok in ["sprite", "icon", "logo", "avatar", "pixel", "adsystem"]):
            continue

        candidates.append(src)

    return unique(candidates)

# ----------------------------- Downloading -----------------------------

class Downloader:
    def __init__(
        self,
        outdir: Path,
        target_count: int,
        concurrency: int = 10,
        timeout: int = 20,
        user_agent: str = SAFE_USER_AGENT,
        min_hq_w: int = 1280,
        min_hq_h: int = 720,
        jitter_ms: Tuple[int, int] = (50, 150),
        max_retries: int = 2,
        gif_only: bool = False,
    ):
        self.outdir = outdir
        self.hqdir = outdir / "high_quality"
        self.outdir.mkdir(parents=True, exist_ok=True)
        self.hqdir.mkdir(parents=True, exist_ok=True)
        self.target_count = target_count
        self.concurrency = concurrency
        self.timeout = timeout
        self.headers = {"User-Agent": user_agent, "Accept": "*/*"}
        self.min_hq_w = min_hq_w
        self.min_hq_h = min_hq_h
        self.jitter_ms = jitter_ms
        self.max_retries = max_retries
        self.gif_only = gif_only

        self.hashes: Set[str] = set()
        self.saved = 0

    async def _fetch_bytes(self, session: ClientSession, url: str) -> Tuple[Optional[bytes], Optional[str]]:
        # polite jitter
        await asyncio.sleep(random.uniform(self.jitter_ms[0], self.jitter_ms[1]) / 1000.0)
        tries = 0
        last_exc: Optional[Exception] = None
        while tries <= self.max_retries:
            try:
                async with session.get(url, timeout=self.timeout, headers=self.headers, allow_redirects=True) as resp:
                    if resp.status != 200:
                        tries += 1
                        continue
                    ct = resp.headers.get("content-type", "")
                    if not is_image_content_type(ct):
                        # Some servers don't set content-type; still try reading
                        data = await resp.read()
                        return data, ct
                    data = await resp.read()
                    return data, ct
            except Exception as e:
                last_exc = e
                tries += 1
        # print(f"[warn] failed {url}: {last_exc}")
        return None, None

    def _hash_bytes(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _save_image(self, data: bytes, url: str, content_type: Optional[str]) -> Optional[Path]:
        """Persist image bytes to disk if they look valid.

        A quick length check helps avoid writing empty placeholder files when a
        server responds with no content or an unexpected payload."""
        if not data:
            return None
        ext = guess_ext(url, content_type)
        if ext.lower() not in VALID_EXTS:
            ext = ".jpg"
        if self.gif_only and ext.lower() != ".gif":
            return None
        h = self._hash_bytes(data)
        if h in self.hashes:
            return None
        self.hashes.add(h)

        # Try to inspect with PIL and categorize
        try:
            img = Image.open(io.BytesIO(data))
            img.verify()  # verify integrity
        except Exception:
            return None
        # Re-open after verify to actually load size
        try:
            img = Image.open(io.BytesIO(data))
        except Exception:
            return None

        is_hq = qualifies_hq(img, self.min_hq_w, self.min_hq_h)

        subdir = self.hqdir if is_hq else self.outdir

        # Attempt to preserve a readable filename from the URL
        basename = os.path.basename(urlparse(url).path)
        basename = basename.split("?")[0].split("#")[0]
        base, _ = os.path.splitext(basename)
        base = safe_filename(base)
        if base:
            filename = f"{base}{ext}"
        else:
            filename = f"{h}{ext}"
        path = subdir / filename
        if path.exists():
            filename = f"{base or h}_{h[:8]}{ext}"
            path = subdir / filename

        try:
            with open(path, "wb") as f:
                f.write(data)
            return path
        except Exception:
            return None

    async def download_many(self, urls: List[str]) -> int:
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=self.timeout, sock_read=self.timeout)
        connector = aiohttp.TCPConnector(limit=self.concurrency)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector, trust_env=True) as session:
            async def fetch_and_save(url: str) -> None:
                data, ct = await self._fetch_bytes(session, url)
                if data:
                    saved_path = self._save_image(data, url, ct)
                    if saved_path:
                        self.saved += 1
                        print(f"[{self.saved}/{self.target_count}] saved: {saved_path}")

            it = iter(urls)
            tasks: Set[asyncio.Task] = set()
            # Prime initial batch
            for _ in range(self.concurrency):
                try:
                    url = next(it)
                except StopIteration:
                    break
                tasks.add(asyncio.create_task(fetch_and_save(url)))

            while tasks and self.saved < self.target_count:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for _ in done:
                    pass  # results already handled in fetch_and_save
                if self.saved >= self.target_count:
                    for t in tasks:
                        t.cancel()
                    break
                for _ in done:
                    try:
                        url = next(it)
                    except StopIteration:
                        continue
                    tasks.add(asyncio.create_task(fetch_and_save(url)))

            await asyncio.gather(*tasks, return_exceptions=True)
        return self.saved


# ----------------------------- Main flow -----------------------------

async def main():
    ap = argparse.ArgumentParser(description="Download images for a search term.")
    ap.add_argument("--search", "-s", required=True, help="Search query, e.g. 'brad pitt'")
    ap.add_argument("-n", type=int, default=100, help="Number of images to save (default 100)")
    ap.add_argument("--outdir", default=".", help="Output base directory (default current dir)")
    ap.add_argument("--concurrency", type=int, default=12, help="Concurrent downloads (default 12)")
    ap.add_argument("--timeout", type=int, default=20, help="Per-request timeout seconds (default 20)")
    ap.add_argument("--user-agent", default=SAFE_USER_AGENT, help="HTTP User-Agent")
    ap.add_argument("--hq-width", type=int, default=1280, help="Min HQ width (default 1280)")
    ap.add_argument("--hq-height", type=int, default=720, help="Min HQ height (default 720)")
    ap.add_argument("--max-candidates", type=int, default=5000, help="Max image URLs to gather before downloading (default 5000)")
    ap.add_argument("--also-scrape-pages", action="store_true", help="Also scrape top web pages for <img> tags")
    ap.add_argument("--pages", type=int, default=30, help="Number of search result pages to parse for additional inline images when --also-scrape-pages is set (default 30)")
    ap.add_argument("--gif", action="store_true", help="Download only GIF images")
    args = ap.parse_args()

    query = args.search.strip()
    folder = slugify(query)
    outdir = Path(args.outdir).expanduser().resolve() / folder
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / "high_quality").mkdir(exist_ok=True)

    print(f"[info] query: {query}")
    print(f"[info] folder: {outdir}")
    print(f"[info] target images: {args.n}")

    # 1) Collect candidate image URLs
    candidates: List[str] = []

    # Gather more than needed to compensate for dead links
    want = min(args.max_candidates, max(args.n * 4, args.n + 200))
    print(f"[info] gathering up to {want} candidates from DuckDuckGo Images...")
    ddg_urls = await ddg_image_urls(query, max_results=want, gif_only=args.gif)
    candidates.extend(ddg_urls)

    if args.also_scrape_pages:
        if not HAVE_BS4:
            print("[warn] BeautifulSoup not installed. Run: pip install beautifulsoup4 lxml", file=sys.stderr)
        else:
            print(f"[info] also scraping up to {args.pages} top pages for inline images...")
            timeout = aiohttp.ClientTimeout(total=None, sock_connect=args.timeout, sock_read=args.timeout)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": args.user_agent}, trust_env=True) as session:
                pages = await ddg_top_pages(query, max_pages=args.pages)
                for i, p in enumerate(pages, 1):
                    imgs = await scrape_imgs_from_page(session, p, timeout=args.timeout)
                    if args.gif:
                        imgs = [u for u in imgs if u.lower().split('?')[0].endswith('.gif')]
                    candidates.extend(imgs)
                    print(f"[info] scraped page {i}/{len(pages)}: +{len(imgs)} images")

    candidates = unique([normalize_img_url(u) for u in candidates if normalize_img_url(u)])
    print(f"[info] total unique candidate URLs: {len(candidates)}")

    if not candidates:
        print("[error] no candidates found. Check your network or adjust flags.")
        sys.exit(1)

    # 2) Download until we hit n
    dl = Downloader(
        outdir=outdir,
        target_count=args.n,
        concurrency=args.concurrency,
        timeout=args.timeout,
        user_agent=args.user_agent,
        min_hq_w=args.hq_width,
        min_hq_h=args.hq_height,
        gif_only=args.gif,
    )

    saved = await dl.download_many(candidates)
    print(f"[done] saved {saved} images to: {outdir}")
    print(f"[done] HQ images: {len(list((outdir/'high_quality').glob('*')))}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[info] interrupted by user")
