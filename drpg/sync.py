from __future__ import annotations

import functools
import html
import logging
import re
import sys
import threading
from datetime import datetime, timedelta
from hashlib import md5
from multiprocessing.pool import ThreadPool
from time import timezone
from typing import TYPE_CHECKING

import httpx

import drpg
from drpg.api import DrpgApi

if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path
    from typing import Any, Callable

    from drpg.config import Config
    from drpg.types import DownloadItem, Product

    NoneCallable = Callable[..., None]
    Decorator = Callable[[NoneCallable], NoneCallable]

logger = logging.getLogger("drpg")


def suppress_errors(*errors: type[Exception]) -> Decorator:
    """Silence but log provided errors."""

    def decorator(func: NoneCallable) -> NoneCallable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> None:
            try:
                return func(*args, **kwargs)
            except errors as e:
                logger.exception(e)

        return wrapper

    return decorator


class DateVersion:
    def __init__(self, raw_date_version: str):
        parts = raw_date_version.split(".")
        assert len(parts) == 3
        self.value = [int(p) for p in parts]

    def __lt__(self, other: DateVersion):
        return self.value < other.value

    def __eq__(self, other: object):
        if not isinstance(other, DateVersion):
            return False
        return self.value == other.value


class DrpgSync:
    """High level DriveThruRPG client that syncs products from a customer's library."""

    def __init__(self, config: Config) -> None:
        self._config = config
        self._api = DrpgApi(config.token)
        self._shutdown_event = threading.Event()
        self._download_client = httpx.Client(timeout=30.0)

    def __enter__(self) -> DrpgSync:
        return self

    def __exit__(self, *args: object) -> None:
        self._download_client.close()

    GITHUB_LATEST_URL = "https://api.github.com/repos/glujan/drpg/releases/latest"

    def update_check(self):
        if self._config.do_check:
            resp = self._download_client.get(self.GITHUB_LATEST_URL)
            if not resp.is_success:
                logger.warning(
                    "Unable to check latest release, continuing: %s %s",
                    resp.status_code,
                    resp.content,
                )
                return
            try:
                data = resp.json()
                version = data["tag_name"]
                if DateVersion(drpg.__version__) < DateVersion(version):
                    logger.warning(
                        "Local version is %s, but %s has been released, so you may see issues when running the tool. Please goto https://github.com/glujan/drpg/releases for new releases",  # noqa: E501
                        drpg.__version__,
                        version,
                    )
                else:
                    logger.debug(
                        "Local version %s is greater than or equal to remote version %s",
                        drpg.__version__,
                        version,
                    )
            except Exception:
                logger.exception("Issue during version checking, continuing")

    def sync(self) -> None:
        """Download all new, updated and not yet synced items to a sync directory."""

        logger.info("Authenticating")
        self._api.token()
        logger.info("Fetching products list")
        process_item_args = (
            (product, item)
            for product in self._api.customer_products()
            for item in product["files"]
            if self._need_download(product, item)
        )

        with ThreadPool(self._config.threads) as pool:
            pool.starmap(self._process_item, process_item_args)
        logger.info("Done!")

    def report(self) -> None:
        """Print a read-only view of the library. Never downloads anything.

        Selects one of --summary, --status or --search based on the config.
        """

        logger.info("Authenticating")
        self._api.token()
        logger.info("Fetching products list")

        progress = _ScanProgress()
        progress.start()
        states: list[tuple[Product, DownloadItem, bool]] = []
        for product in self._api.customer_products():
            for item in product["files"]:
                states.append((product, item, self._need_download(product, item, quiet=True)))
                progress.tick()
        progress.done()

        if self._config.search is not None:
            self._report_search(states)
        elif self._config.status:
            self._report_status(states)
        else:
            self._report_summary(states)

    def _report_summary(self, states: list[tuple[Product, DownloadItem, bool]]) -> None:
        need = sum(1 for *_, needs in states if needs)
        print(f"{len(states) - need} up to date")
        print(f"{need} need download")

    def _report_status(self, states: list[tuple[Product, DownloadItem, bool]]) -> None:
        needers = [(product, item) for product, item, needs in states if needs]
        print(f"{len(states) - len(needers)} up to date")
        print(f"{len(needers)} need download:")
        for product, item in needers:
            print(f"  {self._file_path(product, item)}")

    def _report_search(self, states: list[tuple[Product, DownloadItem, bool]]) -> None:
        term = (self._config.search or "").lower()
        for product, item, needs in states:
            if term in product["name"].lower() or term in item["filename"].lower():
                tag = "[need download]" if needs else "[up to date]   "
                print(f"{tag} {self._file_path(product, item)}")

    @suppress_errors(httpx.HTTPError, PermissionError)
    def _process_item(self, product: Product, item: DownloadItem) -> None:
        """Prepare for and download the item to the sync directory."""

        if self._shutdown_event.is_set():
            return

        path = self._file_path(product, item)

        if self._config.dry_run:
            logger.info("DRY RUN - would have downloaded file: %s", path)
        else:
            logger.info("Processing: %s - %s", product["name"], item["filename"])

            try:
                url_data = self._api.prepare_download_url(product["orderProductId"], item["index"])
            except self._api.PrepareDownloadUrlException:
                logger.warning(
                    "Could not download product: %s - %s",
                    product["name"],
                    item["filename"],
                )
                return

            file_response = self._download_client.get(
                url_data["url"],
                follow_redirects=True,
                headers={
                    "Accept-Encoding": "gzip, deflate, br",
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "*/*",
                },
            )

            if (
                self._config.validate
                and (api_checksum := _newest_checksum(item))
                and (local_checksum := md5(file_response.content).hexdigest()) != api_checksum
            ):
                logger.error(
                    "ERROR: Invalid checksum for %s - %s, skipping saving file (%s != %s))",
                    product["name"],
                    item["filename"],
                    api_checksum,
                    local_checksum,
                )
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(file_response.content)

    def _need_download(self, product: Product, item: DownloadItem, quiet: bool = False) -> bool:
        """Specify whether or not the item needs to be downloaded.

        Set quiet=True to skip the per-file log lines (used by the read-only
        report modes, whose own output would otherwise be buried).
        """

        reason = self._download_reason(product, item)
        if reason is not None:
            if not quiet:
                logger.debug(
                    "Needs download: %s - %s: %s",
                    product["name"],
                    item["filename"],
                    reason,
                )
            return True

        if not quiet:
            logger.info("Up to date: %s - %s", product["name"], item["filename"])
        return False

    def _download_reason(self, product: Product, item: DownloadItem) -> str | None:
        """Return why the item needs downloading, or None if it is up to date."""

        path = self._file_path(product, item)

        if not path.exists():
            return "local file does not exist"

        remote_time = datetime.fromisoformat(product["fileLastModified"]).utctimetuple()
        local_time = (
            datetime.fromtimestamp(path.stat().st_mtime) + timedelta(seconds=timezone)
        ).utctimetuple()
        if remote_time > local_time:
            return "local file is outdated"

        if (
            self._config.use_checksums
            and (checksum := _newest_checksum(item))
            and md5(path.read_bytes()).hexdigest() != checksum
        ):
            return "unmatching checksum"

        return None

    def _file_path(self, product: Product, item: DownloadItem) -> Path:
        publishers_name = _normalize_path_part(
            product.get("publisher", {}).get("name", "Others"), self._config.compatibility_mode
        )
        product_name = _normalize_path_part(product["name"], self._config.compatibility_mode)
        item_name = _normalize_path_part(item["filename"], self._config.compatibility_mode)
        if self._config.omit_publisher:
            return self._config.library_path / product_name / item_name
        else:
            return self._config.library_path / publishers_name / product_name / item_name


class _ScanProgress:
    """A tiny throbber for the read-only library scan.

    Writes to stderr and only when that stream is a terminal, so piping the
    report to a file (or running under tests) stays clean.
    """

    _FRAMES = "|/-\\"
    _STRIDE = 7  # advance the animation every _STRIDE files

    def __init__(self, stream: Any = None) -> None:
        self._stream = sys.stderr if stream is None else stream
        self._enabled = self._stream.isatty()
        self._count = 0

    def start(self) -> None:
        if self._enabled:
            self._render()

    def tick(self) -> None:
        self._count += 1
        if self._enabled and self._count % self._STRIDE == 0:
            self._render()

    def _render(self) -> None:
        frame = self._FRAMES[(self._count // self._STRIDE) % len(self._FRAMES)]
        self._stream.write(f"\r{frame} Scanned {self._count} files...")
        self._stream.flush()

    def done(self) -> None:
        if self._enabled:
            self._stream.write("\r\033[K")  # carriage return + clear to end of line
            self._stream.flush()


def _normalize_path_part(part: str, compatibility_mode: bool) -> str:
    """
    Strip out unwanted characters in parts of the path to the downloaded file representing
    publisher's name, product name, and item name.
    """

    # There are two algorithms for normalizing names. One is the drpg way, and the other
    # is the DriveThruRPG way.
    #
    # Normalization algorithm for DriveThruRPG's client:
    # 1. Replace any characters that are not alphanumeric, period, or space with "_"
    # 2. Replace repeated whitespace with a single space
    # # NOTE: I don't know for sure that step 2 is how their client handles it. I'm guessing.
    #
    # Normalization algorithm for drpg:
    # 1. Unescape any HTML-escaped characters (for example, convert &nbsp; to a space)
    # 2. Replace any of the characters <>:"/\|?* with " - "
    # 3. Replace any repeated " - " separators with a single " - "
    # 4. Replace repeated whitespace with a single space
    #
    # For background, this explains what characters are not allowed in filenames on Windows:
    # https://learn.microsoft.com/en-us/windows/win32/fileio/naming-a-file#naming-conventions
    # Since Windows is the lowest common denominator, we use its restrictions on all platforms.

    if compatibility_mode:
        part = PathNormalizer.normalize_drivethrurpg_compatible(part)
    else:
        part = PathNormalizer.normalize(part)
    return part


def _newest_checksum(item: DownloadItem) -> str | None:
    return max(
        item["checksums"] or [],
        default={"checksum": None},
        key=lambda s: datetime.fromisoformat(s["checksumDate"]),
    )["checksum"]


class PathNormalizer:
    separator_drpg = " - "
    multiple_drpg_separators = f"({separator_drpg})+"
    multiple_whitespaces = re.compile(r"\s+")
    non_standard_characters = re.compile(r"[^a-zA-Z0-9.\s]")

    @classmethod
    def normalize_drivethrurpg_compatible(cls, part: str) -> str:
        separator = "_"
        part = re.sub(cls.non_standard_characters, separator, part)
        part = re.sub(cls.multiple_whitespaces, " ", part)
        return part

    @classmethod
    def normalize(cls, part: str) -> str:
        separator = PathNormalizer.separator_drpg
        part = html.unescape(part)
        part = re.sub(r'[<>:"/\\|?*]', separator, part).strip(separator)
        part = re.sub(PathNormalizer.multiple_drpg_separators, separator, part)
        part = re.sub(PathNormalizer.multiple_whitespaces, " ", part)
        return part
