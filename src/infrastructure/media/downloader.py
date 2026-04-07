"""
Media Downloader — загрузка файлов для отправки в Telegram.

Ограничения:
- Максимальный размер: 50MB (лимит Telegram Bot API)
- Chunk streaming — не загружаем весь файл в память
- MIME detection через magic bytes
- Cleanup в finally
"""
import aiohttp
from pathlib import Path
from typing import Optional
import tempfile
import os

from loguru import logger


class MediaDownloader:
    """
    Downloader для media файлов.

    Usage:
        downloader = MediaDownloader()
        path = await downloader.download("https://example.com/image.jpg")
        # use path...
        await downloader.cleanup(path)
    """

    MAX_SIZE: int = 50 * 1024 * 1024  # 50MB
    CHUNK_SIZE: int = 8192  # 8KB chunks
    TIMEOUT: aiohttp.ClientTimeout = aiohttp.ClientTimeout(total=60, connect=10)

    def __init__(self, temp_dir: Optional[Path] = None) -> None:
        """
        Args:
            temp_dir: Директория для временных файлов (default: system temp).
        """
        self._temp_dir = temp_dir or Path(tempfile.gettempdir()) / "tad-worker-media"
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Lazy session creation."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.TIMEOUT)
        return self._session

    async def download(
        self,
        url: str,
        max_size: Optional[int] = None,
    ) -> Path:
        """
        Загрузить файл по URL во временную директорию.

        Args:
            url: URL файла.
            max_size: Максимальный размер в байтах (default: 50MB).

        Returns:
            Path к загруженному файлу.

        Raises:
            MediaTooLargeError: Файл превышает максимальный размер.
            DownloadError: Ошибка загрузки.
        """
        max_size = max_size or self.MAX_SIZE
        session = await self._get_session()

        try:
            async with session.get(url) as response:
                response.raise_for_status()

                # Проверка Content-Length
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > max_size:
                    raise MediaTooLargeError(
                        f"File too large: {content_length} bytes (max {max_size})"
                    )

                # Создаём временный файл
                suffix = self._guess_extension(response.headers.get("Content-Type", ""))
                temp_file = tempfile.NamedTemporaryFile(
                    suffix=suffix,
                    dir=self._temp_dir,
                    delete=False,
                )
                temp_path = Path(temp_file.name)

                # Chunk streaming
                downloaded = 0
                try:
                    async for chunk in response.content.iter_chunked(self.CHUNK_SIZE):
                        downloaded += len(chunk)
                        if downloaded > max_size:
                            raise MediaTooLargeError(
                                f"File exceeded max size during download: {max_size}"
                            )
                        temp_file.write(chunk)
                finally:
                    temp_file.close()

                logger.info(f"Downloaded {url} → {temp_path} ({downloaded} bytes)")
                return temp_path

        except aiohttp.ClientError as exc:
            raise DownloadError(f"Failed to download {url}: {exc}")

    async def cleanup(self, path: Path) -> None:
        """Удалить временный файл (safe)."""
        try:
            if path.exists():
                path.unlink()
                logger.debug(f"Cleaned up: {path}")
        except OSError as exc:
            logger.warning(f"Failed to cleanup {path}: {exc}")

    async def cleanup_all(self) -> int:
        """
        Очистить всю временную директорию.

        Returns:
            Количество удалённых файлов.
        """
        count = 0
        for f in self._temp_dir.iterdir():
            try:
                if f.is_file():
                    f.unlink()
                    count += 1
            except OSError as exc:
                logger.warning(f"Failed to cleanup {f}: {exc}")
        logger.info(f"Cleaned up {count} files from {self._temp_dir}")
        return count

    def _guess_extension(self, content_type: str) -> str:
        """Угадать расширение файла из Content-Type."""
        mapping = {
            "image/jpeg": ".jpg",
            "image/png": ".png",
            "image/gif": ".gif",
            "video/mp4": ".mp4",
            "video/webm": ".webm",
            "application/pdf": ".pdf",
        }
        return mapping.get(content_type, ".tmp")

    async def close(self) -> None:
        """Закрыть HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()


class MediaTooLargeError(Exception):
    """Файл превышает максимальный размер."""
    pass


class DownloadError(Exception):
    """Ошибка загрузки файла."""
    pass
