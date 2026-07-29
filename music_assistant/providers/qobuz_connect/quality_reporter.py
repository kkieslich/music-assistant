"""Report configured and currently playing audio quality to Qobuz."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol

from .models import AudioQualityReport

if TYPE_CHECKING:
    import logging


class QualitySession(Protocol):
    """Session operations used by the quality reporter."""

    async def send_file_quality_report(self, report: AudioQualityReport) -> bool:
        """Send actual current-file audio properties."""

    async def send_max_quality_report(self, quality: int) -> bool:
        """Send the configured maximum quality."""


class QualityReporter:
    """Keep Qobuz's quality state aligned with real MA stream details."""

    def __init__(
        self,
        session_getter: Callable[[], QualitySession | None],
        file_quality_getter: Callable[[], AudioQualityReport | None],
        logger: logging.Logger,
    ) -> None:
        """Initialize the reporter."""
        self._session_getter = session_getter
        self._file_quality_getter = file_quality_getter
        self._logger = logger
        self._last_max_quality: int | None = None
        self._last_file_quality: AudioQualityReport | None = None

    async def report_current(self, max_quality: int) -> None:
        """Report the configured ceiling and any known current-file format."""
        await self.report_max(max_quality)
        await self.report_file(self._file_quality_getter())

    async def report_max(self, quality: int) -> None:
        """Report the configured quality ceiling."""
        session = self._session_getter()
        if session is None or quality == self._last_max_quality:
            return
        await session.send_max_quality_report(quality)
        self._last_max_quality = quality

    async def report_file(self, report: AudioQualityReport | None) -> None:
        """Report the actual current-file format when MA has resolved it."""
        session = self._session_getter()
        if session is None or report is None or report == self._last_file_quality:
            return
        await session.send_file_quality_report(report)
        self._last_file_quality = report

    def reset(self) -> None:
        """Forget deduplication state after a session or playback transition."""
        self._last_max_quality = None
        self._last_file_quality = None
