"""Tests for reporting configured and actual audio quality independently."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from music_assistant.providers.qobuz_connect.models import (
    AudioQualityReport,
    quality_id_for_format,
)
from music_assistant.providers.qobuz_connect.quality_reporter import QualityReporter


def test_quality_id_for_format_uses_actual_encoding_properties() -> None:
    """Map an actual stream format to Qobuz's coarse quality enum."""
    assert quality_id_for_format("mp3", 44_100, 16) == 5
    assert quality_id_for_format("flac", 44_100, 16) == 6
    assert quality_id_for_format("flac", 44_100, 24) == 7
    assert quality_id_for_format("flac", 96_000, 24) == 7
    assert quality_id_for_format("flac", 192_000, 24) == 27


async def test_report_current_sends_ceiling_and_actual_file_separately() -> None:
    """A 24/192 maximum must coexist with a real 24/44.1 current file."""
    session = SimpleNamespace(
        send_max_quality_report=AsyncMock(),
        send_file_quality_report=AsyncMock(),
    )
    actual = AudioQualityReport(
        quality=7,
        sampling_rate=44_100,
        bit_depth=24,
        channels=2,
    )
    reporter = QualityReporter(
        session_getter=lambda: session,
        file_quality_getter=lambda: actual,
        logger=MagicMock(),
    )

    await reporter.report_current(27)

    session.send_max_quality_report.assert_awaited_once_with(27)
    session.send_file_quality_report.assert_awaited_once_with(actual)


async def test_report_current_omits_unknown_file_and_device_formats() -> None:
    """Unknown actual formats must not be fabricated from the quality ceiling."""
    session = SimpleNamespace(
        send_max_quality_report=AsyncMock(),
        send_file_quality_report=AsyncMock(),
    )
    reporter = QualityReporter(
        session_getter=lambda: session,
        file_quality_getter=lambda: None,
        logger=MagicMock(),
    )

    await reporter.report_current(27)

    session.send_max_quality_report.assert_awaited_once_with(27)
    session.send_file_quality_report.assert_not_awaited()


async def test_file_quality_is_deduplicated_until_reset() -> None:
    """Repeated MA events should not flood identical current-file reports."""
    session = SimpleNamespace(
        send_max_quality_report=AsyncMock(),
        send_file_quality_report=AsyncMock(),
    )
    actual = AudioQualityReport(
        quality=7,
        sampling_rate=44_100,
        bit_depth=24,
        channels=2,
    )
    reporter = QualityReporter(
        session_getter=lambda: session,
        file_quality_getter=lambda: actual,
        logger=MagicMock(),
    )

    await reporter.report_file(actual)
    await reporter.report_file(actual)
    reporter.reset()
    await reporter.report_file(actual)

    assert session.send_file_quality_report.await_count == 2


async def test_max_quality_is_deduplicated_until_reset() -> None:
    """Repeated lifecycle events should not flood an unchanged ceiling."""
    session = SimpleNamespace(
        send_max_quality_report=AsyncMock(),
        send_file_quality_report=AsyncMock(),
    )
    reporter = QualityReporter(
        session_getter=lambda: session,
        file_quality_getter=lambda: None,
        logger=MagicMock(),
    )

    await reporter.report_max(27)
    await reporter.report_max(27)
    reporter.reset()
    await reporter.report_max(27)

    assert session.send_max_quality_report.await_count == 2
