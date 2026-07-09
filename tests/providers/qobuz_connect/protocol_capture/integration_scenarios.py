"""
Live integration scenarios: real web-client actions asserted against MA.

Each scenario resets to a clean state, performs one or more controller
actions via the web client, waits, then asserts MA's observable behaviour
(from its debug log) matches what a Qobuz web-client renderer would do.

Behaviour is grounded against real reference captures (``.runs/``):

- A newly-active renderer immediately continues playing the current track
  (handoff reference: ``rndrSrvrStateUpdated playingState=PLAYING`` at
  ``srvrRndrSetActive``).
- Loop/shuffle mode changes are **controller-managed** — they are broadcast
  to controllers (``srvrCtrlLoopModeSet`` / ``srvrCtrlShuffleModeSet``) but
  are **never** sent to the renderer, which just plays what ``SET_STATE``
  tells it. MA (renderer) must therefore *not* restart audio on a mode
  toggle. Verified in ``ref_modes__client_b`` capture.
- Volume **is** sent to the renderer (``srvrRndrSetVolume``); MA must apply it.

Track ids belong to Daft Punk — Discovery (ALBUM_URL): 16 tracks
1065476..1065491, index 0 == id 1065476.
"""

from __future__ import annotations

import asyncio
import logging

from .integration_harness import IntegrationSession, ScenarioResult, ma_play_media

LOGGER = logging.getLogger(__name__)

TRACK0 = 1065476  # first track of the reference album
ALBUM_A_IDS = set(range(1065476, 1065492))  # Daft Punk — Discovery
ALBUM_B_URL = "https://play.qobuz.com/album/0060694932902"  # Eminem — The Eminem Show
ALBUM_B_IDS = set(range(3879017, 3879037))  # Eminem — The Eminem Show (20 tracks)


async def scenario_handoff(session: IntegrationSession) -> ScenarioResult:
    """
    Hand off from the web client to MA — MA must take over and play.

    A real web renderer, on becoming active, immediately continues playing
    the current track. MA must become active, adopt the current track, and
    stream it.
    """
    result = ScenarioResult(scenario="handoff")
    await session.reset_to_clean_state()

    cursor = session.ma.cursor()
    await session.handoff_to_ma()
    await asyncio.sleep(9)
    ev = session.observe(cursor)

    result.check(
        "MA received SET_ACTIVE and became active",
        any(r.event == "CloudSetActive" and r.active for r in ev.reduces),
    )
    last = ev.last_reduce()
    result.check("MA final state is active", bool(last and last.active))
    result.check(
        "MA started streaming a track",
        bool(ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    if ev.streams:
        result.check(
            "MA streamed the album's current track (0:1065476)",
            ev.streams[-1].track_id == TRACK0,
            detail=f"streamed={ev.streams[-1].track_id} title={ev.streams[-1].title!r}",
        )
    result.check(
        "MA is playing after handoff",
        any(rep.state == 2 for rep in ev.reports) or bool(last and last.playing == "PLAYING"),
    )
    return result


async def scenario_skip_next(session: IntegrationSession) -> ScenarioResult:
    """After handoff, a web-client skip must advance MA to the next track."""
    result = ScenarioResult(scenario="skip_next")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    await asyncio.sleep(8)

    cursor = session.ma.cursor()
    await session.q.skip_next()
    await asyncio.sleep(7)
    ev = session.observe(cursor)

    result.check(
        "MA streamed a new track after skip",
        bool(ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    if ev.streams:
        result.check(
            "skip advanced MA off track 0",
            ev.streams[-1].track_id != TRACK0,
            detail=f"streamed={ev.streams[-1].track_id} title={ev.streams[-1].title!r}",
        )
    plays = [e for e in ev.effects() if e == "MaPlayTrack"]
    result.check(
        "no play storm (<=3 MaPlayTrack for one skip)",
        len(plays) <= 3,
        detail=f"MaPlayTrack count={len(plays)}",
    )
    return result


async def scenario_skip_twice(session: IntegrationSession) -> ScenarioResult:
    """
    Handoff then two skips — the "handover worked, then skip broke it" case.

    MA must follow each skip and end up playing (not silent, not looping).
    """
    result = ScenarioResult(scenario="skip_twice")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    await asyncio.sleep(8)

    cursor = session.ma.cursor()
    await session.q.skip_next()
    await asyncio.sleep(4)
    await session.q.skip_next()
    await asyncio.sleep(7)
    ev = session.observe(cursor)

    result.check(
        "MA streamed after the two skips",
        bool(ev.streams),
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    last = ev.last_reduce()
    result.check("MA final state active", bool(last and last.active))
    result.check(
        "MA is playing after two skips",
        any(rep.state == 2 for rep in ev.reports) or bool(last and last.playing == "PLAYING"),
        detail=f"last={last.raw if last else 'none'}",
    )
    plays = [e for e in ev.effects() if e == "MaPlayTrack"]
    result.check(
        "no play storm (<=4 MaPlayTrack for two skips)",
        len(plays) <= 4,
        detail=f"MaPlayTrack count={len(plays)}",
    )
    return result


async def scenario_new_album_after_handoff(session: IntegrationSession) -> ScenarioResult:
    """
    Load a different album from the app after handoff.

    Reproduces the "MA showed a broken state after I played another song"
    case: MA must adopt the new cloud queue and stream a track from the new
    album, not stay stuck on the old queue.
    """
    result = ScenarioResult(scenario="new_album_after_handoff")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    await asyncio.sleep(8)

    cursor = session.ma.cursor()
    # App loads a different album while MA is the active renderer. The web
    # client re-navigates (WS reconnect churn), so the QueueTracksLoaded and
    # the resulting stream can take ~15-25s to land — poll for the definitive
    # signal (a stream from the new album) rather than snapshotting once.
    await session.q.play_album_by_url(ALBUM_B_URL)
    streamed_b = False
    for _ in range(15):
        await asyncio.sleep(2)
        ev = session.observe(cursor)
        if any(s.track_id in ALBUM_B_IDS for s in ev.streams):
            streamed_b = True
            break
    ev = session.observe(cursor)

    result.check(
        "MA streamed a track from the new album (not album A)",
        streamed_b,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    b_reduces = [
        r for r in ev.reduces if r.current_id.isdigit() and int(r.current_id) in ALBUM_B_IDS
    ]
    result.check(
        "MA canonical current adopted the new album",
        bool(b_reduces),
        detail=f"events={sorted({r.event for r in ev.reduces})}",
    )
    result.check(
        "MA is active + playing the new album",
        any(r.active and r.playing == "PLAYING" for r in b_reduces),
        detail=f"album-B reduces={[(r.active, r.playing) for r in b_reduces][-3:]}",
    )
    return result


async def scenario_modes_dont_disturb(session: IntegrationSession) -> ScenarioResult:
    """
    Toggling loop/shuffle on the controller must not disturb MA's playback.

    Loop/shuffle are controller-managed and never reach the renderer
    (verified in ref_modes capture). A shuffle may reorder the queue
    (QueueTracksLoaded), which MA adopts seamlessly — but audio must never
    restart from a mode toggle.
    """
    result = ScenarioResult(scenario="modes_dont_disturb")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    await asyncio.sleep(8)

    cursor = session.ma.cursor()
    await session.q.toggle_repeat()
    await asyncio.sleep(3)
    await session.q.toggle_shuffle()
    await asyncio.sleep(5)
    ev = session.observe(cursor)

    plays = [e for e in ev.effects() if e == "MaPlayTrack"]
    result.check(
        "mode toggles did not restart playback",
        len(plays) == 0,
        detail=f"MaPlayTrack count={len(plays)} events={[r.event for r in ev.reduces]}",
    )
    last = ev.last_reduce()
    result.check(
        "MA still active and playing after mode toggles",
        bool(last and last.active and last.playing == "PLAYING"),
        detail=f"last={last.raw if last else 'none'}",
    )
    return result


async def scenario_volume(session: IntegrationSession) -> ScenarioResult:
    """
    Verify a web-client volume change reaches MA and is applied.

    Volume is renderer-facing (``srvrRndrSetVolume``); MA must apply it
    without restarting playback.
    """
    result = ScenarioResult(scenario="volume")
    await session.reset_to_clean_state()
    await session.handoff_to_ma()
    await asyncio.sleep(8)

    cursor = session.ma.cursor()
    # Volume travels controller -> cloud -> renderer and can be debounced or
    # briefly dropped; retry the set a few times while polling for the effect.
    got_volume = False
    for attempt in range(3):
        await session.q.set_volume_percent(30 + attempt * 10)
        for _ in range(6):
            await asyncio.sleep(2)
            if "MaSetVolume" in session.ma.events_since(cursor).effects():
                got_volume = True
                break
        if got_volume:
            break
    ev = session.observe(cursor)

    result.check(
        "MA received a volume command and applied it (MaSetVolume)",
        got_volume,
        detail=f"effects={ev.effects()} events={sorted({r.event for r in ev.reduces})}",
    )
    plays = [e for e in ev.effects() if e == "MaPlayTrack"]
    result.check(
        "volume change did not restart playback",
        len(plays) == 0,
        detail=f"MaPlayTrack count={len(plays)}",
    )
    return result


async def scenario_initiate_from_ma(session: IntegrationSession) -> ScenarioResult:
    """
    Start Qobuz playback on the MA side.

    MA must play and claim the cloud renderer role so controllers (the web
    client / phone app) follow it. Requires an MA access token in
    ``MA_TOKEN`` (player commands are authenticated); skips cleanly when unset.
    """
    result = ScenarioResult(scenario="initiate_from_ma")
    await session.reset_to_clean_state()
    await session.q.select_local_output()  # ensure MA is not already active
    await asyncio.sleep(3)

    cursor = session.ma.cursor()
    err = await ma_play_media(f"qobuz://album/{ALBUM_B_URL.rsplit('/', 1)[-1]}")
    if err == "no-token":
        result.check(
            "initiate-from-MA skipped (set MA_TOKEN to run)",
            True,
            detail="MA_TOKEN unset; provide an MA access token to exercise this scenario",
        )
        return result
    result.check("MA accepted the play_media command", err is None, detail=f"error={err}")

    await asyncio.sleep(10)
    ev = session.observe(cursor)
    result.check(
        "MA streamed the Qobuz album it was told to play",
        bool(ev.streams) and ev.streams[-1].track_id in ALBUM_B_IDS,
        detail=f"streams={[s.track_id for s in ev.streams]}",
    )
    last = ev.last_reduce()
    result.check(
        "MA claimed the cloud renderer role (PushSetActive / active)",
        "PushSetActive" in ev.effects() or bool(last and last.active),
        detail=f"effects={sorted(set(ev.effects()))}",
    )
    return result


SCENARIOS = {
    "handoff": scenario_handoff,
    "skip_next": scenario_skip_next,
    "skip_twice": scenario_skip_twice,
    "new_album_after_handoff": scenario_new_album_after_handoff,
    "modes_dont_disturb": scenario_modes_dont_disturb,
    "volume": scenario_volume,
    "initiate_from_ma": scenario_initiate_from_ma,
}
