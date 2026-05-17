# Capture data
To understand the qobuz protocol better, this folder contains captured WebSocket data from the Qobuz Web Client.
The setup consisted of 2 Qobuz Clients:
- 1 Web Client (QWeb)
- 1 MacOS Client (QMac)

## capture-1.json
This capture just shows the initial connect and track changing behaviour.
QMac was already running, the Capture shows the WebSocket data from QWeb after reloading the page for the following process:

- QMac starts a track in its own player
- QMac uses Qobuz Connect to target QWeb while playing the track
- QWeb starts playing the handed-over track
- QWeb changes to another track



## capture-2.json
This capture focuses on advanced queue modification and track favoriting features:

- QWeb empties the queue
- QWeb starts a track
- QMac adds a track to the queue
- QWeb adds a track to the queue
- QWeb pauses
- QWeb starts again
- QWeb reorders queue (moving the currently plaing item 3 positions ahead of the queue)
- QWeb favorites track
- QWeb removes track from favorites


## capture-3.json
Rapid track skipping tests:
- QMac start track from a playlist
- 2x QMac rapidly skips a few tracks -> QWeb only shows and plays the last track that is supposed to play
- QMac skips tracks but in a slower pace -> QWeb shows every track but because of buffering, which is cancelled because of the next track commands, only the last track is played
