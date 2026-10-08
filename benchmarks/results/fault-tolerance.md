32 clients, 60 s measured, 100 ms samples in `fault-timeline.csv`.

| phase | ops/s |
|---|---:|
| 5 nodes up (2-19 s) | 8237 |
| lowest 1 s window after kill -9 of leader n1 at 20 s | 4155 |
| 4 nodes up (22-39 s) | 9233 |
| lowest 1 s window after kill -9 of leader n5 at 40 s | 3739 |
| 3 nodes up (42-59 s) | 9943 |

Time from kill -9 of the leader until a new leader answered /status: 0.42 s (first kill), 0.56 s (second kill).
