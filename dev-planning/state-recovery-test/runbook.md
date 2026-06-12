# State Recovery Test — Runbook

Topic contract: `recovery-input` is created by the apps with
`segment.ms=60000` (segment rolls every 60 s) and `retention.ms=120000`
(rolled segments deleted after 2 min). Generator produces ~100 msg/s.
All start/stop via Quix CLI against the dev workspace
(`quixdev-ttltestenvironment-ttlgenerator`), e.g.
`quix deployments start|stop <deployment>`.

## Variant A — default auto-recover (earliest)

1. Ensure Recovery Filter vars: `AUTO_RECOVER=true`, `OFFSET_RESET=earliest`.
2. Start **Recovery Generator** first (it creates `recovery-input` with the
   aggressive config), then start **Recovery Filter**.
3. Wait ~2 min. Confirm in filter logs: `[STATE-SIZE-RECOVERY]` lines with
   growing `bytes=` and `live_entries=` approaching 1000 (= KEY_COUNT).
4. Stop **Recovery Filter only**. Generator keeps producing.
5. Wait at least 4 min. If the restart in step 6 shows no recovery, the
   broker's retention sweep (log.retention.check.interval.ms, often 5 min)
   has not run yet — stop the filter again and wait a few more minutes.
6. Restart **Recovery Filter**. Expected log evidence, in order:
   - CRITICAL line starting `DESTRUCTIVE STATE RECOVERY: consumer group
     offset <N> for topic <ws>-recovery-input[0] is below the broker low
     watermark <L>` ... `Local state for stream ... has been deleted` ...
     `state_recovery_offset_reset=earliest`.
   - `[STATE-SIZE-RECOVERY]` `bytes=` near 0 immediately after restart,
     then regrowing; `count` values on `recovery-output` restart from 1.
   - Processing resumes from the low watermark (retained backlog is
     reprocessed).
7. PASS if the CRITICAL line appears, the app keeps running (no crash), and
   state size visibly resets.

## Variant B — fail-fast (`AUTO_RECOVER=false`)

1. Set Recovery Filter var `AUTO_RECOVER=false`. Repeat steps 2–5 above.
2. On restart, expected log evidence:
   - ERROR line: `Consumer group offset <N> for topic <ws>-recovery-input[0]
     is below the broker low watermark <L>. State cannot be recovered safely
     from retained source data. ...`
   - Traceback ending in `StateRecoveryOffsetOutOfRange`; the deployment
     crash-loops. No `DESTRUCTIVE STATE RECOVERY` line; local state is NOT
     deleted.
3. PASS if the exception is raised and the service does not silently recover.
4. Cleanup: set `AUTO_RECOVER=true` back, restart once to let it recover.

## Variant C — auto-recover to latest

1. Set `AUTO_RECOVER=true`, `OFFSET_RESET=latest`. Repeat steps 2–5 of A.
2. On restart, expected log evidence:
   - The same CRITICAL `DESTRUCTIVE STATE RECOVERY` line, ending
     `state_recovery_offset_reset=latest`, with the resume offset equal to
     the broker HIGH watermark.
   - Retained backlog is skipped: first `[GEN]`-aged messages on
     `recovery-output` after restart are fresh ones; counts restart from 1.
3. PASS if recovery fires and consumption resumes at the high watermark.

## Teardown

Stop both deployments (`desiredStatus: Stopped` is the resting state).
