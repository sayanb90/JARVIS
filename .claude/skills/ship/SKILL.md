---
name: ship
description: Close out a unit of work. Verifies the change against the ticket's acceptance criteria and the runbook, runs the repo's checks, opens a draft PR keyed to the ticket, moves the ticket to In Review and comments with what changed. Use when the user says they are done, ready to ship, or asks to open a PR.
---

# /ship (pointer)

The procedure lives in the private `dev-kit` repo so it is maintained in one place. This file only points to it and should not change.

1. Find a checkout of dev-kit: try `../dev-kit`, `~/dev-kit`, `/home/user/dev-kit`, or any directory named `dev-kit` next to this repo.
2. Read `skills/ship/SKILL.md` there in full and follow it for this task.
3. If you cannot find it, stop and tell the user: "dev-kit is not available in this session. Add `sayanb90/dev-kit` as an additional repository (cloud) or clone it next to this repo (local), then run /ship again." Do not improvise the procedure.
