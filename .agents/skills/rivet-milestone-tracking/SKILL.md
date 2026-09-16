---
name: rivet-milestone-tracking
description: Plan and track Rivet GitHub milestones through one comprehensive issue checklist. Use when starting or planning a milestone, reviewing milestone work, asking what is next, or updating progress on a milestone issue.
---

# Rivet milestone tracking

Maintain one authoritative implementation-tracker comment on the milestone issue. The tracker must cover the entire milestone, not only the current phase.

Follow `AGENTS.md` and `docs/DESIGN.md`. Planning does not authorize production or test implementation. Updating a GitHub comment is an external mutation, so do it only when the user asks to create, post, maintain, check off, or update the milestone plan or tracker.

## Start or plan a milestone

1. Read the milestone issue, relevant design documentation, current code, tests, and any existing planning comments.
2. Determine what is already complete from repository evidence. Do not infer completion solely from issue prose.
3. Create or replace one issue comment titled `<Milestone> implementation tracker`.
4. Include every planned phase from start through milestone completion. Under each phase, include implementation tasks and meaningful test checkpoints as GitHub checkboxes.
5. Mark already verified work complete and leave all unverified or future work unchecked.
6. Include an explicit `Current position` naming the next phase and step.
7. Include final verification items for formatting, linting, tests, platform check scripts when present, and documentation consistency.
8. Return the direct link to the tracker comment.

Prefer editing the existing authoritative tracker over posting a new tracker. Avoid splitting phase plans across comments when the master tracker can hold them clearly.

## Review milestone work

When the user asks to review, check, or confirm milestone work:

1. Locate the authoritative milestone tracker comment.
2. Inspect the relevant diff and surrounding implementation.
3. Run checks proportional to the reviewed item. A focused test may establish a narrow behavior; phase completion normally requires its relevant suite and quality checks.
4. Report concrete defects before marking the associated item complete.
5. If the reviewed work is complete, edit the existing tracker and change only verified boxes from `[ ]` to `[x]`.
6. Keep incomplete, failing, stubbed, or untested items unchecked.
7. Move `Current position` to the first remaining actionable step.
8. If review finds no newly completed tracker item, leave the comment unchanged and say why.

Never mark an entire phase complete when only its implementation or only its tests are complete unless the tracker explicitly separates those states. Never mark the milestone complete while any required phase, verification, or documentation item remains open.

## Answering progress questions

Use the tracker as the status index, but verify it against the repository when accuracy is in doubt. State the current phase and step, what is complete, and the immediate next task. Update the tracker when the user asked for a review or progress update and repository evidence shows its state has changed.
