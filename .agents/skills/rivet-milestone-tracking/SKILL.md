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
4. Include every planned phase from start through milestone completion. Under each phase, include implementation tasks and meaningful test checkpoints as GitHub checkboxes. Break broad outcomes into nested, independently reviewable steps when each requires a different edit, concept, or verification action.
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

## Guided learning slices

Rivet is a learning project. Tracker checkboxes describe milestone outcomes and may still be too large for one learning turn. Before guiding work on the current checkbox, privately decompose it into ordered micro-steps. Give the learner only the next micro-step, not the entire checkbox implementation.

A micro-step should normally require one small edit in one location or one verification command. It should have one immediate observable result and be reviewable on its own. If guidance asks the learner to create the test shell, arrange fixtures, perform the action, add all assertions, and run multiple checks, it is still too large and must be split further.

### Coverage and abstraction gate

Before proposing or creating any test, stop and answer all of these from repository evidence:

1. **Owning layer:** Which module owns the behavior or invariant?
2. **Existing evidence:** Which existing tests already exercise it at that layer?
3. **Unique regression:** What specific bug in the layer under current work would this new test catch that the owning-layer tests cannot catch?
4. **Public observation:** Can the test prove that regression using only the current layer's public contract or ordinary outputs?
5. **Coupling cost:** Would the setup or assertion need a dependency's private fields, private helpers, binary layout, filenames, or failure-injection tricks that belong to the lower layer?

Create a test only when there is a concrete unique regression and it can be observed at the correct abstraction boundary. A wrapper does not need to repeat every behavior or error case of the wrapped component. For a thin wrapper, lower-layer behavior tests plus generic delegation/error-conversion evidence are sufficient unless the wrapper adds parameter translation, state, branching, lifecycle behavior, or error remapping that can independently be wrong.

If the proposed test fails this gate, do not create a stub and do not ask the learner to implement it. Cite the existing coverage, explain why the test would be redundant or over-coupled, and advance to the next genuine gap. Treat tracker wording as revisable planning, not as authority to violate encapsulation or duplicate coverage.

When a slice needs a new test, create the failing test stub for the learner before assigning implementation steps. The stub must follow the repository's `AGENTS.md`: include only the test attribute, descriptive function signature, focused sequential implementation-guidance comments, behavior TODOs, and a required `todo!()` call. Do not make the learner type boilerplate shells, and do not add imports, setup code, variables, constructors, helpers, or assertions to an unfinished stub. The learner replaces the stub incrementally; never count it as completed coverage. Make each TODO self-sufficient: when its implementation depends on a size, threshold, layout, offset, or other derived value, state the governing formula or invariant, give the concrete fixture shape, and explain the resulting value. Avoid vague directions such as "use a small threshold" when the learner needs unstated domain knowledge to choose it.

When giving the next micro-step, inspect the relevant code and APIs first, then provide:

1. **Parent outcome:** Name the tracker checkbox this micro-step advances.
2. **This action only:** State the one edit or command to perform, its exact location, and the immediate result. Explicitly list what not to do yet.
3. **Minimal context:** Explain only the APIs, values, and Rust mechanics needed for this action. Explain non-obvious constants rather than presenting magic numbers.
4. **Small example:** Provide the smallest relevant syntax example or skeletal shape. Preserve meaningful implementation work for the learner unless the user explicitly requests a current-conversation exception to write completed implementation or test code. Do not disguise a complete solution as fragments.
5. **Single check:** Give one proportional check for this action. Compilation is optional when an intentionally incomplete test shell would fail or warn; say what state to expect.
6. **Handoff:** Tell the learner exactly what small snippet or output to share. Stop there. After review, give the next micro-step and briefly show progress such as `2 of 6` within the parent outcome.

Whenever referring to a repository file or a specific point in one, use a clickable Markdown link to the absolute local path and include the relevant starting line when known, for example `[partition.rs](/absolute/path/src/broker/partition.rs:468)`. Use these links for edit locations, nearby examples, design requirements, and review findings so the learner can navigate directly to the referenced code. Do not substitute a bare path or inline-code filename when a navigable link is available.

Do not front-load later micro-steps as a detailed checklist. A one-line preview of what follows is enough when it helps orientation. Keep the complete decomposition available for continuity, but reveal it progressively.

Prefer concrete instruction over open-ended prompts. Ask Socratic questions only when they help the learner reason about a specific Rust or systems concept; do not require the learner to rediscover architecture or routine API usage already fixed by `docs/DESIGN.md`.
