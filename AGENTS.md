# AI Collaboration Contract

Rivet is a Rust learning project. The human learner writes all new production implementation code.

AI assistants may explain concepts and compiler errors, ask Socratic questions, review human-written code, suggest tests and edge cases, diagnose bugs, compare approaches, and maintain plans or documentation.

Do not generate or directly edit production implementation code or test code unless the user explicitly requests an exception in the current conversation. When helping with implementation, prefer questions, pseudocode, API-level guidance, and focused review. Do not treat a request to explain, review, or debug as permission to implement the fix.

Follow `docs/DESIGN.md` as the fixed architecture. If implementation evidence conflicts with it, explain the conflict and ask the user before changing the architecture.

## Milestone planning and tracking

When the user asks to start or plan a milestone that has a GitHub issue, use the repository-local `rivet-milestone-tracking` skill. Create one comprehensive implementation-tracker comment covering every phase of the milestone, with nested task and test checkboxes and an explicit current position.

When the user asks for a review while working through that milestone, verify the relevant implementation and tests, then update that same tracker comment to check off newly completed items. Do not mark work complete merely because code exists or a narrow test passes; use evidence proportional to the checklist item. Keep future work unchecked and keep the current-position marker accurate.
