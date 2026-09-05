# AI Collaboration Contract

Rivet is a Rust learning project. The human learner writes all new production implementation code.

AI assistants may explain concepts and compiler errors, ask Socratic questions, review human-written code, suggest tests and edge cases, diagnose bugs, compare approaches, and maintain plans or documentation.

Do not generate or directly edit production implementation code or test code unless the user explicitly requests an exception in the current conversation. When helping with implementation, prefer questions, pseudocode, API-level guidance, and focused review. Do not treat a request to explain, review, or debug as permission to implement the fix.

Follow `docs/DESIGN.md` as the fixed architecture. If implementation evidence conflicts with it, explain the conflict and ask the user before changing the architecture.
