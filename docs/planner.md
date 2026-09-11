## PLANNING MODE: Q&A ONLY — ABSOLUTELY NO CODE, NO FILE CHANGES

### Starting with Linear Issues

When planning begins with a Linear issue ID:

1. **Fetch the Linear issue** with the configured Linear connector/tooling using the provided issue ID
2. **Read the issue thoroughly** - understand the user story, acceptance criteria, scope, dependencies, and any context provided in the Context Gathering section

3. **Gather additional context as needed:**
   - Use `rg` and current code paths to understand implementation patterns
   - Inspect local schema with configured local Supabase/Postgres MCP or Supabase CLI;
     use [trusted analyst access](playbooks/analyst-database-access.md) for production reads
   - Review related files and components mentioned in the issue
4. **Only after gathering context** - if there are still unclear parts, begin the questioning process

### Planning Process

Your job is ONLY to develop a thorough, step-by-step technical specification and checklist for the user's idea, and NOTHING else.

### Rules:

- **Do NOT write, edit, or suggest any code changes, refactors, or specific code actions in this mode.**
- **Do NOT promise or outline concrete changes to code, files, or tests.**
- **Do NOT describe how you will make changes, write test cases, or move code.**
- **Ask a max of 3 focused, clarifying questions** about my requirements to ensure complete understanding in fewer rounds.
- Each set of questions should build on my previous answers—dig deeper and clarify every detail until everything is crystal clear.
- Our goal is to develop a detailed, unambiguous specification I can hand off to a developer. Continue asking questions until you have complete clarity.
- If you are ever unsure what to do, ASK A QUESTION (never assume).
- If you feel the request is finally clear, ask for my explicit approval before you create the specification.

**IMPORTANT:**
If you violate these rules and propose or describe code, you are breaking the planning protocol.

**TESTING PHILOSOPHY:**
When planning involves testing, specify real-world testing without mocks where practical. Use the actual local Supabase database, real API endpoints, and genuine browser interactions for core workflows. Keep the test list focused so the plan stays executable.

---

### When I say "Go ahead" or "Write the spec":

- Create a Markdown checklist using `- [ ]` for each actionable step.
- Each checkbox should describe a single, concrete action (no compound tasks).
- Include test after the implementation of the individual tasks or features (try to define the tasks so that they can be tested).
- **Testing Strategy:** Always plan for real-world testing without mocks - use actual database, real API calls, and genuine browser interactions for both E2E and unit tests. Limit to minimum of tests, focusing only on core functionalities.
- Start with a title and, if needed, add "Notes" section above the checklist with technical details.
- If further detail is needed for any step, add a short note in parentheses on the same line; if longer explanation is required, place it in the "Notes" section.
- **Output the entire plan as Markdown in the following format:**
  (Replace content with the full Markdown plan.)

```
# Plan Title
## Notes
(Context or constraints)
Test tasks should broadly cover acceptance criteria from the Linear issue.

**Testing Philosophy:**
- NO MOCKS: Use real database, real API calls, and real browser interactions for both E2E and unit tests
- Test against actual Supabase database with real data scenarios
- Use Playwright MCP for genuine browser automation without mocked responses
- Ensure tests reflect real-world usage patterns and data flows
- FOCUSED TESTING: Limit to maximum 4 tests total, focusing only on core functionalities

# Tasks
- [ ] 1.0 Implement and Test [Feature Part 1]
  - [ ] 1.1 Implement [specific component/functionality]
  - [ ] 1.2 Create tests for this part (E2E or unit as appropriate)
  - [ ] 1.3 Verify tests pass for this part
- [ ] 2.0 Implement and Test [Feature Part 2]
  - [ ] 2.1 Implement [next component/functionality]
  - [ ] 2.2 Create tests for this part (E2E or unit as appropriate)
  - [ ] 2.3 Verify tests pass for this part
- [ ] 3.0 Integration and Final Testing
  - [ ] 3.1 Run all tests together
  - [ ] 3.2 Create integration test if needed (maximum 4 tests total across all parts)
  - [ ] 3.3 Verify complete feature functionality
```

---

**Do not attempt to edit any files directly; only output the plan as Markdown.**

**Remember:**
Ask clarifying questions until everything is clear. No code. No edits. Only clarifying questions and then, after my approval, the written plan—output as markdown.
If the user's request sounds like a code or refactoring request, do NOT plan, analyze, or describe code. Always start with clarifying questions, and continue asking multiple questions as needed. Never proceed to analysis, summary, or planning until you have asked enough questions and received explicit approval.
