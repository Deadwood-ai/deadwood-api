# Linear Issue Template

Use this template when creating new issues in Linear for the DeadTrees project.

## Issue Title

[Brief, descriptive title focusing on user value - avoid technical jargon]

## User Story

**User Story:** As a [user type], I want [functionality], so that [benefit/value].

## Current Problem

[Describe the current situation and why it needs to change. Include specific pain points or limitations.]

## Scope

[List the main deliverables and boundaries of this work:]

- [Main deliverable 1]
- [Main deliverable 2]
- [Main deliverable 3]
- [What is explicitly NOT included]

## Dependencies

[List what needs to be in place before this work can begin:]

- **[System/Component]:** [Specific requirement]
- **[System/Component]:** [Specific requirement]
- **[System/Component]:** [Specific requirement]

## Key Changes Required

[Numbered list of the main technical changes, with brief code examples if helpful:]

1. **[Component/File Name]:**

   ```typescript
   // Current: [current approach]
   // New: [new approach]
   ```

2. **[Component/File Name]:**
   ```typescript
   // [Brief code example showing the change]
   ```

## Files Affected

[List the specific files that will need changes:]

- `[file/path]` - [Description of changes needed]
- `[file/path]` - [Description of changes needed]
- `[file/path]` - [Description of changes needed]

## Technical Details

[Additional technical considerations, edge cases, or implementation notes:]

- [Technical detail 1]
- [Technical detail 2]
- [Technical detail 3]

## Acceptance Criteria

[Clear, testable criteria for when this issue is complete:]

- [ ] [Criteria 1]
- [ ] [Criteria 2]
- [ ] [Criteria 3]

---

## Template Usage Notes

### ⚠️ Important: Review Before Creating

**Always present the implementation plan to the user for review before creating the Linear issue.** Let them confirm the approach, scope, and technical details are correct.

### Context Gathering

Before creating issues, gather necessary context:

- **Database Schema:** Use configured local Supabase/Postgres MCP or Supabase CLI for local tables;
  follow [trusted analyst access](playbooks/analyst-database-access.md) for production reads.
- **Key Tables:** `v2_datasets`, `v2_statuses`, `v2_queue`, `v2_logs`, `v2_labels`, `v2_orthos`
- **Codebase:** Use `rg` and existing module patterns before proposing work.
- **Upload Flow:** Check `frontend/src/components/Upload/`, `api/src/routers/upload.py`, and `api/src/upload/`.
- **Database Operations:** Review `shared/db.py`, `shared/models.py`, and existing migrations.

### MCP Issue Management Workflow

Before creating a new issue, always check for existing similar issues:

1. **Search for existing issues:**

   Search Linear for relevant keywords in the DeadTrees workspace/project.

2. **If similar issue exists:**
   - Update the existing issue instead of creating a duplicate
   - Update the existing Linear issue with new requirements or details
   - **Status Management:** Keep new or updated agent-discovered issues in `Triage`
     unless the user explicitly asks to move them.

3. **Project Assignment:**
   - Always assign issues to the appropriate project
   - Common projects: "Upload Functionality", "Data Visualization", "Authentication"
   - If unclear which project, ask the user explicitly

4. **Status Guidelines:**
   - **New agent-created issues:** Start in `Triage` for human review.
   - **Updated issues:** Do not move status unless the user explicitly asks or the
     current task is specifically Linear triage.
   - **Assignment:** Leave `Triage` and `Backlog` items unassigned unless the user says otherwise.

5. **Label Assignment:**
   - **Always assign appropriate labels** when creating issues
   - **Bug:** Something broken, not working as expected, errors
   - **Feature:** New functionality, enhancement, or capability
   - **Improvement:** Optimization, refactoring, or polish of existing features
   - **Research:** Investigation, spike work, or technical exploration

### Issue Categories

- **Feature:** New functionality or enhancement
- **Bug:** Something broken that needs fixing
- **Improvement:** Optimization or refactoring
- **Research:** Investigation or spike work

### Priority Guidelines

- **Urgent:** Blocking other work or critical user impact
- **High:** Important for current sprint/milestone
- **Medium:** Should be done soon, not blocking
- **Low:** Nice to have, future consideration

### User Types

Common user types for DeadTrees:

- Data manager
- Forest researcher
- Platform administrator
- API consumer
- End user
- System/automated process

### Writing Tips

1. **Start with user value** - Always lead with why this matters to users
2. **Be specific** - Avoid vague terms like "improve" or "enhance"
3. **Include context** - Explain the current state and desired future state
4. **Think dependencies** - What needs to happen first?
5. **Consider scope** - What's included and what's not?
6. **Make it testable** - Acceptance criteria should be verifiable

### Common Patterns

- **Database-first approach:** Create records before file operations
- **Progressive enhancement:** Build basic functionality first, then add features
- **Error handling:** Always consider error states and user feedback
- **Performance:** Consider impact on large datasets and slow connections
