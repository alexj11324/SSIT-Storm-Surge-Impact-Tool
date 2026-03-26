---
name: verify
description: Run ruff lint + pytest to verify code quality before committing
---

Run the following checks in order from the project root. Stop at the first failure and report the issue.

1. **Lint**: `ruff check . --config "line-length=120"`
2. **Format check**: `ruff format --check . --config "line-length=120"`
3. **Tests**: `python -m pytest tests/ -v`

Report a summary of results:
- Number of lint errors (if any)
- Number of format issues (if any)
- Test results (passed/failed/skipped)

If all pass, confirm the codebase is clean.
