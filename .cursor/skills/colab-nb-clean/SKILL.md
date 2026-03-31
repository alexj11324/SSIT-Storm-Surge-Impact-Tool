---
name: colab-nb-clean
description: Cleans Colab-touched Jupyter .ipynb JSON (metadata, bloated outputs) so diffs shrink and EditNotebook matches succeed. Use when Colab/Colaboratory metadata pollutes notebooks, outputs/base64 bloat files, or agents cannot edit cells due to fuzzy match / string not found.
---

# Colab NB Clean (for reliable Agent edits)

## Symptoms

- `EditNotebook` fails exact replace: `old_string` not found, only fuzzy matches.
- Huge diffs and merge conflicts in `outputs`, `metadata`, long base64 blobs.
- **Metadata pollution** from Colab or browser extensions (`colab`, `widgets`, execution counts, etc.) makes the file unstable.

Root cause: **on disk, `.ipynb` is JSON**; it may not match what the IDE shows (newlines, escaping, cell order, hidden fields). **Large outputs** or **non-standard metadata** make matching and review harder.

## Principles (order of operations)

1. **Clear outputs first, then logic**: most size and conflict comes from `outputs` (including image base64).
2. **Then trim metadata**: keep `nbformat` / `nbformat_minor`; drop or narrow the rest per team policy.
3. **Regenerate with one toolchain**: avoid hand-editing JSON line by line.

## Recommended workflow

### 1) Clear outputs (preferred)

Keep only source and structure with `nbconvert`:

```bash
jupyter nbconvert --to notebook --ClearOutputPreprocessor.enabled=True \
  --inplace path/to/notebook.ipynb
```

If `jupyter` is unavailable, use the Python script below.

### 2) Strip Colab / noisy metadata (one-off Python)

Run from project root or a temp directory:

```python
import json
from pathlib import Path

def clean_nb(path: Path) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))
    # Notebook-level keys to drop (extend as needed)
    for k in ("colab", "widgets"):
        data.get("metadata", {}).pop(k, None)

    for cell in data.get("cells", []):
        cell.setdefault("metadata", {})
        # Cell-level Colab, etc.
        for k in ("colab",):
            cell["metadata"].pop(k, None)
        # Clear outputs to shrink file and avoid base64
        cell["outputs"] = []
        cell["execution_count"] = None

    path.write_text(json.dumps(data, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")

clean_nb(Path("notebooks/shelter_demand.ipynb"))
```

Extend the `pop` list for keys your environment adds.

### 3) Validate with nbformat (optional but useful)

```python
import nbformat
nb = nbformat.read("notebooks/shelter_demand.ipynb", as_version=4)
nbformat.validate(nb)
```

If validation fails, the JSON is broken; fix structure before having an Agent edit cells.

### 4) EditNotebook tips (after cleaning)

- **Match must equal the cell source on disk** (indentation, blank lines); watch for hidden characters when copying from the IDE.
- **Prefer a single, long, unique `old_string`**; avoid short strings that appear in multiple cells.
- If it still fails after cleaning: print `repr()` of each line in the target cell to verify spaces.

## Git / ongoing hygiene

- Consider [`nbstripout`](https://github.com/kynan/nbstripout): strip outputs on commit so noise does not enter the repo.
- Team policy: **default to not committing outputs** in shared notebooks, or use a separate branch for “report” notebooks with outputs.

## Anti-patterns

- Manually deleting a few JSON lines to fix Colab leftovers: easy to miss fields or break commas in `cells`.
- Repeated large Agent replacements without clearing outputs: wrong cell matches or timeouts.

## When to use this skill

- User or Agent reports: notebook won’t edit, replace fails, merge after Colab is painful, `.ipynb` file size explodes.
- Before handing a notebook to an Agent for structured edits, run a cleanup pass.
