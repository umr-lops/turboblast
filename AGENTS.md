# AGENTS.md

`turboblast` is a Python library that submits high-throughput **SLURM job
arrays** via [submitit](https://github.com/facebookincubator/submitit). You give
it a text file (one line of CLI args per task) and a bash script; it dispatches
each line as one SLURM array task running `bash <script> <line>`. Large listings
are split into chunks and submitted **sequentially** (one array in flight at a
time) to stay under cluster limits.

## Commands

The repo is a `uv`-managed, nox-driven template (see `noxfile.py`). Prefer `uv`.

```bash
uv sync --group test      # create env + install test deps
uv run pytest             # unit tests (CI runs: pytest -ra --cov)
nox -s lint               # prek (pre-commit) run --all-files
nox -s pylint             # pylint turboblast
nox -s docs               # sphinx build (docs/Makefile)
nox -s build              # python -m build
```

- Linting/formatting is via `pre-commit` (the nox `lint` session uses `prek`,
  the same hook runner). Local `pre-commit run --all-files` is equivalent.
- CI (`.github/workflows/ci.yml`) runs: pre-commit (all files) → pytest on a
  matrix of **Python 3.10 and 3.14 × ubuntu/windows/macos**, with coverage to
  codecov. `cd.yml` builds the sdist/wheel and publishes on release.

## Tooling (from `pyproject.toml`)

- **ruff** (`ruff-check --fix` + `ruff-format`): large rule set, plus ignores
  `PLR09`, `PLR2004`, `TRY003`, `EM102`, `TRY300`, `PERF203`, `BLE001`.
- **mypy**: `strict = true`, files `src|tests|noxfile.py`;
  `disable_error_code = ["type-arg"]` for `turboblast.*`.
- **prettier** formats yaml/markdown/json/etc (`--prose-wrap=always`).
- **pytest** uses `filterwarnings = ["error"]` — **warnings are errors in
  tests**; keep test code warning-clean.

## Layout

- `src/turboblast/blaster.py` — all the logic: `process_line()` (runs one task
  on the compute node via submitit), `submit_chunk_with_retry()`,
  `wait_for_batch_completion()` (blocking poll + stall detection), `main()`
  (chunk + submit loop), `entrypoint()` (CLI).
- `src/turboblast/__init__.py` — exposes `__version__`.
- `src/turboblast/logo.py` — ASCII art shown in `--help`.
- `tests/test_blaster.py` — the unit tests (mock `submitit`/`subprocess`).

## Versioning / release

- Version is **dynamic via flit-scm** from git tags — there is no version field
  to edit anywhere. `src/turboblast/_version.py` is generated at build time (do
  not edit). Release = tag, push, publish (see `cd.yml`).

## Domain gotchas (the Ifremer HPC SLURM limits)

- **`CHUNK_SIZE = 1000` must stay strictly below `MaxArraySize` (= 1001).** Do
  not "simplify" it to 1001 or higher — submissions fail with
  `JobArrayTaskLimit`.
- **`--slurm-array-parallelism` (default 20)** maps to `--array=0-999%N` and
  caps concurrent tasks. Throughput = `parallelism / per-task duration`. For
  short tasks the default of 20 is often very low — raising it (subject to
  cluster capacity / `MaxJobCount`) is the main knob to go faster.
- The **tqdm `s/task` is an aggregate rate** (elapsed / completed), **not** the
  per-task wall time. With N in parallel, a task that actually takes T seconds
  shows as `T/N s/task` — easily misread as "stuck". (See issue #11.)
- submitit array `job_id`s are `base_task` (e.g. `528266_108`). When cancelling
  you must use the **full per-task id**, not the base — `scancel 528266` cancels
  the whole array. (See issue #10: stall detection currently uses the base id.)
- Each task relaunches the bash script (and, in our workloads, an apptainer
  container) — that startup cost dominates short tasks.
- `main()` **blocks** until all tasks reach a terminal state (live progress
  bar). Run long runs under `tmux`/`nohup`.

## Git

- Default branch `main`; never push to it directly. Work on feature branches,
  open PRs. Atomic commits; conventional-commit style is fine (CI/dependabot use
  prefixes like `ci:`, `fix:`, `chore(deps):`).
