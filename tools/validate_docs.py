#!/usr/bin/env python3
"""
validate_docs.py — fast sanity checks that docs match repo reality.

Run from repo root:

  python scripts/validate_docs.py
  python scripts/validate_docs.py --docs-dir docs --strict
  python scripts/validate_docs.py --run-cli-help

What it does (high-signal checks):
- Verifies required doc files exist (or reports if restructured)
- Searches for known legacy / contradictory phrases (e.g. "no CLI yet", "archive is canonical")
- Ensures the canonical Jupyter API sequence is present somewhere in docs
- Ensures key metadata tables are mentioned
- Confirms partition semantics mention date= and month= for 1d
- Discovers console entrypoints from pyproject.toml and can run `--help`
- Cross-checks docs mention at least one discovered CLI command name

This is intentionally lightweight and "rough but useful".
"""

from __future__ import annotations

import argparse
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

# ---- Helpers ---------------------------------------------------------------

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")

def find_md_files(docs_dir: Path) -> List[Path]:
    out = []
    for p in docs_dir.rglob("*.md"):
        if "_review" in p.parts:
            continue
        if p.is_file():
            out.append(p)
    return sorted(out)

def grep(pattern: str, text: str, flags: int = re.IGNORECASE) -> List[Tuple[int, str]]:
    """Return list of (line_no, line) for matches."""
    rx = re.compile(pattern, flags)
    hits = []
    for i, line in enumerate(text.splitlines(), start=1):
        if rx.search(line):
            hits.append((i, line.strip()))
    return hits

def sh(cmd: List[str], cwd: Path, timeout: int = 20) -> Tuple[int, str, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=timeout,
    )
    return p.returncode, p.stdout, p.stderr

# ---- TOML parsing (best-effort) -------------------------------------------

def parse_console_scripts_from_pyproject(pyproject_path: Path) -> Dict[str, str]:
    """
    Best-effort parser for [project.scripts] (PEP 621) and [tool.poetry.scripts].
    Avoids external deps.

    Returns: {script_name: "module:callable"}.
    """
    if not pyproject_path.exists():
        return {}

    txt = read_text(pyproject_path)

    # Very simple state-machine: find section headers and parse key = "value" pairs
    scripts: Dict[str, str] = {}

    def parse_section(section_name: str) -> Dict[str, str]:
        out: Dict[str, str] = {}
        # Match section start: [project.scripts]
        sec_rx = re.compile(rf"^\[{re.escape(section_name)}\]\s*$", re.MULTILINE)
        m = sec_rx.search(txt)
        if not m:
            return out
        start = m.end()

        # Section ends at next [something]
        next_sec = re.search(r"^\[.+?\]\s*$", txt[start:], re.MULTILINE)
        end = start + (next_sec.start() if next_sec else len(txt) - start)
        block = txt[start:end]

        # Parse key = "value" or key="value"
        kv_rx = re.compile(r'^\s*([A-Za-z0-9_.-]+)\s*=\s*"(.*?)"\s*$', re.MULTILINE)
        for km in kv_rx.finditer(block):
            out[km.group(1)] = km.group(2)
        return out

    scripts.update(parse_section("project.scripts"))
    scripts.update(parse_section("tool.poetry.scripts"))

    return scripts

# ---- Checks ---------------------------------------------------------------

@dataclass
class CheckResult:
    ok: bool
    title: str
    details: str = ""

def check_required_docs(docs_dir: Path, expected: List[str]) -> CheckResult:
    missing = []
    for rel in expected:
        if not (docs_dir / rel).exists():
            missing.append(rel)
    if missing:
        return CheckResult(
            ok=False,
            title="Expected doc files present",
            details="Missing (may be fine if restructured):\n  - " + "\n  - ".join(missing),
        )
    return CheckResult(ok=True, title="Expected doc files present")

def check_canonical_run_sequence(all_docs_text: str) -> CheckResult:
    # Accept either exact lines or a fenced block with these calls.
    required_calls = ["fetch_futures()", "ingest_local_downloads_to_lake()", "resample_1m_to_1d()", "build_metadata()"]
    missing = [c for c in required_calls if c not in all_docs_text]
    if missing:
        return CheckResult(
            ok=False,
            title="Canonical Jupyter API run sequence present",
            details="Missing calls in docs:\n  - " + "\n  - ".join(missing),
        )
    return CheckResult(ok=True, title="Canonical Jupyter API run sequence present")

def check_metadata_tables(all_docs_text: str) -> CheckResult:
    tables = [
        "md_file_manifest",
        "md_partition_coverage",
        "md_symbol_dataset_stats",
        "md_alignment_summary",
        "md_liquidity_daily",
        "md_kline_integrity_day",
    ]
    missing = [t for t in tables if t not in all_docs_text]
    if missing:
        return CheckResult(
            ok=False,
            title="Key metadata tables mentioned",
            details="Missing tables in docs:\n  - " + "\n  - ".join(missing),
        )
    return CheckResult(ok=True, title="Key metadata tables mentioned")

def check_partition_semantics(all_docs_text: str) -> CheckResult:
    # Rough but effective: ensure both tokens appear somewhere.
    needed = ["date=", "month="]
    missing = [t for t in needed if t not in all_docs_text]
    if missing:
        return CheckResult(
            ok=False,
            title="Partition semantics include date= and month=",
            details="Missing tokens in docs:\n  - " + "\n  - ".join(missing),
        )
    return CheckResult(ok=True, title="Partition semantics include date= and month=")

def check_fixtures_reference(all_docs_text: str) -> CheckResult:
    token = "tests/fixtures/lake/futures"
    if token not in all_docs_text:
        return CheckResult(
            ok=False,
            title="Fixtures referenced in docs",
            details=f"Docs do not mention {token}",
        )
    return CheckResult(ok=True, title="Fixtures referenced in docs")

def check_legacy_phrases(md_files: List[Path]) -> CheckResult:
    """
    Find phrases that typically indicate doc drift / contradictions.
    This is configurable; add/remove as you like.
    """
    # Patterns with why they matter
    bad_patterns = [
        (r"\bno\s+cli\s+yet\b", "Contradiction if CLI now exists"),
        (r"\barchive\b.*\bcanonical\b", "Archive-era pipeline should not be canonical"),
        (r"\bplaceholder\b.*\bpackage\b", "Package is now canonical"),
        (r"\bmanifest[- ]driven\b.*\bincremental\b", "Incremental is file-scan driven (manifest-driven is backlog)"),
    ]

    hits: List[str] = []
    for p in md_files:
        txt = read_text(p)
        for pat, why in bad_patterns:
            ms = grep(pat, txt)
            for ln, line in ms[:10]:  # cap per file/pattern
                hits.append(f"{p.as_posix()}:{ln}: {line}  [{why}]")

    if hits:
        return CheckResult(
            ok=False,
            title="No legacy/contradictory phrases detected",
            details="Potential contradictions found:\n  - " + "\n  - ".join(hits),
        )

    return CheckResult(ok=True, title="No legacy/contradictory phrases detected")

def check_cli_discovery(repo_root: Path) -> Tuple[CheckResult, Dict[str, str]]:
    scripts = parse_console_scripts_from_pyproject(repo_root / "pyproject.toml")
    if not scripts:
        return (
            CheckResult(
                ok=False,
                title="CLI entrypoints discovered from pyproject.toml",
                details="No [project.scripts] or [tool.poetry.scripts] entries found (or parser couldn't detect them).",
            ),
            scripts,
        )
    formatted = "\n".join([f"  - {k} = {v}" for k, v in scripts.items()])
    return (
        CheckResult(ok=True, title="CLI entrypoints discovered from pyproject.toml", details=formatted),
        scripts,
    )

def check_docs_mention_cli_name(all_docs_text: str, scripts: Dict[str, str]) -> CheckResult:
    if not scripts:
        return CheckResult(ok=True, title="Docs mention CLI command name (skipped: no scripts discovered)")
    names = list(scripts.keys())
    mentioned = [n for n in names if n in all_docs_text]
    if not mentioned:
        return CheckResult(
            ok=False,
            title="Docs mention at least one discovered CLI command name",
            details="Discovered scripts:\n  - " + "\n  - ".join(names) + "\n\nBut none were found in docs text.",
        )
    return CheckResult(ok=True, title="Docs mention at least one discovered CLI command name", details="Mentioned: " + ", ".join(mentioned))

def run_cli_help(repo_root: Path, scripts: Dict[str, str]) -> CheckResult:
    if not scripts:
        return CheckResult(ok=False, title="Run CLI --help (skipped: no scripts discovered)")
    results = []
    ok_any = True
    for name in scripts.keys():
        # Try `<name> --help` relying on current environment PATH.
        try:
            code, out, err = sh([name, "--help"], cwd=repo_root, timeout=20)
            if code != 0:
                ok_any = False
            snippet = (out or err).strip().splitlines()[:12]
            results.append(
                f"{name} --help (exit {code})\n" + "\n".join(f"    {line}" for line in snippet)
            )
        except FileNotFoundError:
            ok_any = False
            results.append(f"{name} --help -> command not found in PATH (is the venv activated?)")
        except subprocess.TimeoutExpired:
            ok_any = False
            results.append(f"{name} --help -> timed out")

    return CheckResult(ok=ok_any, title="CLI --help runnable", details="\n\n".join(results))

# ---- Main ---------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo-root", default=".", help="Repo root (default: .)")
    ap.add_argument("--docs-dir", default="docs", help="Docs directory (default: docs)")
    ap.add_argument("--strict", action="store_true", help="Exit nonzero on any failed check")
    ap.add_argument("--run-cli-help", action="store_true", help="Attempt to run `<script> --help` for discovered CLIs")
    args = ap.parse_args()

    repo_root = Path(args.repo_root).resolve()
    docs_dir = (repo_root / args.docs_dir).resolve()

    if not docs_dir.exists():
        print(f"[FATAL] docs dir not found: {docs_dir}")
        return 2

    md_files = find_md_files(docs_dir)
    if not md_files:
        print(f"[FATAL] No .md files found under: {docs_dir}")
        return 2

    all_docs_text = "\n\n".join(read_text(p) for p in md_files)

    expected = [
        "architecture.md",
        "data_lake_layout.md",
        "decisions.md",
        "ingestion.md",
        "local_download_layout.md",
        "metadata_contracts.md",
        "runbooks.md",
    ]

    checks: List[CheckResult] = []
    checks.append(check_required_docs(docs_dir, expected))
    checks.append(check_canonical_run_sequence(all_docs_text))
    checks.append(check_metadata_tables(all_docs_text))
    checks.append(check_partition_semantics(all_docs_text))
    checks.append(check_fixtures_reference(all_docs_text))
    checks.append(check_legacy_phrases(md_files))

    cli_check, scripts = check_cli_discovery(repo_root)
    checks.append(cli_check)
    checks.append(check_docs_mention_cli_name(all_docs_text, scripts))
    if args.run_cli_help:
        checks.append(run_cli_help(repo_root, scripts))

    failed = 0
    print("\n=== kucoin_lake docs validation ===")
    print(f"Repo: {repo_root}")
    print(f"Docs: {docs_dir}")
    print(f"MD files scanned: {len(md_files)}")
    print("")

    for c in checks:
        status = "OK " if c.ok else "FAIL"
        print(f"[{status}] {c.title}")
        if c.details.strip():
            print(c.details.rstrip())
        print("")
        if not c.ok:
            failed += 1

    if failed and args.strict:
        print(f"FAILED checks: {failed}")
        return 1

    print(f"Failed checks: {failed} (strict={args.strict})")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
