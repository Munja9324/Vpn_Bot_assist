from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


PATTERNS = (
    re.compile(r"(?:\u0420.|\u0421.){3,}"),  # РџРѕРјРѕ... style
    re.compile(r"(?:Ð.|Ñ.){2,}"),            # Ð¢ÐµÐºÑÑ‚ style
    re.compile(r"(?:Ã.|â.){2,}"),            # cp1252/latin1 noise
)


def _is_suspicious(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    return any(pattern.search(stripped) for pattern in PATTERNS)


def _iter_git_added_lines() -> list[tuple[str, int, str]]:
    cmd = ["git", "diff", "--unified=0", "--", "kbrbot", "vpn_kbr.py", ".env.example", "README.md", "docs", "tools"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or "git diff failed")

    lines = result.stdout.splitlines()
    out: list[tuple[str, int, str]] = []
    current_file = ""
    current_line = 0

    for line in lines:
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("@@"):
            # @@ -a,b +c,d @@
            match = re.search(r"\+(\d+)", line)
            if match:
                current_line = int(match.group(1))
            continue
        if not current_file:
            continue
        if line.startswith("+") and not line.startswith("+++"):
            out.append((current_file, current_line, line[1:]))
            current_line += 1
            continue
        if line.startswith("-"):
            continue
        current_line += 1

    return out


def main() -> int:
    try:
        added = _iter_git_added_lines()
    except Exception as error:
        print(f"[mojibake-check] skip: {error}")
        return 0

    problems: list[tuple[str, int, str]] = []
    for file_path, lineno, text in added:
        if _is_suspicious(text):
            problems.append((file_path, lineno, text))

    if not problems:
        print("[mojibake-check] OK: no suspicious newly added lines")
        return 0

    print("[mojibake-check] ERROR: suspicious mojibake in newly added lines:")
    for file_path, lineno, text in problems[:120]:
        print(f" - {file_path}:{lineno}: {text[:200]}")
    if len(problems) > 120:
        print(f" ... and {len(problems) - 120} more")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
