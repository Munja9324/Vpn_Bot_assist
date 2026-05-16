from __future__ import annotations

import re
import subprocess


# Detect classic mojibake markers only.
# 1) UTF-8 text decoded as cp1251: "РџСЂ..."
# 2) UTF-8 text decoded as latin1/cp1252: "ÐÑ..."
# 3) cp1252 artifact tails: "Ã", "â" sequences.
PATTERNS = (
    re.compile(r"(?:\u0420.|\u0421.){3,}"),
    re.compile(r"(?:Ð.|Ñ.){2,}"),
    re.compile(r"(?:Ã.|â.){2,}"),
)


def _is_suspicious(text: str) -> bool:
    sample = text.strip()
    if not sample:
        return False
    if sample.startswith("#"):
        return False
    return any(pattern.search(sample) for pattern in PATTERNS)


def _iter_git_added_lines() -> list[tuple[str, int, str]]:
    cmd = ["git", "diff", "--unified=0", "--", "kbrbot", "vpn_kbr.py", ".env.example", "README.md", "docs", "tools"]
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or "git diff failed")

    out: list[tuple[str, int, str]] = []
    current_file = ""
    current_line = 0

    for line in result.stdout.splitlines():
        if line.startswith("+++ b/"):
            current_file = line[6:]
            continue
        if line.startswith("@@"):
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
        preview = text[:200].encode("unicode_escape", errors="backslashreplace").decode("ascii", errors="replace")
        print(f" - {file_path}:{lineno}: {preview}")
    if len(problems) > 120:
        print(f" ... and {len(problems) - 120} more")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
