from __future__ import annotations

import argparse
from pathlib import Path


MARKERS = (
    "\u0420\u045f",
    "\u0420\u00a0",
    "\u0420\u045a",
    "\u0420\u040c",
    "\u0420\u0403",
    "\u0420\u2014",
    "\u0420\u201c",
    "\u0420\u201d",
    "\u0420\u045b",
    "\u0420\u02dc",
    "\u0420\u0408",
    "\u0420\u0459",
    "\u0420\u2018",
    "\u0420\u2019",
    "\u0420\u00a4",
    "\u0420\u00a7",
    "\u0420\u0401",
    "\u0420\u0407",
    "\u0420\u00b0",
    "\u0420\u00b5",
    "\u0420\u0451",
    "\u0420\u0455",
    "\u0420\u0405",
    "\u0420\u0457",
    "\u0421\u0402",
    "\u0421\u0453",
    "\u0421\u201a",
    "\u0421\u040a",
    "\u0421\u2039",
    "\u0421\u040f",
    "\u0421\u045a",
    "\u0421\u20ac",
    "\u0421\u2030",
    "\u0432\u0402",
    "\u0432\u201e",
    "\u0432\u201a",
)

SKIP_MARKERS = (
    "MOJIBAKE_MARKERS",
    "looks_like_mojibake_text",
    "marker in value",
    "mojibake",
)


def badness(text: str) -> int:
    return sum(text.count(marker) for marker in MARKERS)


def good_cyrillic(text: str) -> int:
    return sum(1 for char in text if ("\u0410" <= char <= "\u044f") or char in {"\u0401", "\u0451"})


def score(text: str) -> int:
    return badness(text) * 20 - good_cyrillic(text)


def should_skip(line: str) -> bool:
    return any(marker in line for marker in SKIP_MARKERS)


def repair_line(line: str) -> str:
    if badness(line) < 1 or should_skip(line):
        return line

    best = line
    best_score = score(line)
    for encoding in ("cp1251", "latin1"):
        try:
            candidate = line.encode(encoding, errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            continue
        candidate_score = score(candidate)
        if candidate_score + 3 < best_score:
            best = candidate
            best_score = candidate_score
    return best


def repair_file(path: Path, *, check: bool) -> int:
    original = path.read_text(encoding="utf-8-sig")
    repaired = "".join(repair_line(line) for line in original.splitlines(keepends=True))
    if repaired == original:
        return 0
    if check:
        print(f"mojibake remains or repair is needed: {path}")
        return 1
    path.write_text(repaired, encoding="utf-8")
    print(f"repaired: {path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    status = 0
    for raw_path in args.paths:
        status |= repair_file(Path(raw_path), check=args.check)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
