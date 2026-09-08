from pathlib import Path


def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8") if p.exists() else ""


def uniq(xs):
    seen, out = set(), []
    for x in xs:
        if x and x not in seen:
            out.append(x);
            seen.add(x)
    return out


def number_with_commas(n, places=2):
    try:
        s = f"{n:.{places}f}"
    except Exception:
        s = str(n)
    if "." in s:
        a, b = s.split(".", 1)
    else:
        a, b = s, ""
    try:
        a = f"{int(a):,}"
    except (TypeError, ValueError):
        # The try/except above only guarded the f-string; a non-numeric value reached int() and
        # raised, taking the whole report down. Fall back to the raw text instead.
        return s
    return f"{a}.{b}"
