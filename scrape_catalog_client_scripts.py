import os, re, time, argparse
from collections import defaultdict
from dotenv import load_dotenv
from typing import Any, Dict, List, Tuple
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

FOLDER = "Client-Side Components/Catalog Client Script"   # repo folder

API_BASE = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

S = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})


def req(method: str, url: str, **kw):
    for attempt in range(6):
        r = S.request(method, url, timeout=30, **kw)
        if r.status_code in (403, 429) or r.status_code >= 500:
            time.sleep(min(2 ** attempt, 20))
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()
    return r


def get_branch_sha() -> str:
    r = req("GET", f"{API_BASE}/repos/{OWNER}/{REPO}/branches/{BRANCH}")
    return r.json()["commit"]["sha"]


def list_tree_recursive(sha: str) -> List[Dict]:
    r = req("GET", f"{API_BASE}/repos/{OWNER}/{REPO}/git/trees/{sha}", params={"recursive": "1"})
    return r.json().get("tree", [])


def fetch_raw(path: str) -> str:
    url = f"{RAW_BASE}/{OWNER}/{REPO}/{BRANCH}/{path}"
    return req("GET", url).text


def group_files(tree: List[Dict]) -> Dict[str, Dict[str, Any]]:
    grouped = defaultdict(lambda: {"readme": "", "js": []})
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue
        rel = path[len(prefix):]
        parts = rel.split("/")
        if len(parts) != 2:
            continue
        folder, filename = parts
        low = filename.lower()
        if low == "readme.md":
            grouped[folder]["readme"] = path
        elif low.endswith(".js"):
            grouped[folder]["js"].append(path)
    return grouped


# -------- README parsers --------

def grab(md: str, patterns: List[str]) -> str:
    for pat in patterns:
        m = re.search(pat, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_type(md: str) -> str:
    common = ["onLoad", "onChange", "onSubmit"]
    for t in common:
        if re.search(rf"\b{t}\b", md, re.IGNORECASE):
            return t
    m = re.search(r"(?i)\btype\s*[:\-]\s*([A-Za-z]+)", md)
    return (m.group(1) if m else "") or ""


def parse_applies_to(md: str) -> str:
    return grab(md, [
        r"(?i)\bapplies\s*to\s*[:\-]\s*(.+)",
        r"(?i)\btable\s*[:\-]\s*(catalog\s*item|sc_cat_item|item)",
    ])


def parse_ui_type(md: str) -> str:
    return grab(md, [
        r"(?i)\bui\s*type\s*[:\-]\s*([A-Za-z ]+)",
    ])


def parse_sys_scope(md: str) -> str:
    return grab(md, [
        r"(?i)\bapplication\s*\|\s*sys_scope\s*[:\-]\s*(.+)",
        r"(?i)\bsys\s*scope\s*[:\-]\s*(.+)",
        r"(?i)\bapplication\s*[:\-]\s*(.+)"
    ])


def parse_cat_item(md: str) -> str:
    return grab(md, [
        r"(?i)\bcat(?:alog)?\s*item\s*[:\-]\s*([^\n]+)",
    ])


def parse_name(md: str, folder: str) -> str:
    v = grab(md, [r"(?i)\bname\s*[:\-]\s*([^\n]+)"])
    return v or folder


def parse_description(md: str) -> str:
    m = re.search(r"(?i)\bdescription\s*[:\-]\s*(.+)", md)
    if m:
        return m.group(1).strip()
    # fallback: first non-heading, non-empty, non-metadata line
    for line in md.splitlines():
        t = line.strip()
        if not t or t.startswith("#"): 
            continue
        if re.match(r"(?i)^(name|applies to|ui type|type|cat\s*item|application|sys scope)\s*[:\-]", t):
            continue
        return t
    return ""


# -------- Script categorization helpers --------

CLIENT_NAME_HINTS = (
    "client",
    "onload",
    "onchange",
    "onsubmit",
    "oncelledit",
    "catalog",
    "portal",
    "script",
    "mrvs",
)

INCLUDE_NAME_HINTS = (
    "include",
    "ajax",
    "util",
    "utils",
    "provider",
    "processor",
    "server",
)


def _normalize_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.lower())


def readme_mentions(filename: str, lines: List[str], keyword: str) -> bool:
    """Return True if README references filename alongside keyword (case-insensitive)."""
    if not lines:
        return False
    token = _normalize_token(os.path.splitext(filename)[0])
    if not token:
        return False
    needle = keyword.lower()
    for line in lines:
        low = line.lower()
        if needle in low:
            if token in _normalize_token(line):
                return True
    return False


def infer_js_role(filename: str, code: str, readme_lines: List[str]) -> str:
    """Best-effort classification of JS files into client vs. script include."""
    name = filename.lower()
    content = code.lower()

    if readme_mentions(filename, readme_lines, "script include") or readme_mentions(filename, readme_lines, "server script"):
        return "script_include"
    if readme_mentions(filename, readme_lines, "client script"):
        return "client"

    if any(hint in name for hint in INCLUDE_NAME_HINTS):
        if "g_form" not in content:
            return "script_include"

    if any(hint in name for hint in CLIENT_NAME_HINTS):
        return "client"

    if any(trigger in content for trigger in ("function onload", "function onchange", "function onsubmit", "function oncelledit")):
        return "client"
    if "g_form" in content or "g_scratchpad" in content:
        return "client"

    if "class.create" in content or "prototype =" in content or "gs." in content or "gliderecord" in content:
        if "g_form" not in content:
            return "script_include"

    return "unknown"


def split_js_files(entries: List[Tuple[str, str]], readme_lines: List[str]) -> Tuple[List[Tuple[str, str]], List[Tuple[str, str]]]:
    """Split list of (filename, code) pairs into client scripts and script includes."""
    clients: List[Tuple[str, str]] = []
    includes: List[Tuple[str, str]] = []
    unknown: List[Tuple[str, str]] = []

    for filename, code in entries:
        role = infer_js_role(filename, code, readme_lines)
        if role == "client":
            clients.append((filename, code))
        elif role == "script_include":
            includes.append((filename, code))
        else:
            unknown.append((filename, code))

    if not clients and unknown:
        clients.append(unknown.pop(0))
    includes.extend(unknown)

    return clients, includes


def combine_scripts(entries: List[Tuple[str, str]]) -> str:
    """Combine multiple script files into a single string (joined by blank lines)."""
    if not entries:
        return ""
    parts = []
    for filename, code in sorted(entries, key=lambda item: item[0].lower()):
        stripped = code.strip()
        if not stripped:
            continue
        parts.append(stripped)
    return "\n\n".join(parts)


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        md  = fetch_raw(files["readme"]) if files["readme"] else ""
        readme_lines = md.splitlines()

        js_entries: List[Tuple[str, str]] = []
        for path in files["js"]:
            filename = os.path.basename(path)
            js_entries.append((filename, fetch_raw(path)))

        client_entries, include_entries = split_js_files(js_entries, readme_lines)
        client_script = combine_scripts(client_entries)
        script_include = combine_scripts(include_entries)

        rows.append({
            "title":       parse_name(md, folder),
            "applies_to":  parse_applies_to(md) or "A Catalog Item",
            "ui_type":     parse_ui_type(md) or "All",
            "sys_scope":   parse_sys_scope(md),
            "type":        parse_type(md),
            "cat_item":    parse_cat_item(md),
            "description": parse_description(md),
            "client_script": client_script,
            "script_include": script_include,
        })

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Catalog Client Scripts")
    ap.add_argument("--out", default="catalog_client_scripts.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    df = df[[
        "title",
        "applies_to",
        "ui_type",
        "sys_scope",
        "type",
        "cat_item",
        "description",
        "client_script",
        "script_include",
    ]]
    df.to_excel(args.out, index=False)
    print(f"Saved {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
