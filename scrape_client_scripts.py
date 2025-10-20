import os, re, time, argparse
from dotenv import load_dotenv
from collections import defaultdict, Counter
from typing import Dict, List
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

FOLDER      = "Client-Side Components/Client Scripts"   # human readable
API_BASE    = "https://api.github.com"
RAW_BASE    = "https://raw.githubusercontent.com"

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


def group_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    """Return mapping: <folder> -> {'readme': path, 'js': path} under our client-scripts folder."""
    grouped = defaultdict(lambda: {"readme": "", "js": ""})
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue
        rel = path[len(prefix):]  # e.g., "Change Label of Field/README.md"
        parts = rel.split("/")
        if len(parts) != 2:
            continue
        folder, filename = parts
        low = filename.lower()
        if low == "readme.md":
            grouped[folder]["readme"] = path
        elif low.endswith(".js"):
            grouped[folder]["js"] = path
    return grouped


# --------- Parsers ---------

def parse_event(md: str) -> str:
    events = ["onChange", "onLoad", "onSubmit", "onCellEdit", "onValueChange"]
    for ev in events:
        if re.search(rf"\b{re.escape(ev)}\b", md, re.IGNORECASE):
            return ev
    m = re.search(r"(?i)\b(type|event)\s*[:\-]\s*([A-Za-z]+)", md)
    return (m.group(2) if m else "") or ""


TABLE_PATTERNS = [
    r"(?i)\btable\s*[:\-]\s*([A-Za-z0-9_\.]+)",
    r"(?i)\bapplies\s*to\s*[:\-]\s*([A-Za-z0-9_\.]+)",
    r"(?i)\bon\s+table\s*[:\-]\s*([A-Za-z0-9_\.]+)",
]
def parse_table_from_readme(md: str) -> str:
    for p in TABLE_PATTERNS:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_description(md: str) -> str:
    m = re.search(r"(?i)\bdescription\s*[:\-]\s*(.+)", md)
    if m: return m.group(1).strip()
    for line in md.splitlines():
        t = line.strip()
        if not t or t.startswith("#"): continue
        if re.match(r"(?i)^(type|event|usage|example|table|applies to|field\s*name|element)\s*[:\-]", t):
            continue
        return t
    return ""


# --- Field name detection ---

# From README (common phrasings)
FIELD_PATTERNS_MD = [
    r"(?i)\bfield\s*name\s*[:\-]\s*([A-Za-z0-9_\.]+)",
    r"(?i)\belement\s*[:\-]\s*([A-Za-z0-9_\.]+)",
    r"(?i)\bfield\s*[:\-]\s*([A-Za-z0-9_\.]+)",
]

def parse_field_from_readme(md: str) -> str:
    for p in FIELD_PATTERNS_MD:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


# From JS (scan g_form API for hard-coded element names)
GF_METHODS = [
    "getValue", "getControl", "setValue", "setDisplay", "setVisible",
    "setMandatory", "setReadOnly", "showFieldMsg", "clearValue",
    "addOption", "removeOption", "addErrorMessage", "addInfoMessage",
    "clearOptions", "disableAttachment"
]
GF_REGEX = re.compile(
    r"g_form\.(?:"
    + "|".join(GF_METHODS)
    + r")\(\s*['\"]([A-Za-z0-9_]+)['\"]",
    re.MULTILINE
)

def parse_fields_from_code(js: str) -> List[str]:
    # Collect candidate element names; return most common(s)
    candidates = [m.group(1) for m in GF_REGEX.finditer(js)]
    if not candidates:
        # Sometimes authors do: var f = 'short_description'; g_form.getValue(f)
        # This is hard to resolve reliably; skip.
        return []
    # De-duplicate but keep the most frequent first
    counts = Counter(candidates)
    top = [name for name, _ in counts.most_common()]
    return top


def parse_table_from_code(js: str) -> str:
    # Weak hints only
    m = re.search(r"(?i)//\s*table\s*[:\-]\s*([A-Za-z0-9_\.]+)", js)
    if m: return m.group(1).strip()
    m = re.search(r"(?i)/\*\s*table\s*[:\-]\s*([A-Za-z0-9_\.]+)\s*\*/", js)
    if m: return m.group(1).strip()
    return ""


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        readme_md = fetch_raw(files["readme"]) if files["readme"] else ""
        js_code   = fetch_raw(files["js"]) if files["js"] else ""

        event = parse_event(readme_md) if readme_md else ""
        table = parse_table_from_readme(readme_md) if readme_md else ""
        if not table and js_code:
            table = parse_table_from_code(js_code)

        desc  = parse_description(readme_md) if readme_md else ""

        # Field name: README first, else infer from code (top 1–3 joined by comma)
        field_name = parse_field_from_readme(readme_md) if readme_md else ""
        if not field_name and js_code:
            fields = parse_fields_from_code(js_code)
            if fields:
                # If multiple, list the first 3 (most frequent) comma-separated.
                field_name = ", ".join(fields[:3])

        rows.append({
            "title": folder,
            "event": event,
            "table": table,
            "field_name": field_name,
            "description": desc,
            "code": js_code,
        })

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Client Scripts (with field_name)")
    ap.add_argument("--out", default="client_scripts.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    df.to_excel(args.out, index=False)
    print(f"Saved {len(df)} rows to {args.out}")

if __name__ == "__main__":
    main()
