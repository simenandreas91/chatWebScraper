import os, re, time, argparse
from collections import defaultdict
from dotenv import load_dotenv
from typing import Dict, List
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


def group_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    grouped = defaultdict(lambda: {"readme": "", "js": ""})
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
            grouped[folder]["js"] = path
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


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        md  = fetch_raw(files["readme"]) if files["readme"] else ""
        js  = fetch_raw(files["js"])     if files["js"] else ""

        rows.append({
            "name":        parse_name(md, folder),
            "applies_to":  parse_applies_to(md) or "A Catalog Item",
            "ui_type":     parse_ui_type(md) or "All",
            "sys_scope":   parse_sys_scope(md),
            "type":        parse_type(md),
            "cat_item":    parse_cat_item(md),
            "description": parse_description(md),
            "script":      js,
        })

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Catalog Client Scripts")
    ap.add_argument("--out", default="catalog_client_scripts.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    df = df[["name", "applies_to", "ui_type", "sys_scope", "type", "cat_item", "description", "script"]]
    df.to_excel(args.out, index=False)
    print(f"Saved {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
