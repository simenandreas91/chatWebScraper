import os, re, time, argparse
from collections import defaultdict
from dotenv import load_dotenv
from typing import Dict, List
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

# Repo folder that holds all widgets
FOLDER = "Modern Development/Service Portal Widgets"

API_BASE = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

S = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})


def req(method: str, url: str, **kw):
    """HTTP request with retries/backoff for rate limits."""
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


def group_widget_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Group widget files by top-level widget folder.
    We capture common filenames used in this repo.
    """
    grouped = defaultdict(lambda: {
        "README": "",
        "HTML": "",
        "CLIENT": "",
        "SERVER": "",
        "CSS": "",
        "DEMO": "",
        "SCHEMA": ""
    })
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue

        rel = path[len(prefix):]  # e.g., "Drag & drop Widget/client.js"
        parts = rel.split("/")
        if len(parts) != 2:
            # We only consider files directly under each widget folder
            continue

        folder, filename = parts
        low = filename.lower()

        if low == "readme.md":
            grouped[folder]["README"] = path
        elif low in ("html.html", "template.html"):
            grouped[folder]["HTML"] = path
        elif low in ("client.js", "script_client.js", "client_script.js"):
            grouped[folder]["CLIENT"] = path
        elif low in ("server.js", "script_server.js", "server_script.js"):
            grouped[folder]["SERVER"] = path
        elif low.endswith(".css") and not grouped[folder]["CSS"]:
            grouped[folder]["CSS"] = path  # first css file wins
        elif low in ("demo_data.json", "demodata.json", "demoData.json"):
            grouped[folder]["DEMO"] = path
        elif low in ("option_schema.json", "options_schema.json", "optionschema.json"):
            grouped[folder]["SCHEMA"] = path

    return grouped


# ---------------- Parsers ----------------

def extract_controller_as(client_js: str) -> str:
    """
    Try to detect controller alias:
      - controllerAs: 'c'
      - var c = this; (most common scaffold)
    """
    m = re.search(r"controllerAs\s*:\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", client_js)
    if m:
        return m.group(1)
    m = re.search(r"\bvar\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*this\s*;", client_js)
    if m:
        return m.group(1)
    return ""


def extract_link_function(client_js: str) -> str:
    """
    Extract a top-level 'function link(...) { ... }' if present.
    """
    # Simple brace-matching approach for link function
    m = re.search(r"\bfunction\s+link\s*\(([^\)]*)\)\s*\{", client_js)
    if not m:
        return ""
    start = m.end()  # index just after '{'
    depth = 1
    i = start
    while i < len(client_js):
        ch = client_js[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return "function link(" + m.group(1) + "){\n" + client_js[start:i] + "\n}"
        i += 1
    return ""


def build_row(name: str, files: Dict[str, str]) -> Dict[str, str]:
    readme = fetch_raw(files["README"]) if files["README"] else ""
    html   = fetch_raw(files["HTML"]) if files["HTML"] else ""
    client = fetch_raw(files["CLIENT"]) if files["CLIENT"] else ""
    server = fetch_raw(files["SERVER"]) if files["SERVER"] else ""
    css    = fetch_raw(files["CSS"]) if files["CSS"] else ""
    demo   = fetch_raw(files["DEMO"]) if files["DEMO"] else ""
    schema = fetch_raw(files["SCHEMA"]) if files["SCHEMA"] else ""

    controller_as = extract_controller_as(client) if client else ""
    link_func     = extract_link_function(client) if client else ""

    repo_path = f"{FOLDER}/{name}"  # <--- NEW

    return {
        "name": name,
        "description": readme,
        "html": html,
        "css": css,
        "server_script": server,
        "client_script": client,
        "controller_as": controller_as,
        "link": link_func,
        "demo_data": demo,
        "option_schema": schema,
        "repo_path": repo_path,   # <--- NEW
    }



def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_widget_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        rows.append(build_row(folder, files))

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Service Portal Widgets from code-snippets repo")
    ap.add_argument("--out", default="sp_widgets.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    df = df[[
    "name", "description", "html", "css", "server_script", "client_script",
    "controller_as", "link", "demo_data", "option_schema", "repo_path"
    ]]

    df.to_excel(args.out, index=False)
    print(f"Saved {len(df)} widgets to {args.out}")


if __name__ == "__main__":
    main()
