import os, re, time, argparse
from datetime import datetime, timezone
from functools import lru_cache
from collections import defaultdict
from dotenv import load_dotenv
from typing import Dict, List
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

# Repo folder that holds all mail scripts
FOLDER = "Integration/Mail Scripts"

API_BASE = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

S = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})

RECENCY_CUTOFF = datetime(2025, 10, 23, tzinfo=timezone.utc)


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


@lru_cache(maxsize=None)
def latest_commit_datetime(path: str):
    if not path:
        return None
    r = req(
        "GET",
        f"{API_BASE}/repos/{OWNER}/{REPO}/commits",
        params={"path": path, "sha": BRANCH, "per_page": 1},
    )
    commits = r.json()
    if not commits:
        return None
    commit_info = commits[0].get("commit", {})
    date_str = (
        commit_info.get("committer", {}) or {}
    ).get("date") or (commit_info.get("author", {}) or {}).get("date")
    if not date_str:
        return None
    return datetime.fromisoformat(date_str.replace("Z", "+00:00"))


def is_recent(paths: List[str]) -> bool:
    for path in paths:
        ts = latest_commit_datetime(path)
        if ts and ts > RECENCY_CUTOFF:
            return True
    return False


def group_mail_scripts_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Group mail script files by subfolder.
    Each subfolder represents a mail script snippet.
    """
    grouped = defaultdict(lambda: {
        "README": "",
        "CODE1": "",
        "CODE2": ""
    })
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue

        rel = path[len(prefix):]  # e.g., "Example Name/README.md"
        parts = rel.split("/")
        if len(parts) < 2:
            continue

        folder = parts[0]
        filename = "/".join(parts[1:])  # Handle if filename has spaces

        low = filename.lower()
        if low == "readme.md":
            grouped[folder]["README"] = path
        elif low.endswith(".js"):
            if not grouped[folder]["CODE1"]:
                grouped[folder]["CODE1"] = path
            elif not grouped[folder]["CODE2"]:
                grouped[folder]["CODE2"] = path  # Second .js file

    return grouped


def extract_code_from_readme(readme: str) -> str:
    """
    Extract JavaScript code from README.md code blocks.
    Looks for ```javascript or ```js blocks.
    """
    pattern = r'```(?:javascript|js)\s*\n(.*?)\n```'
    matches = re.findall(pattern, readme, re.DOTALL)
    if matches:
        return matches[0].strip()  # Return the first/main code block
    return ""


def build_row(name: str, files: Dict[str, str]) -> Dict[str, str]:
    readme = fetch_raw(files["README"]) if files["README"] else ""
    code1_path = files["CODE1"]
    code2_path = files["CODE2"]
    code1 = fetch_raw(code1_path) if code1_path else ""
    code2 = fetch_raw(code2_path) if code2_path else ""

    # If no code files, try extracting from README
    if not code1:
        code1 = extract_code_from_readme(readme)

    repo_path = f"{FOLDER}/{name}"

    return {
        "title": name,
        "description": readme,  # Full Markdown as requested
        "code1": code1,
        "code2": code2,
        "repo_path": repo_path,
    }


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_mail_scripts_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        if not files["README"]:
            continue
        repo_path = f"{FOLDER}/{folder}"
        candidates = [
            files["README"],
            files["CODE1"],
            files["CODE2"],
            repo_path,
        ]
        if not is_recent(candidates):
            continue
        rows.append(build_row(folder, files))

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Mail Scripts from code-snippets repo")
    ap.add_argument("--out-xlsx", default="spreadsheets/mail_scripts.xlsx", help="Output .xlsx filename")
    ap.add_argument("--out-csv", default="spreadsheets/mail_scripts.csv", help="Output .csv filename")
    args = ap.parse_args()

    df = scrape()
    df = df[[
        "title", "description", "code1", "code2", "repo_path"
    ]]

    out_dir = os.path.dirname(args.out_xlsx) or "."
    os.makedirs(out_dir, exist_ok=True)

    df.to_excel(args.out_xlsx, index=False)
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)

    print(f"Saved {len(df)} mail scripts to {args.out_xlsx}")
    if args.out_csv:
        print(f"Saved CSV export to {args.out_csv}")


if __name__ == "__main__":
    main()
