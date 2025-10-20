import os, re, time, argparse
from collections import defaultdict
from dotenv import load_dotenv
from typing import Dict, List
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

# Repo folder that holds all server-side components scripts
FOLDER = "Server-Side Components"

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


def group_server_side_files(tree: List[Dict]) -> Dict[str, Dict[str, Dict[str, str]]]:
    """
    Group server-side components script files by type_folder > snippet_folder.
    Each snippet_folder represents a server-side component snippet.
    """
    grouped = defaultdict(lambda: defaultdict(lambda: {
        "README": "",
        "CODE1": "",
        "CODE2": ""
    }))
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue

        rel = path[len(prefix):]  # e.g., "Business Rules/Example Name/README.md"
        parts = rel.split("/")
        if len(parts) < 3:
            continue

        type_folder = parts[0]
        snippet_folder = parts[1]
        filename = "/".join(parts[2:])  # Handle if filename has spaces or subpaths

        low = filename.lower()
        if low == "readme.md":
            grouped[type_folder][snippet_folder]["README"] = path
        elif low.endswith(".js"):
            files = grouped[type_folder][snippet_folder]
            if not files["CODE1"]:
                files["CODE1"] = path
            elif not files["CODE2"]:
                files["CODE2"] = path  # Second .js file

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


def build_row(type_folder: str, name: str, files: Dict[str, str]) -> Dict[str, str]:
    readme = fetch_raw(files["README"]) if files["README"] else ""
    code1_path = files["CODE1"]
    code2_path = files["CODE2"]
    code1 = fetch_raw(code1_path) if code1_path else ""
    code2 = fetch_raw(code2_path) if code2_path else ""

    # If no code files, try extracting from README
    if not code1:
        code1 = extract_code_from_readme(readme)

    repo_path = f"{FOLDER}/{type_folder}/{name}"

    return {
        "title": name,
        "description": readme,  # Full Markdown as requested
        "code": code1,
        "code2": code2,
        "type": type_folder,
        "repo_path": repo_path,
    }


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_server_side_files(tree)

    rows = []
    for type_folder in sorted(grouped.keys()):
        for snippet_folder, files in sorted(grouped[type_folder].items()):
            if files["README"]:  # Only include if README exists
                rows.append(build_row(type_folder, snippet_folder, files))

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Server-Side Components from code-snippets repo")
    ap.add_argument("--out", default="spreadsheets/server_side_components.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    df = df[[
        "title", "description", "code", "code2", "type", "repo_path"
    ]]

    # Save to XLSX
    xlsx_path = args.out
    df.to_excel(xlsx_path, index=False)

    # Also save to CSV for consistency with other scripts
    csv_path = xlsx_path.replace(".xlsx", ".csv")
    df.to_csv(csv_path, index=False)

    print(f"Saved {len(df)} server-side components to {xlsx_path} and {csv_path}")


if __name__ == "__main__":
    main()
