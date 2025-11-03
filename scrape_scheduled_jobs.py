import os, re, time, argparse
from datetime import datetime, timezone
from functools import lru_cache
from dotenv import load_dotenv
from collections import defaultdict, Counter
from typing import Dict, List
import requests, pandas as pd

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

FOLDER      = "Server-Side Components/Scheduled Jobs"
API_BASE    = "https://api.github.com"
RAW_BASE    = "https://raw.githubusercontent.com"

S = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})

RECENCY_CUTOFF = datetime(2025, 10, 23, tzinfo=timezone.utc)


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


def group_scheduled_jobs_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    """Group scheduled jobs files by folder. Each folder represents a scheduled job snippet."""
    grouped = defaultdict(lambda: {"readme": "", "js": ""})
    prefix = f"{FOLDER}/"
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        if not path.startswith(prefix):
            continue
        rel = path[len(prefix):]  # e.g., "Job Name/README.md"
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


# --------- Parsers for Scheduled Job metadata ---------

def parse_job_name(md: str) -> str:
    patterns = [
        r"(?i)\bjob\s*name\s*[:\-]\s*(.+)",
        r"(?i)\bname\s*[:\-]\s*(.+)",
    ]
    for p in patterns:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_run_as(md: str) -> str:
    m = re.search(r"(?i)\brun\s*as\s*[:\-]\s*(.+)", md)
    return (m.group(1).strip() if m else "")


def parse_run_start(md: str) -> str:
    patterns = [
        r"(?i)\brun\s*start\s*[:\-]\s*(.+)",
        r"(?i)\bstart\s*[:\-]\s*(.+)",
    ]
    for p in patterns:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_run_period(md: str) -> str:
    m = re.search(r"(?i)\brun\s*period\s*[:\-]\s*(.+)", md)
    return (m.group(1).strip() if m else "")


def parse_run_dayofweek(md: str) -> str:
    patterns = [
        r"(?i)\bday\s*of\s*week\s*[:\-]\s*(.+)",
        r"(?i)\bdayofweek\s*[:\-]\s*(.+)",
    ]
    for p in patterns:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_run_dayofmonth(md: str) -> str:
    patterns = [
        r"(?i)\bday\s*of\s*month\s*[:\-]\s*(.+)",
        r"(?i)\bdayofmonth\s*[:\-]\s*(.+)",
    ]
    for p in patterns:
        m = re.search(p, md)
        if m:
            return m.group(1).strip()
    return ""


def parse_run_time(md: str) -> str:
    m = re.search(r"(?i)\btime\s*[:\-]\s*(.+)", md)
    return (m.group(1).strip() if m else "")


def parse_description(md: str) -> str:
    m = re.search(r"(?i)\bdescription\s*[:\-]\s*(.+)", md)
    if m: return m.group(1).strip()
    for line in md.splitlines():
        t = line.strip()
        if not t or t.startswith("#"): continue
        if re.match(r"(?i)^(name|run as|start|period|day|time)\s*[:\-]", t):
            continue
        return t
    return ""


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_scheduled_jobs_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        repo_path = f"{FOLDER}/{folder}"
        candidates = [files["readme"], files["js"], repo_path]
        if not is_recent(candidates):
            continue

        readme_md = fetch_raw(files["readme"]) if files["readme"] else ""
        js_code   = fetch_raw(files["js"]) if files["js"] else ""

        job_name = parse_job_name(readme_md) if readme_md else ""
        run_as = parse_run_as(readme_md) if readme_md else ""
        run_start = parse_run_start(readme_md) if readme_md else ""
        run_period = parse_run_period(readme_md) if readme_md else ""
        run_dayofweek = parse_run_dayofweek(readme_md) if readme_md else ""
        run_dayofmonth = parse_run_dayofmonth(readme_md) if readme_md else ""
        run_time = parse_run_time(readme_md) if readme_md else ""
        desc = parse_description(readme_md) if readme_md else ""

        rows.append({
            "title": folder,
            "description": desc,
            "code": js_code,
            "job_name": job_name,
            "run_as": run_as,
            "run_start": run_start,
            "run_period": run_period,
            "run_dayofweek": run_dayofweek,
            "run_dayofmonth": run_dayofmonth,
            "run_time": run_time,
            "repo_path": f"{FOLDER}/{folder}",
        })

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Scheduled Jobs from code-snippets repo")
    ap.add_argument("--out-xlsx", default="spreadsheets/scheduled_jobs.xlsx", help="Output .xlsx filename")
    ap.add_argument("--out-csv", default="spreadsheets/scheduled_jobs.csv", help="Output .csv filename")
    args = ap.parse_args()

    df = scrape()
    out_dir = os.path.dirname(args.out_xlsx) or "."
    os.makedirs(out_dir, exist_ok=True)

    df.to_excel(args.out_xlsx, index=False)
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)

    print(f"Saved {len(df)} scheduled jobs to {args.out_xlsx}")
    if args.out_csv:
        print(f"Saved CSV export to {args.out_csv}")


if __name__ == "__main__":
    main()
