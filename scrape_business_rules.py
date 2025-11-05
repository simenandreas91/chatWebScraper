import os, re, time, argparse
from collections import defaultdict
from typing import Dict, List

import requests
import pandas as pd
from dotenv import load_dotenv

OWNER  = "ServiceNowDevProgram"
REPO   = "code-snippets"
BRANCH = "main"

# Repo folder that holds all Business Rules
FOLDER = "Server-Side Components/Business Rules"

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


def group_business_rule_files(tree: List[Dict]) -> Dict[str, Dict[str, str]]:
    """
    Group business rule files by subfolder.
    Each subfolder represents a business rule snippet.
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
        filename = "/".join(parts[1:])  # Handle deeper paths

        low = filename.lower()
        if low == "readme.md":
            grouped[folder]["README"] = path
        elif low.endswith(".js"):
            files = grouped[folder]
            if not files["CODE1"]:
                files["CODE1"] = path
            elif not files["CODE2"]:
                files["CODE2"] = path

    return grouped


def extract_code_from_readme(readme: str) -> str:
    """
    Extract JavaScript code from README.md code blocks.
    Looks for ```javascript or ```js blocks.
    """
    pattern = r'```(?:javascript|js)\s*\n(.*?)\n```'
    matches = re.findall(pattern, readme, re.DOTALL)
    if matches:
        return matches[0].strip()
    return ""


ALLOWED_WHEN_VALUES = ("before", "after", "async", "display")

WHEN_PATTERNS = [
    r"(?i)\bwhen\s*to\s*run\s*[:\-]\s*([^\n]+)",
    r"(?i)\brun\s*when\s*[:\-]\s*([^\n]+)",
    r"(?i)\bwhen\s*[:\-]\s*([^\n]+)",
]

WHEN_KEYWORDS = ("before", "after", "async", "asynchronous", "display")

TABLE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
TABLE_STOPWORDS = {
    "a", "an", "and", "any", "applies", "apply", "be", "business", "can", "check",
    "checks", "collection", "collections", "during", "for", "from", "help", "if",
    "input", "in", "is", "it", "label", "log", "name", "names", "on", "open", "or",
    "record", "records", "rule", "rules", "runs", "run", "so", "such", "table",
    "tables", "that", "the", "this", "to", "value", "values", "when", "whenever",
    "with", "as", "added", "appears", "allowed", "top", "easily", "during",
    "changed", "other", "each",
}
TABLE_PREFIX_PREFERENCE = (
    "sys_", "u_", "cmdb_", "sc_", "kb_", "sn_", "x_", "alm_", "hr_", "pa_", "asmt_", "svc_"
)


def normalize_when_value(text: str) -> str:
    if not text:
        return ""
    lowered = text.lower()
    if "asynch" in lowered:
        return "async"
    for token in ALLOWED_WHEN_VALUES:
        if re.search(rf"\b{token}\b", lowered):
            return token
    if "on display" in lowered:
        return "display"
    return ""


def parse_when_to_run(md: str) -> str:
    for pattern in WHEN_PATTERNS:
        m = re.search(pattern, md)
        if m:
            normalized = normalize_when_value(m.group(1))
            if normalized:
                return normalized
    for line in md.splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        lower = text.lower()
        if any(keyword in lower for keyword in WHEN_KEYWORDS):
            normalized = normalize_when_value(text)
            if normalized:
                return normalized
    return ""


def extract_collection_candidates(md: str) -> List[str]:
    patterns = [
        r"(?i)\btable(?:\s+name)?\s*(?:is|:)?\s*([A-Za-z0-9_\-\s/]+)",
        r"(?i)\bcollection(?:\s+name)?\s*(?:is|:)?\s*([A-Za-z0-9_\-\s/]+)",
        r"(?i)\bruns?\s+on\s*(?:the)?\s*([A-Za-z0-9_\-\s/]+)",
        r"(?i)\bapplies\s+to\s*(?:the)?\s*([A-Za-z0-9_\-\s/]+)",
    ]
    results: List[str] = []
    seen = set()
    for pattern in patterns:
        for match in re.findall(pattern, md):
            segment = match.strip()
            if not segment:
                continue
            segment = re.split(r"[\n\r.;]", segment, 1)[0]
            parts = re.split(r"\s+(?:and|or)\s+|[,/]", segment)
            for part in parts:
                candidate = normalize_table_candidate(part)
                if candidate and candidate not in seen:
                    seen.add(candidate)
                    results.append(candidate)
    if not results:
        fallback_pattern = re.compile(
            r"\b(?:sys|u|cmdb|sc|kb|sn|x|alm|hr|pa|asmt)_[a-z0-9_]+\b",
            re.IGNORECASE,
        )
        for match in fallback_pattern.findall(md):
            candidate = normalize_table_candidate(match)
            if candidate and candidate not in seen:
                seen.add(candidate)
                results.append(candidate)
    return results


def normalize_table_candidate(raw: str) -> str:
    if not raw:
        return ""
    cleaned = raw.strip().strip(":*-`'\"")
    if not cleaned:
        return ""
    cleaned = cleaned.replace("/", " ")
    tokens = [tok for tok in re.split(r"\s+", cleaned) if tok]
    if not tokens:
        return ""
    if len(tokens) > 3:
        return ""
    while tokens and tokens[0].lower() in TABLE_STOPWORDS:
        tokens.pop(0)
    while tokens and tokens[-1].lower() in TABLE_STOPWORDS:
        tokens.pop()
    if not tokens:
        return ""
    candidate_parts = []
    for tok in tokens:
        lowered = tok.lower().replace("-", "_")
        if not lowered or lowered in TABLE_STOPWORDS:
            return ""
        if not TABLE_NAME_PATTERN.match(lowered.replace("_", "")):
            return ""
        candidate_parts.append(lowered)
    candidate = "_".join(candidate_parts)
    candidate = re.sub(r"[^\w]", "_", candidate)
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        return ""
    if candidate in TABLE_STOPWORDS:
        return ""
    if len(candidate) < 3:
        return ""
    if not TABLE_NAME_PATTERN.match(candidate):
        return ""
    if len(candidate) > 40:
        return ""
    if len(candidate.split("_")) > 4:
        return ""
    return candidate


def parse_collection(md: str) -> str:
    candidates = extract_collection_candidates(md)
    if candidates:
        return select_best_table_candidate(candidates)
    return ""


def parse_collection_from_code(*codes: str) -> str:
    gliderecord_pattern = re.compile(r"(?i)GlideRecord\(['\"]([A-Za-z0-9_\.]+)['\"]\)")
    for code in codes:
        if not code:
            continue
        m = gliderecord_pattern.search(code)
        if m:
            return m.group(1).strip()
    return ""


def select_best_table_candidate(candidates: List[str]) -> str:
    best = candidates[0]
    best_score = score_table_candidate(best)
    for candidate in candidates[1:]:
        score = score_table_candidate(candidate)
        if score > best_score:
            best = candidate
            best_score = score
    return best


def score_table_candidate(name: str) -> int:
    score = len(name)
    if "_" in name:
        score += 2
    for prefix in TABLE_PREFIX_PREFERENCE:
        if name.startswith(prefix):
            score += 5
            break
    return score


def build_row(name: str, files: Dict[str, str]) -> Dict[str, str]:
    readme = fetch_raw(files["README"]) if files["README"] else ""
    code1_path = files["CODE1"]
    code2_path = files["CODE2"]
    code1 = fetch_raw(code1_path) if code1_path else ""
    code2 = fetch_raw(code2_path) if code2_path else ""

    if not code1:
        code1 = extract_code_from_readme(readme)

    repo_path = f"{FOLDER}/{name}"
    when_to_run = parse_when_to_run(readme)
    collection = parse_collection(readme)
    if not collection:
        collection = parse_collection_from_code(code1, code2)

    return {
        "title": name,
        "description": readme,
        "code": code1,
        "code2": code2,
        "collection": collection,
        "when_to_run": when_to_run,
        "repo_path": repo_path,
    }


def scrape() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)
    grouped = group_business_rule_files(tree)

    rows = []
    for folder, files in sorted(grouped.items()):
        if not files["README"]:
            continue
        rows.append(build_row(folder, files))

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Business Rules from code-snippets repo")
    ap.add_argument("--out-xlsx", default="spreadsheets/business_rules.xlsx", help="Output .xlsx filename")
    ap.add_argument("--out-csv", default="spreadsheets/business_rules.csv", help="Output .csv filename")
    args = ap.parse_args()

    df = scrape()
    df = df[[
        "title", "description", "code", "code2", "collection", "when_to_run", "repo_path"
    ]]

    out_dir = os.path.dirname(args.out_xlsx) or "."
    os.makedirs(out_dir, exist_ok=True)

    df.to_excel(args.out_xlsx, index=False)
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)

    print(f"Saved {len(df)} business rules to {args.out_xlsx}")
    if args.out_csv:
        print(f"Saved CSV export to {args.out_csv}")


if __name__ == "__main__":
    main()
