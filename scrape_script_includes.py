import os
import re
import time
import argparse
from typing import Dict, List, Optional

import requests
import pandas as pd
from dotenv import load_dotenv

OWNER = "ServiceNowDevProgram"
REPO  = "code-snippets"
API_BASE = "https://api.github.com"

# URL-encoded GitHub API path for the Script Includes folder
API_PATH = "Server-Side%20Components/Script%20Includes"

SESSION = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    SESSION.headers.update({"Authorization": f"Bearer {TOKEN}"})


def gh_get(path: str, params: Optional[Dict] = None) -> requests.Response:
    url = f"{API_BASE}{path}"
    for attempt in range(3):
        r = SESSION.get(url, params=params, timeout=30)
        if r.status_code == 403 and "rate limit" in r.text.lower():
            time.sleep(5 + attempt * 5)
            continue
        if r.ok:
            return r
        time.sleep(1 + attempt)
    r.raise_for_status()
    return r


def list_dir(owner: str, repo: str, path: str) -> List[Dict]:
    r = gh_get(f"/repos/{owner}/{repo}/contents/{path}")
    data = r.json()
    return data if isinstance(data, list) else []


def get_raw(url: str) -> str:
    r = SESSION.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def as_bool(val: Optional[str]) -> str:
    """Normalize truthy/falsey text to 'true'/'false'/''."""
    if not val:
        return ""
    v = val.strip().lower()
    if v in ("true", "yes", "y", "1", "enabled", "checked"):
        return "true"
    if v in ("false", "no", "n", "0", "disabled", "unchecked"):
        return "false"
    return ""


# --- README parsers ---------------------------------------------------------

# Lines like:
# - API Name: x_scope.MyInclude
# - Accessible from: All application scopes / This application scope only
# - Active: true/false
# - Glide AJAX enabled: true/false  (aka client_callable)
# - Mobile callable: true/false
# - Sandbox enabled: true/false
README_FIELD_PATTERNS = {
    "api_name":        r"(?i)\bapi\s*name\s*[:\-]\s*(.+)",
    "access":          r"(?i)\baccessible\s*from\s*[:\-]\s*(.+)",
    "active":          r"(?i)\bactive\s*[:\-]\s*([A-Za-z0-9_]+)",
    "client_callable": r"(?i)\b(glide\s*ajax\s*enabled|client\s*callable)\s*[:\-]\s*([A-Za-z0-9_]+)",
    "mobile_callable": r"(?i)\bmobile\s*callable\s*[:\-]\s*([A-Za-z0-9_]+)",
    "sandbox_callable":r"(?i)\bsandbox\s*enabled\s*[:\-]\s*([A-Za-z0-9_]+)",
    "description":     r"(?i)\bdescription\s*[:\-]\s*(.+)",
}

def parse_readme_fields(md: str) -> Dict[str, str]:
    out: Dict[str, str] = {}

    def grab(key: str, pat: str, group: int = 1):
        m = re.search(pat, md)
        if m:
            out[key] = m.group(group).strip()

    grab("api_name", README_FIELD_PATTERNS["api_name"])
    grab("access",   README_FIELD_PATTERNS["access"])
    grab("active",   README_FIELD_PATTERNS["active"])
    m = re.search(README_FIELD_PATTERNS["client_callable"], md)
    if m:
        out["client_callable"] = m.group(2).strip()
    grab("mobile_callable",  README_FIELD_PATTERNS["mobile_callable"])
    grab("sandbox_callable", README_FIELD_PATTERNS["sandbox_callable"])
    grab("description",      README_FIELD_PATTERNS["description"])

    # If no explicit description, take first non-heading/non-empty line.
    if "description" not in out:
        for line in md.splitlines():
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            # skip obvious metadata lines
            if re.match(r"(?i)^(api\s*name|accessible\s*from|active|glide\s*ajax|client\s*callable|mobile\s*callable|sandbox\s*enabled)\s*[:\-]", t):
                continue
            out["description"] = t
            break

    # Normalize booleans
    for k in ("active", "client_callable", "mobile_callable", "sandbox_callable"):
        if k in out:
            out[k] = as_bool(out[k])

    return out


# --- JS parsers (fallback heuristics) ---------------------------------------

def infer_access_from_code(js: str) -> str:
    """
    Very rough hints:
    - If it extends AbstractAjaxProcessor, often used for GlideAjax (older pattern).
    """
    if re.search(r"\bAbstractAjaxProcessor\b", js):
        return ""  # not definitive; leave blank
    return ""


def scrape() -> pd.DataFrame:
    roots = list_dir(OWNER, REPO, API_PATH)
    rows = []

    for ent in sorted(roots, key=lambda x: x.get("name", "").lower()):
        if ent.get("type") != "dir":
            continue

        sub_items = list_dir(OWNER, REPO, ent["path"])

        # README.md
        readme_item = next((i for i in sub_items
                            if i.get("type") == "file" and i["name"].lower() == "readme.md"), None)
        readme_md = ""
        if readme_item and readme_item.get("download_url"):
            try:
                readme_md = get_raw(readme_item["download_url"])
            except Exception:
                readme_md = ""

        # .js (script include code)
        js_item = next((i for i in sub_items
                        if i.get("type") == "file" and i["name"].lower().endswith(".js")), None)
        code = ""
        if js_item and js_item.get("download_url"):
            try:
                code = get_raw(js_item["download_url"])
            except Exception:
                code = ""

        fields = {
            "title": ent["name"],
            "api_name": "",
            "description": "",
            "access": "",
            "active": "",
            "client_callable": "",
            "mobile_callable": "",
            "sandbox_callable": "",
            "script": code,
        }

        if readme_md:
            md_fields = parse_readme_fields(readme_md)
            fields.update({k: md_fields.get(k, fields[k]) for k in fields.keys() if k in md_fields})

        # Code-based fallbacks (light heuristics)
        if not fields["access"] and code:
            fields["access"] = infer_access_from_code(code)

        rows.append(fields)

    return pd.DataFrame(rows)


def main():
    ap = argparse.ArgumentParser(description="Scrape Script Includes from ServiceNowDevProgram/code-snippets")
    ap.add_argument("--out", default="script_includes.xlsx", help="Output .xlsx filename")
    args = ap.parse_args()

    df = scrape()
    # Order columns similar to the ServiceNow form (backend names shown)
    df = df[[
        "title",          # name
        "api_name",       # api_name
        "client_callable",
        "mobile_callable",
        "sandbox_callable",
        "description",
        "access",
        "active",
        "script"
    ]]
    df.to_excel(args.out, index=False)
    print(f"Saved {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
