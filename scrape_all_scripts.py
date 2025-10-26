import argparse
import os
import re
import time
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from dotenv import load_dotenv

OWNER = "ServiceNowDevProgram"
REPO = "code-snippets"
BRANCH = "main"

API_BASE = "https://api.github.com"
RAW_BASE = "https://raw.githubusercontent.com"

BASE_FOLDERS = [
    "Client-Side Components",
    "Core ServiceNow APIs",
    "Integration",
    "Modern Development",
    "Server-Side Components",
    "Specialized Areas",
]

ASSET_FOLDER = "assets"
PAGES_FOLDER = "pages"

SCRIPT_TYPE_LOOKUP: Dict[Tuple[str, str], str] = {
    ("Client-Side Components", "Client Scripts"): "Client Script",
    ("Client-Side Components", "Catalog Client Script"): "Catalog Client Script",
    ("Client-Side Components", "UI Actions"): "UI Action",
    ("Client-Side Components", "UI Macros"): "UI Macro",
    ("Client-Side Components", "UI Pages"): "UI Page",
    ("Client-Side Components", "UI Scripts"): "UI Script",
    ("Client-Side Components", "UX Client Script Include"): "UX Script Include",
    ("Client-Side Components", "UX Client Scripts"): "UX Client Script",
    ("Client-Side Components", "UX Data Broker Transform"): "UX Data Broker Transform",
    ("Core ServiceNow APIs", "GlideAggregate"): "Core API",
    ("Core ServiceNow APIs", "GlideAjax"): "Core API",
    ("Core ServiceNow APIs", "GlideDate"): "Core API",
    ("Core ServiceNow APIs", "GlideDateTime"): "Core API",
    ("Core ServiceNow APIs", "GlideElement"): "Core API",
    ("Core ServiceNow APIs", "GlideFilter"): "Core API",
    ("Core ServiceNow APIs", "GlideHTTPRequest"): "Core API",
    ("Core ServiceNow APIs", "GlideJsonPath"): "Core API",
    ("Core ServiceNow APIs", "GlideModal"): "Core API",
    ("Core ServiceNow APIs", "GlideQuery"): "Core API",
    ("Core ServiceNow APIs", "GlideRecord"): "Core API",
    ("Core ServiceNow APIs", "GlideSystem"): "Core API",
    ("Core ServiceNow APIs", "GlideTableDescriptor"): "Core API",
    ("Integration", "Mail Scripts"): "Mail Script",
    ("Modern Development", "Service Portal Widgets"): "Service Portal Widget",
}

PRIMARY_FIELDS = [
    "name",
    "description",
    "script_type",
    "client_script",
    "script_include",
    "code",
    "code2",
    "client_side_type",
    "type_for_specialized_areas",
    "table",
    "data_table",
    "field_list",
    "html",
    "css",
    "option_schema",
    "link",
    "condition",
    "when_to_run",
    "repo_path",
    "action_name",
    "client_script_v2",
    "onClick",
    "coalesce",
    "source_table",
    "target_table",
    "client_callable",
]

EXTRA_FIELDS = [
    "category",
    "subcategory",
    "description_markdown",
    "server_script",
    "api_name",
    "access",
    "mobile_callable",
    "sandbox_callable",
    "ui_type",
    "sys_scope",
    "catalog_item",
    "applies_to",
    "controller_as",
    "demo_data",
    "scss",
    "notes",
    "run_as",
    "run_start",
    "run_period",
    "run_dayofweek",
    "run_dayofmonth",
    "run_time",
]

ALL_FIELDS = PRIMARY_FIELDS + EXTRA_FIELDS

LIST_FIELDS = {
    "client_script",
    "client_script_v2",
    "script_include",
    "code",
    "code2",
    "server_script",
    "html",
    "css",
    "scss",
    "option_schema",
    "link",
    "demo_data",
    "field_list",
    "data_table",
    "notes",
    "onClick",
}

TEXT_EXTENSIONS = {
    ".css",
    ".scss",
    ".html",
    ".htm",
    ".js",
    ".json",
    ".md",
    ".txt",
    ".xml",
}

S = requests.Session()
load_dotenv()
TOKEN = os.getenv("GITHUB_TOKEN")
if TOKEN:
    S.headers.update({"Authorization": f"Bearer {TOKEN}"})


def req(method: str, url: str, **kw) -> requests.Response:
    for attempt in range(6):
        resp = S.request(method, url, timeout=30, **kw)
        if resp.status_code in (403, 429) or resp.status_code >= 500:
            time.sleep(min(2 ** attempt, 20))
            continue
        resp.raise_for_status()
        return resp
    resp.raise_for_status()
    return resp


def get_branch_sha() -> str:
    resp = req("GET", f"{API_BASE}/repos/{OWNER}/{REPO}/branches/{BRANCH}")
    return resp.json()["commit"]["sha"]


def list_tree_recursive(sha: str) -> List[Dict]:
    resp = req(
        "GET",
        f"{API_BASE}/repos/{OWNER}/{REPO}/git/trees/{sha}",
        params={"recursive": "1"},
    )
    return resp.json().get("tree", [])


def fetch_raw(path: str) -> str:
    url = f"{RAW_BASE}/{OWNER}/{REPO}/{BRANCH}/{path}"
    return req("GET", url).text


def normalize_bool(value: str) -> str:
    low = value.strip().lower()
    if low in {"true", "yes", "y", "1", "enabled", "checked"}:
        return "true"
    if low in {"false", "no", "n", "0", "disabled", "unchecked"}:
        return "false"
    return value.strip()


def first_match(text: str, patterns: Iterable[str]) -> str:
    for pat in patterns:
        match = re.search(pat, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return ""


def parse_description(md: str) -> str:
    desc = first_match(md, [r"(?i)\bdescription\s*[:\-]\s*(.+)"])
    if desc:
        return desc
    for line in md.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if re.match(
            r"(?i)^(name|table|applies|type|event|ui|scope|field|condition|when|action|onclick|source\s*table|target\s*table|coalesce|client\s*callable)\s*[:\-]",
            stripped,
        ):
            continue
        return stripped
    return ""


def parse_event(md: str) -> str:
    for token in ["onChange", "onLoad", "onSubmit", "onCellEdit", "onValueChange"]:
        if re.search(rf"\b{re.escape(token)}\b", md, flags=re.IGNORECASE):
            return token
    match = re.search(r"(?i)\b(type|event)\s*[:\-]\s*([A-Za-z_]+)", md)
    return match.group(2) if match else ""


TABLE_PATTERNS = [
    r"(?i)\btable\s*[:\-]\s*([A-Za-z0-9_\. ]+)",
    r"(?i)\bapplies\s*to\s*[:\-]\s*([A-Za-z0-9_\. ]+)",
    r"(?i)\bon\s+table\s*[:\-]\s*([A-Za-z0-9_\. ]+)",
]


def parse_table_from_readme(md: str) -> str:
    return first_match(md, TABLE_PATTERNS)


GF_METHODS = [
    "getValue",
    "getControl",
    "setValue",
    "setDisplay",
    "setVisible",
    "setMandatory",
    "setReadOnly",
    "showFieldMsg",
    "clearValue",
    "hideAllSections",
    "addInfoMessage",
    "addErrorMessage",
]


def parse_fields_from_code(js: str) -> List[str]:
    tokens: List[str] = []
    for method in GF_METHODS:
        pattern = rf"\bg_form\.{method}\(\s*['\"]([A-Za-z0-9_\.]+)['\"]"
        tokens.extend(re.findall(pattern, js))
    if not tokens:
        return []
    freq = Counter(tokens)
    ordered = sorted(freq.items(), key=lambda item: (-item[1], item[0]))
    return [name for name, _count in ordered]


TABLE_CODE_PATTERNS = [
    r"(?i)current\s*=\s*new\s+GlideRecord\(['\"]([A-Za-z0-9_\.]+)['\"]\)",
    r"(?i)\bGlideRecord\(['\"]([A-Za-z0-9_\.]+)['\"]\)",
    r"(?i)\btableName\s*=\s*['\"]([A-Za-z0-9_\.]+)['\"]",
]


def parse_table_from_code(js: str) -> str:
    for pat in TABLE_CODE_PATTERNS:
        match = re.search(pat, js)
        if match:
            return match.group(1)
    return ""


README_PATTERNS: Dict[str, List[str]] = {
    "name": [r"(?i)\bname\s*[:\-]\s*([^\n]+)"],
    "client_side_type": [
        r"(?i)\b(onChange|onLoad|onSubmit|onCellEdit|onValueChange)\b",
    ],
    "applies_to": [r"(?i)\bapplies\s*to\s*[:\-]\s*([^\n]+)"],
    "condition": [r"(?i)\bcondition\s*[:\-]\s*([^\n]+)"],
    "when_to_run": [
        r"(?i)\bwhen\s*(?:to\s*run)?\s*[:\-]\s*([^\n]+)",
        r"(?i)\bwhen\b\s*[:\-]\s*([^\n]+)",
    ],
    "action_name": [r"(?i)\baction\s*name\s*[:\-]\s*([^\n]+)"],
    "onClick": [r"(?i)\bon\s*click\s*[:\-]\s*([^\n]+)", r"(?i)\bonclick\s*[:\-]\s*([^\n]+)"],
    "coalesce": [r"(?i)\bcoalesce\s*[:\-]\s*([^\n]+)"],
    "source_table": [r"(?i)\bsource\s*table\s*[:\-]\s*([^\n]+)"],
    "target_table": [r"(?i)\btarget\s*table\s*[:\-]\s*([^\n]+)"],
    "client_callable": [
        r"(?i)\b(client\s*callable|glide\s*ajax\s*enabled)\s*[:\-]\s*([^\n]+)"
    ],
    "api_name": [r"(?i)\bapi\s*name\s*[:\-]\s*([^\n]+)"],
    "access": [r"(?i)\baccessible\s*from\s*[:\-]\s*([^\n]+)"],
    "mobile_callable": [r"(?i)\bmobile\s*callable\s*[:\-]\s*([^\n]+)"],
    "sandbox_callable": [r"(?i)\bsandbox\s*enabled\s*[:\-]\s*([^\n]+)"],
    "ui_type": [r"(?i)\bui\s*type\s*[:\-]\s*([^\n]+)"],
    "sys_scope": [r"(?i)\bsys\s*scope\s*[:\-]\s*([^\n]+)", r"(?i)\bapplication\s*[:\-]\s*([^\n]+)"],
    "catalog_item": [r"(?i)\bcat(?:alog)?\s*item\s*[:\-]\s*([^\n]+)"],
    "data_table": [r"(?i)\bdata\s*table\s*[:\-]\s*([^\n]+)"],
    "field_list": [r"(?i)\bfields?\s*[:\-]\s*([^\n]+)"],
    "link": [r"(?i)\blink\s*[:\-]\s*(https?://[^\s]+)"],
    "run_as": [r"(?i)\brun\s*as\s*[:\-]\s*([^\n]+)"],
    "run_start": [r"(?i)\brun\s*start\s*[:\-]\s*([^\n]+)", r"(?i)\bstart\s*[:\-]\s*([^\n]+)"],
    "run_period": [r"(?i)\brun\s*period\s*[:\-]\s*([^\n]+)"],
    "run_dayofweek": [
        r"(?i)\bday\s*of\s*week\s*[:\-]\s*([^\n]+)",
        r"(?i)\bdayofweek\s*[:\-]\s*([^\n]+)",
    ],
    "run_dayofmonth": [
        r"(?i)\bday\s*of\s*month\s*[:\-]\s*([^\n]+)",
        r"(?i)\bdayofmonth\s*[:\-]\s*([^\n]+)",
    ],
    "run_time": [r"(?i)\btime\s*[:\-]\s*([^\n]+)"],
}

BOOL_KEYS = {"client_callable", "mobile_callable", "sandbox_callable", "coalesce"}


def parse_readme_metadata(md: str) -> Dict[str, str]:
    if not md:
        return {}
    data: Dict[str, str] = {}
    for key, patterns in README_PATTERNS.items():
        value = first_match(md, patterns)
        if value:
            data[key] = value
    if "client_callable" in data:
        data["client_callable"] = normalize_bool(data["client_callable"])
    for key in ("mobile_callable", "sandbox_callable", "coalesce"):
        if key in data:
            data[key] = normalize_bool(data[key])
    if "client_side_type" not in data:
        event = parse_event(md)
        if event:
            data["client_side_type"] = event
    table = parse_table_from_readme(md)
    if table and "table" not in data:
        data["table"] = table
    desc = parse_description(md)
    if desc:
        data["description"] = desc
    return data


def extract_code_blocks(md: str) -> List[str]:
    pattern = r"```(?:javascript|js|json|xml|html|css)\s*\n(.*?)\n```"
    return [block.strip() for block in re.findall(pattern, md, flags=re.DOTALL)]


def extract_controller_as(client_js: str) -> str:
    match = re.search(r"controllerAs\s*:\s*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", client_js)
    if match:
        return match.group(1)
    match = re.search(r"\bvar\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*this\s*;", client_js)
    return match.group(1) if match else ""


def extract_link_function(client_js: str) -> str:
    match = re.search(r"\bfunction\s+link\s*\(([^\)]*)\)\s*\{", client_js)
    if not match:
        return ""
    start = match.end()
    depth = 1
    idx = start
    while idx < len(client_js):
        char = client_js[idx]
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                body = client_js[start:idx]
                return f"function link({match.group(1)}){{\n{body}\n}}"
        idx += 1
    return ""


INCLUDE_NAME_HINTS = ("include", "processor", "server", "script_include", "ajax")
CLIENT_NAME_HINTS = ("client", "workspace", "portal")


def readme_mentions(filename: str, lines: List[str], token: str) -> bool:
    norm = re.sub(r"[^A-Za-z0-9]+", "", token.lower())
    for line in lines:
        low = line.lower()
        if token.lower() in low and norm in re.sub(r"[^A-Za-z0-9]+", "", low):
            return True
    return False


def infer_js_role(filename: str, code: str, readme_lines: List[str]) -> str:
    name = filename.lower()
    content = code.lower()
    if readme_mentions(filename, readme_lines, "script include"):
        return "script_include"
    if readme_mentions(filename, readme_lines, "client script"):
        return "client"
    if any(hint in name for hint in INCLUDE_NAME_HINTS):
        if "g_form" not in content:
            return "script_include"
    if any(hint in name for hint in CLIENT_NAME_HINTS):
        return "client"
    if any(
        trigger in content
        for trigger in (
            "function onload",
            "function onchange",
            "function onsubmit",
            "function oncelledit",
            "function onvaluechange",
        )
    ):
        return "client"
    if "g_form" in content or "g_scratchpad" in content:
        return "client"
    if any(token in content for token in ("class.create", "prototype =", "gs.", "gliderecord")):
        if "g_form" not in content:
            return "script_include"
    return "unknown"


def classify_js_role(
    snippet_path: str,
    rel_path: str,
    filename: str,
    code: str,
    readme_lines: List[str],
) -> str:
    lower_path = snippet_path.lower()
    lower_rel = rel_path.lower()
    lower_name = filename.lower()
    content = code.lower()

    if "script include" in lower_path or "script include" in lower_rel:
        return "script_include"
    if "ux client script include" in lower_path:
        return "script_include"
    if "client scripts" in lower_path or "ux client scripts" in lower_path:
        guess = infer_js_role(filename, code, readme_lines)
        return "client" if guess != "script_include" else "script_include"
    if "catalog client script" in lower_path:
        guess = infer_js_role(filename, code, readme_lines)
        if guess == "script_include":
            return "script_include"
        if guess == "client":
            return "client"
    if "service portal widgets" in lower_path:
        if "client" in lower_name:
            return "client"
        if "server" in lower_name:
            return "portal_server"
        if "link" in lower_name:
            return "client"
        return "client" if "g_form" in content else "portal_server"
    if "service portal" in lower_path:
        if "client" in lower_name:
            return "client"
        if "server" in lower_name:
            return "portal_server"
        if "controller" in lower_name:
            return "client"
        return "client" if "g_form" in content else "portal_server"
    if "ui actions" in lower_path:
        if "script include" in lower_name or "scriptinclude" in lower_rel:
            return "script_include"
        if "client" in lower_name or "workspace" in lower_name:
            return "client"
        if "server" in lower_name:
            return "server"
        if "g_form" in content:
            return "client"
        return "server"
    if "scheduled jobs" in lower_path or "background scripts" in lower_path:
        return "server"
    if "transform map scripts" in lower_path:
        return "server"
    if "core servicenow apis" in lower_path:
        return "server"
    if "integration" in lower_path and "mail scripts" in lower_path:
        return "server"
    guess = infer_js_role(filename, code, readme_lines)
    if guess == "client":
        return "client"
    if guess == "script_include":
        return "script_include"
    return "server"


def get_base_folder(path: str) -> Optional[str]:
    for base in BASE_FOLDERS + [ASSET_FOLDER, PAGES_FOLDER]:
        prefix = f"{base}/"
        if path.startswith(prefix):
            return base
    return None


def resolve_snippet_root(base: str, path: str) -> Optional[str]:
    prefix = f"{base}/"
    rel = path[len(prefix) :]
    parts = rel.split("/")
    if len(parts) < 2:
        return None
    return f"{base}/{parts[0]}/{parts[1]}"


def group_snippets(tree: List[Dict]) -> Dict[str, Dict[str, List[str]]]:
    grouped: Dict[str, Dict[str, List[str]]] = {}
    for node in tree:
        if node.get("type") != "blob":
            continue
        path = node.get("path", "")
        base = get_base_folder(path)
        if not base or base in {ASSET_FOLDER, PAGES_FOLDER}:
            continue
        snippet = resolve_snippet_root(base, path)
        if not snippet:
            continue
        entry = grouped.setdefault(snippet, {"readme": "", "files": []})
        if os.path.basename(path).lower() == "readme.md":
            entry["readme"] = path
        else:
            entry["files"].append(path)
    return grouped


def gather_assets(tree: List[Dict]) -> List[str]:
    return [
        node["path"]
        for node in tree
        if node.get("type") == "blob" and node.get("path", "").startswith(f"{ASSET_FOLDER}/")
    ]


def gather_pages(tree: List[Dict]) -> List[str]:
    return [
        node["path"]
        for node in tree
        if node.get("type") == "blob" and node.get("path", "").startswith(f"{PAGES_FOLDER}/")
    ]


def blank_row(name: str, script_type: str, category: str, subcategory: str, repo_path: str) -> Dict[str, object]:
    row: Dict[str, object] = {field: "" for field in ALL_FIELDS}
    for field in LIST_FIELDS:
        row[field] = []
    row["name"] = name
    row["script_type"] = script_type
    row["repo_path"] = repo_path
    row["category"] = category
    row["subcategory"] = subcategory
    return row


def append_value(row: Dict[str, object], field: str, value: str, sep: str = "\n\n") -> None:
    if not value:
        return
    trimmed = value.strip()
    if not trimmed:
        return
    if field in LIST_FIELDS:
        row[field].append(trimmed)
    else:
        if row.get(field):
            row[field] = f"{row[field]}{sep}{trimmed}"
        else:
            row[field] = trimmed


def assign_js(row: Dict[str, object], role: str, content: str) -> None:
    if role == "client":
        target = "client_script" if not row["client_script"] else "client_script_v2"
        append_value(row, target, content)
    elif role == "script_include":
        append_value(row, "script_include", content)
    elif role == "portal_server":
        append_value(row, "server_script", content)
    elif role == "server":
        target = "code" if not row["code"] else "code2"
        append_value(row, target, content)
    else:
        append_value(row, "code", content)


def finalize_row(
    row: Dict[str, object],
    readme_markdown: str,
    script_type: str,
    category: str,
    subcategory: str,
) -> Dict[str, str]:
    text_fields = {
        key: value if isinstance(value, list) else [value]
        for key, value in row.items()
        if key in LIST_FIELDS
    }

    for field, values in text_fields.items():
        cleaned = [v.strip() for v in values if isinstance(v, str) and v.strip()]
        row[field] = "\n\n".join(cleaned)

    for key in ALL_FIELDS:
        if key not in row:
            row[key] = ""
        elif isinstance(row[key], list):
            row[key] = "\n\n".join([str(v).strip() for v in row[key] if str(v).strip()])
        elif isinstance(row[key], str):
            row[key] = row[key].strip()
        else:
            row[key] = str(row[key])

    if not row["description"] and readme_markdown:
        row["description"] = parse_description(readme_markdown)

    if not row["client_side_type"]:
        script_body = row["client_script"] or row["client_script_v2"]
        event = parse_event(readme_markdown or script_body or "")
        if event:
            row["client_side_type"] = event

    if not row["table"]:
        candidate = parse_table_from_readme(readme_markdown or "")
        if not candidate:
            candidate = parse_table_from_code(row["client_script"] or row["code"])
        if candidate:
            row["table"] = candidate

    if not row["field_list"]:
        field_candidates = parse_fields_from_code(row["client_script"] or "")
        if field_candidates:
            row["field_list"] = ", ".join(field_candidates[:3])

    if category == "Specialized Areas" and not row["type_for_specialized_areas"]:
        row["type_for_specialized_areas"] = subcategory
        if script_type == subcategory:
            row["script_type"] = "Specialized Area"

    if (
        category == "Modern Development"
        and subcategory == "Service Portal Widgets"
        and row["client_script"]
    ):
        if not row["controller_as"]:
            row["controller_as"] = extract_controller_as(row["client_script"])
        if not row["link"]:
            link_func = extract_link_function(row["client_script"])
            if link_func:
                append_value(row, "link", link_func)

    if not row["client_callable"] and script_type in {"Script Include", "UX Script Include"}:
        if "AbstractAjaxProcessor" in (row["script_include"] or ""):
            row["client_callable"] = "true"

    row["description_markdown"] = readme_markdown.strip()
    return {key: str(row.get(key, "") or "") for key in ALL_FIELDS}


def process_snippet(snippet_path: str, info: Dict[str, List[str]]) -> Dict[str, str]:
    parts = snippet_path.split("/")
    if len(parts) < 3:
        return {}
    category, subcategory, name = parts[0], parts[1], parts[2]
    script_type = SCRIPT_TYPE_LOOKUP.get((category, subcategory), subcategory)

    readme_md = fetch_raw(info["readme"]) if info.get("readme") else ""
    readme_lines = readme_md.splitlines()
    metadata = parse_readme_metadata(readme_md)
    code_blocks = extract_code_blocks(readme_md)

    row = blank_row(
        metadata.get("name", name),
        script_type,
        category,
        subcategory,
        repo_path=snippet_path,
    )

    for key, value in metadata.items():
        if key in BOOL_KEYS:
            row[key] = normalize_bool(value)
        elif key in row:
            row[key] = value.strip()
        else:
            row[key] = value.strip()

    if metadata.get("description"):
        row["description"] = metadata["description"]

    if "applies_to" in metadata and not row["table"]:
        row["table"] = metadata["applies_to"]

    for path in sorted(info.get("files", [])):
        rel = path[len(snippet_path) + 1 :] if path.startswith(f"{snippet_path}/") else os.path.basename(path)
        _, ext = os.path.splitext(path.lower())
        if ext not in TEXT_EXTENSIONS:
            continue
        content = fetch_raw(path)
        filename = os.path.basename(path)

        if ext in {".html", ".htm"}:
            append_value(row, "html", content)
            continue
        if ext == ".css":
            append_value(row, "css", content)
            continue
        if ext == ".scss":
            append_value(row, "scss", content)
            append_value(row, "css", content)
            continue
        if ext == ".xml":
            target = "code" if not row["code"] else "code2"
            append_value(row, target, content)
            continue
        if ext == ".json":
            lower_rel = rel.lower()
            if "option_schema" in lower_rel or "options_schema" in lower_rel or "option-schema" in lower_rel:
                append_value(row, "option_schema", content)
            elif "demo" in lower_rel or "sample" in lower_rel:
                append_value(row, "demo_data", content)
            else:
                append_value(row, "notes", content)
            continue
        if ext == ".md":
            append_value(row, "notes", content)
            continue
        if ext == ".js":
            role = classify_js_role(snippet_path, rel, filename, content, readme_lines)
            assign_js(row, role, content)
            continue

    if not row["code"] and not row["server_script"] and code_blocks:
        append_value(row, "code", code_blocks[0])
        for extra in code_blocks[1:]:
            append_value(row, "code2", extra)

    return finalize_row(row, readme_md, script_type, category, subcategory)


def process_assets(asset_paths: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for path in sorted(asset_paths):
        name = os.path.basename(path)
        rel = path[len(f"{ASSET_FOLDER}/") :] if path.startswith(f"{ASSET_FOLDER}/") else path
        subcategory = rel.split("/")[0] if "/" in rel else "root"
        script_type = "Asset"
        row = blank_row(name, script_type, ASSET_FOLDER, subcategory, path)
        _, ext = os.path.splitext(path.lower())
        if ext in TEXT_EXTENSIONS:
            content = fetch_raw(path)
            if ext == ".js":
                append_value(row, "code", content)
            elif ext in {".css", ".scss"}:
                append_value(row, "css", content)
            elif ext in {".html", ".htm"}:
                append_value(row, "html", content)
            else:
                append_value(row, "notes", content)
        rows.append(
            finalize_row(row, "", script_type, ASSET_FOLDER, subcategory)
        )
    return rows


def process_pages(page_paths: List[str]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for path in sorted(page_paths):
        if not path.lower().endswith(".html"):
            continue
        name = os.path.basename(path)
        row = blank_row(name, "Static Page", PAGES_FOLDER, "", path)
        content = fetch_raw(path)
        append_value(row, "html", content)
        match = re.search(r"<title>(.*?)</title>", content, flags=re.IGNORECASE | re.DOTALL)
        if match:
            row["description"] = match.group(1).strip()
        rows.append(finalize_row(row, "", "Static Page", PAGES_FOLDER, ""))
    return rows


def scrape_all() -> pd.DataFrame:
    sha = get_branch_sha()
    tree = list_tree_recursive(sha)

    snippets = group_snippets(tree)
    rows: List[Dict[str, str]] = []

    for snippet_path in sorted(snippets.keys()):
        row = process_snippet(snippet_path, snippets[snippet_path])
        if row:
            rows.append(row)

    rows.extend(process_assets(gather_assets(tree)))
    rows.extend(process_pages(gather_pages(tree)))

    df = pd.DataFrame(rows)
    ordered = [field for field in ALL_FIELDS if field in df.columns]
    remaining = [c for c in df.columns if c not in ordered]
    return df[ordered + remaining]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape all ServiceNow code snippets into a unified dataset."
    )
    parser.add_argument(
        "--out-xlsx",
        default="spreadsheets/all_scripts.xlsx",
        help="Output Excel file path.",
    )
    parser.add_argument(
        "--out-csv",
        default="spreadsheets/all_scripts.csv",
        help="Optional CSV output path.",
    )
    args = parser.parse_args()

    df = scrape_all()

    os.makedirs(os.path.dirname(args.out_xlsx), exist_ok=True)
    df.to_excel(args.out_xlsx, index=False)
    if args.out_csv:
        df.to_csv(args.out_csv, index=False)
    print(f"Saved {len(df)} records to {args.out_xlsx}")
    if args.out_csv:
        print(f"Saved CSV export to {args.out_csv}")


if __name__ == "__main__":
    main()
