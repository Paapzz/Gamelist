#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HLTB Worker - полный рабочий скрипт
Реализует:
 - извлечение списка игр из index111.html
 - поиск по HowLongToBeat с учётом сложных названий (слэши, скобки, & и т.д.)
 - подбор лучшего кандидата по текстовой схожести + годовая логика (earliest)
 - парсинг страницы игры для извлечения времени прохождений
 - минимальные debug-дампы (только для проблем)
 - безопасный git commit/push (merge -X ours), с fallback веткой
Параметры:
 - DEBUG: выставлять True для тестовых прогонов (30-40 игр)
 - COMMIT_RESULTS: True/False (по умолчанию True — воркер попытается закоммитить)
"""

from __future__ import annotations
import os
import re
import json
import time
import random
import ast
import traceback
from datetime import datetime
from urllib.parse import quote
from difflib import SequenceMatcher
import subprocess
from typing import List, Dict, Any, Optional

# Playwright import
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception as e:
    print("❗ Playwright import error:", e)
    print("Please ensure dependencies: pip install playwright pyee requests && python -m playwright install chromium")
    raise

# ------------------------
# Configuration / Tunables
# ------------------------
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "hltb_data.json")
REPORT_FILE = os.path.join(OUTPUT_DIR, "scraping_report.json")
DEBUG_DIR = "debug_dumps"
BASE_URL = "https://howlongtobeat.com"

# Timeouts (as requested)
WAIT_SELECTOR_TIMEOUT_MS = 10_000  # wait_for_selector 10s
PAGE_GOTO_TIMEOUT_MS = 17_000      # page.goto 17s

# Attempts and delays
MAX_ATTEMPTS_PER_VARIANT = 2  # initial + 1 retry
SLEEP_MIN = 0.4
SLEEP_MAX = 1.2
LONG_PAUSE_EVERY = 100  # after N games optionally long pause
LONG_PAUSE_MIN = 30
LONG_PAUSE_MAX = 90

# Similarity thresholds (as agreed)
SIM_THRESH_EXACT = 0.95
SIM_THRESH_YEAR = 0.88
SIM_THRESH_SUBSTR = 0.92

# Flags (can be toggled via env)
DEBUG = os.getenv("HLTB_DEBUG", "false").lower() in ("1", "true", "yes")
COMMIT_RESULTS = os.getenv("COMMIT_RESULTS", "true").lower() in ("1", "true", "yes")

# Filename sanitization
INVALID_FILENAME_CHARS_RE = re.compile(r'["<>:\\|?*\r\n]')

# ------------------------
# Utilities
# ------------------------
def ts_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def now_ts():
    return int(time.time())

def sanitize_filename(s: str) -> str:
    s = s.strip()
    s = INVALID_FILENAME_CHARS_RE.sub("_", s)
    s = s.replace("/", "_").replace("\\", "_")
    s = re.sub(r'\s+', '_', s)
    return s[:220]

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if DEBUG:
        os.makedirs(DEBUG_DIR, exist_ok=True)

def write_json_safe(path: str, obj: Any):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def read_file_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def normalized_title(s: str) -> str:
    s = (s or "").lower()
    # keep / for initial parsing; later we'll remove punctuation except spaces
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, normalized_title(a), normalized_title(b)).ratio()

# ------------------------
# Extract games list
# ------------------------
def extract_games_list(html_file: str) -> List[Dict[str, Any]]:
    """
    Попытка извлечь gamesList из JS-объекта; fallback - поиск "Title (YEAR)" или ссылок.
    Возвращает список {title, year or None}
    """
    content = read_file_text(html_file)
    # 1) try JS array: const gamesList = [...]
    m = re.search(r'(?:(?:const|let|var)\s+gamesList\s*=\s*)(\[[\s\S]*?\]);', content, flags=re.MULTILINE)
    if m:
        raw = m.group(1)
        # Normalize JS -> Python
        raw_py = raw.replace('null', 'None').replace('true', 'True').replace('false', 'False')
        # remove trailing commas
        raw_py = re.sub(r',\s*(?=[\]\}])', '', raw_py)
        try:
            parsed = ast.literal_eval(raw_py)
            games = []
            for item in parsed:
                if isinstance(item, str):
                    match = re.match(r'^(.*?)(?:\s*\((\d{4})\))?$', item.strip())
                    title = match.group(1).strip() if match else item.strip()
                    year = int(match.group(2)) if match and match.group(2) else None
                    games.append({"title": title, "year": year})
                elif isinstance(item, dict):
                    title = item.get("title") or item.get("name") or ""
                    year = item.get("year", None)
                    try:
                        year = int(year) if year else None
                    except:
                        year = None
                    games.append({"title": title, "year": year})
            if games:
                print(f"[{ts_str()}] 📄 Parsed gamesList from JS: {len(games)} items.")
                return games
        except Exception as e:
            print(f"[{ts_str()}] ⚠️ Failed to parse JS gamesList: {e}")

    # 2) heuristic: find "Title (YYYY)" in HTML
    matches = re.findall(r'>([^<>]+?)\s*\((\d{4})\)\s*<', content)
    if matches:
        games = []
        for title, year in matches:
            games.append({"title": title.strip(), "year": int(year)})
        print(f"[{ts_str()}] 📄 Heuristic found {len(games)} title(year) matches.")
        return games

    # 3) fallback: extract anchor titles
    anchors = re.findall(r'<a[^>]+href=["\'][^"\']*?game[^"\']*["\'][^>]*>([^<]{2,200}?)</a>', content, flags=re.IGNORECASE)
    uniq = []
    for t in anchors:
        t_clean = re.sub(r'\s+', ' ', t).strip()
        if t_clean not in uniq:
            uniq.append(t_clean)
    if uniq:
        out = []
        for t in uniq:
            m2 = re.match(r'^(.*?)(?:\s*\((\d{4})\))?$', t)
            title = m2.group(1).strip() if m2 else t
            year = int(m2.group(2)) if m2 and m2.group(2) else None
            out.append({"title": title, "year": year})
        print(f"[{ts_str()}] 📄 Fallback link-title extraction found {len(out)} items.")
        return out

    raise RuntimeError(f"Failed to extract games list from {html_file}")

# ------------------------
# Search variants generator
# ------------------------
ROMAN_MAP = [
    ("III", "3"), ("II", "2"), ("IV", "4"), ("V", "5"),
    ("I", "1")
]

def roman_to_arabic_variants(s: str) -> List[str]:
    variants = set()
    variants.add(s)
    for rom, num in ROMAN_MAP:
        if rom in s:
            variants.add(s.replace(rom, num))
        if num in s:
            variants.add(s.replace(num, rom))
    return list(variants)

def without_parentheses(s: str) -> str:
    return re.sub(r'\(.*?\)', '', s).strip()

def ampersand_variants(s: str) -> List[str]:
    v = set()
    v.add(s)
    v.add(s.replace("&", "and"))
    # remove phrases like " & Knuckles" or "& ...)", naive: remove & + following tokens if short
    v.add(re.sub(r'\s*&\s*[^,\)\-]+', '', s).strip())
    return list(v)

def split_slash_parts(s: str) -> List[str]:
    return [p.strip() for p in s.split("/") if p.strip()]

def compute_base_for_slash_no_spaces(s: str, parts: List[str]) -> str:
    # For "Pokémon Red/Blue/Yellow" we want base "Pokémon"
    # Heuristic: take prefix of original up to first part occurrence
    idx = s.find(parts[0])
    if idx > 0:
        base = s[:idx].strip()
        return base if base else parts[0]
    # fallback: use common prefix token(s)
    tokens = [p.split() for p in parts]
    # find longest common prefix across parts' token lists
    common = []
    for i in range(min(len(t) for t in tokens)):
        token = tokens[0][i]
        if all(len(tok) > i and tok[i] == token for tok in tokens):
            common.append(token)
        else:
            break
    if common:
        return " ".join(common)
    # fallback: first token of original
    return s.split()[0] if s.split() else parts[0]

def generate_search_variants(title: str, year: Optional[int] = None) -> List[str]:
    """
    Generate variants in the order described:
    - If slash present: branch into slash_with_spaces and slash_no_spaces logic
    - If no slash: normal sequence
    Returns ordered list of query strings (unique, normalized to sensible forms)
    """
    title = title.strip()
    variants = []
    if "/" in title:
        # detect if slash has spaces around
        if re.search(r'\s/\s', title):
            # slash with spaces -> alternatives A / B
            parts = [p.strip() for p in title.split("/") if p.strip()]
            # iterate each part separately with their sub-variants
            for part in parts:
                # for each part: try part, part_without_parentheses, roman variants, ampersand variants
                v0 = [part]
                v0.append(without_parentheses(part))
                for vv in roman_to_arabic_variants(part):
                    v0.append(vv)
                for vv in ampersand_variants(part):
                    v0.append(vv)
                # preserve order, extend to main list
                for q in v0:
                    q2 = re.sub(r'\s+', ' ', q).strip()
                    if q2 and q2 not in variants:
                        variants.append(q2)
            # fallback: also add full title variations
            variants.append(title)
            variants.append(without_parentheses(title))
            for rom in roman_to_arabic_variants(title):
                if rom not in variants:
                    variants.append(rom)
            for amp in ampersand_variants(title):
                if amp not in variants:
                    variants.append(amp)
        else:
            # slash no spaces: A/B/C style
            parts = split_slash_parts(title)
            base = compute_base_for_slash_no_spaces(title, parts)
            # 1) pairwise combos in order: Base A and B, Base B and C, ...
            for i in range(len(parts)-1):
                left = parts[i]
                right = parts[i+1]
                combo = f"{base} {left} and {right}".strip()
                if combo not in variants:
                    variants.append(combo)
            # 2) single parts in order: Base A, Base B, ...
            for p in parts:
                single = f"{base} {p}".strip()
                if single not in variants:
                    variants.append(single)
            # 3) multi (triple+) in order
            if len(parts) >= 3:
                combo_all = base + " " + " and ".join(parts)
                if combo_all not in variants:
                    variants.append(combo_all)
            # 4) then without parentheses, roman, ampersand on whole title
            if without_parentheses(title) not in variants:
                variants.append(without_parentheses(title))
            for rom in roman_to_arabic_variants(title):
                if rom not in variants:
                    variants.append(rom)
            for amp in ampersand_variants(title):
                if amp not in variants:
                    variants.append(amp)
    else:
        # no slash
        seq = []
        seq.append(title)
        seq.append(without_parentheses(title))
        for rom in roman_to_arabic_variants(title):
            seq.append(rom)
        for amp in ampersand_variants(title):
            seq.append(amp)
        for q in seq:
            q2 = re.sub(r'\s+', ' ', q).strip()
            if q2 and q2 not in variants:
                variants.append(q2)

    # Optionally append title + year (if year given) near front as a strong variant
    if year:
        y_variant = f"{title} {year}"
        if y_variant not in variants:
            variants.insert(0, y_variant)

    # ensure uniqueness preserving order
    uniq = []
    for v in variants:
        v_clean = v.strip()
        if v_clean and v_clean not in uniq:
            uniq.append(v_clean)
    return uniq

# ------------------------
# Candidate extraction helpers
# ------------------------
def extract_candidates_from_html(html: str) -> List[Dict[str, Any]]:
    """
    regex-based fallback to extract anchors /game/<id> and some surrounding text
    """
    pattern = re.compile(r'<a[^>]+href=["\'](?P<href>/game/\d+)[^"\']*["\'][^>]*>(?P<text>.*?)</a>', re.IGNORECASE | re.DOTALL)
    found = []
    for m in pattern.finditer(html):
        raw = m.group('text')
        text = re.sub(r'<[^>]+>', '', raw).strip()
        href = m.group('href')
        # try to get year from text
        ym = re.search(r'\((\d{4})\)', text)
        years = [int(ym.group(1))] if ym else []
        found.append({"text": text, "href": href, "years": years})
    # make unique by href
    seen = set()
    uniq = []
    for c in found:
        if c['href'] not in seen:
            uniq.append(c)
            seen.add(c['href'])
    return uniq

def extract_years_from_text(s: str) -> List[int]:
    return [int(y) for y in re.findall(r'(\d{4})', s)]

# ------------------------
# Candidate selection logic
# ------------------------
def choose_best_candidate(candidates: List[Dict[str, Any]], orig_title: str, input_year: Optional[int]) -> Dict[str, Any]:
    """
    Apply scoring logic and year logic:
    - compute similarity
    - compute candidate_earliest_year
    - apply rules:
        1) score >= 0.95 -> accept
        2) score >= 0.88 AND year_condition -> accept
        3) score >= 0.92 AND candidate contains orig -> accept
        4) else -> fallback best
    Additional rules:
      - year_condition uses candidate_earliest_year <= input_year
      - if multiple high-similarity candidates -> year has stronger weight
    Returns chosen candidate dict with added fields: score, earliest_year
    """
    if not candidates:
        return {}

    scored = []
    for c in candidates:
        txt = c.get("text", "") or ""
        score = similarity(orig_title, txt)
        # boost when orig is substring of candidate (normalized)
        if normalized_title(orig_title) in normalized_title(txt):
            score = max(score, score + 0.02)
        years = c.get("years") or []
        if isinstance(years, list):
            earliest = min(years) if years else None
        else:
            earliest = years if years else None
        c_copy = dict(c)
        c_copy["score"] = float(score)
        c_copy["earliest_year"] = int(earliest) if earliest else None
        scored.append(c_copy)

    # sort by score desc then earliest_year asc (None last)
    def sort_key(x):
        return (x["score"], -1 * (x["earliest_year"] or 9999))
    scored.sort(key=lambda x: (x["score"], - (x["earliest_year"] or 0)), reverse=True)

    # find counts of high-scoring
    high_similar = [c for c in scored if c["score"] >= 0.80]
    ambiguous = len(high_similar) > 1

    # Now apply acceptance rules, iterate top candidates
    best = scored[0]
    for cand in scored:
        s = cand["score"]
        ey = cand.get("earliest_year")
        # Rule 1
        if s >= SIM_THRESH_EXACT:
            cand["reason"] = "score>=SIM_THRESH_EXACT"
            return cand
        # Rule 2: year-based acceptance (with ambiguous consideration)
        if s >= SIM_THRESH_YEAR and ey is not None and input_year is not None:
            # only apply strongly if ambiguous or candidate text looks like 'orig: subtitle' or parts
            looks_like_suffix = (normalized_title(orig_title) in normalized_title(cand.get("text", "")) and ':' in cand.get("text", ""))
            if ambiguous or looks_like_suffix:
                if ey <= input_year:
                    cand["reason"] = "score>=SIM_THRESH_YEAR_and_year_ok_and_ambiguous"
                    return cand
        # Rule 3: substring containment
        if s >= SIM_THRESH_SUBSTR and normalized_title(orig_title) in normalized_title(cand.get("text", "")):
            cand["reason"] = "score>=SIM_THRESH_SUBSTR_and_contains"
            return cand

    # If none matched by rules, fallback: best candidate (tie-breaking by earliest year)
    # handle tie break: if top two within 0.02, pick earliest year
    if len(scored) >= 2:
        top = scored[0]
        second = scored[1]
        if top["score"] - second["score"] < 0.02:
            # pick one with earliest year if available
            top_ey = top.get("earliest_year") or 9999
            sec_ey = second.get("earliest_year") or 9999
            chosen = top if top_ey <= sec_ey else second
            chosen["reason"] = "fallback_tie_earliest"
            return chosen

    best["reason"] = "fallback_best"
    return best

# ------------------------
# Page-level parsing (final page)
# ------------------------
def parse_time_blocks_from_game_page(html_content: str) -> Dict[str, Any]:
    """
    Heuristic parse for times: looks for known labels like Main Story, Main + Extras, Completionist, All Playstyles, Co-Op, Vs.
    Returns dict with keys ms,mpe,comp,all,coop,vs (values or "N/A")
    """
    res = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "all": "N/A", "coop": "N/A", "vs": "N/A"}
    # Normalize text
    text = re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', html_content)).strip()
    # Look for patterns like "Main Story: 10 Hours" or "Main + Extras: 12 Hours"
    patterns = {
        "ms": r'(Main Story|Main story)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
        "mpe": r'(Main \+ Extras|Main \+ extras)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
        "comp": r'(Completionist|Completionist)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
        "all": r'(All Playstyles|All playstyles|All Playstyles|All)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
        "coop": r'(Co-Op|Co-op|Co op)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
        "vs": r'(Vs\.|Vs|Versus)[\s\:\-]*([0-9]+(?:\.[0-9]+)?\s*Hours?)',
    }
    for key, pat in patterns.items():
        m = re.search(pat, html_content, flags=re.IGNORECASE)
        if m:
            res[key] = m.group(2).strip()
    # fallback: broad search for "Hours"
    hours = re.findall(r'([0-9]+(?:\.[0-9]+)?\s*Hours?)', html_content, flags=re.IGNORECASE)
    if hours and res["ms"] == "N/A":
        res["ms"] = hours[0]
    return res

def extract_earliest_year_from_page(html_content: str) -> Optional[int]:
    years = extract_years_from_text(html_content)
    return min(years) if years else None

# ------------------------
# Debug save helper
# ------------------------
def save_debug(name: str, data: Any):
    if not DEBUG:
        return
    ensure_dirs()
    safe = sanitize_filename(name)
    path = os.path.join(DEBUG_DIR, safe)
    try:
        if isinstance(data, (dict, list)):
            write_json_safe(path, data)
        else:
            mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
            with open(path, mode, encoding="utf-8" if mode == "w" else None) as f:
                f.write(data)
        print(f"[{ts_str()}] 🗂️ Saved debug dump: {path}")
    except Exception as e:
        print(f"[{ts_str()}] ❌ Failed to save debug dump {path}: {e}")

# ------------------------
# Core per-game processing
# ------------------------
def process_game(browser, game: Dict[str, Any], index: int, total: int) -> Dict[str, Any]:
    title = game.get("title") or ""
    year = game.get("year")
    display = f"{title} ({year})" if year else title
    print(f"[{ts_str()}] 🎮 Processing {index}/{total}: {display}")

    result = {"input_title": title, "input_year": year, "matched": None, "hltb": {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A","coop":"N/A","vs":"N/A"}, "notes": []}
    ctx = None
    page = None
    try:
        context = browser.new_context()
        page = context.new_page()
        variants = generate_search_variants(title, year)
        # iterate variants in order
        all_candidates = []
        chosen = None

        for variant in variants:
            attempt = 0
            # small random sleep before each variant (to avoid too aggressive hits)
            time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            while attempt < MAX_ATTEMPTS_PER_VARIANT:
                attempt += 1
                try:
                    q = quote(variant, safe='')
                    search_url = f"{BASE_URL}/?q={q}"
                    print(f"[{ts_str()}] 🔍 Searching variant: '{variant}' (attempt {attempt}/{MAX_ATTEMPTS_PER_VARIANT})")
                    try:
                        page.goto(search_url, wait_until="load", timeout=PAGE_GOTO_TIMEOUT_MS)
                    except PlaywrightTimeoutError:
                        print(f"[{ts_str()}] ⚠️ page.goto timeout for {search_url} (continuing with content())")
                    except Exception as e:
                        print(f"[{ts_str()}] ⚠️ navigation error: {e}")

                    candidates = []
                    # try to collect anchors using selector first
                    try:
                        page.wait_for_selector('a[href^="/game/"]', timeout=WAIT_SELECTOR_TIMEOUT_MS)
                        anchors = page.query_selector_all('a[href^="/game/"]')
                        for a in anchors:
                            try:
                                href = a.get_attribute("href") or ""
                                # attempt to capture parent block text for more context (year/subtitle)
                                parent = a.evaluate("node => node.closest('.search_list_row') ? node.closest('.search_list_row').innerText : node.parentElement.innerText")
                                text = a.inner_text().strip()
                                # fallback parent text if text shorter
                                text_full = parent.strip() if parent and isinstance(parent, str) and len(parent.strip()) > len(text) else text
                                years = extract_years_from_text(text_full)
                                candidates.append({"text": text_full, "href": href, "years": years})
                            except Exception:
                                continue
                    except PlaywrightTimeoutError:
                        # fallback: parse content
                        html = page.content()
                        if DEBUG:
                            save_debug(f"{index}_{title}_search_html_{now_ts()}.html", html)
                        parsed = extract_candidates_from_html(html)
                        candidates.extend(parsed)
                    except Exception as e:
                        # other errors: fallback to content parse
                        try:
                            html = page.content()
                            if DEBUG:
                                save_debug(f"{index}_{title}_search_html_{now_ts()}.html", html)
                            parsed = extract_candidates_from_html(html)
                            candidates.extend(parsed)
                        except Exception as e2:
                            print(f"[{ts_str()}] ⚠️ failed to parse search page: {e2}")

                    # dedupe candidates by href
                    seen = set()
                    uniq = []
                    for c in candidates:
                        if not c.get("href"):
                            continue
                        if c["href"] not in seen:
                            uniq.append(c)
                            seen.add(c["href"])
                    candidates = uniq

                    all_candidates.extend(candidates)

                    # Save candidate dump if empty or suspect
                    best_est = None
                    if candidates:
                        best_est = choose_best_candidate(candidates, title, year)
                    else:
                        print(f"[{ts_str()}] 🔎 Candidates on page: 0")
                    # Minimal debug dumps if problematic
                    if (not candidates) or (best_est and best_est.get("score", 0) < 0.80):
                        if DEBUG:
                            save_debug(f"{index}_{title}_candidates_{now_ts()}.json", candidates)
                            # screenshot as bytes
                            try:
                                png = page.screenshot()
                                save_debug(f"{index}_{title}_search_{now_ts()}.png", png)
                            except Exception as e:
                                print(f"[{ts_str()}] ⚠️ screenshot failed: {e}")

                    # If we have candidates, evaluate best
                    if candidates:
                        chosen_candidate = choose_best_candidate(candidates, title, year)
                        print(f"[{ts_str()}] 🏁 Best candidate for variant '{variant}': '{chosen_candidate.get('text')}' score={chosen_candidate.get('score'):.2f} reason={chosen_candidate.get('reason')}")
                        # Accept candidate if rules matched (choose_best_candidate returns reason)
                        if chosen_candidate.get("reason") and chosen_candidate.get("reason") != "fallback_best":
                            chosen = chosen_candidate
                            # break outer loops
                            break
                        # else we may want to accept fallback only if this is last variant
                        # but we'll continue trying other variants to get stronger match
                    # attempt loop end
                except Exception as e:
                    print(f"[{ts_str()}] ❌ Error during search variant '{variant}': {e}")
                    traceback.print_exc()
                # retry small backoff
                if attempt < MAX_ATTEMPTS_PER_VARIANT:
                    time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            if chosen:
                break

        # If not chosen by strict rules, but we have some candidates overall -> take best fallback
        if not chosen and all_candidates:
            chosen = choose_best_candidate(all_candidates, title, year)
            print(f"[{ts_str()}] ⚠️ No strong candidate; using fallback best: '{chosen.get('text')}' score={chosen.get('score'):.2f}")

        if chosen:
            href = chosen.get("href")
            if not href.startswith("/"):
                href = "/" + href
            game_url = BASE_URL + href
            # open final page
            try:
                page.goto(game_url, wait_until="load", timeout=PAGE_GOTO_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                print(f"[{ts_str()}] ⚠️ page.goto timeout for final page {game_url} (continuing with content)")
            try:
                final_html = page.content()
            except Exception:
                final_html = ""
            # parse earliest year from page if available
            earliest = extract_earliest_year_from_page(final_html)
            if earliest:
                chosen["earliest_from_page"] = earliest
            # parse times
            times = parse_time_blocks_from_game_page(final_html)
            result["hltb"].update(times)
            result["matched"] = {"title": chosen.get("text"), "href": href, "score": chosen.get("score"), "earliest_year": chosen.get("earliest_year"), "reason": chosen.get("reason")}
            # Save final page only when questionable or debug
            if DEBUG or (chosen.get("score", 0) < 0.90):
                save_debug(f"{index}_{title}_final_{now_ts()}.html", final_html)
            print(f"[{ts_str()}] ✅ Data found for '{title}' -> matched '{chosen.get('text')}' (score {chosen.get('score'):.2f})")
        else:
            print(f"[{ts_str()}] ⚠️ Data not found for: {title} - recorded as N/A")
            result["notes"].append("No candidate found")
    except Exception as e:
        print(f"[{ts_str()}] ❌ Unexpected error processing {title}: {e}")
        traceback.print_exc()
        result["notes"].append(f"error: {str(e)}")
    finally:
        try:
            if page:
                page.close()
        except Exception:
            pass
    return result

# ------------------------
# Counting and reporting
# ------------------------
def count_hltb_data(results: List[Dict[str, Any]]):
    counts = {"total": len(results), "with_data": 0, "no_data": 0}
    for r in results:
        if r.get("matched"):
            counts["with_data"] += 1
        else:
            counts["no_data"] += 1
    return counts, counts["with_data"], counts["no_data"]

# ------------------------
# Git commit / push logic (safe)
# ------------------------
def safe_commit_and_push(files_to_add: List[str], commit_msg: str):
    """
    Add, commit, merge origin/main with -X ours strategy, push.
    If push fails, create fallback branch and push it.
    """
    try:
        # configure
        name = "hltb-worker"
        email = "hltb-worker@users.noreply.github.com"
        subprocess.run(["git", "config", "user.name", name], check=False)
        subprocess.run(["git", "config", "user.email", email], check=False)

        # fetch and reset to remote main to keep repo clean
        subprocess.run(["git", "fetch", "origin", "main"], check=False)
        subprocess.run(["git", "checkout", "main"], check=False)
        subprocess.run(["git", "reset", "--hard", "origin/main"], check=False)

        # add files
        for f in files_to_add:
            subprocess.run(["git", "add", f], check=False)

        # if nothing to commit, skip
        check = subprocess.run(["git", "diff", "--cached", "--quiet"], check=False)
        if check.returncode == 0:
            print(f"[{ts_str()}] ℹ️ No changes to commit.")
            return True

        subprocess.run(["git", "commit", "-m", commit_msg], check=False)
        # merge remote with our strategy for conflicts
        subprocess.run(["git", "fetch", "origin", "main"], check=False)
        subprocess.run(["git", "merge", "--no-edit", "-s", "recursive", "-X", "ours", "origin/main"], check=False)
        subprocess.run(["git", "add", f], check=False)
        subprocess.run(["git", "commit", "-m", commit_msg], check=False)
        # push
        push = subprocess.run(["git", "push", "origin", "HEAD:main"], check=False)
        if push.returncode == 0:
            print(f"[{ts_str()}] ✅ Pushed results to main.")
            return True
        else:
            # fallback branch
            branch = f"hltb-worker-fallback-{now_ts()}"
            subprocess.run(["git", "checkout", "-b", branch], check=False)
            subprocess.run(["git", "push", "origin", branch], check=False)
            print(f"[{ts_str()}] ⚠️ Push failed; pushed fallback branch {branch}")
            return False
    except Exception as e:
        print(f"[{ts_str()}] ❌ Git push error: {e}")
        return False

# ------------------------
# Main
# ------------------------
def main():
    print(f"[{ts_str()}] 🚀 Starting HLTB Worker")
    ensure_dirs()
    if not os.path.exists(GAMES_LIST_FILE):
        print(f"[{ts_str()}] ❌ {GAMES_LIST_FILE} not found. Place your index111.html in repo root.")
        return

    try:
        games = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        print(f"[{ts_str()}] ❌ Failed to extract games list: {e}")
        return

    total = len(games)
    print(f"[{ts_str()}] 📄 Extracted {total} games")

    # load existing results (to be idempotent)
    results = []
    if os.path.exists(OUTPUT_FILE):
        try:
            results = json.load(open(OUTPUT_FILE, "r", encoding="utf-8"))
        except Exception:
            print(f"[{ts_str()}] ⚠️ Could not read existing output file; starting fresh.")
            results = []

    processed_keys = set((r.get("input_title"), r.get("input_year")) for r in results)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            for idx, g in enumerate(games, start=1):
                key = (g.get("title"), g.get("year"))
                if key in processed_keys:
                    print(f"[{ts_str()}] ℹ️ Skipping {g.get('title')} - already processed")
                    continue
                res = process_game(browser, g, idx, total)
                results.append(res)
                # write progressively
                try:
                    write_json_safe(OUTPUT_FILE, results)
                except Exception as e:
                    print(f"[{ts_str()}] ❌ Failed to write output file: {e}")
                # occasional long pause
                if idx % LONG_PAUSE_EVERY == 0 and idx != 0:
                    print(f"[{ts_str()}] 💤 Long pause to avoid throttling...")
                    time.sleep(random.uniform(LONG_PAUSE_MIN, LONG_PAUSE_MAX))
        finally:
            try:
                browser.close()
            except Exception:
                pass

    counts, with_data, no_data = count_hltb_data(results)
    report = {
        "updated_at": ts_str(),
        "total_input": total,
        "total_processed": len(results),
        "with_data": with_data,
        "no_data": no_data,
        "counts": counts
    }

    try:
        write_json_safe(OUTPUT_FILE, results)
        write_json_safe(REPORT_FILE, report)
        print(f"[{ts_str()}] 🎉 Done — processed {len(results)}/{total} games. with_data={with_data}, no_data={no_data}")
    except Exception as e:
        print(f"[{ts_str()}] ❌ Failed to write final output/report: {e}")

    # Commit & push results if configured
    if COMMIT_RESULTS:
        print(f"[{ts_str()}] 🔧 COMMIT_RESULTS enabled — attempting safe git push.")
        files = []
        # include both output and report
        if os.path.exists(OUTPUT_FILE):
            files.append(OUTPUT_FILE)
        if os.path.exists(REPORT_FILE):
            files.append(REPORT_FILE)
        commit_msg = f"HLTB worker update {ts_str()}"
        ok = safe_commit_and_push(files, commit_msg)
        if not ok:
            print(f"[{ts_str()}] ⚠️ Commit/push did not complete cleanly. Check workflow artifacts or fallback branch.")
    else:
        print(f"[{ts_str()}] ℹ️ COMMIT_RESULTS disabled — results saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
