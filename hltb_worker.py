#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hltb_worker.py
Robust HowLongToBeat scraper/worker with:
 - robust gamesList extraction from index111.html
 - candidate extraction via page.evaluate
 - improved title matching (slashes, &, roman/ar, year bonus)
 - updated page parsing for GameStats blocks + text heuristics
 - sanitized debug dumps (filenames safe for NTFS/upload-artifact)
 - per-run output and atomic commit via GitHub API (retry/backoff)
 - extended logging and dumps (html, screenshot, candidates, scores, game_html)
"""
from __future__ import annotations
import os
import re
import json
import time
import base64
import random
import logging
import requests
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright, Error as PlaywrightError
except Exception as e:
    raise SystemExit(f"Playwright import failed: {e}. Ensure 'pip install playwright' and 'python -m playwright install chromium' performed.")

# ---------------- CONFIG ----------------
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
CANONICAL_OUTPUT_PATH = f"{OUTPUT_DIR}/hltb_data.json"
PER_RUN_FILENAME_TEMPLATE = f"{OUTPUT_DIR}/hltb_data_{{runid}}.json"
DEBUG_DIR = "debug_dumps"
STORAGE_STATE = "playwright_storage.json"

# Playwright timeouts (ms)
PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

# Rate control (tunable)
DELAY_ATTEMPTS = [(0.4, 0.9), (1.5, 2.5), (5.0, 8.0)]
MAX_ATTEMPTS = 3

# Matching thresholds
MIN_ACCEPT_SCORE = 0.6
HIGH_SCORE = 0.95

# GitHub API commit settings
GITHUB_API_MAX_ATTEMPTS = 8
GITHUB_API_INITIAL_BACKOFF = 1.0

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("hltb_worker")

# ---------------- Ensure directories ----------------
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

# ---------------- Filename sanitization ----------------
_INVALID_FILENAME_RE = re.compile(r'[\"<>:\|\*\?\r\n/\\]+')
def sanitize_filename(name: str, max_len: int = 120) -> str:
    if not name:
        return "file"
    s = str(name)
    s = _INVALID_FILENAME_RE.sub('_', s)
    s = re.sub(r'[_\s]+', '_', s).strip('_')
    if len(s) > max_len:
        s = s[:max_len]
    if not s:
        s = "file"
    return s

# ---------------- gamesList extraction ----------------
def extract_games_list(html_file: str) -> List[Dict]:
    if not os.path.exists(html_file):
        raise FileNotFoundError(html_file)
    with open(html_file, "r", encoding="utf-8") as f:
        content = f.read()

    markers = ["const gamesList =", "var gamesList =", "let gamesList ="]
    pos = -1
    for m in markers:
        pos = content.find(m)
        if pos != -1:
            break
    if pos == -1:
        raise ValueError("Не найден gamesList в index111.html")

    start = content.find("[", pos)
    if start == -1:
        raise ValueError("Не найден '[' после gamesList")

    # find matching closing bracket
    bracket = 0
    end = None
    for i, ch in enumerate(content[start:], start):
        if ch == "[":
            bracket += 1
        elif ch == "]":
            bracket -= 1
            if bracket == 0:
                end = i + 1
                break
    if end is None:
        raw_path = os.path.join(DEBUG_DIR, f"gameslist_raw_unclosed_{int(time.time())}.txt")
        with open(raw_path, "w", encoding="utf-8") as rf:
            rf.write(content[start:start+20000])
        raise ValueError(f"Не найдена закрывающая ']' для gamesList. Дамп сохранён: {raw_path}")

    js_array = content[start:end]
    raw_path = os.path.join(DEBUG_DIR, f"gameslist_raw_{int(time.time())}.js")
    with open(raw_path, "w", encoding="utf-8") as rf:
        rf.write(js_array)
    log.info(f"📝 saved raw gamesList to {raw_path}")

    # simple cleaning: remove trailing commas, replace undefined/NaN
    fixed = js_array
    fixed = re.sub(r',\s*(?=[}\]])', '', fixed)
    fixed = re.sub(r'\bundefined\b', 'null', fixed)
    fixed = re.sub(r'\bNaN\b', 'null', fixed)
    fixed_path = os.path.join(DEBUG_DIR, f"gameslist_fixed_{int(time.time())}.json")
    with open(fixed_path, "w", encoding="utf-8") as ff:
        ff.write(fixed)
    log.info(f"📝 saved fixed gamesList to {fixed_path}")

    try:
        data = json.loads(fixed)
        log.info(f"✅ gamesList parsed with {len(data)} items")
        return data
    except Exception as e:
        log.error(f"❌ Failed to parse gamesList: {e}")
        raise

# ---------------- title normalization & similarity ----------------
def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = s.replace('\u2018','\'').replace('\u2019','\'').replace('\u201c','"').replace('\u201d','"')
    s = re.sub(r'[^0-9a-zа-яё\s\-/&]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def clean_title(s: str) -> str:
    s = normalize_title(s)
    s = s.replace('&', ' and ')
    s = s.replace('/', ' ')
    s = re.sub(r'\band\b', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def convert_arabic_to_roman(num_str):
    try:
        n = int(num_str)
    except:
        return None
    vals = [
        (1000, 'M'), (900, 'CM'), (500, 'D'), (400, 'CD'),
        (100, 'C'), (90, 'XC'), (50, 'L'), (40, 'XL'),
        (10, 'X'), (9, 'IX'), (5, 'V'), (4, 'IV'), (1, 'I')
    ]
    res = ""
    for v, r in vals:
        while n >= v:
            res += r
            n -= v
    return res

def convert_roman_to_arabic(rom):
    rom = rom.upper()
    vals = {'M':1000,'D':500,'C':100,'L':50,'X':10,'V':5,'I':1}
    total = 0
    prev = 0
    for ch in rom[::-1]:
        val = vals.get(ch, 0)
        if val < prev:
            total -= val
        else:
            total += val
        prev = val
    return str(total)

def calculate_similarity(original: str, candidate: str) -> float:
    if not original or not candidate:
        return 0.0
    orig = clean_title(original)
    cand = clean_title(candidate)
    if not orig or not cand:
        return 0.0
    if orig == cand:
        return 1.0
    # handle slash parts
    if '/' in original:
        parts = [p.strip() for p in original.split('/')]
        for p in parts:
            if clean_title(p) == cand:
                return 1.0
    # base before colon
    base = original.split(':',1)[0].strip()
    if clean_title(base) == cand:
        return 0.95
    # roman/arabic conversions
    arabic_match = re.search(r'\b(\d{1,3})\b', original)
    if arabic_match:
        roman = convert_arabic_to_roman(arabic_match.group(1))
        if roman and clean_title(re.sub(r'\b' + re.escape(arabic_match.group(1)) + r'\b', roman, original)) == cand:
            return 0.9
    roman_match = re.search(r'\b(I{1,4}|V|VI{0,3}|IX|X)\b', original, flags=re.IGNORECASE)
    if roman_match:
        arab = convert_roman_to_arabic(roman_match.group(1))
        if arab and clean_title(re.sub(r'\b' + re.escape(roman_match.group(1)) + r'\b', arab, original, flags=re.IGNORECASE)) == cand:
            return 0.9
    # token overlap + LCS tokens
    a_tokens = orig.split()
    b_tokens = cand.split()
    if not a_tokens or not b_tokens:
        return 0.0
    common = set(a_tokens).intersection(set(b_tokens))
    precision = len(common) / len(b_tokens)
    recall = len(common) / len(a_tokens)
    # LCS on tokens
    n, m = len(a_tokens), len(b_tokens)
    dp = [[0]*(m+1) for _ in range(n+1)]
    for i in range(1, n+1):
        for j in range(1, m+1):
            if a_tokens[i-1] == b_tokens[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])
    lcs_len = dp[n][m]
    seq = lcs_len / max(1, n)
    score = 0.6*recall + 0.25*precision + 0.15*seq
    return max(0.0, min(1.0, score))

# ---------------- extract candidates from search page ----------------
def extract_candidates_from_page(page, max_candidates:int=200) -> List[Dict]:
    """
    Use page.evaluate to extract anchors with /game/ hrefs and try to capture a year if present.
    Returns list of dicts {href, text, year, context}
    """
    try:
        js = """
        () => {
            const anchors = Array.from(document.querySelectorAll('a[href^="/game/"]'));
            const out = [];
            for (const a of anchors) {
                const href = a.getAttribute('href') || '';
                let text = (a.innerText || '').trim();
                // try sibling context
                let ctx = '';
                if (a.closest('.search_list_row') && a.closest('.search_list_row').innerText) ctx = a.closest('.search_list_row').innerText;
                else if (a.parentElement && a.parentElement.innerText) ctx = a.parentElement.innerText;
                // try find year in ctx or text
                const m = (ctx + ' ' + text).match(/(19\\d{2}|20\\d{2})/);
                const year = m ? m[0] : null;
                out.push({href, text, year, context: ctx});
            }
            return out;
        }
        """
        raw = page.evaluate(js)
        filtered = []
        seen = set()
        for r in raw:
            href = r.get('href') or ''
            txt = r.get('text') or ''
            ctx = r.get('context') or ''
            yr = r.get('year')
            key = (href, txt)
            if href and key not in seen:
                seen.add(key)
                filtered.append({"href": href, "text": txt.strip(), "year": yr, "context": ctx.strip()})
        return filtered[:max_candidates]
    except Exception as e:
        log.warning(f"extract_candidates_from_page exception: {e}")
        return []

# ---------------- helper: parse time strings ----------------
def normalize_time_string(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    s = s.replace('½', '.5')
    m = re.search(r'(\d+(?:\.\d+)?)\s*(hours|hour|hrs|hr|h)\b', s, flags=re.I)
    if m:
        val = float(m.group(1))
        if val.is_integer():
            return f"{int(val)}h"
        else:
            return f"{val:.1f}h"
    m2 = re.search(r'(\d+)\s*(mins|min|m)\b', s, flags=re.I)
    if m2:
        return f"{int(m2.group(1))}m"
    m3 = re.search(r'(\d+(?:\.\d+)?)', s)
    if m3:
        val = float(m3.group(1))
        if val > 10:
            return f"{int(val)}h"
        else:
            return f"{val}?"
    return None

# ---------------- extract hltb times from a game page ----------------
def extract_hltb_data_from_page(page) -> Optional[Dict]:
    """
    Try several strategies:
     1) search for elements with classes containing 'GameStats' or 'game_time'
     2) search for text blocks containing 'Main Story', 'Main + Extras', 'Completionist', 'Vs.'
     3) try small table-like structures
    Returns dict or None
    """
    try:
        selectors = [
            'div[class*="GameStats"]',
            'div[class*="game_times"]',
            'div[class*="game_time"]',
            'table',
            'div[class*="gameTimes"]'
        ]
        page_content = ""
        data = {}
        for sel in selectors:
            try:
                elems = page.query_selector_all(sel)
                for eindex in range(len(elems)):
                    try:
                        txt = elems[eindex].inner_text().strip()
                        page_content += "\n\n" + txt
                    except Exception:
                        continue
            except Exception:
                continue

        if not page_content:
            try:
                page_content = page.inner_text("body")
            except Exception:
                page_content = page.content()

        text = page_content.lower()

        patterns = {
            "ms": r'(main story|main)\s*[:\-–]?\s*([0-9]+(?:\.\d+)?(?:½)?\s*(hours|hour|hrs|hr|h|mins|min|m))',
            "mpe": r'(main\s*\+\s*extras|main \+ extras)\s*[:\-–]?\s*([0-9]+(?:\.\d+)?(?:½)?\s*(hours|hour|h|mins|min|m))',
            "comp": r'(completionist|completion)\s*[:\-–]?\s*([0-9]+(?:\.\d+)?(?:½)?\s*(hours|hour|h|mins|min|m))',
            "vs": r'(vs\.|versus)\s*[:\-–]?\s*([0-9]+(?:\.\d+)?(?:½)?\s*(hours|hour|h|mins|min|m))',
            "all": r'(all styles|all)\s*[:\-–]?\s*([0-9]+(?:\.\d+)?(?:½)?\s*(hours|hour|h|mins|min|m))'
        }
        for k, p in patterns.items():
            m = re.search(p, page_content, flags=re.I)
            if m:
                val = normalize_time_string(m.group(2))
                if val:
                    data[k] = val

        if not data:
            lines = [ln.strip() for ln in page_content.splitlines() if ln.strip()]
            for ln in lines:
                low = ln.lower()
                if 'main story' in low or (('main' in low) and ('story' in low or 'single-player' in low)):
                    m = re.search(r'([0-9]+(?:\.\d+)?(?:½)?\s*(?:hours|hour|hrs|hr|h|mins|min|m))', ln, flags=re.I)
                    if m:
                        data['ms'] = normalize_time_string(m.group(1))
                if 'main' in low and '+' in ln:
                    m = re.search(r'([0-9]+(?:\.\d+)?(?:½)?\s*(?:hours|hour|hrs|hr|h|mins|min|m))', ln, flags=re.I)
                    if m:
                        data['mpe'] = normalize_time_string(m.group(1))
                if 'completionist' in low or 'completion' in low:
                    m = re.search(r'([0-9]+(?:\.\d+)?(?:½)?\s*(?:hours|hour|hrs|hr|h|mins|min|m))', ln, flags=re.I)
                    if m:
                        data['comp'] = normalize_time_string(m.group(1))
                if 'vs' in low or 'versus' in low:
                    m = re.search(r'([0-9]+(?:\.\d+)?(?:½)?\s*(?:hours|hour|hrs|hr|h|mins|min|m))', ln, flags=re.I)
                    if m:
                        data['vs'] = normalize_time_string(m.group(1))

        try:
            m = re.search(r'var\s+gameData\s*=\s*(\{.*?\});', page.content(), flags=re.S)
            if m:
                try:
                    gd = json.loads(m.group(1))
                    for k in ['ms','mpe','comp','vs','all']:
                        if k in gd and gd[k]:
                            data.setdefault(k, gd[k])
                except Exception:
                    pass
        except Exception:
            pass

        if not data:
            ts = int(time.time())
            ghtml_path = os.path.join(DEBUG_DIR, sanitize_filename(f"game_html_{ts}.html"))
            try:
                with open(ghtml_path, "w", encoding="utf-8") as f:
                    f.write(page.content())
                log.info(f"📝 saved raw game html to {ghtml_path}")
            except Exception as e:
                log.debug(f"failed to save game html: {e}")
            return None

        return data
    except Exception as e:
        log.exception(f"extract_hltb_data_from_page exception: {e}")
        return None

# ---------------- scrape candidates + rank ----------------
def find_best_candidate(candidates: List[Dict], original_title: str, expected_year: Optional[int]=None) -> Tuple[Optional[Dict], float]:
    if not candidates:
        return None, 0.0
    orig_clean = clean_title(original_title)
    for c in candidates:
        cand_clean = clean_title(c.get("text",""))
        if cand_clean == orig_clean:
            return c, 1.0
    best = None
    best_score = -1.0
    for c in candidates:
        score = calculate_similarity(original_title, c.get("text",""))
        try:
            cy = c.get("year")
            if expected_year and cy:
                if int(cy) == int(expected_year):
                    score = max(score, 0.9)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best = c
    if best_score < 0.2:
        return None, best_score
    return best, best_score

# ---------------- search attempt logic ----------------
def dump_search_state(page, prefix):
    safe = sanitize_filename(prefix)
    ts = int(time.time())
    try:
        html_path = os.path.join(DEBUG_DIR, f"{safe}_search_html_{ts}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        log.info(f"📝 saved html dump: {html_path}")
    except Exception as e:
        log.debug(f"failed to save html dump: {e}")
    try:
        shot_path = os.path.join(DEBUG_DIR, f"{safe}_screenshot_{ts}.png")
        page.screenshot(path=shot_path, full_page=True)
        log.info(f"📸 saved screenshot: {shot_path}")
    except Exception as e:
        log.debug(f"failed to save screenshot: {e}")
    try:
        cands = extract_candidates_from_page(page)
        cand_path = os.path.join(DEBUG_DIR, f"{safe}_candidates_{ts}.json")
        with open(cand_path, "w", encoding="utf-8") as f:
            json.dump(cands, f, ensure_ascii=False, indent=2)
        log.info(f"🗂 saved candidates: {cand_path}")
    except Exception as e:
        log.debug(f"failed to save candidates dump: {e}")

def search_game_single_attempt(page, title: str, year: Optional[int], idx_info: Dict) -> Tuple[Optional[Dict], Optional[str]]:
    """
    returns (hltb_data, matched_title) on success; (None, "blocked") if blocked; (None, None) otherwise
    """
    variants = []
    variants.append(title)
    variants.append(title.replace('&','and'))
    variants.append(re.sub(r'\(.*?\)','', title).strip())
    if ':' in title:
        variants.append(title.split(':',1)[0].strip())
    if '/' in title:
        parts = [p.strip() for p in title.split('/')]
        if len(parts) >= 2:
            variants.append(parts[0] + " and " + parts[1])
            variants.extend(parts[:3])
    if year:
        variants.append(f"{title} {year}")
    seen = set()
    variants_clean = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            variants_clean.append(v)

    for variant in variants_clean:
        safe_variant = sanitize_filename(variant)
        log.info(f"🔍 Searching variant: '{variant}'")
        try:
            search_url = f"{BASE_URL}/?q={quote(variant, safe='')}"
            try:
                page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
                try:
                    page.wait_for_selector('a[href^=\"/game/\"]', timeout=3500)
                except Exception:
                    pass
            except PlaywrightError as e:
                log.warning(f"Page.goto error: {e}")

            content = page.content().lower()
            if any(x in content for x in ["checking your browser", "access denied", "are you human", "captcha", "cloudflare"]):
                log.warning("🚫 Block detected on search page")
                dump_search_state(page, f"{idx_info.get('index','')}_{safe_variant}")
                return None, "blocked"

            candidates = extract_candidates_from_page(page)
            tsuffix = int(time.time())
            cand_path = os.path.join(DEBUG_DIR, sanitize_filename(f"{idx_info.get('index')}_{title}_candidates_{tsuffix}.json"))
            try:
                with open(cand_path, "w", encoding="utf-8") as f:
                    json.dump(candidates, f, ensure_ascii=False, indent=2)
            except Exception:
                pass

            if not candidates:
                log.info("⚠️ No candidates found for variant; continue")
                try:
                    page.reload(timeout=PAGE_GOTO_TIMEOUT)
                    time.sleep(random.uniform(0.3,0.9))
                    candidates = extract_candidates_from_page(page)
                except Exception:
                    pass
                if not candidates:
                    dump_search_state(page, f"{idx_info.get('index')}_{safe_variant}_empty")
                    continue

            if len(candidates) > 30 and year:
                log.info(f"⚠️ Too many results ({len(candidates)}), retrying search with year {year}")
                try:
                    page.goto(f"{BASE_URL}/?q={quote(variant + ' ' + str(year), safe='')}", timeout=PAGE_GOTO_TIMEOUT)
                    time.sleep(0.8)
                    candidates = extract_candidates_from_page(page)
                except Exception:
                    pass

            best_cand, score = find_best_candidate(candidates, title, expected_year=year)
            log.info(f"🔎 Best candidate: {best_cand.get('text') if best_cand else None} (score={score:.2f})")

            if not best_cand:
                continue

            href = best_cand.get("href")
            if not href:
                continue
            full_url = f"{BASE_URL}{href}"
            try:
                page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT)
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                except Exception:
                    pass
            except PlaywrightError as e:
                log.warning(f"Error opening candidate page: {e}")
                dump_search_state(page, f"{idx_info.get('index')}_{sanitize_filename(best_cand.get('text') or 'candidate')}_open_err")
                continue

            pcontent = page.content().lower()
            if any(x in pcontent for x in ["checking your browser", "access denied", "captcha", "cloudflare"]):
                log.warning("🚫 Block detected on game page")
                dump_search_state(page, f"{idx_info.get('index')}_{sanitize_filename(best_cand.get('text') or 'candidate')}_blocked")
                return None, "blocked"

            hltb_data = extract_hltb_data_from_page(page)
            if hltb_data:
                return hltb_data, best_cand.get("text")
            else:
                log.info("⚠️ No times extracted from candidate page; dumping and continuing")
                dump_search_state(page, f"{idx_info.get('index')}_{sanitize_filename(best_cand.get('text') or 'candidate')}_no_times")
                if score >= HIGH_SCORE:
                    return {"note":"page_opened_but_no_times_extracted"}, best_cand.get("text")
                continue
        except Exception as e:
            log.exception(f"search_game_single_attempt exception for '{title}': {e}")
            dump_search_state(page, f"{idx_info.get('index')}_{sanitize_filename(title)}_err")
            continue
    return None, None

# ---------------- save per-run and commit via GitHub API ----------------
def save_results_per_run(games_list: List[Dict], runid: str) -> str:
    out_name = PER_RUN_FILENAME_TEMPLATE.format(runid=runid)
    with open(out_name, "w", encoding="utf-8") as f:
        json.dump(games_list, f, ensure_ascii=False, indent=2)
    log.info(f"💾 saved per-run results: {out_name}")
    return out_name

def commit_file_to_github(per_run_file_path: str,
                          repo: Optional[str],
                          token: Optional[str],
                          path_in_repo: str = CANONICAL_OUTPUT_PATH,
                          commit_message: Optional[str] = None) -> bool:
    if not token or not repo:
        log.warning("GITHUB_TOKEN or GITHUB_REPOSITORY not set; skipping commit")
        return False
    api_base = f"https://api.github.com/repos/{repo}/contents/{path_in_repo.lstrip('/')}"
    headers = {"Authorization": f"token {token}", "Accept":"application/vnd.github+json"}
    with open(per_run_file_path, "rb") as f:
        content_b64 = base64.b64encode(f.read()).decode()

    if commit_message is None:
        commit_message = f"HLTB: update data run {os.environ.get('GITHUB_RUN_ID','local')}"

    attempt = 0
    backoff = GITHUB_API_INITIAL_BACKOFF
    while attempt < GITHUB_API_MAX_ATTEMPTS:
        attempt += 1
        try:
            resp = requests.get(api_base, headers=headers, timeout=30)
        except Exception as e:
            log.warning(f"GitHub GET error (attempt {attempt}): {e}")
            time.sleep(backoff); backoff *= 2; continue
        if resp.status_code == 200:
            try:
                sha = resp.json().get("sha")
            except Exception:
                sha = None
        elif resp.status_code == 404:
            sha = None
        else:
            log.warning(f"GitHub GET status {resp.status_code}: {resp.text}")
            time.sleep(backoff); backoff *= 2; continue

        payload = {"message": commit_message, "content": content_b64}
        if sha:
            payload["sha"] = sha
        try:
            put = requests.put(api_base, headers=headers, json=payload, timeout=30)
        except Exception as e:
            log.warning(f"GitHub PUT error (attempt {attempt}): {e}")
            time.sleep(backoff); backoff *= 2; continue

        if put.status_code in (200,201):
            log.info("✅ Successfully updated canonical file via GitHub API")
            return True
        else:
            try:
                jr = put.json()
                msg = jr.get("message","")
            except Exception:
                msg = put.text
            log.warning(f"GitHub PUT status {put.status_code}: {msg}")
            if put.status_code in (409,422) or "sha" in msg.lower() or "conflict" in msg.lower():
                log.info(f"Conflict detected; retry after {backoff}s")
                time.sleep(backoff); backoff *= 2; continue
            if put.status_code == 403 and 'rate limit' in msg.lower():
                log.info(f"Rate limited; sleep {backoff*2}s")
                time.sleep(backoff*2); backoff *= 2; continue
            log.error("Fatal error updating file via GitHub API")
            return False
    log.error("Max attempts reached for GitHub API commit")
    return False

# ---------------- main ----------------
def main():
    ensure_dirs()
    try:
        games_list = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        log.exception(f"Failed to extract games list: {e}")
        return

    total = len(games_list)
    log.info(f"📄 Extracted {total} games from {GAMES_LIST_FILE}")

    runid = os.environ.get("GITHUB_RUN_ID") or datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    processed = 0

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox","--disable-setuid-sandbox"])
        context_kwargs = {"viewport":{"width":1280,"height":900}, "locale":"en-US"}
        if os.path.exists(STORAGE_STATE):
            context_kwargs["storage_state"] = STORAGE_STATE
        context = browser.new_context(**context_kwargs)
        page = context.new_page()

        try:
            page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
            log.info("✅ HowLongToBeat reachable")
            try:
                context.storage_state(path=STORAGE_STATE)
            except Exception:
                pass
        except Exception as e:
            log.warning(f"Landing page open failed: {e}")

        for idx, entry in enumerate(games_list, start=1):
            title = None
            year = None
            try:
                if isinstance(entry, dict):
                    title = entry.get("title") or entry.get("name") or ""
                    year = entry.get("year") or entry.get("releaseYear") or None
                elif isinstance(entry, (list, tuple)):
                    title = entry[0] if entry else ""
                    if len(entry) > 1:
                        year = entry[1]
                else:
                    title = str(entry)
                log.info(f"🎮 Processing {idx}/{total}: {title} ({year})")
                idx_info = {"index": idx, "title": title}

                hltb = None
                matched_title = None
                for attempt in range(MAX_ATTEMPTS):
                    res, status = search_game_single_attempt(page, title, year, idx_info)
                    if status == "blocked":
                        wait = 30 + attempt*15
                        log.warning(f"⏸ blocked detected, sleeping {wait}s before retry")
                        time.sleep(wait)
                        continue
                    if res:
                        hltb = res
                        matched_title = status if isinstance(status, str) else None
                        break
                    else:
                        dmin, dmax = DELAY_ATTEMPTS[min(attempt, len(DELAY_ATTEMPTS)-1)]
                        log.info(f"🔁 Retry attempt {attempt+1} for '{title}', sleeping {dmin:.1f}-{dmax:.1f}s")
                        time.sleep(random.uniform(dmin, dmax))

                if hltb:
                    results.append({"title": title, "year": year, "hltb": hltb})
                    log.info(f"✅ Data found for '{title}'")
                else:
                    results.append({"title": title, "year": year, "hltb": {"ms":"N/A","mpe":"N/A","comp":"N/A","vs":"N/A"}})
                    log.warning(f"⚠️ Data not found for: {title} - recorded N/A")
                processed += 1
            except Exception as e:
                log.exception(f"Error processing {title}: {e}")
                results.append({"title": title or str(entry), "year": year, "hltb": {"ms":"N/A","mpe":"N/A","comp":"N/A","vs":"N/A"}})

        try:
            context.close()
            browser.close()
        except Exception:
            pass

    per_run_path = save_results_per_run(results, runid)

    gh_token = os.environ.get("GITHUB_TOKEN")
    gh_repo = os.environ.get("GITHUB_REPOSITORY")
    if gh_token and gh_repo:
        log.info("🔐 Attempting commit to repository via GitHub API")
        ok = commit_file_to_github(per_run_path, repo=gh_repo, token=gh_token, path_in_repo=CANONICAL_OUTPUT_PATH)
        if ok:
            log.info("✅ Commit via API succeeded")
        else:
            log.error("❌ Commit via API failed; leaving per-run file in artifacts")
    else:
        log.info("ℹ️ GITHUB_TOKEN or GITHUB_REPOSITORY not present - skipping API commit")

    try:
        report = {"timestamp": datetime.utcnow().isoformat(), "processed": processed, "total": total, "per_run": per_run_path}
        with open("scraping_report.json", "w", encoding="utf-8") as rf:
            json.dump(report, rf, ensure_ascii=False, indent=2)
        log.info(f"📊 Report created: processed {processed}/{total}")
    except Exception as e:
        log.warning(f"Could not write scraping_report.json: {e}")

if __name__ == "__main__":
    main()
