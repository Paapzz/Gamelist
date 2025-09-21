#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hltb_worker.py
Полный рабочий скрипт:
- устойчивый парсер index111.html -> games list
- поиск HowLongToBeat (Playwright)
- расширенное логирование + дампы debug_dumps/*
- per-run output hltb_data/hltb_data_<runid>.json
- атомарное обновление hltb_data/hltb_data.json в репозитории через GitHub API (PUT /contents/...)
  с retry/backoff для корректной работы при параллельных воркерах.
Requirements:
  - playwright
  - requests
Run:
  - в GitHub Actions: pip install playwright requests; playwright install --with-deps
  - экспортировать GITHUB_TOKEN и GITHUB_REPOSITORY (Actions делает это автоматически)
"""
from __future__ import annotations
import os
import re
import json
import time
import base64
import random
import logging
from datetime import datetime
from urllib.parse import quote
from difflib import SequenceMatcher
from typing import List, Dict, Optional, Tuple

# optionally requests (preferred) - if not installed, fallback to urllib
try:
    import requests
except Exception:
    requests = None

from playwright.sync_api import sync_playwright, Error as PlaywrightError

# ---------------- CONFIG ----------------
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
CANONICAL_OUTPUT_PATH = "hltb_data/hltb_data.json"  # repo path to update
PER_RUN_FILENAME_TEMPLATE = "hltb_data_{runid}.json"
PROGRESS_FILE = "progress.json"
DEBUG_DIR = "debug_dumps"
STORAGE_STATE = "playwright_storage.json"

# Playwright timeouts (ms)
PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

# Rate control
MIN_DELAY = 0.6
MAX_DELAY = 1.6

# Commit retry config
GITHUB_API_MAX_ATTEMPTS = 8
GITHUB_API_INITIAL_BACKOFF = 1.0

# Debug flags
DEBUG_CANDIDATES = True   # dump candidates scores
DUMP_ON_EMPTY = True      # save html + screenshot + candidates on problematic searches
DEBUG_SCORE_THRESHOLD = 0.95

# Logging
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("hltb_worker")

# ---------------- Utilities ----------------
def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(os.path.dirname(CANONICAL_OUTPUT_PATH) or ".", exist_ok=True)
    if DUMP_ON_EMPTY or DEBUG_CANDIDATES:
        os.makedirs(DEBUG_DIR, exist_ok=True)

def ts() -> str:
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def sanitize_filename(s: str) -> str:
    s = s or "unknown"
    s = re.sub(r'[^\w\-_\. ]', '_', s)
    return s[:120]

# ---------------- extract_games_list robust ----------------
def extract_games_list(html_file: str) -> List[Dict]:
    """
    Robust extraction of const gamesList = [...] from index111.html.
    Saves raw/fixed dumps into debug_dumps if cleaning required.
    Returns python list of game dicts.
    """
    if not os.path.exists(html_file):
        raise FileNotFoundError(html_file)
    with open(html_file, "r", encoding="utf-8") as f:
        content = f.read()

    marker = "const gamesList ="
    pos = content.find(marker)
    if pos == -1:
        raise ValueError("Не найден 'const gamesList =' в HTML")

    start = content.find("[", pos)
    if start == -1:
        raise ValueError("Не найден '[' после 'const gamesList ='")

    # find matching closing bracket (handle nested)
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
        # save partial dump
        os.makedirs(DEBUG_DIR, exist_ok=True)
        raw_path = os.path.join(DEBUG_DIR, f"gameslist_raw_unclosed_{int(time.time())}.txt")
        with open(raw_path, "w", encoding="utf-8") as rf:
            rf.write(content[start:start+20000])
        raise ValueError(f"Не найдена закрывающая ']' для gamesList. Сохранён дамп: {raw_path}")

    js_array = content[start:end]
    raw_path = os.path.join(DEBUG_DIR, f"gameslist_raw_{int(time.time())}.js")
    with open(raw_path, "w", encoding="utf-8") as rf:
        rf.write(js_array)
    log.info(f"📝 saved raw gamesList to {raw_path}")

    # Try direct json parse
    try:
        data = json.loads(js_array)
        log.info("✅ gamesList parsed directly with json.loads")
        return data
    except Exception as e:
        log.info(f"⚠️ direct json.loads failed: {e}; attempting cleaning")

    # Cleaning JS to JSON
    fixed = js_array
    try:
        # remove block comments and line comments
        fixed = re.sub(r'/\*.*?\*/', '', fixed, flags=re.DOTALL)
        fixed = re.sub(r'//.*?(?=\r?\n)', '', fixed)
        # convert single-quoted strings to double-quoted (simple)
        fixed = re.sub(r"\'([^'\\]*(?:\\.[^'\\]*)*)\'", lambda m: '"' + m.group(1).replace('"', '\\"') + '"', fixed)
        # quote object keys like: keyName:
        fixed = re.sub(r'([{\[,]\s*)([A-Za-z0-9_\-\$@]+)\s*:', r'\1"\2":', fixed)
        # remove trailing commas
        fixed = re.sub(r',\s*(?=[}\]])', '', fixed)
        fixed = re.sub(r'\bundefined\b', 'null', fixed)
        fixed = re.sub(r'\bNaN\b', 'null', fixed)
    except Exception as e:
        log.warning(f"⚠️ error while cleaning gamesList: {e}")

    fixed_path = os.path.join(DEBUG_DIR, f"gameslist_fixed_{int(time.time())}.json")
    with open(fixed_path, "w", encoding="utf-8") as ff:
        ff.write(fixed)
    log.info(f"📝 saved fixed gamesList to {fixed_path}")

    try:
        data = json.loads(fixed)
        log.info("✅ gamesList parsed after cleaning")
        return data
    except Exception as e2:
        msg = f"❌ failed to parse gamesList even after cleaning: {e2}. See dumps: {raw_path}, {fixed_path}"
        log.error(msg)
        raise ValueError(msg)

# ---------------- title normalization & similarity ----------------
def normalize_title_for_comp(title: str) -> str:
    if not title:
        return ""
    s = title
    # normalize quotes
    s = s.replace('\u2018','\'').replace('\u2019','\'').replace('\u201c','"').replace('\u201d','"')
    s = s.lower()
    s = re.sub(r'\(.*?\)', ' ', s)
    s = re.sub(r'[\u2010-\u2015]', '-', s)
    s = re.sub(r'[^0-9a-zа-яё\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

# ---------------- scraping candidates ----------------
def scrape_game_link_candidates(page, max_candidates: int = 120) -> List[Dict]:
    """
    Try multiple selectors and also parse inline scripts for /game/ links.
    Returns list of {href, text, context}
    """
    try:
        # JS function to extract attribute + text + context from links matched
        script = '''
        (els, maxc) => {
            const out = [];
            for (let i=0;i<Math.min(els.length, maxc);i++){
                try {
                    const e = els[i];
                    const href = e.getAttribute('href') || '';
                    const text = e.innerText || '';
                    const p = e.closest('li') || e.closest('div') || e.parentElement;
                    const ctx = p ? (p.innerText || '') : (e.parentElement ? e.parentElement.innerText : text);
                    out.push({href, text, ctx});
                } catch(e){}
            }
            return out;
        }
        '''
        # try list of selectors (cover variants)
        selectors = [
            'a[href^="/game/"]',
            'li.game_title a[href^="/game/"]',
            'div.search_list_details a[href^="/game/"]',
            'div.search_list a[href^="/game/"]',
            'div[class*="gameList"] a[href^="/game/"]',
        ]
        all_candidates = []
        for sel in selectors:
            try:
                raw = page.eval_on_selector_all(sel, script, max_candidates)
                if raw:
                    for r in raw:
                        if not r: continue
                        href = r.get("href","") or ""
                        txt = r.get("text","") or ""
                        ctx = r.get("ctx","") or ""
                        if href and '/game/' in href:
                            all_candidates.append({"href": href, "text": txt.strip(), "context": ctx.strip()})
                if all_candidates:
                    # dedupe by href
                    seen = set(); ded = []
                    for c in all_candidates:
                        if c["href"] not in seen:
                            seen.add(c["href"]); ded.append(c)
                    return ded[:max_candidates]
            except Exception:
                continue

        # If no anchors found, try inline scripts
        scripts = page.query_selector_all("script")
        parsed = []
        for s in scripts:
            try:
                txt = s.inner_text() or ""
                if "/game/" in txt:
                    for m in re.finditer(r'["\'](\/game\/[^"\']+)["\']', txt):
                        href = m.group(1)
                        # context snippet
                        st = max(0, m.start()-200)
                        en = min(len(txt), m.end()+200)
                        ctx = txt[st:en]
                        # try to find title near this occurrence
                        tmatch = re.search(r'["\']name["\']\s*:\s*["\']([^"\']+)["\']', ctx)
                        title = tmatch.group(1) if tmatch else ""
                        parsed.append({"href": href, "text": title, "context": ctx})
            except Exception:
                continue
        # dedupe parsed
        if parsed:
            seen = set(); out = []
            for c in parsed:
                if c["href"] not in seen:
                    seen.add(c["href"]); out.append(c)
            return out[:max_candidates]
    except Exception as e:
        log.error(f"scrape_game_link_candidates error: {e}")
    return []

def get_year_from_context(ctx: str) -> Optional[int]:
    if not ctx:
        return None
    m = re.findall(r'(19\d{2}|20\d{2})', ctx)
    if m:
        years = [int(x) for x in m]
        return min(years)
    return None

# ---------------- ranking & selection ----------------
def calculate_title_similarity(orig: str, cand: str) -> float:
    if not orig or not cand:
        return 0.0
    try:
        orig_norm = normalize_title_for_comp(orig)
        cand_norm = normalize_title_for_comp(cand)
        if orig_norm == cand_norm:
            return 1.0
        base_tokens = orig_norm.split()
        cand_tokens = cand_norm.split()
        if not base_tokens or not cand_tokens:
            return 0.0
        common = set(base_tokens).intersection(set(cand_tokens))
        precision = len(common) / max(1, len(cand_tokens))
        recall = len(common) / max(1, len(base_tokens))
        # LCS-like measure using tokens
        lcs_len = 0
        # compute simple lcs length on tokens
        # dynamic programming (small lists)
        n, m = len(base_tokens), len(cand_tokens)
        dp = [[0]*(m+1) for _ in range(n+1)]
        for i in range(1, n+1):
            for j in range(1, m+1):
                if base_tokens[i-1] == cand_tokens[j-1]:
                    dp[i][j] = dp[i-1][j-1] + 1
                else:
                    dp[i][j] = max(dp[i-1][j], dp[i][j-1])
        lcs_len = dp[n][m]
        seq = lcs_len / max(1, len(base_tokens))
        base_score = 0.6*recall + 0.25*precision + 0.15*seq
        fuzz = SequenceMatcher(None, orig_norm, cand_norm).ratio()
        score = max(base_score, 0.55*base_score + 0.45*fuzz)
        return float(max(0.0, min(1.0, score)))
    except Exception:
        return 0.0

def find_best_candidate(candidates: List[Dict], original_title: str, game_year: Optional[int], idx_info: Optional[Dict]=None) -> Tuple[Optional[Dict], float]:
    if not candidates:
        return None, 0.0
    orig_clean = normalize_title_for_comp(original_title)
    base_clean = normalize_title_for_comp(original_title.split(":",1)[0].strip())
    # exact match fast path
    for cand in candidates:
        ct = normalize_title_for_comp(cand.get("text",""))
        if ct == orig_clean:
            return cand, 1.0
    # immediate year+base match
    if game_year:
        for cand in candidates:
            ct = normalize_title_for_comp(cand.get("text",""))
            if base_clean and base_clean in ct:
                cy = get_year_from_context(cand.get("context",""))
                if cy and cy == game_year:
                    log.info(f"🎯 exact base+year match: {cand.get('text')} ({cy})")
                    return cand, 0.999
    best = None
    best_score = -1.0
    scores_dump = []
    for cand in candidates:
        cand_text = cand.get("text","")
        cand_ctx = cand.get("context","")
        score = calculate_title_similarity(original_title, cand_text)
        if game_year:
            cy = get_year_from_context(cand_ctx)
            if cy:
                if cy == game_year:
                    score = max(score, 0.9)
                else:
                    diff = abs(game_year - cy)
                    if diff <= 2:
                        score = max(score, 0.75)
        # base presence bonus
        ct = normalize_title_for_comp(cand_text)
        if base_clean and base_clean in ct:
            score += 0.06
        score = max(0.0, min(1.0, score))
        scores_dump.append({"text": cand_text, "href": cand.get("href",""), "year": get_year_from_context(cand_ctx), "score": round(score,4)})
        if score > best_score:
            best_score = score; best = cand
    # dump scores if debug
    if DEBUG_CANDIDATES and idx_info:
        if best_score < DEBUG_SCORE_THRESHOLD:
            p = os.path.join(DEBUG_DIR, f"{idx_info.get('index',0)}_{sanitize_filename(idx_info.get('title',''))}_scores_{int(time.time())}.json")
            try:
                with open(p, "w", encoding="utf-8") as f:
                    json.dump(scores_dump, f, ensure_ascii=False, indent=2)
                log.info(f"🧾 saved scores dump: {p}")
            except Exception:
                pass
    if best and best_score >= 0.25:
        return best, float(best_score)
    # if ambiguous save candidates for offline inspection
    if (DEBUG_CANDIDATES or DUMP_ON_EMPTY) and idx_info:
        try:
            cpath = os.path.join(DEBUG_DIR, f"{idx_info.get('index',0)}_{sanitize_filename(idx_info.get('title',''))}_candidates_{int(time.time())}.json")
            with open(cpath, "w", encoding="utf-8") as f:
                json.dump(candidates, f, ensure_ascii=False, indent=2)
            append_summary_log(idx_info.get('index',0), idx_info.get('title',''), candidates)
            log.info(f"🗂️ saved candidates dump: {cpath}")
        except Exception:
            pass
    return None, 0.0

def append_summary_log(index:int, title:str, candidates:List[Dict]):
    try:
        p = os.path.join(DEBUG_DIR, "summary.log")
        top = candidates[:5]
        top_str = " | ".join([f"{c.get('text','')[:80]} -> {c.get('href','')}" for c in top])
        with open(p, "a", encoding="utf-8") as f:
            f.write(f"{int(time.time())}\t{index}\t{title}\t{len(candidates)}\t{top_str}\n")
    except Exception:
        pass

# ---------------- extract HLTB data from game page ----------------
def round_time_str(s: str) -> Optional[str]:
    if not s: return None
    s = s.replace('½', '.5')
    m = re.search(r'(\d+(?:\.\d+)?)\s*h', s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m2 = re.search(r'(\d+(?:\.\d+)?)\s*Hours?', s, flags=re.IGNORECASE)
    if m2:
        val = float(m2.group(1)); return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m3 = re.search(r'(\d+)\s*m', s, flags=re.IGNORECASE)
    if m3: return f"{int(m3.group(1))}m"
    return None

def extract_hltb_data_from_page(page) -> Optional[Dict]:
    try:
        hltb = {}
        # try to parse tables first
        try:
            tables = page.locator("table")
            for ti in range(tables.count()):
                tbl = tables.nth(ti)
                txt = tbl.inner_text()
                if any(k in txt for k in ["Main Story","Main + Extras","Completionist","Co-Op","Vs.","Single-Player"]):
                    rows = tbl.locator("tr")
                    for ri in range(rows.count()):
                        rtxt = rows.nth(ri).inner_text()
                        if "Main Story" in rtxt or "Single-Player" in rtxt:
                            d = re.search(r'(\d+(?:\.\d+)?\s*h)', rtxt)
                            if d: hltb["ms"] = round_time_str(d.group(1))
                        if "Main + Extras" in rtxt:
                            d = re.search(r'(\d+(?:\.\d+)?\s*h)', rtxt)
                            if d: hltb["mpe"] = round_time_str(d.group(1))
                        if "Completionist" in rtxt:
                            d = re.search(r'(\d+(?:\.\d+)?\s*h)', rtxt)
                            if d: hltb["comp"] = round_time_str(d.group(1))
                        if "Co-Op" in rtxt:
                            d = re.search(r'(\d+(?:\.\d+)?\s*h)', rtxt)
                            if d: hltb["coop"] = round_time_str(d.group(1))
                        if "Vs." in rtxt or "Versus" in rtxt:
                            d = re.search(r'(\d+(?:\.\d+)?\s*h)', rtxt)
                            if d: hltb["vs"] = round_time_str(d.group(1))
        except Exception:
            pass

        # fallback: search page text
        content = page.content()
        if not hltb:
            patterns = {
                "ms": r'(?:Main Story|Single-Player)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "mpe": r'(?:Main \+ Extras)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "comp": r'(?:Completionist)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "coop": r'(?:Co-Op)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "vs": r'(?:Vs\.|Versus)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)'
            }
            for k,p in patterns.items():
                m = re.search(p, content, flags=re.IGNORECASE)
                if m:
                    hltb[k] = round_time_str(m.group(1))
        # try to find store links
        stores = {}
        try:
            for name, sel in [("steam","a[href*='store.steampowered.com']"), ("gog","a[href*='gog.com']"), ("epic","a[href*='epicgames.com']")]:
                loc = page.locator(sel)
                if loc.count() > 0:
                    href = loc.first.get_attribute("href")
                    if href:
                        stores[name] = href
        except Exception:
            pass
        if stores:
            hltb["stores"] = stores
        return hltb if hltb else None
    except Exception as e:
        log.error(f"extract_hltb_data_from_page error: {e}")
        return None

# ---------------- search logic single attempt ----------------
def is_blocked_content(html: str) -> bool:
    s = html.lower()
    checks = ["checking your browser", "cloudflare", "access denied", "please enable javascript", "are you human", "captcha"]
    return any(c in s for c in checks)

def random_delay(min_s=MIN_DELAY, max_s=MAX_DELAY):
    time.sleep(random.uniform(min_s, max_s))

def search_game_single_attempt(page, game_title: str, game_year: Optional[int], idx_info: Optional[Dict]=None) -> Tuple[Optional[Tuple[Dict,str,float]], Optional[str]]:
    """
    Returns ((hltb_data, found_title, score), None) on success,
            (None, "blocked") if blocked,
            (None, None) if not found this attempt.
    """
    try:
        variants = []
        # generate sensible variants
        variants.append(game_title)
        variants.append(game_title.replace('&', 'and'))
        variants.append(re.sub(r'\(.*?\)', '', game_title).strip())
        if ':' in game_title:
            variants.append(game_title.split(":",1)[0].strip())
        if game_year:
            variants.append(f"{game_title} {game_year}")
            variants.append(f"{game_title} \"{game_year}\"")
        # dedupe variants preserving order
        seen = set()
        final_variants = []
        for v in variants:
            if v and v not in seen:
                seen.add(v); final_variants.append(v)

        for variant in final_variants:
            log.info(f"🔍 Searching variant: '{variant}'")
            search_url = f"{BASE_URL}/?q={quote(variant, safe='')}"
            try:
                page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
            except PlaywrightError as e:
                log.warning(f"Page.goto error: {e}")
            # wait a little for results to render
            try:
                page.wait_for_selector('a[href^="/game/"]', timeout=3500)
            except:
                pass
            random_delay()
            page_content = page.content()
            if is_blocked_content(page_content):
                log.warning("🚫 Detected block/anti-bot on search page")
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_state(page, idx_info)
                return None, "blocked"

            candidates = scrape_game_link_candidates(page, max_candidates=160)
            if not candidates:
                log.info("⚠️ Candidates == 0 — trying reload fallback")
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_state(page, idx_info)
                try:
                    page.reload(timeout=PAGE_GOTO_TIMEOUT)
                    try: page.wait_for_selector('a[href^=\"/game/\"]', timeout=2000)
                    except: pass
                    random_delay(0.6, 1.2)
                    candidates = scrape_game_link_candidates(page, max_candidates=160)
                except Exception:
                    candidates = []
            if not candidates:
                # save tiny candidate dump
                if idx_info and (DEBUG_CANDIDATES or DUMP_ON_EMPTY):
                    try:
                        p = os.path.join(DEBUG_DIR, f"{idx_info.get('index',0)}_{sanitize_filename(idx_info.get('title',''))}_empty_candidates_{int(time.time())}.json")
                        with open(p, "w", encoding="utf-8") as f: json.dump([], f)
                        log.info(f"🗂️ saved empty candidates: {p}")
                    except Exception:
                        pass
                continue

            log.info(f"🔎 Found {len(candidates)} candidates for variant '{variant}'")
            # if too many candidates, try narrow by year
            if len(candidates) > 30 and game_year:
                try:
                    yr_q = f"{variant} {game_year}"
                    log.info(f"⚠️ Too many results ({len(candidates)}), retrying search with year: '{yr_q}'")
                    page.goto(f"{BASE_URL}/?q={quote(yr_q, safe='')}", timeout=PAGE_GOTO_TIMEOUT)
                    try: page.wait_for_selector('a[href^=\"/game/\"]', timeout=3000)
                    except: pass
                    random_delay()
                    candidates = scrape_game_link_candidates(page, max_candidates=200)
                except Exception:
                    pass
                log.info(f"🔎 After year-filter: {len(candidates)} candidates")

            best_cand, score = find_best_candidate(candidates, game_title, game_year, idx_info=idx_info)
            if idx_info and DEBUG_CANDIDATES:
                if score < DEBUG_SCORE_THRESHOLD:
                    # save candidate dump for offline analysis
                    try:
                        p = os.path.join(DEBUG_DIR, f"{idx_info.get('index',0)}_{sanitize_filename(idx_info.get('title',''))}_candidates_debug_{int(time.time())}.json")
                        with open(p, "w", encoding="utf-8") as f: json.dump(candidates, f, ensure_ascii=False, indent=2)
                        log.info(f"🧾 saved debug candidates: {p}")
                    except Exception:
                        pass
            if not best_cand:
                log.info("⚠️ No best candidate this variant — continuing with next variant")
                continue

            href = best_cand.get("href")
            if not href:
                continue
            full_url = f"{BASE_URL}{href}"
            try:
                page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT)
            except PlaywrightError:
                log.warning(f"⚠️ goto {full_url} failed")
            try:
                page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            except:
                pass
            random_delay()
            page_content = page.content()
            if is_blocked_content(page_content):
                log.warning("🚫 Block detected on game page")
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_state(page, idx_info)
                return None, "blocked"
            hltb_data = extract_hltb_data_from_page(page)
            if hltb_data:
                return (hltb_data, best_cand.get("text",""), score), None
            else:
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_state(page, idx_info)
                continue
        return None, None
    except Exception as e:
        log.exception(f"search_game_single_attempt exception for '{game_title}': {e}")
        if idx_info and DUMP_ON_EMPTY:
            try:
                dump_search_state(page, idx_info)
            except Exception:
                pass
        return None, None

# ---------------- debugging helpers ----------------
def dump_search_state(page, idx_info):
    try:
        i = idx_info.get("index",0)
        title = idx_info.get("title","")
        # html
        html_path = os.path.join(DEBUG_DIR, f"{i}_{sanitize_filename(title)}_search_html_{int(time.time())}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        log.info(f"📝 saved html dump: {html_path}")
    except Exception as e:
        log.warning(f"dump_search_state html error: {e}")
    try:
        # screenshot
        shot_path = os.path.join(DEBUG_DIR, f"{i}_{sanitize_filename(title)}_screenshot_{int(time.time())}.png")
        page.screenshot(path=shot_path, full_page=True)
        log.info(f"📸 saved screenshot: {shot_path}")
    except Exception as e:
        log.warning(f"dump_search_state screenshot error: {e}")
    try:
        # candidates (re-scrape)
        cands = scrape_game_link_candidates(page, max_candidates=200)
        cand_path = os.path.join(DEBUG_DIR, f"{i}_{sanitize_filename(title)}_candidates_{int(time.time())}.json")
        with open(cand_path, "w", encoding="utf-8") as f:
            json.dump(cands, f, ensure_ascii=False, indent=2)
        log.info(f"🗂️ saved candidates: {cand_path}")
        append_summary_log(i, title, cands)
    except Exception as e:
        log.warning(f"dump_search_state candidates error: {e}")

# ---------------- save results (per-run) ----------------
def save_results_per_run(games_list: List[Dict], runid: str) -> str:
    fn = PER_RUN_FILENAME_TEMPLATE.format(runid=runid)
    out_path = os.path.join(OUTPUT_DIR, fn)
    with open(out_path, "w", encoding="utf-8") as f:
        # newline-delimited or single array? we choose single array for canonical file
        json.dump(games_list, f, ensure_ascii=False, indent=2)
    log.info(f"💾 saved per-run results: {out_path}")
    return out_path

# ---------------- GitHub API commit (atomic) ----------------
def commit_file_to_github(per_run_file_path: str,
                          repo: Optional[str] = None,
                          token: Optional[str] = None,
                          path_in_repo: str = CANONICAL_OUTPUT_PATH,
                          commit_message: Optional[str] = None) -> bool:
    """
    Atomically update (create or update) file at path_in_repo using GitHub API:
    PUT /repos/{owner}/{repo}/contents/{path}
    Implements retry with exponential backoff on sha-conflicts.
    Returns True on success, False on final failure.
    """
    if token is None or repo is None:
        log.warning("GITHUB_TOKEN or GITHUB_REPOSITORY not set; skipping commit")
        return False
    if requests is None:
        log.warning("requests library not installed; skipping commit")
        return False

    with open(per_run_file_path, "rb") as f:
        content_bytes = f.read()
    content_b64 = base64.b64encode(content_bytes).decode()

    owner_repo = repo  # "owner/repo"
    api_base = f"https://api.github.com/repos/{owner_repo}/contents/{path_in_repo.lstrip('/')}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

    if commit_message is None:
        commit_message = f"HLTB: update data from run {os.environ.get('GITHUB_RUN_ID','local')} at {datetime.utcnow().isoformat()}"

    attempt = 0
    backoff = GITHUB_API_INITIAL_BACKOFF
    while attempt < GITHUB_API_MAX_ATTEMPTS:
        attempt += 1
        try:
            # get current file to obtain sha (if exists)
            resp = requests.get(api_base, headers=headers, timeout=30)
        except Exception as e:
            log.warning(f"GitHub API GET error (attempt {attempt}): {e}")
            time.sleep(backoff)
            backoff *= 2
            continue
        if resp.status_code == 200:
            try:
                j = resp.json()
                current_sha = j.get("sha")
            except Exception:
                current_sha = None
        elif resp.status_code == 404:
            current_sha = None
        else:
            log.warning(f"GitHub API GET returned status {resp.status_code}: {resp.text}")
            # transient error -> retry
            time.sleep(backoff)
            backoff *= 2
            continue

        payload = {"message": commit_message, "content": content_b64}
        if current_sha:
            payload["sha"] = current_sha
        # attempt to PUT
        try:
            put_resp = requests.put(api_base, headers=headers, json=payload, timeout=30)
        except Exception as e:
            log.warning(f"GitHub API PUT error (attempt {attempt}): {e}")
            time.sleep(backoff)
            backoff *= 2
            continue

        if put_resp.status_code in (200,201):
            log.info(f"✅ Successfully updated {path_in_repo} in repo {owner_repo} (attempt {attempt})")
            return True
        else:
            # check message
            try:
                jr = put_resp.json()
                message = jr.get("message","")
            except Exception:
                message = put_resp.text
            log.warning(f"GitHub API PUT status {put_resp.status_code} (attempt {attempt}): {message}")
            # if conflict / sha mismatch, retry
            if put_resp.status_code == 409 or "sha" in message.lower() or "conflict" in message.lower():
                log.info(f"Conflict detected; retrying after {backoff}s (attempt {attempt})")
                time.sleep(backoff)
                backoff *= 2
                continue
            # other errors -> maybe rate limit? handle 403 rate limit
            if put_resp.status_code == 403 and 'rate limit' in message.lower():
                reset_sleep = backoff * 2
                log.info(f"Rate limited; sleeping {reset_sleep}s")
                time.sleep(reset_sleep)
                backoff *= 2
                continue
            # unknown fatal -> break
            log.error(f"Fatal error updating file: {put_resp.status_code} - {message}")
            return False
    log.error("❌ Reached max attempts for GitHub API commit; giving up")
    return False

# ---------------- Main loop ----------------
def main():
    ensure_dirs()
    # extract games
    try:
        games_list = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        log.exception(f"Failed to extract games list: {e}")
        raise

    total = len(games_list)
    log.info(f"📄 extracted {total} games")

    # progress start index (optional)
    start_idx = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as pf:
                prog = json.load(pf)
                start_idx = prog.get("current_index", 0)
                log.info(f"📂 resume from index {start_idx}")
        except Exception:
            start_idx = 0

    runid = os.environ.get("GITHUB_RUN_ID") or ts()
    per_run_path = None
    processed = 0
    backoff_state = 0

    # Playwright start
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context_kwargs = {
                "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "viewport": {"width": 1280, "height": 800},
                "locale": "en-US",
            }
            if os.path.exists(STORAGE_STATE):
                context_kwargs["storage_state"] = STORAGE_STATE
            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            try:
                page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
                page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                log.info("✅ HowLongToBeat landing OK")
                # save storage_state
                try:
                    context.storage_state(path=STORAGE_STATE)
                except Exception:
                    pass
            except Exception as e:
                log.warning(f"Landing page open failed: {e}")

            for i in range(start_idx, total):
                entry = games_list[i]
                title = entry.get("title") or ""
                year = entry.get("year")
                index_display = i+1
                log.info(f"🎮 Processing {index_display}/{total}: {title} ({year})")
                idx_info = {"index": index_display, "title": title}
                outcome, status = search_game_single_attempt(page, title, year, idx_info=idx_info)
                if status == "blocked":
                    # increase backoff
                    backoff_state = max(backoff_state * 2, 30)
                    log.warning(f"⏸️ Block detected — sleeping {int(backoff_state)}s")
                    time.sleep(backoff_state)
                # process outcome
                if isinstance(outcome, tuple) and outcome[0] is not None:
                    hltb_data, found_title, score = outcome
                    entry["hltb"] = hltb_data
                    processed += 1
                    log.info(f"✅ Found data for '{title}': matched '{found_title}' (score {score:.2f})")
                else:
                    entry["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "vs": "N/A"}
                    log.warning(f"⚠️ Data not found for: {title} - written N/A")

                # periodic saves
                if (i+1) % 10 == 0:
                    try:
                        with open(PROGRESS_FILE, "w", encoding="utf-8") as pf:
                            json.dump({"current_index": i+1, "total": total, "last": datetime.utcnow().isoformat()}, pf)
                        save_tmp = save_results_per_run(games_list, runid)  # save every 10
                        per_run_path = save_tmp
                    except Exception as e:
                        log.warning(f"Periodic save error: {e}")

                # handle backoff / polite delay
                if backoff_state >= 30:
                    log.info(f"⏸ sleeping backoff_state {int(backoff_state)}s")
                    time.sleep(backoff_state)
                else:
                    random_delay()

            # finished loop
            try:
                per_run_path = save_results_per_run(games_list, runid)
            except Exception as e:
                log.exception(f"Failed final save: {e}")
            try:
                context.close()
            except:
                pass
            try:
                browser.close()
            except:
                pass
    except Exception as e:
        log.exception(f"Playwright run failed: {e}")
        raise

    log.info(f"🎉 Done — processed {processed}/{total} games. Per-run: {per_run_path}")

    # Attempt commit to GitHub canonical path via API (if token and repo set)
    gh_token = os.environ.get("GITHUB_TOKEN")  # provided by Actions
    gh_repo = os.environ.get("GITHUB_REPOSITORY")  # owner/repo
    if gh_token and gh_repo and per_run_path and os.path.exists(per_run_path):
        log.info("🔐 Attempting to commit per-run result to repository via GitHub API")
        commit_msg = f"HLTB: update data (run {os.environ.get('GITHUB_RUN_ID','local')})"
        ok = commit_file_to_github(per_run_path, repo=gh_repo, token=gh_token, path_in_repo=CANONICAL_OUTPUT_PATH, commit_message=commit_msg)
        if ok:
            log.info("✅ Commit successful")
        else:
            log.error("❌ Commit failed (see logs)")
    else:
        log.info("ℹ️ Skipping GitHub commit (GITHUB_TOKEN and/or GITHUB_REPOSITORY not set or per-run file missing)")

# small wrapper to call save_results_per_run (exposed earlier)
def save_results_per_run(games_list: List[Dict], runid: str) -> str:
    fn = PER_RUN_FILENAME_TEMPLATE.format(runid=runid)
    out_path = os.path.join(OUTPUT_DIR, fn)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(games_list, f, ensure_ascii=False, indent=2)
    log.info(f"💾 saved per-run results: {out_path}")
    return out_path

# ---------------- Entrypoint ----------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.exception(f"CRITICAL ERROR: {e}")
        raise
