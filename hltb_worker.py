#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HLTB Worker - robust version with sanitized debug dump filenames
(Full file — contains sanitize_filename() applied to all debug outputs)
"""
import os
import re
import json
import time
import random
import subprocess
from datetime import datetime
from urllib.parse import quote
from pathlib import Path

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ===========================
# CONFIG
# ===========================
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"          # input, should contain const gamesList = [...]
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/hltb_data.json"
DEBUG_DIR = "debug_dumps"
PROGRESS_FILE = "progress.json"

# Playwright/CI tuning
PLAYWRIGHT_LAUNCH_ARGS = ["--no-sandbox", "--disable-setuid-sandbox"]
BROWSER_HEADLESS = True

# Search behaviour
MAX_ATTEMPTS = 3
DELAY_ATTEMPTS = [
    (0.4, 0.7),   # attempt 1 jitter range (min,max) seconds
    (1.0, 1.8),   # attempt 2
    (3.0, 6.0),   # attempt 3 (longer)
]
CANDIDATE_TOO_MANY_THRESHOLD = 30
CANDIDATE_TOO_MANY_RETRY_SUFFIX = " {year}"  # appended when too many results
MIN_MATCH_SCORE = 0.6   # if above, may accept best result (but exact match forces 1.0)

# ===========================
# Helpers
# ===========================
def log_message(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DEBUG_DIR, exist_ok=True)

# sanitize filenames so upload-artifact / NTFS don't choke
_INVALID_FILENAME_RE = re.compile(r'[\"<>:\|\*\?\r\n/\\]+')  # include slash/backslash
def sanitize_filename(name: str, max_len: int = 100) -> str:
    if not name:
        return "file"
    # replace invalid chars with underscore
    s = _INVALID_FILENAME_RE.sub('_', name)
    # collapse repeated underscores/spaces
    s = re.sub(r'[_\s]+', '_', s).strip('_')
    # trim
    if len(s) > max_len:
        s = s[:max_len]
    # ensure not empty
    if not s:
        s = "file"
    return s

# ===========================
# Title cleaning & matching
# ===========================
def normalize_title_for_comparison(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    s = re.sub(r"[’'`]", "", s)
    s = re.sub(r"[^a-z0-9а-яё\s:/&\-]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def clean_title_for_comparison(s: str) -> str:
    if not s:
        return ""
    s = normalize_title_for_comparison(s)
    s = s.replace("&", " and ")
    s = s.replace("/", " ")
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

def calculate_title_similarity(original: str, candidate: str) -> float:
    if not original or not candidate:
        return 0.0

    orig_clean = clean_title_for_comparison(original)
    cand_clean = clean_title_for_comparison(candidate)

    if not orig_clean or not cand_clean:
        return 0.0

    if cand_clean == orig_clean:
        return 1.0

    if "/" in original or " / " in original:
        parts = [p.strip() for p in re.split(r"/", original)]
        for part in parts:
            if clean_title_for_comparison(part) == cand_clean:
                return 1.0
            base = part.split(":", 1)[0].strip()
            if clean_title_for_comparison(base) == cand_clean:
                return 0.9

    base = original.split(":", 1)[0].strip()
    if clean_title_for_comparison(base) == cand_clean:
        return 0.9

    arabic_match = re.search(r'\b(\d+)\b', original)
    if arabic_match:
        roman = convert_arabic_to_roman(arabic_match.group(1))
        if roman:
            if cand_clean == clean_title_for_comparison(re.sub(r'\b' + re.escape(arabic_match.group(1)) + r'\b', roman, original)):
                return 0.9

    roman_match = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', original, flags=re.IGNORECASE)
    if roman_match:
        arab = convert_roman_to_arabic(roman_match.group(1))
        if arab:
            if cand_clean == clean_title_for_comparison(re.sub(r'\b' + re.escape(roman_match.group(1)) + r'\b', arab, original, flags=re.IGNORECASE)):
                return 0.9

    a_tokens = orig_clean.split()
    b_tokens = cand_clean.split()
    if not a_tokens or not b_tokens:
        return 0.0
    common = set(a_tokens).intersection(set(b_tokens))
    precision = len(common) / len(b_tokens) if b_tokens else 0
    recall = len(common) / len(a_tokens) if a_tokens else 0

    def lcs_len(a, b):
        n, m = len(a), len(b)
        if n == 0 or m == 0:
            return 0
        dp = [[0]*(m+1) for _ in range(n+1)]
        for i in range(1, n+1):
            for j in range(1, m+1):
                if a[i-1] == b[j-1]:
                    dp[i][j] = dp[i-1][j-1] + 1
                else:
                    dp[i][j] = max(dp[i-1][j], dp[i][j-1])
        return dp[n][m]
    seq = lcs_len(a_tokens, b_tokens) / max(1, len(a_tokens))

    score = 0.6 * recall + 0.25 * precision + 0.15 * seq
    return max(0.0, min(1.0, score))

# ===========================
# Games list extraction
# ===========================
def extract_games_list(html_file: str):
    log_message(f"🔍 Reading games list file: {html_file}")
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    marker = 'const gamesList ='
    idx = content.find(marker)
    if idx == -1:
        marker2 = 'var gamesList ='
        idx = content.find(marker2)
        if idx == -1:
            raise ValueError("Cannot find gamesList marker in file.")
        else:
            marker = marker2

    start = content.find('[', idx)
    if start == -1:
        raise ValueError("Cannot find '[' start for gamesList array.")

    bracket_count = 0
    end = None
    for i, ch in enumerate(content[start:], start):
        if ch == '[':
            bracket_count += 1
        elif ch == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end = i + 1
                break
    if end is None:
        raise ValueError("Cannot find end of gamesList array.")

    games_json = content[start:end]
    games_json = re.sub(r',\s*([\]\}])', r'\1', games_json)
    try:
        games_list = json.loads(games_json)
    except Exception as e:
        raw_dump_path = os.path.join(DEBUG_DIR, f"gameslist_raw_{int(time.time())}.js")
        try:
            with open(raw_dump_path, 'w', encoding='utf-8') as f:
                f.write(games_json)
            log_message(f"❌ JSON parse failed for gamesList: {e}; saved raw to {raw_dump_path}")
        except Exception as ee:
            log_message(f"❌ JSON parse failed and could not save raw: {ee}")
        raise
    log_message(f"✅ Parsed gamesList with {len(games_list)} items.")
    return games_list

# ===========================
# Debug dump helpers (all filenames sanitized)
# ===========================
def save_debug_html_and_screenshot(page, prefix):
    safe_prefix = sanitize_filename(prefix)
    try:
        html_path = os.path.join(DEBUG_DIR, f"{safe_prefix}_search_html_{int(time.time())}.html")
        with open(html_path, 'w', encoding='utf-8') as f:
            f.write(page.content())
        log_message(f"📝 saved html dump: {html_path}")
    except Exception as e:
        log_message(f"⚠️ Could not save html dump ({safe_prefix}): {e}")

    try:
        png_path = os.path.join(DEBUG_DIR, f"{safe_prefix}_screenshot_{int(time.time())}.png")
        page.screenshot(path=png_path, full_page=True)
        log_message(f"📸 saved screenshot: {png_path}")
    except Exception as e:
        log_message(f"⚠️ Could not save screenshot ({safe_prefix}): {e}")

def save_candidates_dump(candidates, prefix):
    safe_prefix = sanitize_filename(prefix)
    try:
        cand_path = os.path.join(DEBUG_DIR, f"{safe_prefix}_candidates_{int(time.time())}.json")
        with open(cand_path, 'w', encoding='utf-8') as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2)
        log_message(f"🗂️ saved candidates dump: {cand_path}")
    except Exception as e:
        log_message(f"⚠️ Could not save candidates dump ({safe_prefix}): {e}")

def save_scores_dump(scores, prefix):
    safe_prefix = sanitize_filename(prefix)
    try:
        scores_path = os.path.join(DEBUG_DIR, f"{safe_prefix}_scores_{int(time.time())}.json")
        with open(scores_path, 'w', encoding='utf-8') as f:
            json.dump(scores, f, ensure_ascii=False, indent=2)
        log_message(f"🧾 saved scores dump: {scores_path}")
    except Exception as e:
        log_message(f"⚠️ Could not save scores dump ({safe_prefix}): {e}")

def save_empty_candidates_marker(prefix):
    safe_prefix = sanitize_filename(prefix)
    try:
        empty_path = os.path.join(DEBUG_DIR, f"{safe_prefix}_empty_candidates_{int(time.time())}.json")
        with open(empty_path, 'w', encoding='utf-8') as f:
            json.dump([], f, ensure_ascii=False)
        log_message(f"🗂️ saved empty candidates: {empty_path}")
    except Exception as e:
        log_message(f"⚠️ Could not save empty candidates marker ({safe_prefix}): {e}")

# ===========================
# JS extraction & search helpers
# ===========================
def extract_candidates_via_js(page):
    try:
        js = """
        () => {
            const anchors = Array.from(document.querySelectorAll('a[href^="/game/"]'));
            return anchors.map(a => {
                return {
                    text: a.innerText ? a.innerText.trim() : (a.querySelector('.text') ? a.querySelector('.text').innerText.trim() : ''),
                    href: a.getAttribute('href'),
                    year: (() => {
                        let t = a.innerText || '';
                        const m = t.match(/\\b(19\\d{2}|20\\d{2})\\b/);
                        return m ? m[0] : null;
                    })()
                };
            });
        }
        """
        results = page.evaluate(js)
        filtered = []
        seen = set()
        for r in results:
            href = r.get("href") or ""
            text = r.get("text") or ""
            key = (href, text)
            if href and key not in seen:
                seen.add(key)
                filtered.append({"href": href, "text": text, "year": r.get("year")})
        return filtered
    except Exception as e:
        log_message(f"⚠️ extract_candidates_via_js failed: {e}")
        return []

def safe_sleep_between(min_s, max_s):
    if min_s <= 0 and max_s <= 0:
        return
    t = random.uniform(min_s, max_s)
    time.sleep(t)

def attempt_page_goto(page, url, timeout=20000):
    try:
        page.goto(url, timeout=timeout)
        page.wait_for_load_state("domcontentloaded", timeout=max(5000, int(timeout/4)))
        return True
    except PlaywrightTimeoutError:
        log_message(f"⚠️ Page.goto timeout for {url}")
        return False
    except Exception as e:
        log_message(f"⚠️ Page.goto error for {url}: {e}")
        return False

def search_game_single_attempt(page, game_title: str, game_year=None, prefix_for_debug=""):
    try:
        safe_prefix = sanitize_filename(prefix_for_debug)
        log_message(f"🔍 Searching: '{game_title}'")
        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"
        ok = attempt_page_goto(page, search_url, timeout=20000)
        if not ok:
            log_message("⚠️ Failed page navigation while searching.")
            return None

        page_text = page.content()
        low = page_text.lower()
        if "access denied" in low or "blocked" in low:
            log_message("❌ BLOCKED or ACCESS DENIED detected on search page")
            save_debug_html_and_screenshot(page, safe_prefix)
            return None
        if "checking your browser" in low or "cloudflare" in low:
            log_message("⚠️ Cloudflare challenge detected on search page")
            save_debug_html_and_screenshot(page, safe_prefix)
            return None

        safe_sleep_between(0.25, 0.6)

        candidates = extract_candidates_via_js(page)
        save_candidates_dump(candidates, safe_prefix)

        found_count = len(candidates)
        log_message(f"🔎 Found {found_count} candidates for variant '{game_title}'")

        if found_count == 0:
            save_empty_candidates_marker(safe_prefix)
            return None

        scored = []
        for c in candidates:
            text = c.get("text") or ""
            href = c.get("href") or ""
            year = c.get("year")
            score = calculate_title_similarity(game_title, text)
            scored.append({"text": text, "href": href, "year": year, "score": score})

        save_scores_dump(scored, safe_prefix)

        scored_sorted = sorted(scored, key=lambda x: x["score"], reverse=True)
        best = scored_sorted[0]
        log_message(f"🏁 Top candidate: '{best['text']}' score={best['score']:.2f} href={best['href']}")

        if best["score"] >= 1.0 or (best["score"] >= MIN_MATCH_SCORE and game_year and str(game_year) in (best.get("year") or "")):
            page_url = BASE_URL.rstrip("/") + best["href"]
            ok = attempt_page_goto(page, page_url, timeout=20000)
            if not ok:
                log_message("⚠️ Could not open best candidate page.")
                return None
            data = extract_data_from_game_page(page, safe_prefix)
            return (data, best["text"])

        return (None, best["text"])

    except Exception as e:
        log_message(f"❌ search_game_single_attempt exception: {e}")
        try:
            save_debug_html_and_screenshot(page, sanitize_filename(prefix_for_debug or 'search_err'))
        except Exception:
            pass
        return None

# ===========================
# Extract HLTB data from the game page
# ===========================
def extract_data_from_game_page(page, prefix_for_debug="game"):
    try:
        safe_prefix = sanitize_filename(prefix_for_debug)
        save_debug_html_and_screenshot(page, safe_prefix + "_page")

        page_text = page.content()
        hltb_data = {}

        patterns = {
            "ms": r'(?:Main Story|Main)\s*[:\-]\s*([0-9]{1,3}\.?[0-9]? ?(Hours|Hour|Hrs|Hr|H|mins|min|m))',
            "mpe": r'(?:Main \+ Extras|Main \+ Extra|Main \+ Extras)\s*[:\-]\s*([0-9]{1,3}\.?[0-9]? ?(Hours|Hour|Hrs|Hr|H|mins|min|m))',
            "comp": r'(?:Completionist|Completion)\s*[:\-]\s*([0-9]{1,3}\.?[0-9]? ?(Hours|Hour|Hrs|Hr|H|mins|min|m))',
            "all": r'(?:All|Everything)\s*[:\-]\s*([0-9]{1,3}\.?[0-9]? ?(Hours|Hour|Hrs|Hr|H|mins|min|m))',
            "vs": r'Vs\.*\s*[:\-]\s*([0-9]{1,3}\.?[0-9]? ?(Hours|Hour|Hrs|Hr|H|mins|min|m))'
        }
        for k, p in patterns.items():
            m = re.search(p, page.content(), flags=re.IGNORECASE)
            if m:
                hltb_data[k] = {"t": m.group(1).strip()}

        if not hltb_data:
            try:
                m = re.search(r'var gameData = (\{.*?\});', page.content(), flags=re.DOTALL)
                if m:
                    gd = json.loads(m.group(1))
                    for k in ["ms","mpe","comp","all"]:
                        if k in gd:
                            hltb_data[k] = gd[k]
            except Exception:
                pass

        if not hltb_data:
            log_message("⚠️ Extraction heuristics found no time blocks on page.")
            return {"note": "page_opened_but_no_times_extracted"}

        log_message(f"🎯 Extracted HLTB data: {hltb_data}")
        return hltb_data

    except Exception as e:
        log_message(f"❌ extract_data_from_game_page error: {e}")
        return None

# ===========================
# Alternatives + flow
# ===========================
def generate_alternative_titles(game_title: str, game_year=None):
    alts = []
    s = game_title.strip()
    alts.append(s)
    no_par = re.sub(r'\([^)]*\)', '', s).strip()
    if no_par and no_par != s:
        alts.append(no_par)
    if "/" in s:
        parts = [p.strip() for p in s.split("/")]
        if len(parts) >= 2:
            alts.append(f"{parts[0]} and {parts[1]}")
            alts.append(f"{parts[0]} & {parts[1]}")
            alts.extend(parts[:3])
    if "&" in s:
        alts.append(s.replace("&", "and"))
        alts.append(s.replace("&", ""))
    m = re.search(r'\b(\d{1,2})\b', s)
    if m:
        rom = convert_arabic_to_roman(m.group(1))
        if rom:
            alts.append(re.sub(r'\b' + re.escape(m.group(1)) + r'\b', rom, s))
    if game_year:
        alts.append(f'{s} {game_year}')
        alts.append(f'{s} "{game_year}"')
    alts.append(f'"{s}"')
    seen = set()
    out = []
    for a in alts:
        key = a.lower().strip()
        if key not in seen:
            seen.add(key)
            out.append(a)
    return out

def search_game_on_hltb(page, game_title, game_year=None, idx=0):
    prefix = f"{idx}_{game_title.replace('/', ' _ ').replace(' ', '_')}"
    alt_titles = generate_alternative_titles(game_title, game_year)
    log_message(f"🔎 Trying search variants for '{game_title}': {alt_titles}")

    for attempt in range(MAX_ATTEMPTS):
        try:
            dmin, dmax = DELAY_ATTEMPTS[min(attempt, len(DELAY_ATTEMPTS)-1)]
            log_message(f"🔄 Attempt {attempt+1}/{MAX_ATTEMPTS} for '{game_title}' — sleeping {dmin:.2f}-{dmax:.2f}s")
            safe_sleep_between(dmin, dmax)
        except Exception:
            pass

        best_result = None
        best_score = -1.0
        best_title = None

        for v in alt_titles:
            res = search_game_single_attempt(page, v, game_year, prefix)
            if res is None:
                continue
            if isinstance(res, tuple):
                hltb_res, found_title = res
                if hltb_res:
                    log_message(f"✅ Found data for '{game_title}' using variant '{v}' -> matched '{found_title}'")
                    return hltb_res
                else:
                    if found_title:
                        score = calculate_title_similarity(game_title, found_title)
                        if score > best_score:
                            best_score = score
                            best_title = found_title

        if best_score >= 1.0:
            log_message(f"🎯 Best match perfect: {best_title} (score {best_score:.2f})")
            res = search_game_single_attempt(page, best_title, game_year, prefix + "_final")
            if isinstance(res, tuple) and res[0]:
                return res[0]
        elif best_score >= MIN_MATCH_SCORE:
            log_message(f"⚠️ Low but acceptable best score {best_score:.2f} for '{game_title}', will accept if extraction possible.")
            res = search_game_single_attempt(page, best_title, game_year, prefix + "_final")
            if isinstance(res, tuple) and res[0]:
                return res[0]

    log_message(f"⚠️ Data not found for: {game_title} - will record N/A")
    return None

# ===========================
# Git commit helper (best-effort)
# ===========================
def safe_commit_results():
    try:
        subprocess.run(["git", "config", "--local", "user.email", "action@github.com"], check=False)
        subprocess.run(["git", "config", "--local", "user.name", "HLTB Action"], check=False)
        subprocess.run(["git", "add", "-A"], check=False)
        subprocess.run(["git", "commit", "-m", f"HLTB update: {datetime.utcnow().isoformat()}"], check=False)
        branch = subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode().strip()
        log_message(f"🔧 Attempting git pull --rebase --autostash origin {branch}")
        subprocess.run(["git", "pull", "--rebase", "--autostash", "origin", branch], check=False)
        log_message("🔧 Attempting git push origin HEAD")
        subprocess.run(["git", "push", "origin", "HEAD"], check=False)
        log_message("✅ safe_commit_results finished (best-effort).")
    except Exception as e:
        log_message(f"⚠️ safe_commit_results failed: {e}")

# ===========================
# Main
# ===========================
def main():
    log_message("🚀 Starting HLTB Worker (robust mode)")
    ensure_dirs()

    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Games list file {GAMES_LIST_FILE} not found. Exiting.")
        return

    try:
        games_list = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        log_message(f"💥 Fatal: cannot parse games list: {e}")
        return

    open_mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    out_f = open(OUTPUT_FILE, open_mode, encoding='utf-8')
    log_message(f"📄 Output will be written to {OUTPUT_FILE}")

    with sync_playwright() as p:
        log_message("🌐 Launching browser...")
        browser = p.chromium.launch(headless=BROWSER_HEADLESS, args=PLAYWRIGHT_LAUNCH_ARGS)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            locale="en-US"
        )
        page = context.new_page()

        try:
            ok = attempt_page_goto(page, BASE_URL, timeout=15000)
            if not ok:
                log_message("⚠️ Could not reach HowLongToBeat homepage - continuing but results may be unreliable.")
            else:
                title = page.title()
                log_message(f"📄 HLTB page title: {title}")
        except Exception as e:
            log_message(f"⚠️ Error during site check: {e}")

        total = len(games_list)
        processed = 0
        for idx, g in enumerate(games_list):
            try:
                title = g.get("title") if isinstance(g, dict) else (g[0] if isinstance(g, (list, tuple)) else str(g))
                year = g.get("year") if isinstance(g, dict) else None
                log_message(f"🎮 Processing {idx+1}/{total}: {title} ({year})")
                res = search_game_on_hltb(page, title, year, idx)
                if res:
                    g_out = {"title": title, "year": year, "hltb": res}
                else:
                    g_out = {"title": title, "year": year, "hltb": {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A"}}
                out_f.write(json.dumps(g_out, ensure_ascii=False) + "\n")
                out_f.flush()
                processed += 1
                log_message(f"✅ Written result for '{title}'")
            except Exception as e:
                log_message(f"❌ Unexpected error while processing index {idx}: {e}")

        log_message(f"🎉 Done — processed {processed}/{total} games")
        try:
            context.close()
            browser.close()
        except Exception:
            pass

    out_f.close()

    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            lines = [l.strip() for l in f if l.strip()]
        total_saved = len(lines)
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_saved": total_saved,
            "file": OUTPUT_FILE
        }
        with open("scraping_report.json", "w", encoding='utf-8') as rf:
            json.dump(report, rf, ensure_ascii=False, indent=2)
        log_message(f"📊 Report created: {total_saved} entries (scraping_report.json)")
    except Exception as e:
        log_message(f"⚠️ Could not write scraping_report.json: {e}")

    safe_commit_results()

if __name__ == "__main__":
    main()
