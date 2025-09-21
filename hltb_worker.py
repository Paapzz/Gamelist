# hltb_worker.py
# ВЕРСИЯ: добавлено расширенное логирование/дампы для отладки (HTML, screenshot, candidates, scores)
# Замените текущий файл этим.

import json
import time
import random
import re
import os
from datetime import datetime
from urllib.parse import quote, unquote
from playwright.sync_api import sync_playwright

BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/hltb_data.json"
PROGRESS_FILE = "progress.json"

# Таймауты
PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

# Rate control
MIN_DELAY = 0.6
MAX_DELAY = 1.6
INITIAL_BACKOFF = 5
BACKOFF_MULTIPLIER = 2.0
MAX_BACKOFF = 300

# ---------------- DEBUG / DUMPS (настраиваемые) ----------------
DEBUG_CANDIDATES = False         # Если True — сохраняет кандидатов и оценки при спорных выборках
DUMP_ON_EMPTY = True            # Если True — сохраняет HTML + screenshot когда кандидатов == 0
DUMP_DIR = "debug_dumps"        # куда сохранять дампы
DEBUG_SCORE_THRESHOLD = 0.95    # порог: если лучший score < threshold -> сохранить дамп (при DEBUG_CANDIDATES=True)
VERBOSE = True

# ------------------ Утилиты и логирование ------------------

def log_message(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def setup_directories():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if DUMP_ON_EMPTY or DEBUG_CANDIDATES:
        os.makedirs(DUMP_DIR, exist_ok=True)

def sanitize_filename(s):
    # безопасное имя файла: ограничим длину и уберем опасные символы
    s = s or "unknown"
    s = re.sub(r'[^\w\-_\. ]', '_', s)
    return s[:120]

# ----------------- Normalization & similarity (как раньше) -----------------

def clean_title_for_comparison(title):
    if not title:
        return ""
    s = title
    s = s.replace('\u2018','\'').replace('\u2019','\'').replace('\u201c','"').replace('\u201d','"')
    s = s.lower()
    s = re.sub(r'\(.*?\)', ' ', s)
    s = re.sub(r'[\u2010-\u2015]', '-', s)
    s = re.sub(r'[^0-9a-zа-яё\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def normalize_title_for_comparison(title):
    if not title:
        return ""
    s = f" {title} "
    roman_to_arabic = {
        ' i ': ' 1 ', ' ii ': ' 2 ', ' iii ': ' 3 ', ' iv ': ' 4 ', ' v ': ' 5 ',
        ' vi ': ' 6 ', ' vii ': ' 7 ', ' viii ': ' 8 ', ' ix ': ' 9 ', ' x ': ' 10 '
    }
    for r,a in roman_to_arabic.items():
        s = s.replace(r, a)
    return s.strip()

def convert_arabic_to_roman(num_str):
    try:
        num = int(num_str)
        map_ = {1:"I",2:"II",3:"III",4:"IV",5:"V",6:"VI",7:"VII",8:"VIII",9:"IX",10:"X"}
        return map_.get(num, num_str)
    except:
        return num_str

def convert_roman_to_arabic(roman_str):
    try:
        roman_to_arabic_map = {'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
                              'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10}
        return str(roman_to_arabic_map.get(roman_str.upper(), roman_str))
    except:
        return roman_str

def lcs_length(a_tokens, b_tokens):
    n, m = len(a_tokens), len(b_tokens)
    if n == 0 or m == 0:
        return 0
    dp = [[0] * (m + 1) for _ in range(n + 1)]
    for i in range(1, n + 1):
        for j in range(1, m + 1):
            if a_tokens[i-1] == b_tokens[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])
    return dp[n][m]

def calculate_title_similarity(original, candidate):
    if not original or not candidate:
        return 0.0
    try:
        orig_norm = clean_title_for_comparison(normalize_title_for_comparison(original))
        cand_norm = clean_title_for_comparison(normalize_title_for_comparison(candidate))
        if orig_norm == cand_norm:
            return 1.0
        m = re.search(r'\b(\d+)\b', original)
        if m:
            roman = convert_arabic_to_roman(m.group(1))
            if roman:
                if clean_title_for_comparison(normalize_title_for_comparison(re.sub(r'\b'+re.escape(m.group(1))+r'\b', roman, original))) == cand_norm:
                    return 1.0
        m2 = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', original, flags=re.IGNORECASE)
        if m2:
            arab = convert_roman_to_arabic(m2.group(1))
            if arab:
                if clean_title_for_comparison(normalize_title_for_comparison(re.sub(r'\b'+re.escape(m2.group(1))+r'\b', arab, original))) == cand_norm:
                    return 1.0
        base = original.split(":",1)[0].strip()
        base_norm = clean_title_for_comparison(normalize_title_for_comparison(base))
        if base_norm and base_norm == cand_norm:
            return 0.98
        a_tokens = orig_norm.split()
        b_tokens = cand_norm.split()
        if not a_tokens or not b_tokens:
            return 0.0
        common = set(a_tokens).intersection(set(b_tokens))
        precision = len(common) / len(b_tokens)
        recall = len(common) / len(a_tokens)
        lcs_len = lcs_length(a_tokens, b_tokens)
        seq = (lcs_len / len(a_tokens)) if len(a_tokens)>0 else 0.0
        score = 0.6 * recall + 0.25 * precision + 0.15 * seq
        return float(max(0.0, min(1.0, score)))
    except Exception:
        return 0.0

def generate_alternative_titles(game_title):
    if not game_title:
        return []
    seen = set()
    out = []
    def add(s):
        if not s: return
        s2 = re.sub(r'\s+', ' ', s).strip()
        if s2 and s2 not in seen:
            seen.add(s2); out.append(s2)
    add(game_title)
    if '/' in game_title or ',' in game_title:
        parts = [p.strip() for p in re.split(r'[\/,]', game_title) if p.strip()]
        for p in parts:
            add(p)
        if len(parts) >= 2:
            add(f"{parts[0]} and {parts[1]}")
            add(f"{parts[0]} & {parts[1]}")
        if len(parts) >= 3:
            add(f"{parts[0]} and {parts[1]} and {parts[2]}")
            add(f"{parts[0]} & {parts[1]} & {parts[2]}")
        add(game_title.split(":",1)[0].strip())
    else:
        base = game_title.split(":",1)[0].strip()
        add(base); add(game_title)
        m = re.search(r'\b(\d+)\b', game_title)
        if m:
            r = convert_arabic_to_roman(m.group(1))
            if r and r != m.group(1):
                add(re.sub(r'\b'+re.escape(m.group(1))+r'\b', r, game_title))
        rm = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', game_title, flags=re.IGNORECASE)
        if rm:
            a = convert_roman_to_arabic(rm.group(1))
            if a and a != rm.group(1):
                add(re.sub(r'\b'+re.escape(rm.group(1))+r'\b', a, game_title, flags=re.IGNORECASE))
    return out

# ----------------- Fast candidate scraping (single eval call) -----------------

def scrape_game_link_candidates(page, max_candidates=80):
    try:
        script = f'''
        (els, maxc) => {{
            const out = [];
            for (let i=0;i<Math.min(els.length, maxc);i++) {{
                try {{
                    const e = els[i];
                    const href = e.getAttribute('href') || '';
                    const text = e.innerText || '';
                    const p = e.closest('li') || e.closest('div') || e.parentElement;
                    const ctx = p ? (p.innerText || '') : (e.parentElement ? e.parentElement.innerText : text);
                    out.push({{href, text, ctx}});
                }} catch(e) {{ }}
            }}
            return out;
        }}
        '''
        raw = page.eval_on_selector_all('a[href^="/game/"]', script, max_candidates)
        candidates = []
        for r in raw:
            if not r: continue
            href = r.get("href","") or ""
            txt = r.get("text","") or ""
            ctx = r.get("ctx","") or ""
            candidates.append({"href": href, "text": txt.strip(), "context": ctx.strip()})
        return candidates
    except Exception:
        return []

def get_year_from_context_text(ctx_text):
    if not ctx_text:
        return None
    m = re.findall(r'(\b19\d{2}\b|\b20\d{2}\b)', ctx_text)
    if m:
        years = [int(x) for x in m]
        return min(years)
    return None

# ----------------- Candidate ranking & debug dumps -----------------

def dump_candidates_file(prefix_idx, game_title, candidates, suffix="candidates"):
    try:
        if not (DEBUG_CANDIDATES or DUMP_ON_EMPTY):
            return None
        name = f"{prefix_idx}_{sanitize_filename(game_title)}_{suffix}_{int(time.time())}.json"
        path = os.path.join(DUMP_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2)
        log_message(f"🗂️ Сохранён дамп кандидатов: {path}")
        return path
    except Exception as e:
        log_message(f"⚠️ Не удалось сохранить дамп кандидатов: {e}")
        return None

def dump_scores_file(prefix_idx, game_title, score_list, suffix="scores"):
    try:
        if not DEBUG_CANDIDATES:
            return None
        name = f"{prefix_idx}_{sanitize_filename(game_title)}_{suffix}_{int(time.time())}.json"
        path = os.path.join(DUMP_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(score_list, f, ensure_ascii=False, indent=2)
        log_message(f"🧾 Сохранён дамп оценок кандидатов: {path}")
        return path
    except Exception as e:
        log_message(f"⚠️ Не удалось сохранить дамп оценок: {e}")
        return None

def dump_search_html(page, prefix_idx, game_title, suffix="search_html"):
    try:
        if not DUMP_ON_EMPTY:
            return None
        name = f"{prefix_idx}_{sanitize_filename(game_title)}_{suffix}_{int(time.time())}.html"
        path = os.path.join(DUMP_DIR, name)
        content = page.content()
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        log_message(f"📝 Сохранён HTML дамп: {path}")
        return path
    except Exception as e:
        log_message(f"⚠️ Не удалось сохранить HTML дамп: {e}")
        return None

def dump_screenshot(page, prefix_idx, game_title, suffix="screenshot"):
    try:
        if not DUMP_ON_EMPTY:
            return None
        name = f"{prefix_idx}_{sanitize_filename(game_title)}_{suffix}_{int(time.time())}.png"
        path = os.path.join(DUMP_DIR, name)
        page.screenshot(path=path, full_page=True)
        log_message(f"📸 Сохранён скриншот: {path}")
        return path
    except Exception as e:
        log_message(f"⚠️ Не удалось сохранить скриншот: {e}")
        return None

def find_best_candidate(candidates, original_title, game_year=None, idx_info=None):
    """
    candidates: list of dict {href,text,context}
    Если DEBUG_CANDIDATES=True — при спорном выборе сохраняет дамп оценок.
    """
    if not candidates:
        return None, 0.0
    orig_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title))
    base_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title.split(":",1)[0].strip()))
    is_slash = '/' in original_title or ',' in original_title
    slash_parts = []
    if is_slash:
        raw_parts = [p.strip() for p in re.split(r'[\/,]', original_title) if p.strip()]
        slash_parts = [clean_title_for_comparison(p) for p in raw_parts]
    best = None; best_score = -1.0
    scores_dump = []

    # exact-clean
    for cand in candidates:
        ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
        if ct == orig_clean:
            return cand, 1.0

    # base + year quick
    if game_year:
        for cand in candidates:
            ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
            if base_clean and base_clean in ct:
                cy = get_year_from_context_text(cand.get("context",""))
                if cy and cy == game_year:
                    return cand, 0.999

    # scoring
    for cand in candidates:
        cand_text = cand.get("text","")
        cand_ctx = cand.get("context","")
        score = calculate_title_similarity(original_title, cand_text)
        if game_year:
            cy = get_year_from_context_text(cand_ctx)
            if cy:
                if cy == game_year:
                    score = max(score, 0.9)
                else:
                    diff = abs(game_year - cy)
                    if diff <= 2:
                        score = max(score, 0.75)
        ct = clean_title_for_comparison(normalize_title_for_comparison(cand_text))
        if base_clean and base_clean in ct:
            score += 0.06
        if is_slash:
            parts_matched = 0
            for p in slash_parts:
                if p and all(tok in ct for tok in p.split()):
                    parts_matched += 1
            min_req = max(1, (len(slash_parts)+1)//2)
            if parts_matched < min_req:
                score -= 0.45
        # clamp
        score = max(0.0, min(1.0, score))
        scores_dump.append({"text": cand_text, "href": cand.get("href",""), "year": get_year_from_context_text(cand_ctx), "score": round(score, 4)})
        if score > best_score:
            best_score = score; best = cand

    # debug: сохраняем оценки, если включено и если best_score < threshold
    if DEBUG_CANDIDATES and idx_info:
        if best_score < DEBUG_SCORE_THRESHOLD:
            try:
                dump_scores_file(idx_info.get("index",0), idx_info.get("title",""), scores_dump)
            except Exception:
                pass

    if best and best_score >= 0.25:
        return best, float(best_score)
    # если спорно — всё равно сохранить дамп кандидатов (если включено)
    if (DEBUG_CANDIDATES or DUMP_ON_EMPTY) and idx_info:
        try:
            dump_candidates_file(idx_info.get("index",0), idx_info.get("title",""), candidates)
        except Exception:
            pass
    return None, 0.0

# ----------------- Parsing HLTB page (как было) -----------------

def round_time(time_str):
    if not time_str:
        return None
    s = str(time_str).replace('½', '.5')
    m = re.search(r'(\d+(?:\.\d+)?)\s*h', s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m2 = re.search(r'(\d+(?:\.\d+)?)\s*Hours?', s, flags=re.IGNORECASE)
    if m2:
        val = float(m2.group(1))
        return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m3 = re.search(r'(\d+)\s*m', s, flags=re.IGNORECASE)
    if m3:
        return f"{int(m3.group(1))}m"
    m4 = re.search(r'(\d+(?:\.\d+)?)', s)
    if m4:
        val = float(m4.group(1))
        if val >= 1:
            return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
        return f"{int(val*60)}m"
    return None

def _parse_time_polled_from_text(text):
    try:
        polled = None
        pm = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)?', text)
        if pm:
            s = pm.group(1)
            if 'k' in s.lower():
                polled = int(float(s.lower().replace('k','')) * 1000)
            else:
                polled = int(float(s))
        time_pat = r'(\d+h\s*\d+m|\d+h|\d+(?:\.\d+)?\s*Hours?|\d+(?:\.\d+)?[½]?)'
        matches = re.findall(time_pat, text)
        if matches:
            avg = matches[0].strip()
            res = {}
            t = round_time(avg)
            if t: res["t"] = t
            if polled: res["p"] = polled
            return res if res else None
        if polled:
            return {"p": polled}
        return None
    except Exception:
        return None

def extract_hltb_data_from_page(page):
    try:
        hltb_data = {}
        content = page.content()
        try:
            tables = page.locator("table")
            for ti in range(tables.count()):
                tbl = ta
