# hltb_worker.py
# Полная версия с улучшениями:
# - несколько вариантов поиска
# - годовой бонус / мгновенный выбор по названию+году
# - fuzzy/difflib + LCS scoring
# - retry с годом при >30 результатов
# - storage_state для Playwright
# - детальные дампы и summary.log
# - совместимость с запуском в GitHub Actions (дампы/артефакты)

import json
import time
import random
import re
import os
from datetime import datetime
from urllib.parse import quote
from difflib import SequenceMatcher
from playwright.sync_api import sync_playwright, Error as PlaywrightError

# ------------- Конфигурация -------------
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/hltb_data.json"
PROGRESS_FILE = "progress.json"

PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

MIN_DELAY = 0.6
MAX_DELAY = 1.6

INITIAL_BACKOFF = 5
BACKOFF_MULTIPLIER = 2.0
MAX_BACKOFF = 300

# Debug / dumps
DEBUG_CANDIDATES = True     # если True — сохраняем оценки кандидатов
DUMP_ON_EMPTY = True        # если True — сохраняем search_html + screenshot + candidates при пустых результатах
DUMP_DIR = "debug_dumps"
DEBUG_SCORE_THRESHOLD = 0.95
VERBOSE = True

# Playwright storage state file (cookies/session) — помогает уменьшить блокировки
STORAGE_STATE_FILE = "playwright_storage.json"

# ------------- Утилиты и логирование -------------
def log_message(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")

def setup_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if DUMP_ON_EMPTY or DEBUG_CANDIDATES:
        os.makedirs(DUMP_DIR, exist_ok=True)

def sanitize_filename(s):
    s = s or "unknown"
    s = re.sub(r'[^\w\-_\. ]', '_', s)
    return s[:120]

# ------------- Парсинг gamesList из index111.html -------------
def extract_games_list(html_file):
    """
    Надёжно извлекает JS-массив const gamesList = [...] из index111.html
    Сохраняет raw и fixed дамп в debug_dumps для диагностики при ошибках.
    """
    def remove_js_comments(s):
        s = re.sub(r'/\*.*?\*/', '', s, flags=re.DOTALL)
        s = re.sub(r'//.*?(?=\r?\n)', '', s)
        return s

    def quote_object_keys(s):
        # Добавляет кавычки к ключам вида keyName:
        s = re.sub(r'([{\[,]\s*)([A-Za-z0-9_\-\$@]+)\s*:', r'\1"\2":', s)
        return s

    def single_to_double_quotes(s):
        s = s.replace("\\'", "'")
        s = re.sub(r"\'([^'\\]*(?:\\.[^'\\]*)*)\'", lambda m: '"' + m.group(1).replace('"', '\\"') + '"', s)
        return s

    def remove_trailing_commas(s):
        s = re.sub(r',\s*(?=[}\]])', '', s)
        return s

    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()

    marker = 'const gamesList ='
    pos = content.find(marker)
    if pos == -1:
        raise ValueError("Не найден 'const gamesList =' в HTML файле")

    start = content.find('[', pos)
    if start == -1:
        raise ValueError("Не найден '[' после 'const gamesList ='")

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
        ts = int(time.time())
        os.makedirs(DUMP_DIR, exist_ok=True)
        raw_path = f"{DUMP_DIR}/gameslist_raw_unclosed_{ts}.html"
        with open(raw_path, "w", encoding="utf-8") as rf:
            rf.write(content[start:start+20000])
        raise ValueError(f"Не найдена закрывающая ']' для gamesList. Дамп: {raw_path}")

    games_js = content[start:end]
    os.makedirs(DUMP_DIR, exist_ok=True)
    ts = int(time.time())
    raw_path = f"{DUMP_DIR}/gameslist_raw_{ts}.js"
    with open(raw_path, "w", encoding="utf-8") as rf:
        rf.write(games_js)
    log_message(f"📝 Сохранён raw gamesList: {raw_path}")

    # Попытка json.loads напрямую
    try:
        games_list = json.loads(games_js)
        log_message("✅ gamesList распарсен напрямую")
        return games_list
    except Exception as e:
        log_message(f"⚠️ Прямой json.loads не прошёл: {e}; пробуем очистку...")

    fixed = games_js
    try:
        fixed = remove_js_comments(fixed)
        fixed = single_to_double_quotes(fixed)
        fixed = quote_object_keys(fixed)
        fixed = remove_trailing_commas(fixed)
        fixed = re.sub(r'\bundefined\b', 'null', fixed)
        fixed = re.sub(r'\bNaN\b', 'null', fixed)
    except Exception as e:
        log_message(f"⚠️ Ошибка очистки gamesList: {e}")

    fixed_path = f"{DUMP_DIR}/gameslist_fixed_{ts}.json"
    with open(fixed_path, "w", encoding="utf-8") as ff:
        ff.write(fixed)
    log_message(f"📝 Сохранён преобразованный gamesList: {fixed_path}")

    try:
        games_list = json.loads(fixed)
        log_message("✅ gamesList распарсен после очистки")
        return games_list
    except Exception as e2:
        msg = f"❌ НЕ удалось распарсить gamesList: {e2}. Посмотрите дампы: {raw_path}, {fixed_path}"
        log_message(msg)
        raise ValueError(msg)

# ------------- Нормализация названий и схожесть -------------
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

def fuzzy_ratio(a, b):
    return SequenceMatcher(None, a, b).ratio()

def calculate_title_similarity(original, candidate):
    if not original or not candidate:
        return 0.0
    try:
        orig_norm = clean_title_for_comparison(normalize_title_for_comparison(original))
        cand_norm = clean_title_for_comparison(normalize_title_for_comparison(candidate))
        if orig_norm == cand_norm:
            return 1.0
        # try arabic <-> roman normalization
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
        base_score = 0.6 * recall + 0.25 * precision + 0.15 * seq
        # combine with fuzzy ratio for robustness
        fuzz = fuzzy_ratio(orig_norm, cand_norm)
        score = max(base_score, 0.55 * base_score + 0.45 * fuzz)
        return float(max(0.0, min(1.0, score)))
    except Exception:
        return 0.0

# ------------- Генерация альтернатив/вариантов поиска -------------
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

def build_search_variants(game_title, game_year=None):
    s = (game_title or "").strip()
    variants = []
    variants.append(s)
    variants.append(s.replace('&', 'and'))
    variants.append(re.sub(r'\(.*?\)', '', s).strip())
    if ':' in s:
        variants.append(s.split(':',1)[0].strip())
    if game_year:
        variants.append(f"{s} {game_year}")
        variants.append(f"{s} \"{game_year}\"")
    variants.append(re.sub(r'[^\w\s]', ' ', s).strip())
    # dedupe
    out = []
    seen = set()
    for v in variants:
        if v and v not in seen:
            seen.add(v); out.append(v)
    return out

# ------------- Скрапинг кандидатов -------------
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

# ------------- Дампы и summary.log -------------
def append_summary_log(prefix_idx, game_title, candidates):
    try:
        os.makedirs(DUMP_DIR, exist_ok=True)
        summary_path = os.path.join(DUMP_DIR, "summary.log")
        top = candidates[:5]
        top_str = " | ".join([f"{c.get('text','')[:80].replace(chr(10),' ')} -> {c.get('href','')}" for c in top])
        with open(summary_path, "a", encoding="utf-8") as sf:
            sf.write(f"{int(time.time())}\t{prefix_idx}\t{game_title}\t{len(candidates)}\t{top_str}\n")
    except Exception:
        pass

def dump_candidates_file(prefix_idx, game_title, candidates, suffix="candidates"):
    try:
        if not (DEBUG_CANDIDATES or DUMP_ON_EMPTY):
            return None
        name = f"{prefix_idx}_{sanitize_filename(game_title)}_{suffix}_{int(time.time())}.json"
        path = os.path.join(DUMP_DIR, name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2)
        log_message(f"🗂️ Сохранён дамп кандидатов: {path}")
        append_summary_log(prefix_idx, game_title, candidates)
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

# ------------- Ранжирование кандидатов -------------
def find_best_candidate(candidates, original_title, game_year=None, idx_info=None):
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

    # exact-clean fast path
    for cand in candidates:
        ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
        if ct == orig_clean:
            return cand, 1.0

    # immediate year+base match: если базовое название содержится и есть точно совпадающий год в контексте
    if game_year:
        for cand in candidates:
            ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
            if base_clean and base_clean in ct:
                cy = get_year_from_context_text(cand.get("context",""))
                if cy and cy == game_year:
                    log_message(f"🎯 Полный матч по названию+году: {cand.get('text')} (year {cy})")
                    return cand, 0.999

    # scoring loop
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
        score = max(0.0, min(1.0, score))
        scores_dump.append({"text": cand_text, "href": cand.get("href",""), "year": get_year_from_context_text(cand_ctx), "score": round(score, 4)})
        if score > best_score:
            best_score = score; best = cand

    # debug: preserve scores dump if requested
    if DEBUG_CANDIDATES and idx_info:
        if best_score < DEBUG_SCORE_THRESHOLD:
            try:
                dump_scores_file(idx_info.get("index",0), idx_info.get("title",""), scores_dump)
            except Exception:
                pass

    if best and best_score >= 0.25:
        return best, float(best_score)

    # in ambiguous cases, save candidates for inspection
    if (DEBUG_CANDIDATES or DUMP_ON_EMPTY) and idx_info:
        try:
            dump_candidates_file(idx_info.get("index",0), idx_info.get("title",""), candidates)
        except Exception:
            pass
    return None, 0.0

# ------------- Извлечение данных со страницы игры -------------
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
        val = float(m2.group(1)); return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m3 = re.search(r'(\d+)\s*m', s, flags=re.IGNORECASE)
    if m3: return f"{int(m3.group(1))}m"
    m4 = re.search(r'(\d+(?:\.\d+)?)', s)
    if m4:
        val = float(m4.group(1))
        if val >= 1: return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
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
                tbl = tables.nth(ti)
                ttxt = tbl.inner_text()
                if any(k in ttxt for k in ["Main Story","Main + Extras","Completionist","Co-Op","Vs.","Competitive","Single-Player"]):
                    rows = tbl.locator("tr")
                    for ri in range(rows.count()):
                        rtxt = rows.nth(ri).inner_text()
                        if "Main Story" in rtxt or "Single-Player" in rtxt:
                            d = _parse_time_polled_from_text(rtxt)
                            if d: hltb_data["ms"] = d
                        if "Main + Extras" in rtxt:
                            d = _parse_time_polled_from_text(rtxt)
                            if d: hltb_data["mpe"] = d
                        if "Completionist" in rtxt:
                            d = _parse_time_polled_from_text(rtxt)
                            if d: hltb_data["comp"] = d
                        if "Co-Op" in rtxt:
                            d = _parse_time_polled_from_text(rtxt)
                            if d: hltb_data["coop"] = d
                        if "Vs." in rtxt or "Competitive" in rtxt:
                            d = _parse_time_polled_from_text(rtxt)
                            if d: hltb_data["vs"] = d
        except Exception:
            pass

        # Text-based fallback search around keywords
        try:
            for keyword, key in [("Vs.", "vs"), ("Co-Op", "coop"), ("Single-Player", "ms")]:
                elems = page.locator(f"text={keyword}")
                for i in range(min(elems.count(), 6)):
                    try:
                        el = elems.nth(i)
                        surrounding = el.evaluate("(e) => (e.closest('div') || e.parentElement || e).innerText")
                        if key == "vs" and "vs" not in hltb_data:
                            m = re.search(r'(?:Vs\.|Versus)[^\d]{0,40}?(\d+(?:\.\d+)?(?:½)?)', surrounding, flags=re.IGNORECASE)
                            if m:
                                hltb_data["vs"] = {"t": round_time(m.group(1))}
                        if key == "coop" and "coop" not in hltb_data:
                            m = re.search(r'Co-Op[^\d]{0,40}?(\d+(?:\.\d+)?(?:½)?)', surrounding, flags=re.IGNORECASE)
                            if m:
                                hltb_data["coop"] = {"t": round_time(m.group(1))}
                        if key == "ms" and "ms" not in hltb_data:
                            m = re.search(r'(?:Single-Player|Main Story)[^\d]{0,40}?(\d+(?:\.\d+)?(?:½)?)', surrounding, flags=re.IGNORECASE)
                            if m:
                                hltb_data["ms"] = {"t": round_time(m.group(1))}
                    except Exception:
                        continue
        except Exception:
            pass

        if not hltb_data:
            patterns = {
                "ms": r'(?:Main Story|Single-Player)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "mpe": r'(?:Main \+ Extras)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "comp": r'(?:Completionist)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "coop": r'(?:Co-Op)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)',
                "vs": r'(?:Vs\.|Versus)[^\n]{0,160}?(\d+(?:\.\d+)?(?:½)?\s*h?)'
            }
            for k, p in patterns.items():
                m = re.search(p, content, flags=re.IGNORECASE)
                if m:
                    hltb_data[k] = {"t": round_time(m.group(1))}
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
            hltb_data["stores"] = stores
        return hltb_data if hltb_data else None
    except Exception as e:
        log_message(f"❌ Ошибка extract_hltb_data_from_page: {e}")
        return None

# ------------- Поисковая попытка (варианты) -------------
def random_delay(min_s=MIN_DELAY, max_s=MAX_DELAY):
    time.sleep(random.uniform(min_s, max_s))

def is_blocked_content(page_content):
    s = page_content.lower()
    checks = ["access denied", "checking your browser", "cloudflare", "please enable javascript", "are you human"]
    return any(c in s for c in checks)

def save_storage_state(context):
    try:
        context.storage_state(path=STORAGE_STATE_FILE)
    except Exception:
        pass

def load_storage_state_if_exists():
    if os.path.exists(STORAGE_STATE_FILE):
        return STORAGE_STATE_FILE
    return None

def search_game_single_attempt(page, game_title, game_year=None, idx_info=None):
    """
    Попытка поиска — возвращает ((hltb_data, found_title, score), None)
    или (None, "blocked") или (None, None)
    """
    try:
        log_message(f"🔍 Ищем: '{game_title}'")
        variants = build_search_variants(game_title, game_year)
        # try variants in order
        for variant in variants:
            safe_title = quote(variant, safe="")
            search_url = f"{BASE_URL}/?q={safe_title}"
            try:
                page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
            except PlaywrightError as e:
                log_message(f"⚠️ Page.goto error for '{variant}': {e}")
            # small wait for selector
            try:
                page.wait_for_selector('a[href^="/game/"]', timeout=3500)
            except:
                pass
            random_delay()
            page_content = page.content()
            if is_blocked_content(page_content):
                log_message("❌ Возможная блокировка/Cloudflare на странице поиска")
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_html(page, idx_info.get("index",0), idx_info.get("title",""))
                    dump_screenshot(page, idx_info.get("index",0), idx_info.get("title",""))
                return None, "blocked"

            candidates = scrape_game_link_candidates(page, max_candidates=120)
            if not candidates:
                log_message("⚠️ Кандидатов 0 — короткий reload (fallback)")
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_html(page, idx_info.get("index",0), idx_info.get("title",""))
                    dump_screenshot(page, idx_info.get("index",0), idx_info.get("title",""))
                try:
                    page.reload(timeout=PAGE_GOTO_TIMEOUT)
                    try:
                        page.wait_for_selector('a[href^=\"/game/\"]', timeout=2000)
                    except:
                        pass
                    random_delay(0.6, 1.2)
                    candidates = scrape_game_link_candidates(page, max_candidates=120)
                except Exception:
                    candidates = []

            if not candidates:
                if idx_info and (DEBUG_CANDIDATES or DUMP_ON_EMPTY):
                    dump_candidates_file(idx_info.get("index",0), idx_info.get("title",""), candidates)
                continue  # try next variant

            # если слишком много кандидатов — повторим поиск с годом
            if len(candidates) > 30 and game_year:
                try:
                    q = f"{variant} {game_year}"
                    log_message(f"⚠️ Слишком много результатов ({len(candidates)}), повторный поиск: '{q}'")
                    page.goto(f"{BASE_URL}/?q={quote(q, safe='')}", timeout=PAGE_GOTO_TIMEOUT)
                    try:
                        page.wait_for_selector('a[href^=\"/game/\"]', timeout=3000)
                    except:
                        pass
                    random_delay()
                    candidates = scrape_game_link_candidates(page, max_candidates=160)
                except Exception:
                    pass

            # rank candidates
            best_cand, score = find_best_candidate(candidates, game_title, game_year, idx_info=idx_info)

            # debug dumps if ambiguous
            if idx_info and DEBUG_CANDIDATES and score < DEBUG_SCORE_THRESHOLD:
                dump_candidates_file(idx_info.get("index",0), idx_info.get("title",""), candidates)

            if not best_cand:
                # try next variant
                continue

            href = best_cand.get("href")
            if not href:
                continue
            full_url = f"{BASE_URL}{href}"
            try:
                page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT)
            except PlaywrightError:
                log_message(f"⚠️ Ошибка перехода на страницу игры: {full_url}")
            try:
                page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            except:
                pass
            random_delay()
            page_content = page.content()
            if is_blocked_content(page_content):
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_html(page, idx_info.get("index",0), idx_info.get("title",""))
                    dump_screenshot(page, idx_info.get("index",0), idx_info.get("title",""))
                return None, "blocked"
            hltb_data = extract_hltb_data_from_page(page)
            if hltb_data:
                # save storage state after successful fetch
                try:
                    save_storage_state(page.context)
                except Exception:
                    pass
                return (hltb_data, best_cand["text"], score), None
            else:
                if idx_info and DUMP_ON_EMPTY:
                    dump_search_html(page, idx_info.get("index",0), idx_info.get("title",""))
                    dump_screenshot(page, idx_info.get("index",0), idx_info.get("title",""))
                # try next variant
                continue

        # tried all variants
        return None, None

    except Exception as e:
        log_message(f"⚠️ Ошибка search_game_single_attempt('{game_title}'): {e}")
        if idx_info and DUMP_ON_EMPTY:
            try:
                dump_search_html(page, idx_info.get("index",0), idx_info.get("title",""))
                dump_screenshot(page, idx_info.get("index",0), idx_info.get("title",""))
            except:
                pass
        return None, None

# ------------- Координация попыток поиска -------------
def search_game_on_hltb(page, game_title, game_year=None, backoff_base=0):
    max_attempts = 3
    backoff = backoff_base
    best_result = None
    best_score = 0.0

    for attempt in range(max_attempts):
        if attempt > 0:
            if backoff > 0:
                delay = backoff
            else:
                delay = random.uniform(3, 6) if attempt == 1 else random.uniform(6, 12)
            log_message(f"🔄 Попытка {attempt+1}/{max_attempts} для '{game_title}' — пауза {int(delay)}s")
            time.sleep(delay)

        idx_info = {"index": 0, "title": game_title}
        outcome, status = search_game_single_attempt(page, game_title, game_year, idx_info=idx_info)
        if isinstance(outcome, tuple) and outcome[0] is not None:
            hltb, found_title, score = outcome
            if score >= 0.98:
                log_message(f"🎯 Найдено идеальное совпадение: '{found_title}' (score {score:.2f})")
                return hltb, 0
            if score > best_score:
                best_score = score; best_result = hltb
            if score >= 0.7:
                return hltb, 0
        else:
            if status == "blocked":
                backoff = max(INITIAL_BACKOFF if backoff_base<=0 else backoff_base * BACKOFF_MULTIPLIER, INITIAL_BACKOFF)
                backoff = min(backoff, MAX_BACKOFF)
                log_message(f"🚫 Обнаружена блокировка — backoff -> {int(backoff)}s")
                time.sleep(backoff)
                continue

        # альтернативные названия (генерируем осторожно)
        alts = generate_alternative_titles(game_title)
        for alt in alts:
            if alt == game_title:
                continue
            outcome_alt, status_alt = search_game_single_attempt(page, alt, game_year, idx_info={"index":0,"title":alt})
            if isinstance(outcome_alt, tuple) and outcome_alt[0] is not None:
                hltb, found_title, score = outcome_alt
                if score >= 0.98:
                    log_message(f"🎯 Найден идеальный результат для альтернативы '{alt}': '{found_title}'")
                    return hltb, 0
                if score > best_score:
                    best_score = score; best_result = hltb
            elif status_alt == "blocked":
                backoff = max(INITIAL_BACKOFF if backoff_base<=0 else backoff_base * BACKOFF_MULTIPLIER, INITIAL_BACKOFF)
                backoff = min(backoff, MAX_BACKOFF)
                log_message(f"🚫 Блок при альтернативном поиске — backoff -> {int(backoff)}s")
                time.sleep(backoff)
                continue

    if best_result:
        log_message(f"🏆 Возвращаю лучший доступный результат (score {best_score:.2f})")
        return best_result, max(0, backoff)
    return None, max(0, backoff)

# ------------- Сохранение и основной цикл -------------
def save_results(games_data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i, game in enumerate(games_data):
            if i > 0: f.write("\n")
            json.dump(game, f, separators=(',', ':'), ensure_ascii=False)
    log_message(f"💾 Результаты сохранены: {OUTPUT_FILE}")

def save_progress(current_index, total_games):
    progress_data = {"current_index": current_index, "total_games": total_games, "last_updated": datetime.now().isoformat()}
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)

def update_html_with_hltb(html_file, hltb_data):
    try:
        with open(html_file, 'r', encoding='utf-8') as f: content = f.read()
        start = content.find('const gamesList = ')
        if start == -1: return False
        start = content.find('[', start)
        bracket_count = 0; end = start
        for i,ch in enumerate(content[start:], start):
            if ch == '[': bracket_count += 1
            elif ch == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end = i+1; break
        new = content[:start] + json.dumps(hltb_data, ensure_ascii=False) + content[end:]
        with open(html_file, 'w', encoding='utf-8') as f: f.write(new)
        return True
    except Exception as e:
        log_message(f"❌ Ошибка update_html_with_hltb: {e}")
        return False

def main():
    log_message("🚀 Запуск HLTB Worker (улучшенная версия)")
    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден")
        return
    setup_dirs()
    try:
        games_list = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        log_message(f"💥 Не удалось извлечь games_list: {e}")
        raise

    total_games = len(games_list)
    log_message(f"📄 Извлечено {total_games} игр")

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f: prog = json.load(f)
            start_index = prog.get("current_index", 0)
            log_message(f"📂 Продолжаем с {start_index}")
        except Exception:
            start_index = 0

    backoff_state = 0
    processed = 0

    storage_path = load_storage_state_if_exists()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # создаём context с storage_state если есть
        context_args = {
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "viewport": {"width":1280,"height":800},
            "locale":"en-US",
        }
        try:
            if storage_path:
                context_args["storage_state"] = storage_path
        except Exception:
            pass
        context = browser.new_context(**context_args)
        page = context.new_page()

        try:
            page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            log_message("✅ HowLongToBeat доступен")
            # save storage state after a successful landing page
            try:
                save_storage_state(context)
            except:
                pass
        except Exception as e:
            log_message(f"⚠️ Ошибка при открытии сайта: {e}")

        for i in range(start_index, total_games):
            game = games_list[i]
            title = game.get("title") or ""
            year = game.get("year")
            log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {title} ({year})")
            idx_info = {"index": i+1, "title": title}
            hltb_data, new_backoff = search_game_on_hltb(page, title, year, backoff_base=backoff_state)
            if new_backoff and new_backoff > backoff_state:
                backoff_state = new_backoff
            else:
                backoff_state = max(0, backoff_state * 0.6)
            if hltb_data:
                game["hltb"] = hltb_data
                processed += 1
                log_message(f"✅ Данные найдены для '{title}'")
            else:
                game["hltb"] = {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A"}
                log_message(f"⚠️ Данные не найдены для: {title} - записано N/A")
            # периодическое сохранение прогресса и результатов
            if (i+1) % 10 == 0:
                save_progress(i+1, total_games)
                save_results(games_list)
            if backoff_state >= 30:
                log_message(f"⏸️ Sleeping backoff_state {int(backoff_state)}s")
                time.sleep(backoff_state)
            else:
                random_delay()
        try:
            browser.close()
        except:
            pass

    save_results(games_list)
    if update_html_with_hltb(GAMES_LIST_FILE, games_list):
        log_message("✅ HTML обновлён")
    log_message(f"🎉 Готово — обработано {processed}/{total_games} игр")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"💥 Критическая ошибка: {e}")
        raise
