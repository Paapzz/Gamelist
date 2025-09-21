# hltb_worker.py
# Версия: оптимизация скорости + детерминированный выбор совпадений + adaptive backoff
# Замените старый файл этим.
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

# Таймауты (умеренные, чтобы не слишком быстро считать таймаутами)
PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

# Rate control: целевые значения для throughput
# Для 1000 игр за 6 часов среднее время ~21.6 секунд/игру.
# Мы целимся порядка 12-20 сек/игра при нормальном поведении.
MIN_DELAY = 0.6    # минимальная пауза между действиями
MAX_DELAY = 1.6    # небольшая случайная пауза (ускорено)
# Поведение при блокировках
INITIAL_BACKOFF = 5      # секунда начального бэкоффа при проблемах
BACKOFF_MULTIPLIER = 2.0 # множитель при повторной проблеме
MAX_BACKOFF = 300        # максимум бэкоффа 5 минут

# Переменные для логирования/отладки
DEBUG_CANDIDATES = True  # включите True для дампа кандидатов на спорные названия
VERBOSE = True

def log_message(message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {message}")

def setup_directories():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# -------------------- Утилиты для нормализации/сравнения --------------------

def clean_title_for_comparison(title):
    if not title:
        return ""
    s = title
    s = s.replace('\u2018','\'').replace('\u2019','\'').replace('\u201c','"').replace('\u201d','"')
    s = s.lower()
    s = re.sub(r'\(.*?\)', ' ', s)         # убираем скобочные части (часто лишние)
    s = re.sub(r'[\u2010-\u2015]', '-', s) # normalize dashes
    s = re.sub(r'[^0-9a-zа-яё\s]', ' ', s) # разрешаем буквы/цифры/пробел (русские на случай)
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
    """
    Детерминированная метрика:
    - сначала быстрые exact-сравнения на очищённых строках (если совпадают -> 1.0)
    - совпадение базовой части (до ':') даёт сильный буст
    - далее LCS/recall/precision комбинированная оценка
    """
    if not original or not candidate:
        return 0.0
    try:
        orig_norm = clean_title_for_comparison(normalize_title_for_comparison(original))
        cand_norm = clean_title_for_comparison(normalize_title_for_comparison(candidate))
        # exact
        if orig_norm == cand_norm:
            return 1.0
        # try arabic <-> roman quick swaps on original (cover cases like 'III'/'3')
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

        # base (before colon) boost
        base = original.split(":",1)[0].strip()
        base_norm = clean_title_for_comparison(normalize_title_for_comparison(base))
        if base_norm and base_norm == cand_norm:
            return 0.98  # почти идеальное

        # token-based metric
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
    """Генерация альтернатив: slash -> and/& combos, base before colon, roman/arabic variants."""
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
    # slash/comma separators
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
        add(base)
        add(game_title)
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

# ------------------ Быстрая загрузка кандидатов (speed improvement) ------------------

def scrape_game_link_candidates(page, max_candidates=80):
    """
    Быстро получает список кандидатов (href, text, context_text) с search-страницы
    с помощью одного evaluate -> уменьшает количество roundtrips.
    """
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
                }} catch(e) {{ /* ignore */ }}
            }}
            return out;
        }}
        '''
        raw = page.eval_on_selector_all('a[href^="/game/"]', script, max_candidates)
        # Normalize to Python list of dicts
        candidates = []
        for r in raw:
            if not r: continue
            href = r.get("href","") or ""
            txt = r.get("text","") or ""
            ctx = r.get("ctx","") or ""
            candidates.append({"href": href, "text": txt.strip(), "context": ctx.strip()})
        return candidates
    except Exception as e:
        # fallback: empty list
        return []

def get_year_from_context_text(ctx_text):
    """Ищем 4-значный год в контексте"""
    if not ctx_text:
        return None
    m = re.findall(r'(\b19\d{2}\b|\b20\d{2}\b)', ctx_text)
    if m:
        years = [int(x) for x in m]
        return min(years)
    return None

# ------------------ Выбор лучшего кандидата ------------------

def find_best_candidate(candidates, original_title, game_year=None):
    """
    candidates: list of dict {href,text,context}
    Возвращает (candidate_dict, score) или (None,0.0)
    Детерминированно выбираем:
      - сначала exact-clean совпадение
      - затем base + year match
      - затем комбинированный скоринг
    """
    if not candidates:
        return None, 0.0
    orig_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title))
    base_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title.split(":",1)[0].strip()))
    is_slash = '/' in original_title or ',' in original_title
    # prepare slash parts
    slash_parts = []
    if is_slash:
        raw_parts = [p.strip() for p in re.split(r'[\/,]', original_title) if p.strip()]
        slash_parts = [clean_title_for_comparison(p) for p in raw_parts]

    best = None
    best_score = -1.0

    # 1) exact-clean search (fast)
    for cand in candidates:
        ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
        if ct == orig_clean:
            return cand, 1.0

    # 2) base + year quick win
    if game_year:
        for cand in candidates:
            ct = clean_title_for_comparison(normalize_title_for_comparison(cand["text"]))
            if base_clean and base_clean in ct:
                cy = get_year_from_context_text(cand.get("context",""))
                if cy and cy == game_year:
                    return cand, 0.999

    # 3) scoring loop
    for cand in candidates:
        cand_text = cand.get("text","")
        cand_ctx = cand.get("context","")
        # compute similarity
        score = calculate_title_similarity(original_title, cand_text)
        # year score boost
        if game_year:
            cy = get_year_from_context_text(cand_ctx)
            if cy:
                if cy == game_year:
                    score = max(score, 0.9)
                else:
                    diff = abs(game_year - cy)
                    if diff <= 2:
                        score = max(score, 0.75)
        # base containment boost
        ct = clean_title_for_comparison(normalize_title_for_comparison(cand_text))
        if base_clean and base_clean in ct:
            score += 0.06

        # slash handling: require minimal matching parts
        if is_slash:
            parts_matched = 0
            for p in slash_parts:
                if p and all(tok in ct for tok in p.split()):
                    parts_matched += 1
            min_req = max(1, (len(slash_parts)+1)//2)  # ceil(N/2)
            if parts_matched < min_req:
                score -= 0.45  # penalty for not matching enough parts

        if score > best_score:
            best_score = score
            best = cand

    # threshold to accept
    if best and best_score >= 0.25:
        return best, float(max(0.0, min(1.0, best_score)))
    return None, 0.0

# ------------------ Извлечение HLTB-данных со страницы ------------------

def round_time(time_str):
    if not time_str:
        return None
    s = str(time_str).replace('½', '.5')
    m = re.search(r'(\d+(?:\.\d+)?)\s*h', s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m = re.search(r'(\d+(?:\.\d+)?)\s*hours?', s, flags=re.IGNORECASE)
    if m:
        val = float(m.group(1))
        return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
    m = re.search(r'(\d+)\s*m', s, flags=re.IGNORECASE)
    if m:
        return f"{int(m.group(1))}m"
    m = re.search(r'(\d+(?:\.\d+)?)', s)
    if m:
        val = float(m.group(1))
        if val >= 1:
            return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
        return f"{int(val*60)}m"
    return None

def extract_hltb_data_from_page(page):
    """
    Попытка извлечения данных с таргетной страницы игры.
    Используем таблицы -> текстовые блоки -> fallback regex.
    """
    try:
        hltb_data = {}
        content = page.content()

        # 1) Таблицы
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

        # 2) Блоки текста (Co-Op, Vs., Single-Player)
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

        # 3) Fallback regex по HTML целиком
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

        # stores
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
        log_message(f"❌ Ошибка извлечения данных со страницы: {e}")
        return None

def _parse_time_polled_from_text(text):
    """Парсер строки таблицы: ищет первое время и (опционально) polled"""
    try:
        polled = None
        pm = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)?', text)
        if pm:
            s = pm.group(1)
            if 'k' in s.lower():
                polled = int(float(s.lower().replace('k','')) * 1000)
            else:
                polled = int(float(s))
        # первое время
        time_pat = r'(\d+h\s*\d+m|\d+h|\d+(?:\.\d+)?\s*Hours?|\d+(?:\.\d+)?[½]?)'
        matches = re.findall(time_pat, text)
        if matches:
            avg = matches[0].strip()
            res = {}
            t = round_time(avg)
            if t:
                res["t"] = t
            if polled:
                res["p"] = polled
            return res if res else None
        if polled:
            return {"p": polled}
        return None
    except Exception as e:
        return None

# ------------------ Search attempt + adaptive backoff ------------------

def random_delay(min_s=MIN_DELAY, max_s=MAX_DELAY):
    time.sleep(random.uniform(min_s, max_s))

def search_game_single_attempt(page, game_title, game_year=None, backoff=0):
    """
    Одна попытка поиска: использует scrape_game_link_candidates для быстродействия.
    backoff — дополнительная пауза (сек) перед началом (используется при блокировках).
    """
    try:
        if backoff > 0:
            log_message(f"⏳ Backoff {backoff}s перед попыткой поиска '{game_title}'")
            time.sleep(backoff)

        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"

        page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        random_delay()

        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower() or ("checking your browser" in page_content.lower() and "cloudflare" in page_content.lower()):
            log_message("❌ Возможная блокировка при поиске (страница содержит предупреждение)")
            return None, "blocked"

        candidates = scrape_game_link_candidates(page, max_candidates=80)
        if DEBUG_CANDIDATES and VERBOSE:
            log_message(f"🔎 Кандидатов на странице: {len(candidates)}")

        if len(candidates) > 30:
            # пробуем точный поиск в кавычках (быстрый повтор)
            quoted = f'"{game_title}"'
            page.goto(f"{BASE_URL}/?q={quote(quoted, safe='')}", timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            random_delay()
            page_content = page.content()
            if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                return None, "blocked"
            candidates = scrape_game_link_candidates(page, max_candidates=80)

        if not candidates:
            return None, None

        best_cand, score = find_best_candidate(candidates, game_title, game_year)
        # Debug print of candidates if ambiguous
        if DEBUG_CANDIDATES and VERBOSE:
            if best_cand is None or score < 0.95:
                dump = [{"text": c["text"], "href": c["href"], "year": get_year_from_context_text(c["context"])} for c in candidates[:20]]
                log_message(f"🔍 Список кандидатов (обрезка): {json.dumps(dump, ensure_ascii=False)[:800]}")

        if not best_cand:
            return None, None

        # Навигация на страницу избранного кандидата (единственный переход)
        href = best_cand.get("href")
        if not href:
            return None, None
        full_url = f"{BASE_URL}{href}"
        page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        random_delay()

        # Проверка блокировки на странице игры
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            return None, "blocked"

        hltb_data = extract_hltb_data_from_page(page)
        if hltb_data:
            return (hltb_data, best_cand["text"], score), None
        else:
            return None, None
    except Exception as e:
        log_message(f"⚠️ Ошибка search_game_single_attempt для '{game_title}': {e}")
        return None, None

def search_game_on_hltb(page, game_title, game_year=None, backoff_base=0):
    """
    Ретрайлогика с adaptive backoff.
    backoff_base — текущее значение backoff (сек) от предыдущих блокировок; возвращает (result, new_backoff)
    """
    max_attempts = 3
    backoff = backoff_base
    best_result = None
    best_score = 0.0

    for attempt in range(max_attempts):
        if attempt > 0:
            # при повторной попытке небольшая пауза; если backoff задан - используем его
            if backoff > 0:
                delay = backoff
            else:
                delay = random.uniform(3, 6) if attempt == 1 else random.uniform(6, 12)
            log_message(f"🔄 Попытка {attempt+1}/{max_attempts} для '{game_title}' — пауза {int(delay)}s")
            time.sleep(delay)

        result, status = search_game_single_attempt(page, game_title, game_year, backoff=0 if status_not_block(status:=None) else 0)
        # note: status variable set by function; to keep code safe we detect above via return tuple

        # search_game_single_attempt returns either ((hltb_data, found_title, score), None) or (None, "blocked"/None)
        if isinstance(result, tuple) and result:
            # success
            hltb, found_title, score = result
            # perfect match if score >= 0.98
            if score >= 0.98:
                log_message(f"🎯 Найдено идеальное совпадение: '{found_title}' (score {score:.2f})")
                return hltb, 0  # reset backoff on success
            # keep best
            if score > best_score:
                best_score = score
                best_result = hltb
            # if reasonable, return
            if score >= 0.7:
                return hltb, 0
            # otherwise, try alternatives
        else:
            # result is None; status maybe "blocked"
            if status == "blocked":
                # increase backoff
                backoff = max(backoff_base * BACKOFF_MULTIPLIER if backoff_base>0 else INITIAL_BACKOFF, INITIAL_BACKOFF)
                backoff = min(backoff, MAX_BACKOFF)
                log_message(f"🚫 Обнаружена блокировка — увеличиваем backoff до {int(backoff)}s")
                # Sleep a bit before next attempt
                time.sleep(backoff)
                continue
            # else try alternatives
        # alternatives
        alts = generate_alternative_titles(game_title)
        for alt in alts:
            if alt == game_title:
                continue
            # small quick attempts for alternatives
            res, st = search_game_single_attempt(page, alt, game_year)
            if isinstance(res, tuple) and res:
                hltb, found_title, score = res
                if score >= 0.98:
                    log_message(f"🎯 Найден идеальный результат для альтернативы '{alt}': '{found_title}'")
                    return hltb, 0
                if score > best_score:
                    best_score = score
                    best_result = hltb
            elif st == "blocked":
                backoff = max(backoff_base*BACKOFF_MULTIPLIER if backoff_base>0 else INITIAL_BACKOFF, INITIAL_BACKOFF)
                backoff = min(backoff, MAX_BACKOFF)
                log_message(f"🚫 Блок при альтернативном поиске — backoff -> {int(backoff)}s")
                time.sleep(backoff)
                continue

    if best_result:
        log_message(f"🏆 Возвращаем лучший доступный результат (score {best_score:.2f})")
        return best_result, max(0, backoff)
    return None, max(0, backoff)

def status_not_block(s):
    return s != "blocked"

# ------------------ Сохранение/утилиты ------------------

def save_results(games_data):
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i, game in enumerate(games_data):
            if i > 0:
                f.write("\n")
            json.dump(game, f, separators=(',', ':'), ensure_ascii=False)
    log_message(f"💾 Результаты сохранены в {OUTPUT_FILE}")

def save_progress(games_data, current_index, total_games):
    progress_data = {
        "current_index": current_index,
        "total_games": total_games,
        "last_updated": datetime.now().isoformat()
    }
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, ensure_ascii=False, indent=2)

def update_html_with_hltb(html_file, hltb_data):
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        start = content.find('const gamesList = ')
        if start == -1:
            return False
        start = content.find('[', start)
        bracket_count = 0
        end = start
        for i,ch in enumerate(content[start:], start):
            if ch == '[': bracket_count += 1
            elif ch == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end = i+1
                    break
        new = content[:start] + json.dumps(hltb_data, ensure_ascii=False) + content[end:]
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(new)
        return True
    except Exception as e:
        log_message(f"❌ Ошибка обновления HTML: {e}")
        return False

# ------------------ main: loop + adaptive backoff control ------------------

def main():
    log_message("🚀 Запуск HLTB Worker (оптимизированная версия)")
    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден")
        return
    setup_directories()
    with open(GAMES_LIST_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    # извлекаем gamesList массив
    start = content.find('const gamesList = ')
    if start == -1:
        log_message("❌ Не найден gamesList в HTML")
        return
    start = content.find('[', start)
    bracket_count = 0
    end = start
    for i,ch in enumerate(content[start:], start):
        if ch == '[': bracket_count += 1
        elif ch == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end = i+1
                break
    games_list = json.loads(content[start:end])
    total_games = len(games_list)
    log_message(f"📄 Извлечено {total_games} игр")

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                prog = json.load(f)
            start_index = prog.get("current_index", 0)
            log_message(f"📂 Продолжаем с позиции {start_index}")
        except:
            start_index = 0

    backoff_state = 0  # хранит текущее значение backoff (сек)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            viewport={"width":1280, "height":800},
            locale="en-US"
        )
        page = context.new_page()
        try:
            page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            log_message("✅ HowLongToBeat доступен")
        except Exception as e:
            log_message(f"⚠️ Ошибка при проверке сайта: {e}")

        start_time = time.time()
        processed = 0
        for i in range(start_index, total_games):
            game = games_list[i]
            title = game.get("title") or ""
            year = game.get("year")
            log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {title} ({year})")

            hltb_data, new_backoff = search_game_on_hltb(page, title, year, backoff_base=backoff_state)
            # update backoff state: if new_backoff > 0 -> set it; otherwise decay
            if new_backoff and new_backoff > backoff_state:
                backoff_state = new_backoff
            else:
                # gentle decay of backoff when things good
                backoff_state = max(0, backoff_state * 0.6)

            if hltb_data:
                game["hltb"] = hltb_data
                processed += 1
                log_message(f"✅ Данные найдены для '{title}'")
            else:
                game["hltb"] = {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A"}
                log_message(f"⚠️ Данные не найдены для: {title} - записано N/A")

            # сохраняем прогресс и небольшая пауза
            if (i+1) % 25 == 0:
                save_progress(games_list, i+1, total_games)
            # адаптивная маленькая пауза (если backoff_state большой — уходим в sleep)
            if backoff_state >= 30:
                log_message(f"⏸️ Ждем backoff_state {int(backoff_state)}s из-за предыдущих ошибок/блокировок")
                time.sleep(backoff_state)
            else:
                random_delay()

        browser.close()

    save_results(games_list)
    if update_html_with_hltb(GAMES_LIST_FILE, games_list):
        log_message("✅ HTML файл обновлён")
    log_message(f"🎉 Готово — обработано {processed}/{total_games} игр")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"💥 Критическая ошибка: {e}")
        raise
