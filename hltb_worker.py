# hltb_worker.py
# Обновлённая версия с улучшенной логикой выбора совпадений (Doom II, Pokémon, Tetris, Half-Life 2 и т.д.)
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

PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

BREAK_INTERVAL_MIN = 8 * 60
BREAK_INTERVAL_MAX = 10 * 60
BREAK_DURATION_MIN = 40
BREAK_DURATION_MAX = 80

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def setup_directories():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def extract_games_list(html_file):
    with open(html_file, 'r', encoding='utf-8') as f:
        content = f.read()
    start = content.find('const gamesList = ')
    if start == -1:
        raise ValueError("Не найден const gamesList в HTML файле")
    start = content.find('[', start)
    # находим соответствующую закрывающую скобку
    bracket_count = 0
    end = start
    for i, ch in enumerate(content[start:], start):
        if ch == '[':
            bracket_count += 1
        elif ch == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end = i + 1
                break
    games_json = content[start:end]
    games_list = json.loads(games_json)
    return games_list

# ------------------ Вспомогательные парсеры названий/времени ------------------

def clean_title_for_comparison(title):
    if not title:
        return ""
    s = title.lower()
    s = re.sub(r'[\u2018\u2019\u201c\u201d]', "'", s)  # normalize quotes
    s = re.sub(r'[^\w\s]', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s

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
        return str(roman_to_arabic_map.get(roman_str, roman_str))
    except:
        return roman_str

def normalize_title_for_comparison(title):
    if not title:
        return ""
    s = f" {title} "
    roman_to_arabic = {
        ' I ': ' 1 ', ' II ': ' 2 ', ' III ': ' 3 ', ' IV ': ' 4 ', ' V ': ' 5 ',
        ' VI ': ' 6 ', ' VII ': ' 7 ', ' VIII ': ' 8 ', ' IX ': ' 9 ', ' X ': ' 10 '
    }
    for r,a in roman_to_arabic.items():
        s = s.replace(r, a)
    return s.strip()

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
        def sim(a, b):
            a_c = clean_title_for_comparison(normalize_title_for_comparison(a))
            b_c = clean_title_for_comparison(normalize_title_for_comparison(b))
            if a_c == b_c:
                return 1.0
            a_tokens = a_c.split()
            b_tokens = b_c.split()
            if not a_tokens or not b_tokens:
                return 0.0
            common = set(a_tokens).intersection(set(b_tokens))
            precision = len(common) / len(b_tokens)
            recall = len(common) / len(a_tokens)
            lcs = lcs_length(a_tokens, b_tokens)
            seq = lcs / len(a_tokens) if len(a_tokens) > 0 else 0
            score = 0.6 * recall + 0.25 * precision + 0.15 * seq
            return max(0.0, min(1.0, score))
        # direct exact (ignore punctuation)
        if clean_title_for_comparison(original) == clean_title_for_comparison(candidate):
            return 1.0
        # try roman/arabic swaps
        arab = re.search(r'\b(\d+)\b', original)
        if arab:
            roman = convert_arabic_to_roman(arab.group(1))
            if roman and clean_title_for_comparison(re.sub(r'\b'+re.escape(arab.group(1))+r'\b', roman, original)) == clean_title_for_comparison(candidate):
                return 1.0
        rom = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', original)
        if rom:
            arabic = convert_roman_to_arabic(rom.group(1))
            if arabic and clean_title_for_comparison(re.sub(r'\b'+re.escape(rom.group(1))+r'\b', arabic, original)) == clean_title_for_comparison(candidate):
                return 1.0
        return float(sim(original, candidate))
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
    # if slash-separated, generate combinations
    if '/' in game_title:
        parts = [p.strip() for p in re.split(r'[\/,]', game_title) if p.strip()]
        # generate single parts, pairwise "and" and all
        for p in parts:
            add(p)
        if len(parts) >= 2:
            add(f"{parts[0]} and {parts[1]}")
            add(f"{parts[0]} & {parts[1]}")
        if len(parts) >= 3:
            add(f"{parts[0]} and {parts[1]} and {parts[2]}")
            add(f"{parts[0]} & {parts[1]} & {parts[2]}")
        # also the base before colon
        base = game_title.split(":",1)[0].strip()
        add(base)
    else:
        add(game_title.split(":",1)[0].strip())
        m = re.search(r'\b(\d+)\b', game_title)
        if m:
            r = convert_arabic_to_roman(m.group(1))
            if r and r != m.group(1):
                add(re.sub(r'\b'+re.escape(m.group(1))+r'\b', r, game_title))
        rm = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', game_title)
        if rm:
            a = convert_roman_to_arabic(rm.group(1))
            if a and a != rm.group(1):
                add(re.sub(r'\b'+re.escape(rm.group(1))+r'\b', a, game_title))
    return out

# ------------------ Поиск и выбор лучшего результата ------------------

def get_year_from_search_context(link):
    try:
        context_text = link.evaluate("""(el) => {
            const p = el.closest('li') || el.closest('div') || el.parentElement;
            return p ? p.innerText : el.innerText;
        }""")
        if not context_text:
            return None
        matches = re.findall(r'(\b19\d{2}\b|\b20\d{2}\b)', context_text)
        if matches:
            years = [int(m) for m in matches]
            return min(years)
    except Exception:
        return None
    return None

def find_best_match_with_year(page, game_links, original_title, game_year=None):
    """
    Возвращает (link_element, link_title, score) или (None, "", 0.0)
    Улучшенная логика:
    - Если original содержит '/', требуем совпадение нескольких частей
    - При наличии game_year год весит сильнее
    - Если best_score невысок, но найден кандидат с точным совпадением базового названия + годом, выбираем его
    """
    try:
        best_match = None
        best_score = -1.0
        best_title = ""
        limit = min(game_links.count(), 30)
        orig_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title))
        orig_tokens = set(orig_clean.split())

        is_slash = ('/' in original_title) or (',' in original_title and len(original_title.split(','))>1)
        slash_parts = []
        if is_slash:
            # extract meaningful words from parts (remove 'Pokémon' repeated)
            raw_parts = [p.strip() for p in re.split(r'[\/,]', original_title) if p.strip()]
            # remove common prefix tokens like 'Pokémon' to focus on parts
            for p in raw_parts:
                p_clean = clean_title_for_comparison(p)
                # keep last word(s)
                slash_parts.append(p_clean)
            min_required_parts = min(2, len(slash_parts))
        else:
            min_required_parts = 0

        # iterate candidates
        candidates_info = []
        for i in range(limit):
            link = game_links.nth(i)
            link_text = ""
            try:
                link_text = link.inner_text().strip()
            except Exception:
                continue
            if not link_text:
                continue
            title_score = calculate_title_similarity(original_title, link_text)

            # year from context
            hltb_year = None
            try:
                hltb_year = get_year_from_search_context(link)
            except:
                hltb_year = None

            year_score = 0.0
            if game_year and hltb_year:
                if hltb_year == game_year:
                    year_score = 1.0
                else:
                    diff = abs(game_year - hltb_year)
                    if diff <= 2:
                        year_score = 0.8
                    elif diff <= 5:
                        year_score = 0.5
                    else:
                        year_score = 0.1

            # token overlap
            tokens = set(clean_title_for_comparison(link_text).split())
            token_overlap = len(orig_tokens.intersection(tokens)) / (len(orig_tokens) or 1)
            boost = 0.0
            if token_overlap >= 0.75:
                boost += 0.12
            if clean_title_for_comparison(link_text) == orig_clean:
                boost += 0.25
            # if candidate contains base before colon of original -> boost
            base = original_title.split(":",1)[0].strip()
            if base and base.lower() in link_text.lower():
                boost += 0.12

            # special handling for slash titles: count how many parts matched
            parts_matched = 0
            if is_slash:
                lc = clean_title_for_comparison(link_text)
                for p in slash_parts:
                    # require meaningful token match
                    if p and all(tok in lc for tok in p.split()):
                        parts_matched += 1

            # weighting: give year more weight when provided
            if game_year:
                combined = title_score * 0.30 + year_score * 0.55 + boost
            else:
                combined = title_score * 0.75 + year_score * 0.25 + boost

            # penalize slash-names that don't include enough parts
            if is_slash and parts_matched < min_required_parts:
                combined -= 0.45  # strong penalty so ROMhacks with single part won't win

            candidates_info.append((combined, link, link_text, title_score, year_score, parts_matched, hltb_year))

            if combined > best_score:
                best_score = combined
                best_match = link
                best_title = link_text

        # if best_score low, attempt fallback: prefer candidate with exact base + year match
        if (not best_match or best_score < 0.25) and game_links.count() > 0:
            for i in range(min(game_links.count(), 60)):
                link = game_links.nth(i)
                try:
                    lt = link.inner_text().strip()
                except:
                    continue
                lc = clean_title_for_comparison(lt)
                base = clean_title_for_comparison(original_title.split(":",1)[0])
                if base and base in lc:
                    y = get_year_from_search_context(link)
                    if game_year and y == game_year:
                        log_message(f"🎯 Fallback: выбран кандидат по базовому названию + году: '{lt}' (год: {y})")
                        return link, lt, 1.0
        # final threshold
        if best_match and best_score >= 0.25:
            if game_year:
                log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {best_score:.2f}, ожидаемый год: {game_year})")
            else:
                log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {best_score:.2f})")
            return best_match, best_title, best_score
        return None, "", 0.0

    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return None, "", 0.0

# ------------------ Извлечение данных со страницы (как раньше, с fallback) ------------------

def round_time(time_str):
    if not time_str or time_str == "N/A":
        return None
    try:
        s = time_str.replace('½', '.5')
        # Try to extract hours first
        m = re.search(r'(\d+(?:\.\d+)?)\s*h', s)
        if m:
            val = float(m.group(1))
            return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
        m2 = re.search(r'(\d+(?:\.\d+)?)\s*Hours?', s, flags=re.IGNORECASE)
        if m2:
            val = float(m2.group(1))
            return f"{int(val)}h" if val == int(val) else f"{val:.1f}h"
        m3 = re.search(r'(\d+)\s*m', s)
        if m3:
            return f"{int(m3.group(1))}m"
        # fallback numeric
        num = re.search(r'(\d+(?:\.\d+)?)', s)
        if num:
            v = float(num.group(1))
            if v >= 1:
                return f"{int(v)}h" if v == int(v) else f"{v:.1f}h"
            else:
                return f"{int(v*60)}m"
    except Exception:
        pass
    return None

def extract_hltb_row_data(row_text):
    try:
        polled = None
        pm = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)?', row_text)
        if pm:
            s = pm.group(1)
            if 'k' in s.lower():
                polled = int(float(s.lower().replace('k','')) * 1000)
            else:
                polled = int(float(s))
        time_pat = r'(\d+h\s*\d+m|\d+h|\d+(?:\.\d+)?\s*Hours?|\d+(?:\.\d+)?[½]?)'
        matches = re.findall(time_pat, row_text)
        times = [m.strip() for m in matches] if matches else []
        result = {}
        if times:
            result["t"] = round_time(times[0])
        if polled:
            result["p"] = polled
        return result if result else None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения строки: {e}")
        return None

def extract_store_links(page):
    try:
        store_links = {}
        map_sel = {
            "steam":"a[href*='store.steampowered.com']",
            "gog":"a[href*='gog.com']",
            "epic":"a[href*='epicgames.com']",
        }
        for name, sel in map_sel.items():
            loc = page.locator(sel)
            if loc.count() > 0:
                href = loc.first.get_attribute("href")
                if href:
                    store_links[name] = href
        return store_links if store_links else None
    except Exception:
        return None

def extract_hltb_data_from_page(page):
    try:
        hltb_data = {}
        # try tables first
        try:
            tables = page.locator("table")
            for ti in range(tables.count()):
                try:
                    table = tables.nth(ti)
                    txt = table.inner_text()
                    if any(k in txt for k in ["Main Story","Main + Extras","Completionist","Co-Op","Vs.","Competitive","Single-Player"]):
                        rows = table.locator("tr")
                        for ri in range(rows.count()):
                            rtxt = rows.nth(ri).inner_text()
                            if "Main Story" in rtxt or "Single-Player" in rtxt:
                                d = extract_hltb_row_data(rtxt)
                                if d: hltb_data["ms"] = d
                            if "Main + Extras" in rtxt:
                                d = extract_hltb_row_data(rtxt)
                                if d: hltb_data["mpe"] = d
                            if "Completionist" in rtxt:
                                d = extract_hltb_row_data(rtxt)
                                if d: hltb_data["comp"] = d
                            if "Co-Op" in rtxt:
                                d = extract_hltb_row_data(rtxt)
                                if d: hltb_data["coop"] = d
                            if "Vs." in rtxt or "Competitive" in rtxt:
                                d = extract_hltb_row_data(rtxt)
                                if d: hltb_data["vs"] = d
                except Exception:
                    continue
        except Exception:
            pass

        # blocks search
        try:
            for key, sel in [("vs", "Vs."), ("coop", "Co-Op"), ("ms", "Single-Player")]:
                elems = page.locator(f"text={sel}")
                for i in range(min(elems.count(), 6)):
                    try:
                        el = elems.nth(i)
                        s = el.evaluate("(e) => (e.closest('div') || e.parentElement || e).innerText")
                        if key == "vs" and "vs" not in hltb_data:
                            pat = r'(?:Vs\.|Versus)[^\d]{0,20}(\d+(?:\.\d+)?(?:½)?)'
                            m = re.search(pat, s, flags=re.IGNORECASE)
                            if m:
                                hltb_data["vs"] = {"t": round_time(m.group(1))}
                        if key == "coop" and "coop" not in hltb_data:
                            m = re.search(r'Co-Op[^\d]{0,20}(\d+(?:\.\d+)?(?:½)?)', s, flags=re.IGNORECASE)
                            if m:
                                hltb_data["coop"] = {"t": round_time(m.group(1))}
                        if key == "ms" and "ms" not in hltb_data:
                            m = re.search(r'(?:Single-Player|Main Story)[^\d]{0,20}(\d+(?:\.\d+)?(?:½)?)', s, flags=re.IGNORECASE)
                            if m:
                                hltb_data["ms"] = {"t": round_time(m.group(1))}
                    except Exception:
                        continue
        except Exception:
            pass

        # fallback regex over entire page content
        if not hltb_data:
            content = page.content()
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

        stores = extract_store_links(page)
        if stores:
            hltb_data["stores"] = stores

        return hltb_data if hltb_data else None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных со страницы: {e}")
        return None

# ------------------ Поиск одной попытки и ретраи ------------------

def random_delay(min_s, max_s):
    time.sleep(random.uniform(min_s, max_s))

def search_game_single_attempt(page, game_title, game_year=None):
    try:
        log_message(f"🔍 Ищем: '{game_title}'")
        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"
        page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        random_delay(0.8, 1.8)

        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ Возможно блокировка при поиске")
            return None

        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()

        if found_count > 30:
            log_message(f"⚠️  Слишком много результатов ({found_count}), пробуем точный поиск в кавычках")
            quoted = f'"{game_title}"'
            page.goto(f"{BASE_URL}/?q={quote(quoted, safe='')}", timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            random_delay(0.8, 1.8)
            game_links = page.locator('a[href^="/game/"]')
            found_count = game_links.count()

        if found_count == 0:
            return None

        best_match, best_title, similarity = find_best_match_with_year(page, game_links, game_title, game_year)
        # fallback: if not selected, try a direct scan for base+year
        if not best_match:
            base = clean_title_for_comparison(game_title.split(":",1)[0])
            for i in range(min(game_links.count(), 60)):
                link = game_links.nth(i)
                try:
                    lt = link.inner_text().strip()
                except:
                    continue
                lc = clean_title_for_comparison(lt)
                if base and base in lc:
                    y = get_year_from_search_context(link)
                    if game_year and y == game_year:
                        log_message(f"🎯 Fallback: выбрали '{lt}' по базе + году")
                        best_match = link
                        best_title = lt
                        similarity = 1.0
                        break

        if not best_match:
            return None

        href = best_match.get_attribute("href")
        if not href:
            return None
        full = f"{BASE_URL}{href}"
        page.goto(full, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        random_delay(0.9, 2.1)
        hltb_data = extract_hltb_data_from_page(page)
        return (hltb_data, best_title) if hltb_data else None

    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def search_game_on_hltb(page, game_title, game_year=None):
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]

    log_message(f"🔍 Ищем оригинальное название: '{game_title}' (год: {game_year})")
    result = search_game_single_attempt(page, game_title, game_year)
    if result is not None:
        hltb_data, found_title = result
        score = calculate_title_similarity(game_title, found_title) if found_title else 0
        if score >= 1.0:
            return hltb_data
        best_result = hltb_data
        best_score = score
        best_title = found_title
    else:
        log_message("❌ Оригинальное название не найдено, пробуем альтернативы...")
        best_result = None
        best_score = 0.0
        best_title = None

    # альтернативы
    alts = generate_alternative_titles(game_title)
    for attempt in range(max_attempts):
        if attempt > 0:
            log_message(f"🔄 Попытка {attempt+1}/{max_attempts} для '{game_title}'")
            if isinstance(delays[attempt], tuple):
                mn, mx = delays[attempt]
                log_message(f"⏳ Пауза {mn}-{mx} сек...")
                random_delay(mn, mx)
            else:
                time.sleep(delays[attempt])

        for alt in alts:
            if alt == game_title:
                continue
            res = search_game_single_attempt(page, alt, game_year)
            if res is not None:
                hltb_data, found_title = res
                score = calculate_title_similarity(game_title, found_title if found_title else alt)
                # if slash original, ensure found_title contains enough parts (handled in find_best_match)
                if score >= 1.0:
                    log_message(f"🎯 Найден идеальный результат для {alt}: '{found_title}'")
                    return hltb_data
                if score > best_score:
                    best_score = score
                    best_result = hltb_data
                    best_title = found_title

        if best_result is not None:
            log_message(f"🏆 Лучший результат: '{best_title}' (схожесть: {best_score:.2f})")
            return best_result

    return best_result

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

def check_break_time(start_time, games_processed):
    elapsed_seconds = time.time() - start_time
    if elapsed_seconds >= random.randint(BREAK_INTERVAL_MIN, BREAK_INTERVAL_MAX):
        dur = random.randint(BREAK_DURATION_MIN, BREAK_DURATION_MAX)
        log_message(f"⏸️ Перерыв {dur} секунд... (обработано {games_processed})")
        time.sleep(dur)
        return time.time()
    return start_time

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

# ------------------ main ------------------

def main():
    log_message("🚀 Запуск HLTB Worker (обновлённая логика выбора)")
    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден")
        return
    setup_directories()
    games_list = extract_games_list(GAMES_LIST_FILE)
    total_games = len(games_list)
    log_message(f"📄 Извлечено {total_games} игр")

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
            start_index = progress.get("current_index", 0)
            log_message(f"📂 Продолжаем с {start_index}")
        except:
            start_index = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36", viewport={"width":1280,"height":800}, locale="en-US")
        page = context.new_page()
        try:
            page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            log_message("✅ HowLongToBeat доступен")
        except Exception as e:
            log_message(f"⚠️ Предупреждение: при проверке сайта возникла ошибка: {e}")

        start_time = time.time()
        for i in range(start_index, total_games):
            game = games_list[i]
            title = game.get("title") or ""
            year = game.get("year")
            log_message(f"🎮🎮🎮 Обрабатываю {i+1}/{total_games}: {title} ({year})")
            hltb = search_game_on_hltb(page, title, year)
            if hltb:
                game["hltb"] = hltb
                log_message(f"✅ Найдены данные для '{title}': {hltb}")
            else:
                game["hltb"] = {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A"}
                log_message(f"⚠️ Данные не найдены для: {title} - записано N/A")
            if (i+1) % 25 == 0:
                save_progress(games_list, i+1, total_games)
            start_time = check_break_time(start_time, i+1)
        browser.close()

    save_results(games_list)
    if update_html_with_hltb(GAMES_LIST_FILE, games_list):
        log_message("✅ HTML обновлён")
    log_message("🎉 Завершено!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"💥 Критическая ошибка: {e}")
        raise
