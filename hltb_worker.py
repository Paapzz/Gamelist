# hltb_worker.py
# Исправленная и улучшенная версия вашего скраппера (исходный: загруженный файл). :contentReference[oaicite:1]{index=1}

print("🚀 HLTB Worker запускается...")

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

# Таймауты и интервалы (увеличены для надёжности)
PAGE_GOTO_TIMEOUT = 30000
PAGE_LOAD_TIMEOUT = 20000

BREAK_INTERVAL_MIN = 8 * 60
BREAK_INTERVAL_MAX = 10 * 60
BREAK_DURATION_MIN = 40
BREAK_DURATION_MAX = 80

def setup_directories():
    """Настройка директорий"""
    print(f"📁 Создаем директорию: {OUTPUT_DIR}")
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"✅ Директория {OUTPUT_DIR} создана/существует")
    except Exception as e:
        print(f"❌ Ошибка создания директории: {e}")
        raise

def log_message(message):
    """Логирование только в консоль"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
    except Exception as e:
        print(f"Ошибка логирования: {e}")
        print(f"Сообщение: {message}")

def extract_games_list(html_file):
    """Извлекает список игр из HTML файла"""
    try:
        log_message(f"📖 Читаем файл {html_file}...")
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()

        log_message(f"📄 Файл прочитан, размер: {len(content)} символов")

        log_message("🔍 Ищем 'const gamesList = ['...")
        start = content.find('const gamesList = [')
        if start == -1:
            raise ValueError("Не найден const gamesList в HTML файле")

        log_message(f"✅ Найден const gamesList на позиции {start}")

        log_message("🔍 Ищем закрывающую скобку массива...")
        bracket_count = 0
        end = start
        for i, char in enumerate(content[start:], start):
            if char == '[':
                bracket_count += 1
            elif char == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end = i + 1
                    break

        if bracket_count != 0:
            raise ValueError("Не найден конец массива gamesList")

        log_message(f"✅ Найден конец массива на позиции {end}")

        log_message("✂️ Извлекаем JSON...")
        games_json = content[start:end]
        games_json = games_json.replace('const gamesList = ', '')

        log_message(f"📝 JSON извлечен, размер: {len(games_json)} символов")
        log_message("🔄 Парсим JSON...")

        games_list = json.loads(games_json)
        log_message(f"✅ Извлечено {len(games_list)} игр из HTML файла")
        return games_list

    except Exception as e:
        log_message(f"❌ Ошибка извлечения списка игр: {e}")
        raise

# -------------------------- Вспомогательные функции времени --------------------------

def parse_time_to_hours(time_str):
    """Парсит время в формате 'Xh Ym' или 'X Hours' в часы и минуты"""
    if not time_str or time_str == "N/A":
        return 0, 0

    s = time_str.replace("Hours", "").replace("hours", "").strip()

    hours_match = re.search(r'(\d+(?:\.\d+)?)h', s)
    minutes_match = re.search(r'(\d+)m', s)

    hours = float(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0

    if hours == 0 and minutes == 0:
        number_match = re.search(r'(\d+(?:\.\d+)?)', s)
        if number_match:
            hours = float(number_match.group(1))
            if hours != int(hours):
                minutes = int((hours - int(hours)) * 60)
                hours = int(hours)

    if hours != int(hours):
        minutes += int((hours - int(hours)) * 60)
        hours = int(hours)

    return hours, minutes

def round_time(time_str):
    """Округляет время к ближайшему значению"""
    if not time_str or time_str == "N/A":
        return None

    hours, minutes = parse_time_to_hours(time_str)

    hours = int(hours)

    if minutes <= 14:
        return f"{hours}h"
    elif minutes <= 44:
        return f"{hours}.5h"
    else:
        return f"{hours + 1}h"

# -------------------------- Вспомогательное сравнение названий --------------------------

def clean_title_for_comparison(title):
    """Очищает название игры для сравнения"""
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

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
    """Нормализация: заменяем римские цифры на арабские, убираем лишние пробелы"""
    try:
        roman_to_arabic = {
            ' I ': ' 1 ', ' II ': ' 2 ', ' III ': ' 3 ', ' IV ': ' 4 ', ' V ': ' 5 ',
            ' VI ': ' 6 ', ' VII ': ' 7 ', ' VIII ': ' 8 ', ' IX ': ' 9 ', ' X ': ' 10 '
        }
        s = f" {title} "
        for r,a in roman_to_arabic.items():
            s = s.replace(r, a)
        return s.strip()
    except Exception as e:
        log_message(f"❌ Ошибка нормализации названия: {e}")
        return title

def lcs_length(a_tokens, b_tokens):
    """LCS для токенов"""
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
    Улучшённая метрика схожести:
    - преобразования рим↔араб
    - прямые совпадения части -> 1.0
    - совпадение базовой части (до ':') -> 0.9
    - в остальных случаях комбинированная оценка recall/precision/LCS
    """
    def _sim(a, b):
        a_clean = clean_title_for_comparison(normalize_title_for_comparison(a))
        b_clean = clean_title_for_comparison(normalize_title_for_comparison(b))
        if a_clean == b_clean:
            return 1.0
        a_tokens = a_clean.split()
        b_tokens = b_clean.split()
        if not a_tokens or not b_tokens:
            return 0.0
        common = set(a_tokens).intersection(set(b_tokens))
        precision = len(common) / len(b_tokens)
        recall = len(common) / len(a_tokens)
        lcs_len = lcs_length(a_tokens, b_tokens)
        seq = (lcs_len / len(a_tokens)) if len(a_tokens) > 0 else 0.0
        score = 0.65 * recall + 0.2 * precision + 0.15 * seq
        return max(0.0, min(1.0, score))

    try:
        if not original or not candidate:
            return 0.0

        cand_clean = clean_title_for_comparison(normalize_title_for_comparison(candidate))

        if "/" in original:
            parts = [p.strip() for p in (original.replace(" / ", "/")).split("/")]
            # генерируем нормализованные формы части и их баз
            for part in parts:
                part_clean = clean_title_for_comparison(normalize_title_for_comparison(part))
                base = part.split(":",1)[0].strip()
                base_clean = clean_title_for_comparison(normalize_title_for_comparison(base))
                # вариации араб<->рим
                arabic_match = re.search(r'\b(\d+)\b', part)
                if arabic_match:
                    roman = convert_arabic_to_roman(arabic_match.group(1))
                    if roman:
                        if cand_clean == clean_title_for_comparison(normalize_title_for_comparison(re.sub(r'\b' + re.escape(arabic_match.group(1)) + r'\b', roman, part))):
                            return 1.0
                # рим -> араб
                roman_match = re.search(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', part)
                if roman_match:
                    arab = convert_roman_to_arabic(roman_match.group(1))
                    if arab:
                        if cand_clean == clean_title_for_comparison(normalize_title_for_comparison(re.sub(r'\b' + re.escape(roman_match.group(1)) + r'\b', arab, part))):
                            return 1.0

                if cand_clean == part_clean:
                    return 1.0
                if cand_clean == base_clean:
                    return 0.9

            # иначе максимальная симметрия по частям
            best = 0.0
            for part in parts:
                best = max(best, _sim(part, candidate))
            best = max(best, _sim(original, candidate))
            return float(best)

        orig_clean = clean_title_for_comparison(normalize_title_for_comparison(original))
        if orig_clean == cand_clean:
            return 1.0

        return float(_sim(original, candidate))
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести: {e}")
        return 0.0

def generate_alternative_titles(game_title):
    """Генерация альтернатив (сохранена оригинальная логика, немного упрощена)"""
    if not game_title:
        return []

    alternatives = []
    seen = set()
    def add(a):
        if not a: return
        a = re.sub(r'\s+', ' ', a).strip()
        if a and a not in seen:
            seen.add(a); alternatives.append(a)

    add(game_title)

    if "/" in game_title:
        parts = [p.strip() for p in game_title.replace(" / ", "/").split("/")]
        is_enum = all(len(p.split()) == 1 and ":" not in p for p in parts)
        if not is_enum:
            for p in parts:
                add(p)
        else:
            for p in parts:
                add(p)
            if len(parts) >= 2:
                add(f"{parts[0]} and {parts[1]}")
                add(f"{parts[0]} & {parts[1]}")
    else:
        # вариации с рим/араб и без двоеточия
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

    # сортировка по длине (короткие в конце) чтобы длинные точные варианты первыми
    alternatives = sorted(alternatives, key=lambda s: len(s.split()), reverse=True)
    return alternatives

# -------------------------- Поиск и выбор лучшего результата --------------------------

def get_year_from_search_context(link):
    """
    Пытаемся извлечь год из контекста элемента ссылки (без перехода на страницу).
    Возвращаем int год или None.
    """
    try:
        # Получаем ближайший контейнерный текст — это дешевле, чем переход по ссылке
        context_text = link.evaluate("""(el) => {
            const p = el.closest('li') || el.closest('div') || el.parentElement;
            return p ? p.innerText : el.innerText;
        }""")
        if not context_text:
            return None
        # ищем 4-значные года
        matches = re.findall(r'(\b19\d{2}\b|\b20\d{2}\b)', context_text)
        if matches:
            years = [int(m) for m in matches]
            return min(years)
    except Exception:
        return None
    return None

def find_best_match_with_year(page, game_links, original_title, game_year=None):
    """
    Находит наиболее подходящий результат из списка найденных игр с учётом названия.
    НЕ делает навигацию по каждой ссылке (экономим переходы).
    Возвращает (link_element, link_title, score).
    """
    try:
        best_match = None
        best_score = -1.0
        best_title = ""
        limit = min(game_links.count(), 20)  # расширил до 20, но без навигации
        orig_clean = clean_title_for_comparison(normalize_title_for_comparison(original_title))
        orig_tokens = set(orig_clean.split())

        for i in range(limit):
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            if not link_text:
                continue

            title_score = calculate_title_similarity(original_title, link_text)

            # попытка получить год из контейнера результата (без перехода)
            hltb_year = None
            if game_year:
                try:
                    hltb_year = get_year_from_search_context(link)
                except:
                    hltb_year = None

            year_score = 0.0
            if game_year and hltb_year:
                # простая оценка: совпадение == 1, близость ±2 года => 0.8 и т.д.
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

            # boost если ссылка содержит все важные токены оригинала
            tokens = set(clean_title_for_comparison(link_text).split())
            token_overlap = len(orig_tokens.intersection(tokens)) / (len(orig_tokens) or 1)
            boost = 0.0
            if token_overlap >= 0.75:
                boost += 0.15
            if clean_title_for_comparison(link_text) == orig_clean:
                boost += 0.2

            combined = title_score * 0.75 + year_score * 0.25 + boost
            if combined > best_score:
                best_score = combined
                best_match = link
                best_title = link_text

        if best_match and best_score >= 0.25:
            if game_year:
                log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {best_score:.2f}, ожидаемый год: {game_year})")
            else:
                log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {best_score:.2f})")
            return best_match, best_title, best_score
        else:
            return None, "", 0.0

    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return None, "", 0.0

# -------------------------- Извлечение данных со страницы игры --------------------------

def extract_hltb_data_from_page(page):
    """
    Извлекает данные HLTB со страницы игры.
    Алгоритм:
      1) Парсим таблицы (как раньше)
      2) Ищем текстовые блоки 'Vs.', 'Co-Op', 'Single-Player'
      3) Fallback: regex по page.content()
    """
    try:
        hltb_data = {}
        page_content = page.content()

        # 1) Таблицы (сохранена ваша логика, но защита от исключений)
        try:
            tables = page.locator("table")
            table_count = tables.count()
            for table_idx in range(table_count):
                try:
                    table = tables.nth(table_idx)
                    table_text = table.inner_text()
                    if any(keyword in table_text for keyword in ["Main Story", "Main + Extras", "Completionist", "Co-Op", "Competitive", "Vs."]):
                        rows = table.locator("tr")
                        row_count = rows.count()
                        for row_idx in range(row_count):
                            try:
                                row_text = rows.nth(row_idx).inner_text().strip()
                                if "Main Story" in row_text and "ms" not in hltb_data:
                                    d = extract_hltb_row_data(row_text)
                                    if d: hltb_data["ms"] = d
                                elif "Main + Extras" in row_text and "mpe" not in hltb_data:
                                    d = extract_hltb_row_data(row_text)
                                    if d: hltb_data["mpe"] = d
                                elif "Completionist" in row_text and "comp" not in hltb_data:
                                    d = extract_hltb_row_data(row_text)
                                    if d: hltb_data["comp"] = d
                                elif "Co-Op" in row_text and "coop" not in hltb_data:
                                    d = extract_hltb_row_data(row_text)
                                    if d: hltb_data["coop"] = d
                                elif "Competitive" in row_text and "vs" not in hltb_data:
                                    d = extract_hltb_row_data(row_text)
                                    if d: hltb_data["vs"] = d
                            except Exception as e:
                                log_message(f"⚠️ Ошибка обработки строки таблицы {row_idx}: {e}")
                                continue
                except Exception as e:
                    log_message(f"⚠️ Ошибка обработки таблицы {table_idx}: {e}")
                    continue
        except Exception as e:
            log_message(f"⚠️ Ошибка перебора таблиц: {e}")

        # 2) Ищем отдельные блоки 'Vs.', 'Co-Op', 'Single-Player'
        try:
            for keyword, keyname in [("Vs.", "vs"), ("Co-Op", "coop"), ("Single-Player", "ms")]:
                elems = page.locator(f"text={keyword}")
                cnt = elems.count()
                for i in range(min(cnt, 10)):
                    try:
                        el = elems.nth(i)
                        surrounding_text = el.evaluate("(e) => (e.closest('div') || e.parentElement || e).innerText")
                        if keyword == "Vs." and "vs" not in hltb_data:
                            vs_data = extract_vs_data_from_text(surrounding_text)
                            if vs_data:
                                hltb_data["vs"] = vs_data
                        elif keyword == "Co-Op" and "coop" not in hltb_data:
                            coop_data = extract_coop_data_from_text(surrounding_text)
                            if coop_data:
                                hltb_data["coop"] = coop_data
                        elif keyword == "Single-Player" and "ms" not in hltb_data:
                            sp_data = extract_single_player_data_from_text(surrounding_text)
                            if sp_data:
                                hltb_data["ms"] = sp_data
                    except Exception as e:
                        log_message(f"⚠️ Ошибка обработки блока {keyword} #{i}: {e}")
                        continue
        except Exception as e:
            log_message(f"⚠️ Ошибка поиска мультиплеерных блоков: {e}")

        # 3) Fallback: регексп по page_content, на случай нестандартной вёрстки
        if not hltb_data:
            try:
                # шаблоны для разных категорий
                patterns = {
                    "ms": r'(?:Main Story|Single-Player)[^\n\r]{0,160}?(\d+h(?:\s*\d+m)?|\d+(?:\.\d+)?\s*Hours?)',
                    "mpe": r'(?:Main \+ Extras|Main \+ Extras)[^\n\r]{0,160}?(\d+h(?:\s*\d+m)?|\d+(?:\.\d+)?\s*Hours?)',
                    "comp": r'(?:Completionist)[^\n\r]{0,160}?(\d+h(?:\s*\d+m)?|\d+(?:\.\d+)?\s*Hours?)',
                    "coop": r'(?:Co-Op)[^\n\r]{0,160}?(\d+h(?:\s*\d+m)?|\d+(?:\.\d+)?\s*Hours?)',
                    "vs": r'(?:Vs\.|Versus)[^\n\r]{0,160}?(\d+(?:\.\d+)?[½]?|\d+h(?:\s*\d+m)?|\d+(?:\.\d+)?\s*Hours?)'
                }
                for k, pat in patterns.items():
                    m = re.search(pat, page_content, flags=re.IGNORECASE)
                    if m:
                        tstr = m.group(1)
                        if '½' in tstr:
                            tstr = tstr.replace('½', '.5')
                        # форматируем похожим образом
                        try:
                            hours = float(re.sub(r'[^\d\.]', '', tstr))
                            if hours >= 1:
                                formatted = f"{int(hours)}h" if hours == int(hours) else f"{hours:.1f}h"
                            else:
                                formatted = f"{int(hours*60)}m"
                        except:
                            formatted = round_time(tstr)
                        hltb_data[k] = {"t": formatted}
                if hltb_data:
                    log_message("🎯 Данные найдены через fallback-регексп по содержимому страницы")
            except Exception as e:
                log_message(f"⚠️ Ошибка fallback-парсинга: {e}")

        # store links
        store_links = extract_store_links(page)
        if store_links:
            hltb_data["stores"] = store_links

        return hltb_data if hltb_data else None

    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных со страницы: {e}")
        return None

def extract_store_links(page):
    """Извлекает ссылки на магазины со страницы игры"""
    try:
        store_links = {}
        store_selectors = {
            "steam": "a[href*='store.steampowered.com']",
            "epic": "a[href*='epicgames.com']",
            "gog": "a[href*='gog.com']",
            "humble": "a[href*='humblebundle.com']",
            "itch": "a[href*='itch.io']",
            "battlenet": "a[href*='battle.net']",
            "psn": "a[href*='playstation.com']",
            "xbox": "a[href*='xbox.com']",
            "nintendo": "a[href*='nintendo.com']"
        }
        for store_name, selector in store_selectors.items():
            try:
                el = page.locator(selector).first
                if el.count() > 0:
                    href = el.get_attribute("href")
                    if href:
                        # GOG через adtraction
                        if store_name == "gog" and "adtraction.com" in href:
                            match = re.search(r'url=([^&]+)', href)
                            if match:
                                href = unquote(match.group(1))
                        store_links[store_name] = href
            except Exception:
                continue
        if store_links:
            log_message(f"🛒 Найдены ссылки на магазины: {list(store_links.keys())}")
        return store_links if store_links else None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения ссылок на магазины: {e}")
        return None

# -------------------------- Парсинг строк таблицы (оставил вашу логику и немного упростил) --------------------------

def extract_hltb_row_data(row_text):
    """Извлекает данные из строки таблицы HLTB (новый формат)"""
    try:
        polled = None
        # Пытаемся найти число опрошенных
        polled_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)?', row_text)
        if polled_match:
            polled_str = polled_match.group(1)
            if 'K' in polled_str.upper():
                number = float(polled_str.replace('K','').replace('k',''))
                polled = int(number * 1000)
            else:
                polled = int(float(polled_str))

        # Ищем временные блоки
        combined_pattern = r'(\d+h\s*\d+m|\d+h|\d+(?:\.\d+)?\s*Hours?|\d+(?:\.\d+)?[½]?)'
        matches = re.findall(combined_pattern, row_text)
        times = [re.sub(r'\s+', ' ', m.strip()) for m in matches] if matches else []

        if not times and polled is None:
            return None

        result = {}
        if times:
            avg = times[0]
            # при необходимости используем median в times[1]
            result["t"] = round_time(avg)
        if polled:
            result["p"] = polled

        # доп. поля для single/multi если есть
        if "Main Story" in row_text or "Single-Player" in row_text:
            if len(times) >= 4:
                result["r"] = round_time(times[2])
                result["l"] = round_time(times[3])
        elif "Co-Op" in row_text or "Competitive" in row_text or "Vs." in row_text:
            if len(times) >= 4:
                result["min"] = round_time(times[2])
                result["max"] = round_time(times[3])

        return result if result else None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из строки: {e}")
        return None

# -------------------------- Разные extract_* для текста (как раньше) --------------------------

def extract_vs_data_from_text(text):
    try:
        clean_text = text.replace('\n',' ').replace('\r',' ')
        patterns = [
            r'Vs\.\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',
            r'Vs\.\s+(\d+(?:\.\d+)?)\s*Hours?',
            r'Versus[^\d]*(\d+(?:\.\d+)?)\s*Hours?'
        ]
        for pat in patterns:
            m = re.search(pat, clean_text, flags=re.IGNORECASE)
            if m:
                tstr = m.group(1).replace('½', '.5')
                try:
                    hours = float(tstr)
                    formatted = f"{int(hours)}h" if hours == int(hours) else f"{hours:.1f}h"
                except:
                    formatted = round_time(tstr)
                log_message(f"✅ Найдены Vs. данные: {formatted}")
                return {"t": formatted}
        return None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Vs. данных: {e}")
        return None

def extract_coop_data_from_text(text):
    try:
        clean_text = text.replace('\n',' ').replace('\r',' ')
        patterns = [r'Co-Op[^\d]*(\d+(?:\.\d+)?)\s*Hours?', r'Co-Op\s*\|\s*(\d+(?:\.\d+)?)']
        for pat in patterns:
            m = re.search(pat, clean_text, flags=re.IGNORECASE)
            if m:
                tstr = m.group(1).replace('½', '.5')
                try:
                    hours = float(tstr)
                    formatted = f"{int(hours)}h" if hours == int(hours) else f"{hours:.1f}h"
                except:
                    formatted = round_time(tstr)
                log_message(f"✅ Найдены Co-Op данные: {formatted}")
                return {"t": formatted}
        return None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Co-Op данных: {e}")
        return None

def extract_single_player_data_from_text(text):
    try:
        clean_text = text.replace('\n',' ').replace('\r',' ')
        patterns = [r'Single-Player[^\d]*(\d+(?:\.\d+)?)\s*Hours?', r'Main Story[^\d]*(\d+(?:\.\d+)?)']
        for pat in patterns:
            m = re.search(pat, clean_text, flags=re.IGNORECASE)
            if m:
                tstr = m.group(1).replace('½', '.5')
                try:
                    hours = float(tstr)
                    formatted = f"{int(hours)}h" if hours == int(hours) else f"{hours:.1f}h"
                except:
                    formatted = round_time(tstr)
                log_message(f"✅ Найдены Single-Player данные: {formatted}")
                return {"t": formatted}
        return None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Single-Player данных: {e}")
        return None

# -------------------------- Поиск одной попытки и основной поиск с ретраями --------------------------

def search_game_single_attempt(page, game_title, game_year=None):
    """Одна попытка поиска игры на HLTB (без излишних переходов)"""
    try:
        log_message(f"🔍 Ищем: '{game_title}'")
        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"

        page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)

        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при поиске!")
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка при поиске - ждем...")
            time.sleep(3)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует поиск")
                return None

        random_delay(1.5, 3.0)

        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()

        # если слишком много результатов, попробуем точный запрос в кавычках
        if found_count > 30:
            log_message(f"⚠️  Слишком много результатов ({found_count}), пробуем точный поиск в кавычках")
            quoted_title = f'"{game_title}"'
            quoted_url = f"{BASE_URL}/?q={quote(quoted_title, safe='')}"
            page.goto(quoted_url, timeout=PAGE_GOTO_TIMEOUT)
            page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
            random_delay(1.5, 3.0)
            game_links = page.locator('a[href^="/game/"]')
            found_count = game_links.count()

        if found_count == 0:
            return None

        # выбираем лучший кандидат (без перехода по каждому)
        best_match, best_title, similarity = find_best_match_with_year(page, game_links, game_title, game_year)
        if not best_match:
            return None

        best_url = best_match.get_attribute("href")
        if not best_url:
            return None

        # если слабая схожесть и нет года — пропускаем
        if similarity < 0.5 and not game_year:
            log_message(f"⚠️  Низкая схожесть ({similarity:.2f}), пробуем альтернативное название")
            return None

        full_url = f"{BASE_URL}{best_url}"
        # Переходим один раз на страницу лучшего кандидата и парсим данные
        page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
        random_delay(1.5, 3.0)

        # проверяем наличие блокировки после перехода
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры!")
            return None

        hltb_data = extract_hltb_data_from_page(page)
        # если получили данные — возвращаем их с найденным названием
        return (hltb_data, best_title) if hltb_data else None

    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def search_game_on_hltb(page, game_title, game_year=None):
    """Ищет игру на HLTB и возвращает данные с повторными попытками"""
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]

    good_result = None
    good_score = 0
    good_title = None

    log_message(f"🔍 Ищем оригинальное название: '{game_title}' (год: {game_year})")
    result_data = search_game_single_attempt(page, game_title, game_year)

    if result_data is not None:
        hltb_data, found_title = result_data
        score = calculate_title_similarity(game_title, found_title) if found_title else 0

        if score >= 1.0:
            log_message(f"🎯 Найдено идеальное совпадение: '{found_title}' (схожесть: {score:.2f})")
            return hltb_data

        log_message(f"📝 Сохраняем результат: '{found_title}' (схожесть: {score:.2f})")
        good_result = hltb_data
        good_score = score
        good_title = found_title
    else:
        log_message("❌ Оригинальное название не найдено, пробуем альтернативы...")

    alternative_titles = generate_alternative_titles(game_title)

    for attempt in range(max_attempts):
        try:
            if attempt > 0:
                log_message(f"🔄 Попытка {attempt + 1}/{max_attempts} для '{game_title}'")
                if isinstance(delays[attempt], tuple):
                    min_delay, max_delay = delays[attempt]
                    log_message(f"⏳ Пауза {min_delay}-{max_delay} секунд...")
                    random_delay(min_delay, max_delay)
                else:
                    log_message(f"⏳ Пауза {delays[attempt]} секунд...")
                    time.sleep(delays[attempt])

            best_result = good_result
            best_score = good_score
            best_title = good_title

            for alt_title in alternative_titles:
                if alt_title == game_title:
                    continue
                result_data = search_game_single_attempt(page, alt_title, game_year)
                if result_data is not None:
                    hltb_data, found_title = result_data
                    score = calculate_title_similarity(game_title, found_title if found_title else alt_title)
                    if score >= 1.0:
                        log_message(f"🎯 Найден идеальный результат: '{found_title}' (схожесть: {score:.2f})")
                        return hltb_data
                    if score > best_score:
                        best_score = score
                        best_result = hltb_data
                        best_title = found_title

            if best_result is not None:
                log_message(f"🏆 Лучший результат: '{best_title}' (схожесть: {best_score:.2f})")
                return best_result

        except Exception as e:
            log_message(f"❌ Ошибка попытки {attempt + 1} для '{game_title}': {e}")
            if attempt == max_attempts - 1:
                log_message(f"💥 Все попытки исчерпаны для '{game_title}'")
                return None

    return None

# -------------------------- Остальные утилиты и main --------------------------

def random_delay(min_seconds, max_seconds):
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def check_break_time(start_time, games_processed):
    elapsed_seconds = time.time() - start_time
    break_interval = random.randint(BREAK_INTERVAL_MIN, BREAK_INTERVAL_MAX)
    if elapsed_seconds >= break_interval:
        break_duration = random.randint(BREAK_DURATION_MIN, BREAK_DURATION_MAX)
        log_message(f"⏸️  Перерыв {break_duration} секунд... (обработано {games_processed} игр)")
        time.sleep(break_duration)
        return time.time()
    return start_time

def count_hltb_data(hltb_data):
    categories = {"ms": 0, "mpe": 0, "comp": 0, "all": 0, "coop": 0, "vs": 0}
    total_polled = {"ms": 0, "mpe": 0, "comp": 0, "all": 0, "coop": 0, "vs": 0}
    na_count = 0

    for game in hltb_data:
        if "hltb" in game:
            if (isinstance(game["hltb"], dict) and 
                game["hltb"].get("ms") == "N/A" and 
                game["hltb"].get("mpe") == "N/A" and 
                game["hltb"].get("comp") == "N/A" and 
                game["hltb"].get("all") == "N/A"):
                na_count += 1
                continue

            for category in categories:
                if category in game["hltb"] and game["hltb"][category] and game["hltb"][category] != "N/A":
                    categories[category] += 1
                    if isinstance(game["hltb"][category], dict) and "p" in game["hltb"][category]:
                        total_polled[category] += game["hltb"][category]["p"]

    return categories, total_polled, na_count

def save_progress(games_data, current_index, total_games):
    progress_data = {
        "current_index": current_index,
        "total_games": total_games,
        "processed_games": len([g for g in games_data if "hltb" in g]),
        "last_updated": datetime.now().isoformat(),
        "status": "in_progress" if current_index < total_games else "completed"
    }
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)

def save_results(games_data):
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for i, game in enumerate(games_data):
                if i > 0:
                    f.write("\n")
                json.dump(game, f, separators=(',', ':'), ensure_ascii=False)

        categories, total_polled, na_count = count_hltb_data(games_data)
        successful = len([g for g in games_data if "hltb" in g])

        log_message(f"💾 Результаты сохранены в {OUTPUT_FILE}")
        log_message(f"📊 Статистика: {successful}/{len(games_data)} игр с данными HLTB")
        log_message(f"📊 Main Story: {categories['ms']} ({total_polled['ms']} голосов), Main+Extras: {categories['mpe']} ({total_polled['mpe']} голосов)")
        log_message(f"📊 Completionist: {categories['comp']} ({total_polled['comp']} голосов), All: {categories['all']} ({total_polled['all']} голосов)")
        log_message(f"📊 Co-Op: {categories['coop']} ({total_polled['coop']} голосов), Vs: {categories['vs']} ({total_polled['vs']} голосов)")
        log_message(f"📊 N/A (не найдено): {na_count} игр")

    except Exception as e:
        log_message(f"❌ Ошибка сохранения результатов: {e}")
        raise

def log_progress(current, total, start_time):
    elapsed = time.time() - start_time
    rate = current / elapsed * 60 if elapsed > 0 else 0
    eta = (total - current) / rate if rate > 0 else 0
    log_message(f"📊 {current}/{total} | {rate:.1f} игр/мин | ETA: {eta:.0f} мин")

def update_html_with_hltb(html_file, hltb_data):
    try:
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()

        start = content.find('const gamesList = [')
        if start == -1:
            raise ValueError("Не найден const gamesList в HTML файле")

        end = content.find('];', start) + 2
        if end == 1:
            raise ValueError("Не найден конец массива gamesList")

        new_games_list = json.dumps(hltb_data, separators=(',', ':'), ensure_ascii=False)
        new_content = content[:start] + f'const gamesList = {new_games_list}' + content[end:]

        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(new_content)

        log_message(f"✅ HTML файл обновлен: {html_file}")
        return True

    except Exception as e:
        log_message(f"❌ Ошибка обновления HTML: {e}")
        return False

def main():
    print("🔧 Функция main() вызвана")
    log_message("🚀 Запуск HLTB Worker")
    log_message(f"📁 Рабочая директория: {os.getcwd()}")
    log_message(f"📄 Ищем файл: {GAMES_LIST_FILE}")

    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден!")
        return

    log_message(f"✅ Файл {GAMES_LIST_FILE} найден, размер: {os.path.getsize(GAMES_LIST_FILE)} байт")
    setup_directories()
    log_message("📁 Директории настроены")

    try:
        log_message("🔍 Начинаем извлечение списка игр...")
        games_list = extract_games_list(GAMES_LIST_FILE)
        total_games = len(games_list)
        log_message(f"✅ Извлечено {total_games} игр")

        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
            start_index = progress.get("current_index", 0)
            log_message(f"📂 Продолжаем с позиции {start_index}")
        else:
            start_index = 0

        log_message("🌐 Запускаем Playwright...")
        with sync_playwright() as p:
            log_message("🚀 Запускаем Chromium...")
            browser = p.chromium.launch(headless=True)
            log_message("✅ Chromium запущен")

            log_message("🔧 Создаем контекст браузера...")
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                locale="en-US"
            )
            log_message("✅ Контекст создан")

            log_message("📄 Создаем новую страницу...")
            page = context.new_page()
            log_message("✅ Страница создана")

            log_message("🔍 Проверяем доступность HowLongToBeat.com...")
            try:
                page.goto(BASE_URL, timeout=PAGE_GOTO_TIMEOUT)
                page.wait_for_load_state("domcontentloaded", timeout=PAGE_LOAD_TIMEOUT)
                title = page.title()
                log_message(f"📄 Заголовок страницы: {title}")
                search_box = page.locator('input[type="search"], input[name="q"]')
                if search_box.count() > 0:
                    log_message("✅ Поисковая строка найдена - сайт доступен")
                else:
                    log_message("⚠️ Поисковая строка не найдена - возможны проблемы")

                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP! Сайт заблокировал доступ")
                    return
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message("⚠️ Cloudflare проверка браузера - ждем...")
                    time.sleep(3)
                    page_content = page.content()
                    if "checking your browser" in page_content.lower():
                        log_message("❌ Cloudflare блокирует доступ")
                        return

                log_message("✅ Сайт доступен, начинаем обработку игр")

            except Exception as e:
                log_message(f"❌ Ошибка проверки доступности сайта: {e}")
                log_message("⚠️ Продолжаем работу, но возможны проблемы...")

            start_time = time.time()
            processed_count = 0
            blocked_count = 0

            for i in range(start_index, total_games):
                game = games_list[i]
                game_title = game.get("title") or ""
                game_year = game.get("year")

                log_message(f"🎮🎮🎮 Обрабатываю {i+1}/{total_games}: {game_title} ({game_year})")

                hltb_data = search_game_on_hltb(page, game_title, game_year)

                if hltb_data:
                    game["hltb"] = hltb_data
                    processed_count += 1
                    blocked_count = 0
                    log_message(f"✅ Найдены данные: {hltb_data}")
                else:
                    game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "all": "N/A"}
                    log_message(f"⚠️  Данные не найдены для: {game_title} - записано N/A")

                    page_content = page.content()
                    if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                        blocked_count += 1
                        log_message(f"🚫 Блокировка обнаружена ({blocked_count}/3)")
                        if blocked_count >= 3:
                            log_message("💥 Слишком много блокировок подряд! Останавливаем работу.")
                            break

                start_time = check_break_time(start_time, i + 1)

                if (i + 1) % 50 == 0:
                    save_progress(games_list, i + 1, total_games)
                    log_progress(i + 1, total_games, start_time)

            browser.close()

        save_results(games_list)

        successful = len([g for g in games_list if "hltb" in g])
        log_message(f"🎉 Завершено! Обработано {successful}/{total_games} игр ({successful/total_games*100:.1f}%)")

        log_message("🔄 Обновление HTML файла с данными HLTB...")
        if update_html_with_hltb(GAMES_LIST_FILE, games_list):
            log_message("✅ HTML файл успешно обновлен!")
        else:
            log_message("❌ Не удалось обновить HTML файл")

    except Exception as e:
        log_message(f"💥 Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    print("🎯 Запускаем main()...")
    try:
        main()
        print("✅ main() завершен успешно")
    except Exception as e:
        print(f"💥 Критическая ошибка в main(): {e}")
        import traceback
        traceback.print_exc()
        raise
