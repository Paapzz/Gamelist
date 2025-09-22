#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HLTB Worker (устойчивый вариант)
Автор: (исправления, отладка) — ваш помощник
Назначение:
 - взять список игр (index111.html)
 - для каждой игры искать оригинал на howlongtobeat.com
 - собрать времена (ms, mpe, comp, all, coop, vs)
 - сохранять результаты в hltb_data/hltb_data.json
 - при DEBUG: сохранять дампы в debug_dumps/
"""
import os
import re
import json
import time
import random
import ast
import shutil
import traceback
from datetime import datetime
from urllib.parse import quote
from difflib import SequenceMatcher

# Playwright import + helpful error if dependency missing
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
except Exception as e:
    print("❗ Ошибка импорта playwright. Убедитесь, что зависимости установлены (playwright, pyee).")
    print("  Рекомендуем: pip install playwright pyee && playwright install chromium")
    raise

# --- Настройки ---
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "hltb_data.json")
DEBUG_DIR = "debug_dumps"
DEBUG = True  # включить/выключить дампы
COMMIT_RESULTS = False  # если True — скрипт попытается закоммитить (требует git-credentials в окружении)
MAX_ATTEMPTS_PER_VARIANT = 3
WAIT_SELECTOR_TIMEOUT_MS = 5000  # первый wait_for_selector
FINAL_PAGE_TIMEOUT_MS = 20000
MIN_SLEEP = 0.4
MAX_SLEEP = 0.9

# characters not allowed in artifact uploads (Windows/NTFS), plus other odd ones
INVALID_FILENAME_CHARS = r'["<>:\\|?*\r\n]'

# --- Утилиты ---
def now_ts():
    return int(time.time())

def ts_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

def sanitize_filename(s: str) -> str:
    """Убирает опасные символы из имён файлов, заменяет пробелы — подчёркиванием."""
    s = re.sub(INVALID_FILENAME_CHARS, "_", s)
    s = s.replace(" ", "_")
    # Ограничим длину
    return s[:200]

def ensure_dirs():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if DEBUG:
        os.makedirs(DEBUG_DIR, exist_ok=True)

def write_json_safe(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def read_file_text(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def normalized_title(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[^0-9a-zа-яё\s]', ' ', s)  # keep letters and numbers, remove punctuation
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def similarity(a: str, b: str) -> float:
    """SequenceMatcher ratio on normalized strings."""
    return SequenceMatcher(None, normalized_title(a), normalized_title(b)).ratio()

# --- Извлечение списка игр из index111.html ---
def extract_games_list(html_file: str):
    """
    Пытается извлечь массив gamesList из HTML (var/const gamesList = [...]).
    Если не удаётся, пытается более простые эвристики (поиск строк вида Title (YEAR) или карточек).
    Возвращает список словарей {'title':..., 'year':... (int or None)}
    """
    content = read_file_text(html_file)
    # 1) Попробовать найти JS-структуру: const gamesList = [...]
    m = re.search(r'const\s+gamesList\s*=\s*(\[[\s\S]*?\]);', content, flags=re.MULTILINE)
    if m:
        raw = m.group(1)
        # Подготовить для ast.literal_eval: заменим JS null/true/false на Python
        raw_py = raw.replace('null', 'None').replace('true', 'True').replace('false', 'False')
        # Уберём возможно встречающиеся `` крошечные JS-функции (редко), но в основном это список объектов
        try:
            parsed = ast.literal_eval(raw_py)
            # ожидаем список, каждый элемент либо строка, либо dict с 'title'/'year'
            games = []
            for item in parsed:
                if isinstance(item, str):
                    # пытаться извлечь год
                    m2 = re.match(r'^(.*?)(?:\s*\((\d{4})\))?$', item.strip())
                    title = m2.group(1).strip()
                    year = int(m2.group(2)) if m2.group(2) else None
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
                print(f"📄 Parsed gamesList from JS: {len(games)} items.")
                return games
        except Exception as e:
            print("⚠️ Не удалось распарсить gamesList через ast.literal_eval:", e)
            # fall through to heuristics

    # 2) Найти все appearance "Title (YYYY)" в тексте — эвристика
    matches = re.findall(r'>([^<>]+?)\s*\((\d{4})\)\s*<', content)
    games = []
    for title, year in matches:
        games.append({"title": title.strip(), "year": int(year)})
    if games:
        print(f"📄 Heuristic found {len(games)} title(year) matches.")
        return games

    # 3) Поиск ссылок/карточек с классами (общая эвристика)
    titles = re.findall(r'<a[^>]+href=["\'][^"\']*?game[^"\']*["\'][^>]*>([^<]{2,200}?)</a>', content)
    uniq = []
    for t in titles:
        t_clean = re.sub(r'\s+', ' ', t).strip()
        if t_clean not in uniq:
            uniq.append(t_clean)
    if uniq:
        print(f"📄 Fallback link-title extraction found {len(uniq)} items.")
        out = []
        for t in uniq:
            m2 = re.match(r'^(.*?)(?:\s*\((\d{4})\))?$', t)
            title = m2.group(1).strip()
            year = int(m2.group(2)) if m2.group(2) else None
            out.append({"title": title, "year": year})
        return out

    raise RuntimeError("Не удалось извлечь список игр из " + html_file)

# --- HLTB helper: извлечь кандидатов со страницы (фоллбеки) ---
def extract_candidates_from_content(html_content: str):
    """
    Ищет внутри html_content все ссылки на /game/<id> и текст ссылок.
    Возвращает список {'title':..., 'href':..., 'year': maybe}
    """
    # Найдём все anchors с /game/
    pattern = re.compile(r'<a[^>]+href=["\'](?P<href>/game/\d+)[^"\']*["\'][^>]*>(?P<text>.*?)</a>', re.IGNORECASE | re.DOTALL)
    found = []
    for m in pattern.finditer(html_content):
        text = re.sub(r'<[^>]+>', '', m.group('text')).strip()
        href = m.group('href')
        # Попытаться вытащить год из текста
        ym = re.search(r'\((\d{4})\)', text)
        year = int(ym.group(1)) if ym else None
        found.append({"text": text, "href": href, "year": year})
    # Уникализируем по href
    seen = set()
    uniq = []
    for c in found:
        if c['href'] not in seen:
            uniq.append(c)
            seen.add(c['href'])
    return uniq

# --- Парсер блоков времени со страницы игры (финальная страница) ---
def parse_time_blocks_from_game_page(html_content: str):
    """
    Эвристическое извлечение блоков времени (ms, main+extras, comp, all, coop, vs)
    Ничего не гарантирует — по факту лучше оставлять эту логику простой и стабильной.
    Возвращает словарь с ключами ms,mpe,comp,all,coop,vs либо "N/A".
    """
    # Простая эвристика: ищем блоки вида "Main Story: X Hours" / "Main + Extras: Y Hours" на странице.
    res = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "all": "N/A", "coop": "N/A", "vs": "N/A"}
    # Ищем цифры/время
    # Пример: <div class="game_times"> ... "Main Story" ... <div class="time"> 6 Hours </div>
    blocks = re.findall(r'(?P<label>Main Story|Main \+ Extras|Completionist|All Playstyles|Co-Op|Vs\.)\s*[:\-\s]*.*?(?P<time>\d+\s*Hours|\d+\s*½|\d+:\d+)', html_content, flags=re.IGNORECASE | re.DOTALL)
    # Попробуем маппинг
    for label, time_str in blocks:
        label_norm = label.lower()
        if "main story" in label_norm:
            res["ms"] = time_str.strip()
        elif "main + extras" in label_norm or "main + extras" in label_norm.lower():
            res["mpe"] = time_str.strip()
        elif "completionist" in label_norm:
            res["comp"] = time_str.strip()
        elif "all playstyles" in label_norm or "all" == label_norm.lower():
            res["all"] = time_str.strip()
        elif "co-op" in label_norm or "co op" in label_norm:
            res["coop"] = time_str.strip()
        elif "vs" in label_norm:
            res["vs"] = time_str.strip()
    # Возвращаем словарь (если не нашли ничего — останется N/A)
    return res

# --- Основной рабочий процесс для одной игры ---
def process_game(browser, game, index, total):
    title = game.get("title") or ""
    year = game.get("year")
    display = f"{title} ({year})" if year else title
    print(f"[{ts_str()}] 🎮 Обрабатываю {index}/{total}: {display}")
    page = None
    result = {"input_title": title, "input_year": year, "hltb": {"ms":"N/A","mpe":"N/A","comp":"N/A","all":"N/A","coop":"N/A","vs":"N/A"}, "matched": None}
    try:
        context = browser.new_context()
        page = context.new_page()
        # Перебор вариаций наименования
        variants = generate_search_variants(title, year)
        best = None
        best_score = -1
        best_href = None
        # Перебираем варианты (вариант -> попытки)
        for variant in variants:
            q = quote(variant, safe='')
            search_url = f"{BASE_URL}/?q={q}"
            print(f"[{ts_str()}] 🔍 Ищем: '{variant}' -> URL: {search_url}")
            attempt = 0
            while attempt < MAX_ATTEMPTS_PER_VARIANT:
                attempt += 1
                # короткая пауза между попытками
                sleep_time = random.uniform(MIN_SLEEP, MAX_SLEEP)
                if attempt > 1:
                    print(f"[{ts_str()}] 🔄 Попытка {attempt}/{MAX_ATTEMPTS_PER_VARIANT} для '{title}' — пауза {sleep_time:.1f}s")
                time.sleep(sleep_time)
                try:
                    page.goto(search_url, wait_until="load", timeout=FINAL_PAGE_TIMEOUT_MS)
                except PlaywrightTimeoutError:
                    print(f"[{ts_str()}] ⚠️ Page.goto таймаут для {search_url}, попробуем читать содержимое страницы.")
                except Exception as e:
                    print(f"[{ts_str()}] ⚠️ Ошибка навигации: {e}")
                # Попытка найти кандидатов через стандартный локатор (JS-зависимый)
                candidates = []
                try:
                    # Ждём коротко основной контейнер кандидатов (если страница динамическая, может таймаутить)
                    page.wait_for_selector('a[href^="/game/"]', timeout=WAIT_SELECTOR_TIMEOUT_MS)
                    # Соберём элементы
                    anchors = page.query_selector_all('a[href^="/game/"]')
                    for a in anchors:
                        try:
                            href = a.get_attribute("href")
                            txt = a.inner_text().strip()
                            # попытка получить год (он может быть в соседнем элементе) — упрощённо: парсим из текста
                            ym = re.search(r'\((\d{4})\)', txt)
                            yv = int(ym.group(1)) if ym else None
                            candidates.append({"text": txt, "href": href, "year": yv})
                        except Exception:
                            continue
                except PlaywrightTimeoutError:
                    # Если wait_for_selector таймаутит — извлечём content и распарсим anchors через regex
                    print(f"[{ts_str()}] ⚠️ wait_for_selector таймаут — попробуем прочитать локаторы (возможно страница загружена динамически)")
                    content = page.content()
                    # Debug dumps
                    if DEBUG:
                        save_debug_file(f"{index}_{title}_search_html_{now_ts()}.html", content)
                    parsed = extract_candidates_from_content(content)
                    candidates.extend(parsed)
                except Exception as e:
                    print(f"[{ts_str()}] ⚠️ Ошибка получения кандидатов: {e}")
                    try:
                        content = page.content()
                        parsed = extract_candidates_from_content(content)
                        candidates.extend(parsed)
                    except Exception:
                        pass

                # Сохраняем дамп кандидатов
                if DEBUG:
                    fn = f"{index}_{title}_candidates_{now_ts()}.json"
                    save_debug_file(fn, candidates)

                # Если candidates больше 0 — оцениваем схожесть
                if not candidates:
                    print(f"[{ts_str()}] 🔎 Кандидатов на странице: 0")
                    # fallback: попробуем следующий вариант (или ре-ре загрузку)
                    continue

                # Оцениваем кандидатов: используем normalized similarity и годное совпадение
                for cand in candidates:
                    cand_title = cand.get("text", "")
                    cand_href = cand.get("href")
                    cand_year = cand.get("year")
                    score = similarity(title, cand_title)
                    # boost если год совпадает
                    if year and cand_year and year == cand_year:
                        score = max(score, 0.98)  # сильный сигнал
                    # boost для точного текстового совпадения
                    if normalized_title(title) == normalized_title(cand_title):
                        score = 1.0
                    # keep best
                    if score > best_score:
                        best_score = score
                        best = cand_title
                        best_href = cand_href
                # Если лучший кандидат хорош — остановимся
                print(f"[{ts_str()}] 🏁 Лучший кандидат на этой странице: '{best}' (score {best_score:.2f}) href={best_href}")
                if best_score >= 0.95:
                    break  # дальнейшие варианты не нужны
            # Если нашли хороший — выходим из variants
            if best_score >= 0.95:
                break

        # Финал: если best_href — откроем страницу игры и парсим времена
        if best_href:
            # нормализуем href (иногда без слеша)
            if not best_href.startswith("/"):
                best_href = "/" + best_href
            game_url = BASE_URL + best_href
            print(f"[{ts_str()}] 🔍 Открываем страницу игры: {game_url}")
            try:
                page.goto(game_url, wait_until="load", timeout=FINAL_PAGE_TIMEOUT_MS)
            except PlaywrightTimeoutError:
                print(f"[{ts_str()}] ⚠️ Page.goto таймаут при открытии конечной страницы, попробуем content()")
            content = page.content()
            # сохраняем финальную страницу
            if DEBUG:
                save_debug_file(f"{index}_{title}_final_page_search_html_{now_ts()}.html", content)
            # парсим времена (эвристика)
            parsed_times = parse_time_blocks_from_game_page(content)
            result["hltb"].update(parsed_times)
            result["matched"] = {"title": best, "href": best_href, "score": best_score}
            print(f"[{ts_str()}] ✅ Данные найдены для '{title}' -> matched '{best}' (score {best_score:.2f})")
        else:
            print(f"[{ts_str()}] ⚠️  Данные не найдены для: {title} - записано N/A")
            result["matched"] = None

    except Exception as e:
        print(f"[{ts_str()}] ❌ Ошибка обработки {title}: {e}")
        traceback.print_exc()
    finally:
        try:
            if page:
                page.close()
            # close context is automatic when browser.close()
        except Exception:
            pass
    return result

# --- Варианты поисковых строк ---
def generate_search_variants(title: str, year=None):
    """Создаёт устойчивый набор строк для поиска по HLTB"""
    v = []
    # базовый
    v.append(title)
    # без спецсимволов
    v.append(re.sub(r'[&/\\]', ' ', title))
    # годовые варианты
    if year:
        v.append(f"{title} {year}")
        v.append(f'{title} "{year}"')
    # римские -> арабские и наоборот (простая замена II <-> 2)
    v.append(title.replace(" II", " 2").replace("III", "3").replace("IV", "4"))
    v.append(title.replace(" 2", " II").replace("3", "III"))
    # несколько коротких вариаций: разбить по /
    if "/" in title:
        parts = [p.strip() for p in title.split("/")]
        for p in parts:
            if p and p not in v:
                v.append(p)
    # убрать скобки (например (& Knuckles))
    v.append(re.sub(r'\(.*?\)', '', title).strip())
    # уникализировать порядок
    uniq = []
    for s in v:
        s2 = re.sub(r'\s+', ' ', s).strip()
        if s2 and s2 not in uniq:
            uniq.append(s2)
    return uniq

# --- Debug save helpers ---
def save_debug_file(name: str, data):
    """Сохраняет дампы в DEBUG_DIR с безопасным именем. data может быть str или JSON-serializable."""
    ensure_dirs()
    safe = sanitize_filename(name)
    path = os.path.join(DEBUG_DIR, safe)
    try:
        if isinstance(data, (dict, list)):
            write_json_safe(path, data)
        else:
            # assume text or bytes-like
            mode = "wb" if isinstance(data, (bytes, bytearray)) else "w"
            with open(path, mode) as f:
                if mode == "w":
                    f.write(data)
                else:
                    f.write(data)
        print(f"[{ts_str()}] 🗂️ Сохранён дамп: {path}")
    except Exception as e:
        print(f"[{ts_str()}] ❌ Ошибка сохранения дампа {path}: {e}")

# --- Основной entrypoint ---
def main():
    print("🚀 Запуск HLTB Worker (устойчивый режим)")
    ensure_dirs()
    # Проверим входной файл
    if not os.path.exists(GAMES_LIST_FILE):
        raise SystemExit(f"❌ Файл {GAMES_LIST_FILE} не найден. Поместите туда список игр.")
    try:
        games_list = extract_games_list(GAMES_LIST_FILE)
    except Exception as e:
        print("❌ Критическая ошибка при извлечении списка игр:", e)
        raise

    total = len(games_list)
    print(f"[{ts_str()}] 📄 Извлечено {total} игр")
    # load existing output if exists (чтобы не перезаписывать)
    existing = []
    if os.path.exists(OUTPUT_FILE):
        try:
            existing = json.load(open(OUTPUT_FILE, "r", encoding="utf-8"))
        except Exception:
            existing = []

    results = existing[:]  # начнём с уже существующих результатов

    # запускаем playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
        try:
            for idx, g in enumerate(games_list, start=1):
                # если уже есть результат для этой игры по input_title -> пропускаем
                already = next((r for r in results if r.get("input_title") == g.get("title") and r.get("input_year") == g.get("year")), None)
                if already:
                    print(f"[{ts_str()}] ℹ️ Пропускаем {g.get('title')} - уже есть результат.")
                    continue
                res = process_game(browser, g, idx, total)
                results.append(res)
                # пишем промежуточно
                try:
                    write_json_safe(OUTPUT_FILE, results)
                except Exception as e:
                    print(f"[{ts_str()}] ❌ Ошибка сохранения результатов: {e}")
        finally:
            try:
                browser.close()
            except Exception:
                pass

    # финально: пересчёт статистики
    categories, total_polled, na_count = count_hltb_data(results)
    report = {
        "updated_at": ts_str(),
        "total_input": total,
        "total_processed": len(results),
        "na_count": na_count,
        "counts": categories,
        "polled": total_polled
    }
    # записать отчет
    try:
        write_json_safe(OUTPUT_FILE, results)
        write_json_safe(os.path.join(OUTPUT_DIR, "scraping_report.json"), report)
        print(f"[{ts_str()}] 🎉 Готово — обработано {len(results)}/{total} игр")
    except Exception as e:
        print(f"[{ts_str()}] ❌ Ошибка финальной записи: {e}")

    # опционально: попытаться сделать git commit/push (если включено)
    if COMMIT_RESULTS:
        try:
            print(f"[{ts_str()}] 🔧 COMMIT_RESULTS включён — пытаемся закоммитить результаты.")
            # Предполагается, что в окружении есть git + credentials (Actions)
            os.system("git config user.email 'hltb-worker@example.com' || true")
            os.system("git config user.name 'HLTB Worker' || true")
            os.system("git add -A || true")
            os.system("git commit -m 'HLTB worker: update results' || true")
            # пытаемся аккуратно подтянуть изменения и затем push
            os.system("git pull --rebase --autostash origin main || true")
            os.system("git push origin HEAD:main || true")
            print(f"[{ts_str()}] ✅ Попытка коммита завершена (см. вывод git).")
        except Exception as e:
            print(f"[{ts_str()}] ⚠️ Ошибка при попытке commit/push: {e}")

if __name__ == "__main__":
    main()
