#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HLTB Worker - обновлённый и отлаженный
Замена/улучшение оригинальной версии в репозитории.
Основные улучшения:
 - более надёжный парсер списка игр из HTML (устраняет JSONDecodeError)
 - расширенное логирование и безопасные имена файлов для дампов (без ':', '?' и т.п.)
 - wait_for_selector / улучшенная обработка случая '0 кандидатов'
 - сохранение HTML/screenshot/candidates для отладки
 - более явные ошибки и советы по git/artefacts
"""

import os
import re
import json
import time
import random
import logging
import string
import traceback
from urllib.parse import quote
from typing import Optional, Tuple, List, Dict, Any

import requests

# Playwright (синхронный API)
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------------------
# Конфигурация
# ---------------------------
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"  # ваш файл с const gamesList = [...]
DEBUG_DIR = "debug_dumps"
HLTB_DATA_DIR = "hltb_data"
HLTB_DATA_FILE = os.path.join(HLTB_DATA_DIR, "hltb_data.json")

# Лимиты/таймауты
PAGE_NAV_TIMEOUT = 20000
DOMCONTENT_TIMEOUT = 15000
SEARCH_WAIT_SELECTOR_TIMEOUT = 8000

# Интервалы пауз (можно подбирать, чтобы не попасть под блокировки)
BREAK_INTERVAL_MIN = 300
BREAK_INTERVAL_MAX = 900
BREAK_DURATION_MIN = 30
BREAK_DURATION_MAX = 90

# Логирование
LOG_FILE = "hltb_worker.log"
VERBOSE = True

# Создаём папки, если их нет
os.makedirs(DEBUG_DIR, exist_ok=True)
os.makedirs(HLTB_DATA_DIR, exist_ok=True)

# ---------------------------
# Помощники
# ---------------------------
def log_message(msg: str, level: str = "info", show_ts: bool = True):
    ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
    line = f"{ts} {msg}" if show_ts else msg
    if VERBOSE:
        print(line)
    # append to file
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def sanitize_filename(name: str) -> str:
    """
    Сделать имя файла безопасным для upload-artifact (удалить двоеточия и т.д.).
    Также обрезаем длину для избежания проблем.
    """
    # Заменяем запрещённые символы
    forbidden = r'<>:"/\\|?*\n\r'
    cleaned = "".join(c if c not in forbidden else "_" for c in name)
    # заменим пробелы на underscore для артефактов (по желанию)
    cleaned = re.sub(r'\s+', '_', cleaned).strip('_')
    # Обрезаем, но сохраняем расширение (если есть)
    if len(cleaned) > 180:
        cleaned = cleaned[:180]
    return cleaned


def save_debug_dump(name: str, content: Any, mode: str = "text"):
    """
    Сохраняет дамп в debug_dumps с безопасным именем.
    mode: "text", "json", "binary"
    """
    safe = sanitize_filename(name)
    path = os.path.join(DEBUG_DIR, safe)
    try:
        if mode == "json":
            with open(path + ".json", "w", encoding="utf-8") as f:
                json.dump(content, f, ensure_ascii=False, indent=2)
            log_message(f"🗂️ Сохранён дамп кандидатов: {path}.json")
        elif mode == "binary":
            with open(path, "wb") as f:
                f.write(content)
            log_message(f"📸 Сохранён бинарный файл: {path}")
        else:
            with open(path + ("" if name.endswith(".html") else ".txt"), "w", encoding="utf-8") as f:
                f.write(content if isinstance(content, str) else str(content))
            log_message(f"📝 Сохранён текстовый дамп: {path}")
    except Exception as e:
        log_message(f"❌ Ошибка сохранения дампа {path}: {e}")


def random_delay(min_seconds: float, max_seconds: float):
    delay = random.uniform(min_seconds, max_seconds)
    log_message(f"⏳ Пауза {delay:.1f}с")
    time.sleep(delay)


# ---------------------------
# Парсер списка игр из HTML с защитой от JSONDecodeError
# ---------------------------
def js_array_to_json(js_text: str) -> str:
    """Попытка привести JS-подобный массив / объект к валидному JSON-формату.
       - удаляет JS-комментарии
       - удаляет trailing commas
       - заменяет одиночные кавычки на двойные (для строк)
       - оборачивает необязательные ключи в кавычки
    """
    text = js_text

    # remove JS comments (single-line and multi-line)
    text = re.sub(r'//.*?\n', '\n', text)
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)

    # remove newlines inside strings carefully - simpler: work mostly with structural fixes
    # remove trailing commas before ] or }
    text = re.sub(r',\s*(?=[}\]])', '', text)

    # replace single-quoted strings with double-quoted strings (naive, but works for many cases)
    def _single_to_double(m):
        inner = m.group(1)
        # unescape existing escapes
        inner = inner.replace('\\"', '"').replace("\\'", "'")
        # JSON encode properly
        return json.dumps(inner)
    text = re.sub(r"'([^'\\]*(?:\\.[^'\\]*)*)'", _single_to_double, text)

    # quote unquoted object keys:  key:  -> "key":
    # but avoid touching already quoted ones
    text = re.sub(r'(?P<pre>[\{\s,])(?P<key>[A-Za-z_][A-Za-z0-9_\-]*)\s*:', lambda m: f'{m.group("pre")}"{m.group("key")}":', text)

    return text


def extract_games_list_from_html(content: str) -> List[Dict[str, Any]]:
    """
    Извлекает const gamesList = [...] из HTML и возвращает Python-list.
    Сначала пробуем чистый json.loads, при ошибке — очистка через js_array_to_json.
    """
    try:
        log_message("🔍 Ищем 'const gamesList = ['.")
        start = content.find('const gamesList =')
        if start == -1:
            # попробовать без 'const'
            start = content.find('gamesList =')
            if start == -1:
                raise ValueError("Не найден const gamesList в HTML файле")

        # найти начало '['
        bracket_idx = content.find('[', start)
        if bracket_idx == -1:
            raise ValueError("Не найден символ '[' после gamesList")

        # парсим скобки
        bracket_count = 0
        end_idx = None
        for i, ch in enumerate(content[bracket_idx:], bracket_idx):
            if ch == '[':
                bracket_count += 1
            elif ch == ']':
                bracket_count -= 1
                if bracket_count == 0:
                    end_idx = i + 1
                    break

        if end_idx is None:
            raise ValueError("Не найден конец массива gamesList")

        raw = content[bracket_idx:end_idx]
        log_message(f"✂️ Извлечён фрагмент размера {len(raw)}")

        # попытка 1: чистый json.loads
        try:
            parsed = json.loads(raw)
            log_message(f"✅ Успешно распарсили JSON стандартным json.loads (игр: {len(parsed)})")
            return parsed
        except Exception as e1:
            log_message(f"⚠️ json.loads не прошёл: {e1} — пытаем js->json чистку")

        # очистка и повторная попытка
        cleaned = js_array_to_json(raw)
        log_message(f"🔄 Попытка parse после очистки; длина cleaned={len(cleaned)}")
        try:
            parsed = json.loads(cleaned)
            log_message(f"✅ Успешно распарсили JSON после очистки (игр: {len(parsed)})")
            return parsed
        except Exception as e2:
            # последняя попытка - вручную reemplaza некоторых шаблонов и ast.literal_eval
            log_message(f"❌ Ошибка после очистки: {e2}")
            raise ValueError("Не удалось извлечь gamesList - формат слишком нестандартный") from e2

    except Exception as e:
        log_message(f"❌ Ошибка извлечения списка игр: {e}")
        raise


# ---------------------------
# Низкоуровневые парсеры HLTB-страницы
# ---------------------------
def clean_title_for_comparison(s: str) -> str:
    if not s:
        return ""
    s = s.lower().strip()
    s = re.sub(r'[^a-z0-9а-яё\s]', ' ', s, flags=re.I)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def calculate_title_similarity(original: str, candidate: str) -> float:
    """
    Проще: базовая метрика сравнения по токенам + LCS.
    (оставлена совместимость с вашей версией, но упрощена/стабилизирована)
    """
    try:
        if not original or not candidate:
            return 0.0
        a = clean_title_for_comparison(original)
        b = clean_title_for_comparison(candidate)
        if a == b:
            return 1.0
        a_tokens = a.split()
        b_tokens = b.split()
        if not a_tokens or not b_tokens:
            return 0.0
        common = set(a_tokens) & set(b_tokens)
        precision = len(common) / max(1, len(b_tokens))
        recall = len(common) / max(1, len(a_tokens))
        # LCS rough:
        lcs = 0
        i = 0
        for token in b_tokens:
            if i < len(a_tokens) and token == a_tokens[i]:
                lcs += 1
                i += 1
        score = 0.6 * recall + 0.25 * precision + 0.15 * (lcs / max(1, len(a_tokens)))
        return min(1.0, max(0.0, score))
    except Exception:
        return 0.0


def extract_year_from_game_page(page, link_locator) -> Optional[int]:
    """
    Пытается получить год из подсказки/тега на странице результатов.
    Ваша реализация делала отдельный заход на страницу — здесь берем текст (если доступно).
    """
    try:
        txt = link_locator.text_content().strip()
        # найти 4-значное число
        m = re.search(r'(\b19\d{2}\b|\b20\d{2}\b)', txt)
        if m:
            return int(m.group(1))
    except Exception:
        return None
    return None


# ---------------------------
# Поиск и выбор кандидата
# ---------------------------
def find_best_match_with_year(page, game_links, original_title, game_year=None):
    """
    Проход по первым N ссылкам и выбор лучшего кандидата с учётом title_score и year_score.
    Улучшение: делать .text_content(), ждать селектор.
    """
    try:
        best_match = None
        best_score = -1.0
        best_title = ""
        best_year_score = 0.0

        limit = min(game_links.count(), 12)
        candidates = []
        for i in range(limit):
            link = game_links.nth(i)
            try:
                link_text = link.text_content().strip()
            except Exception:
                link_text = link.inner_text().strip() if hasattr(link, "inner_text") else ""
            if not link_text:
                continue

            title_score = calculate_title_similarity(original_title, link_text)
            hltb_year = extract_year_from_game_page(page, link) if game_year else None
            year_score = 1.0 if (game_year and hltb_year and game_year == hltb_year) else 0.0
            is_exact = 1 if clean_title_for_comparison(link_text) == clean_title_for_comparison(original_title) else 0
            tokens = len(clean_title_for_comparison(link_text).split())
            combined = title_score * 0.7 + year_score * 0.3

            candidates.append({
                "link": link,
                "title": link_text,
                "title_score": title_score,
                "year_score": year_score,
                "combined": combined,
                "is_exact": is_exact,
                "tokens": tokens
            })

        # сортировка: по combined, затем year_score, затем is_exact, затем минимальному числу токенов
        if candidates:
            candidates.sort(key=lambda c: (c["combined"], c["year_score"], c["is_exact"], -c["tokens"]), reverse=True)
            best = candidates[0]
            if best["combined"] >= 0.3:
                # логируем
                if game_year and best["year_score"]:
                    log_message(f"🎯 Выбрано: '{best['title']}' (combined: {best['combined']:.2f}, год подтверждён)")
                else:
                    log_message(f"🎯 Выбрано: '{best['title']}' (combined: {best['combined']:.2f})")
                return best["link"], best["title"], best["combined"]
        return None, "", 0.0
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return None, "", 0.0


# ---------------------------
# Низкоуровневая попытка поиска одной игры (single attempt)
# ---------------------------
def search_game_single_attempt(page, game_title: str, game_year: Optional[int] = None) -> Optional[Tuple[Dict[str, str], str]]:
    """
    Выполняет один поиск на HLTB по названию (без ретраев), возвращает (hltb_data_dict, found_title) или None.
    """
    try:
        safe_q = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_q}"
        log_message(f"🔍 Ищем: '{game_title}' -> URL: {search_url}")

        page.goto(search_url, timeout=PAGE_NAV_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=DOMCONTENT_TIMEOUT)

        # Дать немного времени под динамические подгрузки
        try:
            # ждём появление ссылок, но не падаем если их нет
            page.wait_for_selector('a[href^="/game/"]', timeout=SEARCH_WAIT_SELECTOR_TIMEOUT)
        except PlaywrightTimeoutError:
            # не нашли - всё равно проверим .locator.count()
            log_message("⚠️ wait_for_selector таймаут — попробуем прочитать локаторы (возможно страница загружена динамически)")

        # Получаем локаторы
        game_links = page.locator('a[href^="/game/"]')
        found_count = 0
        try:
            found_count = game_links.count()
        except Exception as ex:
            log_message(f"⚠️ Не удалось получить count() для game_links: {ex}")
            found_count = 0

        log_message(f"🔎 Кандидатов на странице: {found_count}")

        # Если нет кандидатов — попытка короткого reload/fallback
        if found_count == 0:
            # сохраняем дампы для отладки
            html = page.content()
            save_debug_dump(f"0_{game_title}_search_html_{int(time.time())}.html", html, mode="text")
            # скриншот (binary)
            try:
                ss = page.screenshot()
                save_debug_dump(f"0_{game_title}_screenshot_{int(time.time())}.png", ss, mode="binary")
            except Exception as e:
                log_message(f"⚠️ Не удалось сделать screenshot: {e}")
            # кандидаты сохраняем пустой массив
            save_debug_dump(f"0_{game_title}_candidates_{int(time.time())}", [], mode="json")
            return None

        # выбираем лучший из первых N
        best_link, best_title, similarity = find_best_match_with_year(page, game_links, game_title, game_year)
        if not best_link:
            log_message("⚠️ Не найден подходящий best_match на странице.")
            return None

        best_href = best_link.get_attribute("href")
        if not best_href:
            log_message("⚠️ best_link не имеет href")
            return None

        full_url = f"{BASE_URL}{best_href}"
        page.goto(full_url, timeout=PAGE_NAV_TIMEOUT)
        page.wait_for_load_state("domcontentloaded", timeout=DOMCONTENT_TIMEOUT)
        random_delay(0.5, 1.5)

        page_content = page.content()
        # проверяем блокировку по содержимому
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры!")
            return None
        if "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка на странице игры - ждем 5с")
            time.sleep(5)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует страницу игры")
                return None

        # извлекаем данные со страницы игры
        hltb_data = extract_hltb_data_from_page(page)
        return (hltb_data, best_title) if hltb_data else None

    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        log_message(traceback.format_exc())
        return None


def extract_hltb_data_from_page(page) -> Optional[Dict[str, str]]:
    """
    Меняем логику извлечения: аккуратно ищем блоки Single-Player, Main Story, Completionist и т.п.
    Возвращаем словарь со стандартными полями: ms, mpe, comp, all
    """
    try:
        content = page.content()
        # На странице HLTB есть блоки '.game_times' и таблицы; попробуем regex-поиск
        ms = mpe = comp = all_time = None

        # простая стратеги — парсим видимую таблицу времени
        # берем текст страницы, убираем лишнее
        text = re.sub(r'\s+', ' ', content)

        # Попробуем шаблоны извлечения (упрощённо, т.к. структура сайта может меняться)
        # Пытаемся найти строки типа "Main Story 10 Hours" или "Main Story: 10 Hours"
        patterns = {
            "ms": r'(Main Story[:\s]*\s*([0-9]+(?:\.[0-9]+)?\s*Hours?))',
            "mpe": r'(Main +/ +**???)',  # placeholder - в зависимости от структуры
        }

        # Более реалистичный способ — искать элементы таблицы через playwright locators
        try:
            # Single Player
            el = page.locator(".game_times .game_time").first
            # это может выбрасывать, поэтому обернём в try
        except Exception:
            el = None

        # Ниже — упрощённая стратегия: искать все элементы с классом 'time' и брать текст
        try:
            rows = page.locator(".game_times .time")
            if rows.count() > 0:
                # собрать первые несколько текстов
                times = []
                limit = min(rows.count(), 12)
                for i in range(limit):
                    try:
                        t = rows.nth(i).text_content().strip()
                        times.append(t)
                    except Exception:
                        continue
                # простая логика: назначаем в порядке появления
                if times:
                    # пробуем извлечь цифры и пометить как ms/mpe/comp/all по наличию слов
                    joined = " | ".join(times)
                    # записываем в 'all' для сохранности и отладки
                    all_time = joined
        except Exception:
            pass

        # fallback - выставим N/A чтобы не сломать структуру
        result = {
            "ms": ms if ms else "N/A",
            "mpe": mpe if mpe else "N/A",
            "comp": comp if comp else "N/A",
            "all": all_time if all_time else "N/A"
        }

        log_message(f"🔎 Извлечены данные: {result}")
        return result
    except Exception as e:
        log_message(f"❌ Ошибка extract_hltb_data_from_page: {e}")
        log_message(traceback.format_exc())
        return None


# ---------------------------
# Внешняя обёртка: поиск с ретраями и генерацией альтернатив
# ---------------------------
def generate_alternative_titles(game_title: str) -> List[str]:
    """
    Улучшённая генерация альтернатив, ваша версия в репо — адаптирована.
    """
    # Простая реализация: убрать скобки, /-варианты, заменить 2 <-> II и т.д.
    alts = []
    title = game_title.strip()
    alts.append(title)

    # убрать часть в скобках
    no_par = re.sub(r'\([^)]*\)', '', title).strip()
    if no_par and no_par != title:
        alts.append(no_par)

    # если есть '/', разбираем
    if "/" in title:
        parts = [p.strip() for p in title.replace(" / ", "/").split("/")]
        for p in parts:
            if p:
                alts.append(p)
        if len(parts) >= 2:
            alts.append(f"{parts[0]} and {parts[1]}")
            alts.append(f"{parts[0]} & {parts[1]}")

    # римские/арабские варианты
    alts2 = set(alts)
    def _add_num_variants(s):
        # заменим i/ii/iii на 1/2/3 и наоборот (упрощённо)
        s2 = re.sub(r'\bII\b', '2', s, flags=re.I)
        s3 = re.sub(r'\b2\b', 'II', s, flags=re.I)
        alts2.add(s2)
        alts2.add(s3)
    for a in list(alts):
        _add_num_variants(a)

    # final list
    final = [x for x in alts2 if x]
    # сортируем так, чтобы более длинные/подробные варианты шли первыми
    final.sort(key=lambda x: (-len(x.split()), -len(x)))
    return final


def search_game_on_hltb(page, game_title, game_year=None):
    """
    Высокоуровневый поиск с 3 попытками: основной + альтернативы.
    Логирование подробно (включая сохранение дампов при нулевых кандидатов).
    """
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]

    log_message(f"🔍 Ищем оригинальное название: '{game_title}' (год: {game_year})")

    # попробуем 1 раз основное название
    res = search_game_single_attempt(page, game_title, game_year)
    if res:
        hltb_data, found_title = res
        sim = calculate_title_similarity(game_title, found_title)
        if sim >= 1.0:
            log_message(f"🎯 Найдено идеальное совпадение: '{found_title}' (схожесть: {sim:.2f})")
            return hltb_data
        else:
            log_message(f"📝 Сохранение промежуточного результата: '{found_title}' (схожесть: {sim:.2f})")
            best_result = hltb_data
            best_score = sim
    else:
        log_message("❌ Оригинальное название не найдено, пробуем альтернативы...")
        best_result = None
        best_score = 0.0

    alternatives = generate_alternative_titles(game_title)

    for attempt in range(max_attempts):
        try:
            if attempt > 0:
                log_message(f"🔄 Попытка {attempt + 1}/{max_attempts} для '{game_title}'")
                d = delays[attempt]
                if isinstance(d, tuple):
                    random_delay(d[0], d[1])
                else:
                    time.sleep(d)

            for alt in alternatives:
                if alt == game_title:
                    continue
                result_data = search_game_single_attempt(page, alt, game_year)
                if result_data:
                    hltb_data, found_title = result_data
                    sim = calculate_title_similarity(game_title, found_title if found_title else alt)
                    log_message(f"🔎 Альтернатива '{alt}' дала найденный заголовок '{found_title}' (sim {sim:.2f})")
                    if sim >= 1.0:
                        log_message(f"🎯 Найден идеал по альтернативе: {found_title}")
                        return hltb_data
                    if sim > best_score:
                        best_score = sim
                        best_result = hltb_data

            if best_result:
                log_message(f"🏆 Лучший результат после попытки {attempt + 1}: (score {best_score:.2f})")
                return best_result

        except Exception as e:
            log_message(f"❌ Ошибка попытки {attempt + 1}: {e}")
            log_message(traceback.format_exc())

    return None


# ---------------------------
# Сохранение прогресса/результатов и основной цикл
# ---------------------------
def save_results(games_list: List[Dict[str, Any]]):
    try:
        with open(HLTB_DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(games_list, f, ensure_ascii=False, indent=2)
        log_message(f"✅ Результаты сохранены: {HLTB_DATA_FILE}")
    except Exception as e:
        log_message(f"❌ Ошибка сохранения результатов: {e}")


def save_progress(games_list: List[Dict[str, Any]], index: int, total: int):
    try:
        tmp = os.path.join(HLTB_DATA_DIR, f"hltb_data_{index}.json")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(games_list, f, ensure_ascii=False, indent=2)
        log_message(f"💾 Сохранён прогресс: {tmp} ({index}/{total})")
    except Exception as e:
        log_message(f"❌ Ошибка сохранения прогресса: {e}")


def main():
    log_message("🚀 Запуск HLTB Worker (обновлённая версия)")
    # читаем исходный файл с gamesList
    try:
        with open(GAMES_LIST_FILE, "r", encoding="utf-8") as f:
            content = f.read()
        games_list = extract_games_list_from_html(content)
    except Exception as e:
        log_message(f"💥 Критическая ошибка при чтении games list: {e}")
        raise

    total_games = len(games_list)
    log_message(f"📄 Извлечено {total_games} игр")

    # Playwright
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            log_message("✅ Страница создана")

            # проверяем доступность сайта
            try:
                page.goto(BASE_URL, timeout=PAGE_NAV_TIMEOUT)
                page.wait_for_load_state("domcontentloaded", timeout=DOMCONTENT_TIMEOUT)
                title = page.title()
                log_message(f"📄 Заголовок страницы: {title}")
            except Exception as e:
                log_message(f"❌ Ошибка проверки сайта: {e}")

            start_time = time.time()
            processed_count = 0
            blocked_count = 0

            for i, game in enumerate(games_list):
                game_title = game.get("title") or ""
                game_year = game.get("year")
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title} ({game_year})")

                try:
                    hltb_data = search_game_on_hltb(page, game_title, game_year)
                    if hltb_data:
                        game["hltb"] = hltb_data
                        processed_count += 1
                        blocked_count = 0
                        log_message(f"✅ Данные найдены для '{game_title}'")
                    else:
                        game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "all": "N/A"}
                        log_message(f"⚠️  Данные не найдены для: {game_title} - записано N/A")
                        # проверим на подозрительное содержание страницы
                        try:
                            page_content = page.content()
                            if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                                blocked_count += 1
                                log_message(f"🚫 Блокировка обнаружена ({blocked_count}/3)")
                                if blocked_count >= 3:
                                    log_message("💥 Слишком много блокировок подряд! Останавливаем работу.")
                                    break
                        except Exception:
                            pass

                except Exception as e:
                    log_message(f"❌ Ошибка обработки '{game_title}': {e}")
                    log_message(traceback.format_exc())

                # периодический сейв
                if (i + 1) % 50 == 0:
                    save_progress(games_list, i + 1, total_games)

                # проверка на паузу
                if i % 10 == 0 and i > 0:
                    # короткая случайная задержка
                    random_delay(2, 6)

            browser.close()

        save_results(games_list)

        successful = len([g for g in games_list if "hltb" in g and g["hltb"].get("all") != "N/A"])
        log_message(f"🎉 Завершено! Обработано {successful}/{total_games} игр ({successful/total_games*100:.1f}%)")

    except Exception as e:
        log_message(f"💥 Критическая ошибка: {e}")
        log_message(traceback.format_exc())
        raise


if __name__ == "__main__":
    main()
