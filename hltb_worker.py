#!/usr/bin/env python3

print("🚀 HLTB Worker запускается...")
print("📦 Импортируем модули...")

import json
import time
import random
import re
import os
from datetime import datetime
from urllib.parse import quote
from playwright.sync_api import sync_playwright

# Конфигурация
BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index111.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/hltb_data.json"
PROGRESS_FILE = "progress.json"

# Переменные окружения для GitHub Actions
DEBUG_MODE = os.getenv("HLTB_DEBUG", "false").lower() == "true"
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", "0"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "0"))

# Задержки для GitHub Actions (6 часов работы)
BREAK_INTERVAL_MIN = 15 * 60  # 15 минут в секундах
BREAK_INTERVAL_MAX = 25 * 60  # 25 минут в секундах
BREAK_DURATION_MIN = 60  # 60 секунд
BREAK_DURATION_MAX = 120  # 120 секунд
LONG_PAUSE_EVERY = 50  # Длинная пауза каждые 50 игр
LONG_PAUSE_DURATION_MIN = 30  # 30 секунд
LONG_PAUSE_DURATION_MAX = 90  # 90 секунд

def setup_directories():
    """Настройка директорий"""
    print(f"📁 Создаем директорию: {OUTPUT_DIR}")
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"✅ Директория {OUTPUT_DIR} создана/существует")
    except Exception as e:
        print(f"❌ Ошибка создания директории: {e}")
        raise
    
def log_message(message, level="INFO"):
    """Детальное логирование для отладки"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        print(log_entry)
        
        # Дополнительное логирование в файл для отладки
        if DEBUG_MODE or level in ["ERROR", "WARNING", "DEBUG"]:
            try:
                with open("hltb_debug.log", "a", encoding="utf-8") as f:
                    f.write(log_entry + "\n")
            except:
                pass  # Игнорируем ошибки записи в файл
                
    except Exception as e:
        print(f"Ошибка логирования: {e}")
        print(f"Сообщение: {message}")

def log_debug(message):
    """Логирование отладочной информации"""
    log_message(message, "DEBUG")

def log_warning(message):
    """Логирование предупреждений"""
    log_message(message, "WARNING")

def log_error(message):
    """Логирование ошибок"""
    log_message(message, "ERROR")

def count_hltb_data(hltb_data):
    """Подсчитывает количество данных HLTB по категориям"""
    categories = {"ms": 0, "mpe": 0, "comp": 0, "all": 0, "coop": 0, "vs": 0}
    total_polled = {"ms": 0, "mpe": 0, "comp": 0, "all": 0, "coop": 0, "vs": 0}
    na_count = 0
    
    for game in hltb_data:
        if "hltb" in game:
            # Проверяем, не является ли это N/A записью
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
                    # Подсчитываем общее количество голосов
                    if isinstance(game["hltb"][category], dict) and "p" in game["hltb"][category]:
                        total_polled[category] += game["hltb"][category]["p"]
    
    return categories, total_polled, na_count

def extract_games_list(html_file):
    """Извлекает список игр из HTML файла согласно логике из logs.py"""
    try:
        log_message(f"📖 Читаем файл {html_file}...")
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        log_message(f"📄 Файл прочитан, размер: {len(content)} символов")
        
        # Показываем первые 500 символов для отладки
        log_message(f"📄 Первые 500 символов файла: {content[:500]}")
        
        # Ищем все возможные паттерны gamesList
        gameslist_patterns = [
            r'const\s+gamesList\s*=',
            r'let\s+gamesList\s*=',
            r'var\s+gamesList\s*=',
            r'gamesList\s*=',
            r'gamesList\s*:'
        ]
        
        found_patterns = []
        for pattern in gameslist_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                found_patterns.append(pattern)
        
        log_message(f"📄 Найденные паттерны gamesList: {found_patterns}")
        
        # Способ 1: JS-array parsing
        log_message("🔍 Пробуем JS-array parsing...")
        games_list = try_js_array_parsing(content)
        if games_list:
            log_message(f"✅ Извлечено {len(games_list)} игр через JS-array parsing")
            return games_list
        
        # Способ 2: Поиск любых JSON объектов с title (приоритетный fallback)
        log_message("🔍 Пробуем поиск JSON объектов...")
        games_list = try_json_objects_search(content)
        if games_list:
            log_message(f"✅ Извлечено {len(games_list)} игр через JSON поиск")
            return games_list
        
        # Способ 3: Heuristic regex
        log_message("🔍 Пробуем heuristic regex...")
        games_list = try_heuristic_regex(content)
        if games_list:
            log_message(f"✅ Извлечено {len(games_list)} игр через heuristic regex")
            return games_list
        
        # Способ 4: Fallback на anchors
        log_message("🔍 Пробуем anchor fallback...")
        games_list = try_anchor_fallback(content)
        if games_list:
            log_message(f"✅ Извлечено {len(games_list)} игр через anchor fallback")
            return games_list
        
        raise ValueError("Не удалось извлечь список игр ни одним из способов")
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения списка игр: {e}")
        raise

def try_js_array_parsing(content):
    """Пытается извлечь список игр через JS-array parsing"""
    try:
        # Ищем const/let/var gamesList = [ ... ];
        patterns = [
            r'const\s+gamesList\s*=\s*\[(.*?)\];',
            r'let\s+gamesList\s*=\s*\[(.*?)\];',
            r'var\s+gamesList\s*=\s*\[(.*?)\];',
            r'gamesList\s*=\s*\[(.*?)\];',
            r'const\s+gamesList\s*=\s*\[(.*?)\]',
            r'let\s+gamesList\s*=\s*\[(.*?)\]',
            r'var\s+gamesList\s*=\s*\[(.*?)\]',
            r'gamesList\s*=\s*\[(.*?)\]'
        ]
        
        # Сначала найдем все вхождения gamesList в файле
        gameslist_positions = []
        for match in re.finditer(r'gamesList', content):
            start = max(0, match.start() - 50)
            end = min(len(content), match.end() + 50)
            context = content[start:end]
            gameslist_positions.append((match.start(), context))
        
        log_message(f"📝 Найдено {len(gameslist_positions)} вхождений 'gamesList' в файле")
        for i, (pos, context) in enumerate(gameslist_positions[:3]):  # Показываем первые 3
            log_message(f"📝 Вхождение {i+1} (позиция {pos}): {context}")
        
        for i, pattern in enumerate(patterns):
            log_message(f"📝 Проверяем паттерн {i+1}: {pattern}")
            match = re.search(pattern, content, re.DOTALL)
            if match:
                log_message(f"✅ Паттерн {i+1} найден!")
                array_content = match.group(1)
                log_message(f"📝 Найден JS массив, размер: {len(array_content)} символов")
                log_message(f"📝 Первые 200 символов: {array_content[:200]}")
                
                # Преобразуем JS в Python-safe
                # Удаляем trailing commas
                array_content = re.sub(r',\s*\]', ']', array_content)
                array_content = re.sub(r',\s*$', '', array_content)
                
                # Заменяем null/true/false
                array_content = array_content.replace('null', 'None')
                array_content = array_content.replace('true', 'True')
                array_content = array_content.replace('false', 'False')
                
                # Заменяем JavaScript объекты на Python словари
                # {"key": "value"} -> {"key": "value"}
                # Но нужно заменить одинарные кавычки на двойные для ключей
                array_content = re.sub(r"'([^']+)':", r'"\1":', array_content)
                
                log_message(f"📝 Обработанный массив (первые 200 символов): {array_content[:200]}")
                
                try:
                    # Парсим как Python код
                    import ast
                    games_list = ast.literal_eval('[' + array_content + ']')
                    
                    # Преобразуем в нужный формат
                    formatted_games = []
                    for game in games_list:
                        if isinstance(game, str):
                            # "Title (YYYY)" -> {"title": "Title", "year": YYYY}
                            title, year = extract_title_and_year(game)
                            formatted_games.append({"title": title, "year": year})
                        elif isinstance(game, dict):
                            # Извлекаем title и year из объекта
                            title = game.get("title", "")
                            year = game.get("year")
                            formatted_games.append({"title": title, "year": year})
                    
                    log_message(f"✅ Извлечено {len(formatted_games)} игр из JS массива")
                    return formatted_games
                    
                except Exception as parse_error:
                    log_message(f"❌ Ошибка парсинга JS массива: {parse_error}")
                    # Попробуем альтернативный способ - извлечение через regex
                    return extract_games_from_js_objects(array_content)
        
        return None
        
    except Exception as e:
        log_message(f"⚠️ JS-array parsing не удался: {e}")
        return None

def extract_games_from_js_objects(array_content):
    """Извлекает игры из JS объектов через regex"""
    try:
        formatted_games = []
        
        # Ищем все объекты в массиве
        # Паттерн для поиска объектов: {"key": "value", ...}
        object_pattern = r'\{[^}]*"title"[^}]*"year"[^}]*\}'
        matches = re.findall(object_pattern, array_content)
        
        log_message(f"📝 Найдено {len(matches)} объектов через regex")
        
        for match in matches:
            try:
                # Извлекаем title
                title_match = re.search(r'"title":\s*"([^"]*)"', match)
                title = title_match.group(1) if title_match else ""
                
                # Извлекаем year
                year_match = re.search(r'"year":\s*(\d+)', match)
                year = int(year_match.group(1)) if year_match else None
                
                if title:
                    formatted_games.append({"title": title, "year": year})
                    log_message(f"📝 Извлечена игра: {title} ({year})")
                    
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки объекта: {e}")
                continue
        
        log_message(f"✅ Извлечено {len(formatted_games)} игр через regex")
        return formatted_games if formatted_games else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения через regex: {e}")
        return None

def try_json_objects_search(content):
    """Поиск любых JSON объектов с title в файле"""
    try:
        formatted_games = []
        
        # Ищем все объекты с "title" в файле
        # Паттерн для поиска объектов: {"title": "...", ...}
        object_pattern = r'\{[^}]*"title"[^}]*\}'
        matches = re.findall(object_pattern, content)
        
        log_message(f"📝 Найдено {len(matches)} объектов с title через JSON поиск")
        
        for match in matches:
            try:
                # Извлекаем title
                title_match = re.search(r'"title":\s*"([^"]*)"', match)
                title = title_match.group(1) if title_match else ""
                
                # Извлекаем year (если есть)
                year_match = re.search(r'"year":\s*(\d+)', match)
                year = int(year_match.group(1)) if year_match else None
                
                if title:
                    formatted_games.append({"title": title, "year": year})
                    log_message(f"📝 JSON поиск: {title} ({year})")
                    
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки JSON объекта: {e}")
                continue
        
        log_message(f"✅ JSON поиск: извлечено {len(formatted_games)} игр")
        return formatted_games if formatted_games else None
        
    except Exception as e:
        log_message(f"❌ Ошибка JSON поиска: {e}")
        return None

def try_heuristic_regex(content):
    """Пытается извлечь список игр через heuristic regex"""
    try:
        # Ищем шаблоны >Title (YYYY)< в HTML
        pattern = r'>([^<]+)\s*\((\d{4})\)<'
        matches = re.findall(pattern, content)
        
        games_list = []
        for title, year in matches:
            title = title.strip()
            if title and year:
                games_list.append({"title": title, "year": int(year)})
        
        return games_list if games_list else None
        
    except Exception as e:
        log_message(f"⚠️ Heuristic regex не удался: {e}")
        return None

def try_anchor_fallback(content):
    """Fallback на anchors - собирает все <a ...>Title</a> ссылки"""
    try:
        # Ищем все <a ...>Title</a> ссылки
        pattern = r'<a[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, content)
        
        games_list = []
        for match in matches:
            title = match.strip()
            # Очищаем от тегов и лишних пробелов
            title = re.sub(r'<[^>]+>', '', title)
            title = re.sub(r'\s+', ' ', title).strip()
            
            if title:
                # Пытаемся извлечь год из названия
                title_clean, year = extract_title_and_year(title)
                games_list.append({"title": title_clean, "year": year})
        
        return games_list if games_list else None
        
    except Exception as e:
        log_message(f"⚠️ Anchor fallback не удался: {e}")
        return None

def extract_title_and_year(text):
    """Извлекает название и год из текста"""
    try:
        # Ищем паттерн "Title (YYYY)"
        match = re.search(r'^(.+?)\s*\((\d{4})\)$', text.strip())
        if match:
            title = match.group(1).strip()
            year = int(match.group(2))
            return title, year
        
        # Если год не найден, возвращаем весь текст как название
        return text.strip(), None
        
    except Exception as e:
        log_message(f"⚠️ Ошибка извлечения названия и года: {e}")
        return text.strip(), None

def parse_time_to_hours(time_str):
    """Парсит время в формате 'Xh Ym' или 'X Hours' в часы и минуты"""
    if not time_str or time_str == "N/A":
        return 0, 0
    
    # Убираем "Hours" если есть
    time_str = time_str.replace("Hours", "").strip()
    
    # Ищем часы и минуты (поддерживаем дробные часы)
    hours_match = re.search(r'(\d+(?:\.\d+)?)h', time_str)
    minutes_match = re.search(r'(\d+)m', time_str)
    
    hours = float(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    
    # Если нет "h" и "m", но есть только число (часы)
    if hours == 0 and minutes == 0:
        number_match = re.search(r'(\d+(?:\.\d+)?)', time_str)
        if number_match:
            hours = float(number_match.group(1))
            # Конвертируем дробную часть в минуты
            if hours != int(hours):
                minutes = int((hours - int(hours)) * 60)
                hours = int(hours)
    
    # Если часы дробные, конвертируем дробную часть в минуты
    if hours != int(hours):
        minutes += int((hours - int(hours)) * 60)
        hours = int(hours)
    
    return hours, minutes

def round_time(time_str):
    """Округляет время к ближайшему значению"""
    if not time_str or time_str == "N/A":
        return None
    
    hours, minutes = parse_time_to_hours(time_str)
    
    # Убеждаемся, что hours - целое число
    hours = int(hours)
    
    if minutes <= 14:
        return f"{hours}h"           # 0-14 мин → целый час
    elif minutes <= 44:
        return f"{hours}.5h"         # 15-44 мин → +0.5 часа
    else:
        return f"{hours + 1}h"       # 45-59 мин → +1 час

def random_delay(min_seconds, max_seconds):
    """Случайная задержка в указанном диапазоне"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def check_break_time(start_time, games_processed):
    """Проверяет, нужен ли перерыв"""
    elapsed_seconds = time.time() - start_time
    
    # Длинная пауза каждые LONG_PAUSE_EVERY игр
    if games_processed % LONG_PAUSE_EVERY == 0 and games_processed > 0:
        long_pause_duration = random.randint(LONG_PAUSE_DURATION_MIN, LONG_PAUSE_DURATION_MAX)
        log_message(f"⏸️  Длинная пауза {long_pause_duration} секунд... (обработано {games_processed} игр)")
        time.sleep(long_pause_duration)
        return time.time()  # Обновляем время начала
    
    # Рандомный интервал между перерывами
    break_interval = random.randint(BREAK_INTERVAL_MIN, BREAK_INTERVAL_MAX)
    
    if elapsed_seconds >= break_interval:
        # Рандомная длительность перерыва
        break_duration = random.randint(BREAK_DURATION_MIN, BREAK_DURATION_MAX)
        log_message(f"⏸️  Перерыв {break_duration} секунд... (обработано {games_processed} игр)")
        time.sleep(break_duration)
        return time.time()  # Обновляем время начала
    
    return start_time

def search_game_on_hltb(page, game_title, game_year=None):
    """Ищет игру на HLTB и возвращает данные с повторными попытками"""
    max_attempts = 2  # Уменьшено для GitHub Actions
    delays = [0, (30, 45)]  # Увеличены задержки для GitHub Actions
    
    # Генерируем поисковые варианты согласно логике из logs.py
    search_variants = generate_search_variants(game_title, game_year)
    log_message(f"🔄 Поисковые варианты для '{game_title}': {search_variants[:5]}...")  # Показываем первые 5
    
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
            
            # Пробуем все варианты поиска
            best_candidate = None
            best_score = 0
            all_candidates = []
            
            for variant in search_variants:
                candidates = search_candidates_on_hltb(page, variant)
                if candidates:
                    all_candidates.extend(candidates)
                    # Выбираем лучшего кандидата
                    chosen_candidate = choose_best_candidate(candidates, game_title, game_year)
                    if chosen_candidate and chosen_candidate.get('score', 0) > best_score:
                        best_score = chosen_candidate.get('score', 0)
                        best_candidate = chosen_candidate
                    
                    # Если нашли очень хорошее совпадение, прекращаем поиск
                    if best_score >= 0.95:
                        log_message(f"🎯 Найдено отличное совпадение! Прекращаем поиск.")
                        break
            
            if best_candidate and best_candidate.get('score', 0) >= 0.6:
                # Открываем финальную страницу и извлекаем данные
                hltb_data = extract_hltb_data_from_candidate(page, best_candidate)
                if hltb_data:
                    if attempt > 0:
                        log_message(f"✅ Успешно найдено с попытки {attempt + 1}")
                    log_message(f"🏆 Лучший результат: '{best_candidate.get('text', '')}' (схожесть: {best_score:.2f})")
                    return hltb_data
                else:
                    # Сохраняем отладочную информацию если данные не извлечены
                    save_debug_info(game_title, search_variants, all_candidates, best_candidate, None)
            else:
                # Сохраняем отладочную информацию если кандидат не найден
                if best_score < 0.6:
                    save_debug_info(game_title, search_variants, all_candidates, best_candidate, None)
            
        except Exception as e:
            log_message(f"❌ Ошибка попытки {attempt + 1} для '{game_title}': {e}")
            if attempt == max_attempts - 1:
                log_message(f"💥 Все попытки исчерпаны для '{game_title}'")
                return None
    
    return None

def search_candidates_on_hltb(page, search_variant):
    """Ищет кандидатов на HLTB для заданного варианта поиска"""
    try:
        log_message(f"🔍 Ищем кандидатов для: '{search_variant}'")
        
        # Кодируем название для URL
        safe_title = quote(search_variant, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"
        
        # Переходим на страницу поиска
        page.goto(search_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
        # Проверяем на блокировку
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при поиске!")
            return []
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка при поиске - ждем...")
            time.sleep(5)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует поиск")
                return []
        
        # Ждем загрузки результатов поиска (React контент)
        random_delay(2, 4)  # Случайная задержка 2-4 секунды
        
        # Ищем все ссылки на игры
        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()
        
        # Если результатов нет, ждем еще немного
        if found_count == 0:
            random_delay(2, 4)
            found_count = game_links.count()
        
        if found_count == 0:
            return []
        
        # Собираем кандидатов
        candidates = []
        for i in range(min(found_count, 10)):  # Ограничиваем первыми 10 результатами
            try:
                link = game_links.nth(i)
                href = link.get_attribute("href")
                text = link.inner_text().strip()
                
                if href and text:
                    # Пытаемся получить дополнительную информацию (год, платформы)
                    years = extract_years_from_candidate(link)
                    
                    candidate = {
                        "text": text,
                        "href": href,
                        "years": years
                    }
                    candidates.append(candidate)
                    
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки кандидата {i}: {e}")
                continue
        
        log_message(f"📊 Найдено {len(candidates)} кандидатов для '{search_variant}'")
        return candidates
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска кандидатов для '{search_variant}': {e}")
        return []

def extract_years_from_candidate(link_element):
    """Извлекает годы из элемента кандидата"""
    try:
        # Пытаемся получить родительский элемент с дополнительной информацией
        parent = link_element.locator("..")
        if parent.count() > 0:
            parent_text = parent.inner_text()
        else:
            parent_text = link_element.inner_text()
        
        # Ищем годы в тексте
        years = []
        year_matches = re.findall(r'\b(19|20)\d{2}\b', parent_text)
        for year_match in year_matches:
            try:
                year = int(year_match)
                if 1950 <= year <= 2030:  # Разумные границы для игр
                    years.append(year)
            except:
                continue
        
        return sorted(set(years))  # Уникальные, отсортированные годы
        
    except Exception as e:
        log_message(f"⚠️ Ошибка извлечения годов: {e}")
        return []

def choose_best_candidate(candidates, orig_title, input_year):
    """Выбирает лучшего кандидата согласно логике из logs.py"""
    if not candidates:
            return None
        
    try:
        # Вычисляем score для каждого кандидата
        scored_candidates = []
        for candidate in candidates:
            score = calculate_title_similarity(orig_title, candidate["text"])
            
            # Убираем бонус за подстроку - он завышает оценки
            # Теперь полагаемся только на алгоритм схожести
            
            # earliest_year = min(candidate.years) если есть годы
            earliest_year = min(candidate["years"]) if candidate["years"] else None
            
            scored_candidate = {
                **candidate,
                "score": score,
                "earliest_year": earliest_year
            }
            scored_candidates.append(scored_candidate)
        
        # Сортировка: по score desc, tie-break по earliest_year (меньше — лучше)
        scored_candidates.sort(key=lambda x: (-x["score"], x["earliest_year"] or 9999))
        
        # Определяем ambiguous если больше одного кандидата с score >= 0.80
        high_score_candidates = [c for c in scored_candidates if c["score"] >= 0.80]
        ambiguous = len(high_score_candidates) > 1
        
        # Правила принятия
        best_candidate = scored_candidates[0]
        
        # Rule 1: score >= 0.95 → принять
        if best_candidate["score"] >= 0.95:
            best_candidate["reason"] = "score_>=_0.95"
            return best_candidate
        
        # Rule 2: score >= 0.88 and candidate_earliest_year <= input_year
        if (best_candidate["score"] >= 0.88 and 
            best_candidate["earliest_year"] and 
            input_year and 
            best_candidate["earliest_year"] <= input_year):
            
            reason = "score_>=_0.88_and_year_ok"
            if ambiguous:
                reason += "_and_ambiguous"
            best_candidate["reason"] = reason
            return best_candidate
        
        # Rule 3: score >= 0.92 and candidate contains orig substring
        if (best_candidate["score"] >= 0.92 and 
            orig_normalized in candidate_normalized):
            best_candidate["reason"] = "score_>=_0.92_and_contains_orig"
            return best_candidate
        
        # Rule 4 (fallback): выбрать лучшего кандидата
        if len(scored_candidates) >= 2:
            top1, top2 = scored_candidates[0], scored_candidates[1]
            if abs(top1["score"] - top2["score"]) < 0.02:
                # Если топ-2 почти равны, выбрать с наименьшим earliest_year
                if (top1["earliest_year"] and top2["earliest_year"] and 
                    top1["earliest_year"] > top2["earliest_year"]):
                    best_candidate = top2
        
        best_candidate["reason"] = "fallback_best_score"
        return best_candidate
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего кандидата: {e}")
        return candidates[0] if candidates else None

def extract_hltb_data_from_candidate(page, candidate):
    """Извлекает данные HLTB со страницы выбранного кандидата"""
    try:
        href = candidate.get("href")
        if not href:
            return None
        
        # Переходим на страницу игры
        full_url = f"{BASE_URL}{href}"
        page.goto(full_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
        # Проверяем на блокировку
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры!")
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка на странице игры - ждем...")
            time.sleep(5)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует страницу игры")
                return None
        
        # Ждем загрузки данных игры
        random_delay(2, 4)
        
        # Извлекаем данные из таблицы
        hltb_data = extract_hltb_data_from_page(page)
        return hltb_data
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных с страницы кандидата: {e}")
        return None



def clean_title_for_comparison(title):
    """Очищает название игры для сравнения"""
    import re
    # Убираем лишние символы, приводим к нижнему регистру
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    # Убираем лишние пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned


def convert_arabic_to_roman(num_str):
    """Конвертирует арабские цифры в римские"""
    try:
        num = int(num_str)
        if num == 1:
            return "I"
        elif num == 2:
            return "II"
        elif num == 3:
            return "III"
        elif num == 4:
            return "IV"
        elif num == 5:
            return "V"
        elif num == 6:
            return "VI"
        elif num == 7:
            return "VII"
        elif num == 8:
            return "VIII"
        elif num == 9:
            return "IX"
        elif num == 10:
            return "X"
        else:
            return num_str
    except:
        return num_str

def generate_search_variants(title, year=None):
    """Генерирует поисковые варианты согласно логике из logs.py"""
    variants = []
    
    # Если есть год, добавляем вариант с годом в начало (сильный вариант)
    if year:
        variants.append(f"{title} {year}")
    
    # Основной вариант
    variants.append(title)
    
    # Обрабатываем названия с "/"
    if "/" in title:
        if " / " in title:  # Слэш окружен пробелами (A / B)
            parts = [part.strip() for part in title.split(" / ")]
            
            # Для каждой части генерируем sub-variants
            for part in parts:
                part_variants = generate_part_variants(part)
                variants.extend(part_variants)
            
            # Добавляем full-title-variants
            full_variants = generate_part_variants(title)
            variants.extend(full_variants)
            
        else:  # Слэш без пробелов (A/B/C)
            parts = [part.strip() for part in title.split("/")]
            
            # Определяем Base (префикс)
            base = determine_base(parts)
            
            # Парные варианты: Base A and B, Base B and C, etc.
            for i in range(len(parts) - 1):
                for j in range(i + 1, len(parts)):
                    if base:
                        pair_variant = f"{base} {parts[i]} and {parts[j]}"
                    else:
                        pair_variant = f"{parts[i]} and {parts[j]}"
                    variants.append(pair_variant)
            
            # Одиночные варианты: Base A, Base B, Base C
            for part in parts:
                if base:
                    single_variant = f"{base} {part}"
                else:
                    single_variant = part
                variants.append(single_variant)
            
            # Тройные / все вместе
            if len(parts) >= 3:
                if base:
                    triple_variant = f"{base} {' and '.join(parts)}"
                else:
                    triple_variant = " and ".join(parts)
                variants.append(triple_variant)
            
            # Without_parentheses, римские/ampersand для полного title
            full_variants = generate_part_variants(title)
            variants.extend(full_variants)
    else:
        # Если слэша нет - стандартная последовательность
        part_variants = generate_part_variants(title)
        variants.extend(part_variants)
    
    # Уникализируем, сохраняя порядок
    seen = set()
    unique_variants = []
    for variant in variants:
        if variant not in seen:
            seen.add(variant)
            unique_variants.append(variant)
    
    return unique_variants

def generate_part_variants(part):
    """Генерирует варианты для части названия"""
    variants = [part]
    
    # 1. part_without_parentheses — удаляем (...)
    without_parentheses = re.sub(r'\([^)]*\)', '', part).strip()
    if without_parentheses and without_parentheses != part:
        variants.append(without_parentheses)
    
    # 2. римско↔арабские варианты (II ↔ 2, т.д.)
    roman_variants = generate_roman_variants(part)
    variants.extend(roman_variants)
    
    # 3. ampersand-variants (&→and, и удалить короткие & Suffix)
    ampersand_variants = generate_ampersand_variants(part)
    variants.extend(ampersand_variants)
    
    return variants

def determine_base(parts):
    """Определяет базовую часть из списка частей"""
    if not parts:
        return ""
    
    # Эвристика: если первая часть содержит пробелы, берем все слова кроме последнего
    first_part = parts[0]
    if " " in first_part:
        words = first_part.split()
        if len(words) >= 2:
            # Проверяем, что последнее слово не является общим (Red, Blue, Yellow, etc.)
            last_word = words[-1].lower()
            common_words = {"red", "blue", "yellow", "green", "black", "white", "gold", "silver", "crystal"}
            if last_word in common_words:
                return " ".join(words[:-1])
    
    return ""

def generate_roman_variants(text):
    """Генерирует варианты с римскими/арабскими цифрами"""
    variants = []
    
    # Арабские → римские
    arabic_pattern = r'\b(\d+)\b'
    matches = re.findall(arabic_pattern, text)
    
    for match in matches:
        roman = convert_arabic_to_roman(match)
        if roman != match:
            roman_variant = re.sub(r'\b' + match + r'\b', roman, text)
            variants.append(roman_variant)
    
    # Римские → арабские
    roman_pattern = r'\b([IVX]+)\b'
    roman_matches = re.findall(roman_pattern, text)
    
    for match in roman_matches:
        arabic = convert_roman_to_arabic(match)
        if arabic != match:
            arabic_variant = re.sub(r'\b' + match + r'\b', arabic, text)
            variants.append(arabic_variant)
    
    return variants

def generate_ampersand_variants(text):
    """Генерирует варианты с амперсандом"""
    variants = []
    
    # & → and
    if "&" in text:
        and_variant = text.replace("&", "and")
        variants.append(and_variant)
    
    # and → &
    if " and " in text:
        amp_variant = text.replace(" and ", " & ")
        variants.append(amp_variant)
    
    # Удаляем короткие & Suffix (например, "Game & DLC" → "Game")
    amp_suffix_pattern = r'\s*&\s*\w{1,3}\b'
    without_suffix = re.sub(amp_suffix_pattern, '', text).strip()
    if without_suffix and without_suffix != text:
        variants.append(without_suffix)
    
    return variants

def convert_roman_to_arabic(roman_str):
    """Конвертирует римские цифры в арабские"""
    roman_to_arabic = {
        'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
        'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10
    }
    return str(roman_to_arabic.get(roman_str, roman_str))

def calculate_title_similarity(title1, title2):
    """Вычисляет схожесть между двумя названиями игр"""
    try:
        # Нормализуем названия для сравнения (конвертируем римские цифры в арабские)
        normalized1 = normalize_title_for_comparison(title1)
        normalized2 = normalize_title_for_comparison(title2)
        
        # Простой алгоритм схожести на основе общих слов
        words1 = set(normalized1.split())
        words2 = set(normalized2.split())
        
        if not words1 or not words2:
            return 0.0
        
        # Вычисляем пересечение слов
        common_words = words1.intersection(words2)
        total_words = words1.union(words2)
        
        # Базовая схожесть по словам
        word_similarity = len(common_words) / len(total_words)
        
        # Бонус за точное совпадение
        if normalized1 == normalized2:
            return 1.0
        
        # Убираем бонус за подстроку - он завышает оценки
        # Вместо этого используем более строгую оценку
        
        # Бонус за общие длинные слова (более 4 символов)
        long_common = [w for w in common_words if len(w) > 4]
        if long_common:
            word_similarity += 0.05 * len(long_common)  # Уменьшен бонус
        
        # Штраф за значительную разницу в длине названий
        length_diff = abs(len(normalized1) - len(normalized2))
        max_length = max(len(normalized1), len(normalized2))
        if max_length > 0:
            length_penalty = (length_diff / max_length) * 0.1
            word_similarity -= length_penalty
        
        return max(0.0, min(word_similarity, 1.0))
        
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести: {e}")
        return 0.0

def normalize_title_for_comparison(title):
    """Нормализует название для сравнения, конвертируя римские цифры в арабские"""
    try:
        import re
        
        # Словарь для конвертации римских цифр в арабские
        roman_to_arabic = {
            'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
            'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
        }
        
        # Заменяем римские цифры на арабские
        normalized = title
        for roman, arabic in roman_to_arabic.items():
            # Ищем римские цифры как отдельные слова
            pattern = r'\b' + roman + r'\b'
            normalized = re.sub(pattern, arabic, normalized)
        
        return normalized
        
    except Exception as e:
        log_message(f"❌ Ошибка нормализации названия: {e}")
        return title

def extract_gamestats_data(page):
    """Извлекает данные из GameStats блока (ul/li/h4-h5)"""
    try:
        hltb_data = {}
        
        # Ищем GameStats блок
        gamestats = page.locator('.GameStats_game_times__ ul li')
        if gamestats.count() == 0:
            # Альтернативный селектор
            gamestats = page.locator('[class*="GameStats"] ul li')
        
        if gamestats.count() == 0:
            return None
        
        log_message(f"📊 Найден GameStats блок с {gamestats.count()} элементами")
        
        for i in range(gamestats.count()):
            try:
                li = gamestats.nth(i)
                
                # Ищем h4 (категория) и h5 (время)
                h4 = li.locator('h4')
                h5 = li.locator('h5')
                
                if h4.count() > 0 and h5.count() > 0:
                    category = h4.inner_text().strip()
                    time_text = h5.inner_text().strip()
                    
                    # Пропускаем пустые или "--" значения
                    if not time_text or time_text == "--":
                        continue
                    
                    # Определяем тип категории
                    category_key = None
                    if "Main Story" in category:
                        category_key = "ms"
                    elif "Main + Extras" in category or "Main +Extra" in category:
                        category_key = "mpe"
                    elif "Completionist" in category:
                        category_key = "comp"
                    elif "Co-Op" in category or "Coop" in category:
                        category_key = "coop"
                    elif "Vs." in category or "Competitive" in category:
                        category_key = "vs"
                    
                    if category_key:
                        # Обрабатываем время
                        rounded_time = round_time(time_text)
                        if rounded_time:
                            hltb_data[category_key] = {"t": rounded_time}
                            log_message(f"📊 GameStats: {category} -> {rounded_time}")
                
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки GameStats элемента {i}: {e}")
                continue
        
        return hltb_data if hltb_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения GameStats данных: {e}")
        return None

def extract_earliest_year_from_page(page):
    """Извлекает самый ранний год со страницы игры"""
    try:
        # Получаем весь текст страницы
        page_text = page.inner_text()
        
        # Ищем все годы в тексте
        years = []
        year_matches = re.findall(r'\b(19|20)\d{2}\b', page_text)
        for year_match in year_matches:
            try:
                year = int(year_match)
                if 1950 <= year <= 2030:  # Разумные границы для игр
                    years.append(year)
            except:
                continue
        
        if years:
            earliest_year = min(years)
            log_message(f"📅 Найден самый ранний год на странице: {earliest_year}")
            return earliest_year
        
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения года со страницы: {e}")
        return None

def extract_hltb_data_from_page(page):
    """Извлекает данные HLTB со страницы игры"""
    try:
        hltb_data = {}
        
        # Сначала пробуем извлечь из GameStats блока (самый надежный)
        gamestats_data = extract_gamestats_data(page)
        if gamestats_data:
            hltb_data.update(gamestats_data)
            log_message(f"📊 Найдены данные в GameStats: {list(gamestats_data.keys())}")
        
        # Ищем все таблицы на странице
        tables = page.locator("table")
        table_count = tables.count()
        
        for table_idx in range(table_count):
            try:
                table = tables.nth(table_idx)
                table_text = table.inner_text()
                
                # Проверяем, содержит ли таблица нужные ключевые слова
                if any(keyword in table_text for keyword in ["Main Story", "Main + Extras", "Completionist", "Co-Op", "Competitive", "Vs."]):
                    log_message(f"📊 Обрабатываем таблицу {table_idx + 1}")
                    
                    # Получаем все строки таблицы
                    rows = table.locator("tr")
                    row_count = rows.count()
                    
                    for row_idx in range(row_count):
                        try:
                            row_text = rows.nth(row_idx).inner_text().strip()
                            
                            # Парсим строки с данными (только если еще не найдены)
                            if "Main Story" in row_text and "ms" not in hltb_data:
                                hltb_data["ms"] = extract_hltb_row_data(row_text)
                            elif "Main + Extras" in row_text and "mpe" not in hltb_data:
                                hltb_data["mpe"] = extract_hltb_row_data(row_text)
                            elif "Completionist" in row_text and "comp" not in hltb_data:
                                hltb_data["comp"] = extract_hltb_row_data(row_text)
                            elif "Co-Op" in row_text and "coop" not in hltb_data:
                                hltb_data["coop"] = extract_hltb_row_data(row_text)
                            elif "Competitive" in row_text and "vs" not in hltb_data:
                                hltb_data["vs"] = extract_hltb_row_data(row_text)
                                
                        except Exception as e:
                            log_message(f"⚠️ Ошибка обработки строки {row_idx}: {e}")
                            continue
                            
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки таблицы {table_idx}: {e}")
                continue
        
        # Ищем отдельные блоки с "Vs." (не в таблицах)
        try:
            vs_elements = page.locator('text="Vs."')
            vs_count = vs_elements.count()
            if vs_count > 0:
                for i in range(min(3, vs_count)):  # Проверяем первые 3 вхождения
                    try:
                        vs_element = vs_elements.nth(i)
                        surrounding_text = vs_element.evaluate("(e) => (e.closest('div')||e.parentElement||e).innerText")
                        
                        # Если это не таблица и содержит время, извлекаем данные
                        if "Hours" in surrounding_text and "table" not in str(vs_element.locator("..").get_attribute("tagName")).lower():
                            vs_data = extract_vs_data_from_text(surrounding_text)
                            if vs_data and "vs" not in hltb_data:
                                hltb_data["vs"] = vs_data
                                log_message(f"🎯 Найдены Vs. данные в отдельном блоке: {vs_data}")
                    except Exception as e:
                        log_message(f"⚠️ Ошибка обработки Vs. блока {i}: {e}")
                        continue
        except Exception as e:
            log_message(f"⚠️ Ошибка поиска Vs. блоков: {e}")
        
        # Если найдены только Vs. данные (чисто мультиплеерные игры), добавляем их как основную категорию
        if hltb_data and "vs" in hltb_data and len(hltb_data) == 1:
            log_message("🎮 Обнаружена чисто мультиплеерная игра, добавляем Vs. как основную категорию")
            # Не добавляем дополнительных категорий, оставляем только vs
        elif hltb_data and "vs" in hltb_data and len(hltb_data) == 2 and "stores" in hltb_data:
            log_message("🎮 Обнаружена чисто мультиплеерная игра с магазинами")
            # Не добавляем дополнительных категорий, оставляем только vs и stores
        
        # Собираем ссылки на магазины
        store_links = extract_store_links(page)
        if store_links:
            hltb_data["stores"] = store_links
        
        # Логируем итоговые результаты
        if hltb_data:
            categories = []
            for key, value in hltb_data.items():
                if key != "stores" and isinstance(value, dict) and "t" in value:
                    categories.append(f"{key}: {value['t']}")
            if categories:
                log_message(f"📊 Найдены категории: {', '.join(categories)}")
        
        return hltb_data if hltb_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных со страницы: {e}")
        return None

def extract_store_links(page):
    """Извлекает ссылки на магазины со страницы игры"""
    try:
        store_links = {}
        
        # Ищем ссылки на популярные магазины
        store_selectors = {
            "steam": "a[href*='store.steampowered.com']",
            "epic": "a[href*='epicgames.com']",
            "gog": "a[href*='gog.com']",
            "humble": "a[href*='humblebundle.com']",
            "itch": "a[href*='itch.io']",
            "origin": "a[href*='origin.com']",
            "uplay": "a[href*='uplay.com']",
            "battlenet": "a[href*='battle.net']",
            "psn": "a[href*='playstation.com']",
            "xbox": "a[href*='xbox.com']",
            "nintendo": "a[href*='nintendo.com']"
        }
        
        for store_name, selector in store_selectors.items():
            try:
                link_element = page.locator(selector).first
                if link_element.count() > 0:
                    href = link_element.get_attribute("href")
                    if href:
                        # Очищаем реферальные ссылки для GOG
                        if store_name == "gog" and "adtraction.com" in href:
                            # Извлекаем прямую ссылку из реферальной
                            import re
                            match = re.search(r'url=([^&]+)', href)
                            if match:
                                href = match.group(1)
                                # Декодируем URL
                                from urllib.parse import unquote
                                href = unquote(href)
                        
                        store_links[store_name] = href
            except:
                continue
        
        if store_links:
            log_message(f"🛒 Найдены ссылки на магазины: {list(store_links.keys())}")
        
        return store_links if store_links else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения ссылок на магазины: {e}")
        return None

def extract_hltb_row_data(row_text):
    """Извлекает данные из строки таблицы HLTB (новый формат)"""
    try:
        import re
        
        # Ищем количество голосов (поддерживаем K формат и табы)
        # Примеры: "Main Story 54 660h 37m" -> 54, "Main Story	1.7K	15h 31m" -> 1700
        polled_match = re.search(r'^[A-Za-z\s/\+]+\s+(\d+(?:\.\d+)?[Kk]?)\s+', row_text)
        if not polled_match:
            # Альтернативный поиск с табами: "Main Story	1.7K	15h 31m"
            polled_match = re.search(r'^[A-Za-z\s/\+]+\t+(\d+(?:\.\d+)?[Kk]?)\t+', row_text)
        if not polled_match:
            # Альтернативный поиск: число перед первым временем
            polled_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s+(?:\d+h|\d+\s*Hours?)', row_text)
        
        polled = None
        if polled_match:
            polled_str = polled_match.group(1)
            if 'K' in polled_str.upper():
                # Конвертируем K в тысячи
                number = float(polled_str.replace('K', '').replace('k', ''))
                polled = int(number * 1000)
            else:
                polled = int(float(polled_str))
        
        # Ищем времена в правильном порядке
        times = []
        
        # Убираем название категории и количество голосов из начала строки
        # Пример: "Main Story 707 5h 7m 5h 2h 45m 9h 1m" -> "5h 7m 5h 2h 45m 9h 1m"
        # Или: "Main Story	1.7K	15h 31m	15h	11h 37m	25h 37m" -> "15h 31m	15h	11h 37m	25h 37m"
        time_part = re.sub(r'^[A-Za-z\s/\+]+\s+\d+(?:\.\d+)?[Kk]?\s+', '', row_text)
        if time_part == row_text:  # Если не сработало, пробуем с табами
            time_part = re.sub(r'^[A-Za-z\s/\+]+\t+\d+(?:\.\d+)?[Kk]?\t+', '', row_text)
        
        # Парсим времена в правильном порядке: Average, Median, Rushed, Leisure
        # Формат: "5h 7m 5h 2h 45m 9h 1m"
        
        # Используем более точный подход - ищем все времена по порядку их появления
        # Объединенный паттерн для всех форматов времени (поддерживаем табы и пробелы)
        combined_pattern = r'(\d+h\s*\d+m|\d+(?:\.\d+)?[½]?\s*Hours?|\d+h)'
        
        # Ищем все времена в порядке их появления в строке
        matches = re.findall(combined_pattern, time_part)
        for match in matches:
            # Убираем лишние пробелы и табы
            clean_match = re.sub(r'\s+', ' ', match.strip())
            times.append(clean_match)
        
        if len(times) < 1:
            return None
        
        # Определяем тип данных по названию строки
        is_single_player = any(keyword in row_text for keyword in ["Main Story", "Main + Extras", "Completionist"])
        is_multi_player = any(keyword in row_text for keyword in ["Co-Op", "Competitive"])
        
        result = {}
        
        # Берем первые два времени (Average и Median)
        average_time = times[0] if len(times) > 0 else None
        median_time = times[1] if len(times) > 1 else None
        
        # Вычисляем среднее между Average и Median
        final_time = calculate_average_time(average_time, median_time)
        result["t"] = round_time(final_time) if final_time else None
        
        if polled:
            result["p"] = polled
        
        # Добавляем дополнительные времена в зависимости от типа
        if is_single_player and len(times) >= 4:
            # Single-Player: Average, Median, Rushed, Leisure
            result["r"] = round_time(times[2])  # Rushed (сокращенно и округлено)
            result["l"] = round_time(times[3])  # Leisure (сокращенно и округлено)
            
        elif is_multi_player and len(times) >= 4:
            # Multi-Player: Average, Median, Least, Most
            result["min"] = round_time(times[2])  # Least (сокращенно и округлено)
            result["max"] = round_time(times[3])  # Most (сокращенно и округлено)
            
        return result
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из строки: {e}")
        return None

def calculate_average_time(time1_str, time2_str):
    """Вычисляет среднее время между двумя временными значениями"""
    try:
        def parse_time_to_minutes(time_str):
            if not time_str:
                return 0
            
            # Убираем "Hours" если есть
            time_str = time_str.replace("Hours", "").strip()
            
            total_minutes = 0
            
            # Парсим часы и минуты
            if "h" in time_str and "m" in time_str:
                # Формат "660h 37m"
                import re
                hours_match = re.search(r'(\d+)h', time_str)
                minutes_match = re.search(r'(\d+)m', time_str)
                
                if hours_match:
                    total_minutes += int(hours_match.group(1)) * 60
                if minutes_match:
                    total_minutes += int(minutes_match.group(1))
                    
            elif "h" in time_str:
                # Только часы "660h"
                import re
                hours_match = re.search(r'(\d+)h', time_str)
                if hours_match:
                    total_minutes = int(hours_match.group(1)) * 60
                    
            elif time_str.replace(".", "").isdigit():
                # Только число (часы)
                total_minutes = float(time_str) * 60
                
            return total_minutes
        
        minutes1 = parse_time_to_minutes(time1_str)
        minutes2 = parse_time_to_minutes(time2_str)
        
        if minutes1 == 0 and minutes2 == 0:
            # Если оба времени равны 0, возвращаем первое доступное, но обработанное
            return round_time(time1_str or time2_str) if (time1_str or time2_str) else None
        
        if minutes2 == 0:
            # Если нет второго времени, возвращаем первое, но обработанное
            return round_time(time1_str) if time1_str else None
        
        # Вычисляем среднее
        avg_minutes = (minutes1 + minutes2) / 2
        
        # Конвертируем обратно в часы
        hours = avg_minutes / 60
        
        # Применяем умное округление
        if hours >= 1:
            if hours == int(hours):
                return f"{int(hours)}h"
            else:
                return f"{hours:.1f}h"
        else:
            return f"{int(avg_minutes)}m"
            
    except Exception as e:
        log_message(f"❌ Ошибка вычисления среднего времени: {e}")
        return time1_str or time2_str

def extract_vs_data_from_text(text):
    """Извлекает Vs. данные из текста"""
    try:
        import re
        
        # Убираем переносы строк для читаемого лога
        clean_text = text.replace('\n', ' ').replace('\r', ' ')
        log_message(f"🔍 Ищем Vs. данные в тексте: '{clean_text[:200]}...'")
        
        # Ищем различные форматы Vs. данных
        patterns = [
            r'Vs\.\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',  # "Vs. | 1767 Hours"
            r'Vs\.\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Vs. 1767 Hours"
            r'Vs\.\s*(\d+(?:\.\d+)?)\s*Hours?',        # "Vs.1767 Hours"
            r'Vs\.\s*(\d+(?:\.\d+)?[½]?)\s*Hours?',    # "Vs. 1767½ Hours"
        ]
        
        for pattern in patterns:
            vs_match = re.search(pattern, text)
            if vs_match:
                time_str = vs_match.group(1)
                # Обрабатываем дробные часы с ½
                if '½' in time_str:
                    time_str = time_str.replace('½', '.5')
                
                hours = float(time_str)
                
                if hours >= 1:
                    if hours == int(hours):
                        formatted_time = f"{int(hours)}h"
                    else:
                        formatted_time = f"{hours:.1f}h"
                else:
                    formatted_time = f"{int(hours * 60)}m"
                
                log_message(f"✅ Найдены Vs. данные: {formatted_time}")
                return {"t": formatted_time}
        
        log_message("❌ Vs. данные не найдены")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Vs. данных: {e}")
        return None

def extract_time_and_polled_from_row(row_text):
    """Извлекает время и количество голосов из строки таблицы"""
    try:
        # Ищем время в формате "Xh Ym"
        time_match = re.search(r'(\d+h\s*\d*m)', row_text)
        if time_match:
            time_str = time_match.group(1)
            rounded_time = round_time(time_str)
            
            # Ищем количество голосов - более гибкий поиск
            polled_count = None
            
            # Вариант 1: Ищем число перед "Polled"
            polled_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)', row_text, re.IGNORECASE)
            if polled_match:
                polled_str = polled_match.group(1)
                polled_count = parse_polled_number(polled_str)
            
            # Вариант 2: Ищем число в начале строки (часто количество голосов идет первым)
            if not polled_count:
                first_number_match = re.search(r'^(\d+(?:\.\d+)?[Kk]?)', row_text.strip())
                if first_number_match:
                    polled_str = first_number_match.group(1)
                    polled_count = parse_polled_number(polled_str)
            
            # Вариант 3: Ищем любое число в строке (если другие варианты не сработали)
            if not polled_count:
                any_number_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)', row_text)
                if any_number_match:
                    polled_str = any_number_match.group(1)
                    polled_count = parse_polled_number(polled_str)
            
            # Возвращаем объект с временем и количеством голосов (сокращенные названия)
            result = {"t": rounded_time}
            if polled_count:
                result["p"] = polled_count
            
            return result
        return None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из строки: {e}")
        return None

def parse_polled_number(polled_str):
    """Парсит число голосов из строки"""
    try:
        if 'K' in polled_str.upper():
            return int(float(polled_str.upper().replace('K', '')) * 1000)
        else:
            return int(float(polled_str))
    except:
        return None


def save_progress(games_data, current_index, total_games):
    """Сохраняет прогресс выполнения"""
    progress_data = {
        "current_index": current_index,
        "total_games": total_games,
        "processed_games": len([g for g in games_data if "hltb" in g]),
        "last_updated": datetime.now().isoformat(),
        "status": "in_progress" if current_index < total_games else "completed"
    }
    
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)

def save_debug_info(game_title, search_variants, candidates, chosen_candidate, hltb_data):
    """Сохраняет отладочную информацию для проблемных случаев"""
    try:
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "game_title": game_title,
            "search_variants": search_variants,
            "candidates": candidates,
            "chosen_candidate": chosen_candidate,
            "hltb_data": hltb_data
        }
        
        debug_file = f"debug_{game_title.replace(' ', '_').replace('/', '_')[:50]}.json"
        with open(debug_file, "w", encoding="utf-8") as f:
            json.dump(debug_info, f, indent=2, ensure_ascii=False)
        
        log_debug(f"Отладочная информация сохранена в {debug_file}")
        
    except Exception as e:
        log_error(f"Ошибка сохранения отладочной информации: {e}")

def save_results(games_data):
    """Сохраняет финальные результаты в компактном формате"""
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for i, game in enumerate(games_data):
                # Каждая игра на отдельной строке, без отступов
                if i > 0:
                    f.write("\n")
                json.dump(game, f, separators=(',', ':'), ensure_ascii=False)
        
        # Подсчитываем статистику
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
    """Логирует прогресс выполнения"""
    elapsed = time.time() - start_time
    rate = current / elapsed * 60 if elapsed > 0 else 0  # игр в минуту
    eta = (total - current) / rate if rate > 0 else 0  # оставшееся время
    
    log_message(f"📊 {current}/{total} | {rate:.1f} игр/мин | ETA: {eta:.0f} мин")

def update_html_with_hltb(html_file, hltb_data):
    """Обновляет HTML файл с новыми данными HLTB в компактном формате"""
    try:
        # Загружаем HTML
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Находим и заменяем gamesList
        start = content.find('const gamesList = [')
        if start == -1:
            raise ValueError("Не найден const gamesList в HTML файле")
        
        # Ищем конец массива - может быть ]; или просто ]
        end = content.find('];', start)
        if end == -1:
            end = content.find('\n];', start)
        if end == -1:
            end = content.find(']', start)
        if end == -1:
            raise ValueError("Не найден конец массива gamesList")
        
        end += 1  # Включаем символ ]
        if content[end] == ';':
            end += 1  # Включаем точку с запятой если есть
        
        # Создаем компактный JSON с HLTB данными (в одну строку)
        new_games_list = json.dumps(hltb_data, separators=(',', ':'), ensure_ascii=False)
        new_content = content[:start] + f'const gamesList = {new_games_list}' + content[end:]
        
        # Сохраняем обновленный HTML
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        log_message(f"✅ HTML файл обновлен: {html_file}")
        return True
        
    except Exception as e:
        log_message(f"❌ Ошибка обновления HTML: {e}")
        return False

def main():
    """Основная функция воркера"""
    print("🔧 Функция main() вызвана")
    log_message("🚀 Запуск HLTB Worker")
    log_message(f"📁 Рабочая директория: {os.getcwd()}")
    log_message(f"📄 Ищем файл: {GAMES_LIST_FILE}")
    log_message(f"🔧 Режим отладки: {DEBUG_MODE}")
    log_message(f"📦 Чанк: {CHUNK_INDEX}, размер: {CHUNK_SIZE}")
    
    # Проверяем существование файла
    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден!")
        return
    
    log_message(f"✅ Файл {GAMES_LIST_FILE} найден, размер: {os.path.getsize(GAMES_LIST_FILE)} байт")
    
    setup_directories()
    log_message("📁 Директории настроены")
    
    try:
        log_message("🔍 Начинаем извлечение списка игр...")
        # Извлекаем список игр
        games_list = extract_games_list(GAMES_LIST_FILE)
        total_games = len(games_list)
        log_message(f"✅ Извлечено {total_games} игр")
        
        # Обработка чанков для GitHub Actions
        if CHUNK_SIZE > 0 and CHUNK_INDEX >= 0:
            start_idx = CHUNK_INDEX * CHUNK_SIZE
            end_idx = min(start_idx + CHUNK_SIZE, total_games)
            games_list = games_list[start_idx:end_idx]
            log_message(f"📦 Обрабатываем чанк {CHUNK_INDEX}: игры {start_idx}-{end_idx-1} ({len(games_list)} игр)")
        elif CHUNK_INDEX > 0:
            log_message(f"⚠️ CHUNK_INDEX={CHUNK_INDEX} но CHUNK_SIZE=0, обрабатываем все игры")
        
        # Загружаем существующий прогресс, если есть
        if os.path.exists(PROGRESS_FILE):
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                progress = json.load(f)
            start_index = progress.get("current_index", 0)
            log_message(f"📂 Продолжаем с позиции {start_index}")
        else:
            start_index = 0
        
        # Запускаем браузер
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
            
            # Проверяем доступность сайта и возможный бан IP
            log_message("🔍 Проверяем доступность HowLongToBeat.com...")
            try:
                page.goto(BASE_URL, timeout=15000)
                page.wait_for_load_state("domcontentloaded", timeout=10000)
                
                # Проверяем заголовок страницы
                title = page.title()
                log_message(f"📄 Заголовок страницы: {title}")
                
                # Проверяем наличие основных элементов
                search_box = page.locator('input[type="search"], input[name="q"]')
                if search_box.count() > 0:
                    log_message("✅ Поисковая строка найдена - сайт доступен")
                else:
                    log_message("⚠️ Поисковая строка не найдена - возможны проблемы")
                
                # Проверяем на блокировку
                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP! Сайт заблокировал доступ")
                    return
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message("⚠️ Cloudflare проверка браузера - ждем...")
                    time.sleep(5)
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
            blocked_count = 0  # Счетчик блокировок
            
            # Обрабатываем игры
            for i in range(start_index, total_games):
                game = games_list[i]
                game_title = game.get("title", "")
                game_year = game.get("year")
                
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title} ({game_year})")
                
                # Ищем данные на HLTB с новой логикой
                hltb_data = search_game_on_hltb(page, game_title, game_year)
                
                if hltb_data:
                    game["hltb"] = hltb_data
                    processed_count += 1
                    blocked_count = 0  # Сбрасываем счетчик блокировок при успехе
                    log_message(f"✅ Найдены данные: {hltb_data}")
                else:
                    # Записываем N/A если данные не найдены
                    game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "all": "N/A"}
                    log_message(f"⚠️  Данные не найдены для: {game_title} - записано N/A")
                    
                    # Проверяем, не было ли блокировки
                    page_content = page.content()
                    if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                        blocked_count += 1
                        log_message(f"🚫 Блокировка #{blocked_count}")
                        
                        # Если много блокировок подряд - останавливаемся
                        if blocked_count >= 3:
                            log_message("💥 Слишком много блокировок подряд! Останавливаем работу.")
                            log_message("🔄 Рекомендуется подождать и попробовать позже.")
                            break
                
                # Вежливая задержка между играми для GitHub Actions
                random_delay(0.4, 1.2)  # Небольшая пауза, чтобы не выглядеть как бот
                
                # Проверяем перерыв
                start_time = check_break_time(start_time, i + 1)
                
                # Сохраняем прогресс каждые 25 игр (уменьшено для GitHub Actions)
                if (i + 1) % 25 == 0:
                    save_progress(games_list, i + 1, total_games)
                    log_progress(i + 1, total_games, start_time)
            
            browser.close()
        
        # Сохраняем финальные результаты
        save_results(games_list)
        
        # Финальная статистика
        successful = len([g for g in games_list if "hltb" in g])
        log_message(f"🎉 Завершено! Обработано {successful}/{total_games} игр ({successful/total_games*100:.1f}%)")
        
        # Обновляем HTML файл с новыми данными
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
