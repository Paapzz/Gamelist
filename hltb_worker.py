#!/usr/bin/env python3
"""
HLTB Worker - обновленная версия с логикой из logs.py

Основные улучшения:
- Улучшенная генерация поисковых вариантов согласно logs.py
- Более точное ранжирование кандидатов с учетом года и схожести
- Оптимизированные тайминги для GitHub Actions (6 часов работы)
- Улучшенная логика работы с годами
- Отладочные функции для сохранения дампов
- Fallback извлечение кандидатов из HTML
- Использование SequenceMatcher для более точного расчета схожести

Настройки для GitHub Actions:
- Уменьшены таймауты страниц (12s/8s вместо 17s/10s)
- Случайные задержки 0.4-1.2 секунды между запросами
- Длинные паузы каждые 100 игр (30-60 секунд)
- Отладочные дампы при HLTB_DEBUG=true
"""

print("🚀 HLTB Worker запускается...")

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

# Оптимизированные задержки для GitHub Actions (6 часов работы)
# Уменьшены для максимальной эффективности, но сохранены для стабильности

# Таймауты для страниц (уменьшены для GitHub Actions)
PAGE_GOTO_TIMEOUT_MS = 12000  # 12 секунд (было 17)
WAIT_SELECTOR_TIMEOUT_MS = 8000  # 8 секунд (было 10)

# Настройки для GitHub Actions
LONG_PAUSE_EVERY = 100  # Пауза каждые 100 игр
LONG_PAUSE_DURATION = (30, 60)  # 30-60 секунд
RANDOM_DELAY_RANGE = (0.4, 1.2)  # Случайная задержка между запросами

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
    """Извлекает список игр из HTML файла"""
    try:
        log_message(f"📖 Читаем файл {html_file}...")
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        log_message(f"📄 Файл прочитан, размер: {len(content)} символов")
        
        # Находим начало и конец массива gamesList
        log_message("🔍 Ищем 'const gamesList = ['...")
        start = content.find('const gamesList = [')
        if start == -1:
            raise ValueError("Не найден const gamesList в HTML файле")
        
        log_message(f"✅ Найден const gamesList на позиции {start}")
        
        # Ищем закрывающую скобку массива
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
        
        # Извлекаем JSON
        log_message("✂️ Извлекаем JSON...")
        games_json = content[start:end]
        games_json = games_json.replace('const gamesList = ', '')
        
        log_message(f"📝 JSON извлечен, размер: {len(games_json)} символов")
        log_message("🔄 Парсим JSON...")
        
        games_list = json.loads(games_json)
        
        # Обрабатываем игры - год должен быть в отдельном параметре
        for i, game in enumerate(games_list):
            if isinstance(game, dict):
                # Если уже есть структура с title и year
                if "title" in game and "year" in game:
                    continue  # Уже правильно структурировано
                elif "title" in game:
                    # Есть title, но нет year - ищем год в title как fallback
                    title = game["title"]
                    years = extract_years_from_text(title)
                    game["year"] = min(years) if years else None
                else:
                    # Неправильная структура - пытаемся исправить
                    log_message(f"⚠️ Неправильная структура игры: {game}")
            elif isinstance(game, str):
                # Если игра представлена как строка, пытаемся извлечь год
                years = extract_years_from_text(game)
                if years:
                    # Преобразуем в объект
                    game_obj = {"title": game, "year": min(years)}
                    games_list[i] = game_obj
                else:
                    game_obj = {"title": game, "year": None}
                    games_list[i] = game_obj
        
        log_message(f"✅ Извлечено {len(games_list)} игр из HTML файла")
        return games_list
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения списка игр: {e}")
        raise

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

def random_delay(min_seconds=None, max_seconds=None):
    """Случайная задержка в указанном диапазоне или по умолчанию"""
    if min_seconds is None or max_seconds is None:
        min_seconds, max_seconds = RANDOM_DELAY_RANGE
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def save_debug_dumps(page, game_title, debug_type, candidates=None):
    """Сохраняет отладочные дампы согласно логике logs.py"""
    try:
        if os.getenv("HLTB_DEBUG") != "true":
            return
        
        debug_dir = "hltb_debug"
        os.makedirs(debug_dir, exist_ok=True)
        
        # Очищаем название для имени файла
        safe_title = re.sub(r'[^\w\s-]', '', game_title).strip()
        safe_title = re.sub(r'[-\s]+', '_', safe_title)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Сохраняем скриншот
        screenshot_path = f"{debug_dir}/{debug_type}_{safe_title}_{timestamp}.png"
        page.screenshot(path=screenshot_path)
        
        # Сохраняем HTML
        html_path = f"{debug_dir}/{debug_type}_{safe_title}_{timestamp}.html"
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(page.content())
        
        # Сохраняем кандидатов если есть
        if candidates:
            candidates_path = f"{debug_dir}/{debug_type}_{safe_title}_{timestamp}_candidates.json"
            with open(candidates_path, "w", encoding="utf-8") as f:
                json.dump(candidates, f, indent=2, ensure_ascii=False)
        
        log_message(f"🔍 DEBUG: Сохранены дампы для '{game_title}' ({debug_type})")
        
    except Exception as e:
        log_message(f"❌ Ошибка сохранения отладочных дампов: {e}")

def extract_candidates_from_html(html_content):
    """Извлекает кандидатов из HTML контента как fallback"""
    try:
        import re
        candidates = []
        
        # Ищем ссылки на игры в HTML
        pattern = r'<a[^>]*href="(/game/\d+)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, html_content)
        
        for href, text in matches:
            # Очищаем текст от HTML тегов
            clean_text = re.sub(r'<[^>]+>', '', text).strip()
            if clean_text:
                years = extract_years_from_text(clean_text)
                candidates.append({
                    "text": clean_text,
                    "href": href,
                    "years": years,
                    "context": clean_text
                })
        
        return candidates
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения кандидатов из HTML: {e}")
        return []

def extract_best_from_html_candidates(candidates, game_title, game_year):
    """Извлекает лучшего кандидата из HTML кандидатов"""
    try:
        if not candidates:
            return None, "", 0
        
        # Используем ту же логику выбора лучшего кандидата
        best_result = choose_best_candidate(candidates, game_title, game_year)
        
        if best_result and best_result["score"] >= 0.3:
            log_message(f"🎯 Выбран HTML кандидат: '{best_result['candidate']['text']}' (схожесть: {best_result['score']:.2f}, причина: {best_result['reason']})")
            # Возвращаем mock объект для совместимости
            return MockLink(best_result["candidate"]["href"]), best_result["candidate"]["text"], best_result["score"]
        
        return None, "", 0
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего HTML кандидата: {e}")
        return None, "", 0

class MockLink:
    """Mock объект для совместимости с Playwright link"""
    def __init__(self, href):
        self.href = href
    
    def get_attribute(self, attr):
        if attr == "href":
            return self.href
        return None


def search_game_on_hltb(page, game_title, game_year=None):
    """Ищет игру на HLTB и возвращает данные с повторными попытками"""
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]  # Паузы между попытками в секундах
    
    # Генерируем все альтернативные названия согласно логике logs.py
    alternative_titles = generate_alternative_titles(game_title, game_year)
    log_message(f"🔄 Альтернативные названия для '{game_title}': {alternative_titles}")
    
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
            
            # Пробуем все альтернативные названия и выбираем лучший результат
            best_result = None
            best_score = 0
            best_title = ""
            best_found_title = ""
            
            for alt_title in alternative_titles:
                result_data = search_game_single_attempt(page, alt_title, game_year)
                if result_data is not None:
                    # result_data теперь содержит (hltb_data, found_title)
                    hltb_data, found_title = result_data
                    
                    # Вычисляем схожесть между оригинальным названием и найденным результатом
                    score = calculate_title_similarity(
                        clean_title_for_comparison(game_title),
                        clean_title_for_comparison(found_title) if found_title else clean_title_for_comparison(alt_title)
                    )
                    
                    if score > best_score:
                        best_score = score
                        best_result = hltb_data
                        best_title = alt_title
                        best_found_title = found_title
                    
                    # Если нашли идеальное совпадение (100%), прекращаем поиск
                    if score >= 1.0:
                        log_message(f"🎯 Найдено идеальное совпадение! Прекращаем поиск.")
                        break
            
            if best_result is not None:
                if attempt > 0:
                    log_message(f"✅ Успешно найдено с попытки {attempt + 1}")
                log_message(f"🏆 Лучший результат: '{best_found_title}' (схожесть: {best_score:.2f})")
                return best_result
            
        except Exception as e:
            log_message(f"❌ Ошибка попытки {attempt + 1} для '{game_title}': {e}")
            if attempt == max_attempts - 1:
                log_message(f"💥 Все попытки исчерпаны для '{game_title}'")
                return None
    
    return None

def search_game_single_attempt(page, game_title, game_year=None):
    """Одна попытка поиска игры на HLTB"""
    try:
        log_message(f"🔍 Ищем: '{game_title}'")
        
        # Кодируем название для URL
        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"
        
        # Переходим на страницу поиска
        page.goto(search_url, timeout=PAGE_GOTO_TIMEOUT_MS)
        page.wait_for_load_state("domcontentloaded", timeout=WAIT_SELECTOR_TIMEOUT_MS)
        
        # Проверяем на блокировку после перехода
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при поиске!")
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка при поиске - ждем...")
            time.sleep(5)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует поиск")
                return None
        
        # Ждем загрузки результатов поиска (React контент)
        random_delay()  # Случайная задержка согласно настройкам
        
        # Ищем все ссылки на игры
        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()
        
        # Если результатов нет, ждем еще немного
        if found_count == 0:
            random_delay()  # Случайная задержка согласно настройкам
            found_count = game_links.count()
        
        # Если много результатов, ждем дольше для полной загрузки
        if found_count > 10:
            log_message(f"📊 Найдено {found_count} результатов, ждем дополнительную загрузку...")
            random_delay(1.5, 3.0)  # Дополнительная задержка для большого количества результатов
            found_count = game_links.count()  # Пересчитываем после ожидания
        
        # Fallback: если селектор не сработал, извлекаем из HTML
        if found_count == 0:
            log_message("⚠️ Селектор не сработал, пробуем извлечь из HTML...")
            candidates_from_html = extract_candidates_from_html(page.content())
            if candidates_from_html:
                log_message(f"✅ Найдено {len(candidates_from_html)} кандидатов в HTML")
                # Создаем mock game_links для совместимости
                return extract_best_from_html_candidates(candidates_from_html, game_title, game_year)
        
        if found_count == 0:
            # Сохраняем отладочную информацию при отсутствии результатов
            save_debug_dumps(page, game_title, "no_results", candidates=None)
            return None
        
        # Выбираем наиболее подходящий результат с учетом года
        best_match, best_title, similarity = find_best_match(page, game_links, game_title, game_year)
        if not best_match:
            return None
        
        # Сохраняем данные выбранной игры
        best_url = best_match.get_attribute("href")
        
        # Логируем выбор
        log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {similarity:.2f})")
        
        # Если схожесть меньше 0.6, возвращаем None для попытки альтернативного названия
        if similarity < 0.6:
            log_message(f"⚠️  Низкая схожесть ({similarity:.2f}), пробуем альтернативное название")
            return None
        
        # Переходим на страницу выбранной игры
        full_url = f"{BASE_URL}{best_url}"
        
        page.goto(full_url, timeout=PAGE_GOTO_TIMEOUT_MS)
        page.wait_for_load_state("domcontentloaded", timeout=WAIT_SELECTOR_TIMEOUT_MS)
        
        # Проверяем на блокировку на странице игры
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
        
        # Ждем загрузки данных игры (React контент)
        random_delay()  # Случайная задержка согласно настройкам
        
        # Извлекаем данные из таблицы
        hltb_data = extract_hltb_data_from_page(page)
        
        # Проверяем год на странице игры согласно логике logs.py
        if hltb_data and game_year:
            page_year = extract_earliest_year_from_page(page)
            if page_year:
                # Логика: если год на странице больше входного года, это может быть ремейк/переиздание
                # Но мы принимаем его, если схожесть была достаточно высокой
                if page_year > game_year:
                    log_message(f"⚠️ Год на странице ({page_year}) больше входного ({game_year}) - возможен ремейк")
                    # Не блокируем, но логируем для информации
                elif page_year < game_year:
                    log_message(f"ℹ️ Год на странице ({page_year}) меньше входного ({game_year}) - возможна ранняя версия")
        
        return (hltb_data, best_title) if hltb_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def find_best_match(page, game_links, original_title, input_year=None):
    """Находит наиболее подходящий результат из списка найденных игр с учетом года"""
    try:
        candidates = []
        
        # Собираем всех кандидатов с их данными
        for i in range(min(game_links.count(), 10)):  # Проверяем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            
            if link_text:
                # Получаем href
                href = link.get_attribute("href")
                
                # Пытаемся получить контекст (год/платформы) из родительского элемента
                try:
                    # Ищем родительский элемент с дополнительной информацией
                    parent_element = link.locator("..")
                    context_text = parent_element.inner_text().strip()
                    
                    # Извлекаем годы из контекста
                    years = extract_years_from_text(context_text)
                    
                    # Если годы не найдены в контексте, пробуем из самого текста ссылки
                    if not years:
                        years = extract_years_from_text(link_text)
                    
                except Exception as e:
                    # Если не удалось получить контекст, используем только текст ссылки
                    context_text = link_text
                    years = extract_years_from_text(link_text)
                
                candidates.append({
                    "text": link_text,
                    "href": href,
                    "years": years,  # Годы извлечены из контекста поиска
                    "context": context_text
                })
        
        # Используем новую логику выбора лучшего кандидата согласно logs.py
        best_result = choose_best_candidate(candidates, original_title, input_year)
        
        if best_result and best_result["score"] >= 0.3:
            # Находим соответствующий link элемент
            for i in range(min(game_links.count(), 10)):
                link = game_links.nth(i)
                if link.get_attribute("href") == best_result["candidate"]["href"]:
                    log_message(f"🎯 Выбран кандидат: '{best_result['candidate']['text']}' (схожесть: {best_result['score']:.2f}, причина: {best_result['reason']})")
                    return link, best_result["candidate"]["text"], best_result["score"]
        
        # Сохраняем отладочные дампы при проблемах с кандидатами
        if best_result and best_result["score"] < 0.80:
            save_debug_dumps(page, original_title, "low_score", candidates)
        elif not best_result:
            save_debug_dumps(page, original_title, "no_candidates", candidates)
        
        return None, "", 0
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return game_links.first if game_links.count() > 0 else None, "", 0

def clean_title_for_comparison(title):
    """Очищает название игры для сравнения"""
    import re
    # Убираем лишние символы, приводим к нижнему регистру
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    # Убираем лишние пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_years_from_text(text):
    """Извлекает все годы из текста"""
    import re
    years = []
    # Ищем 4-значные числа от 1950 до текущего года
    current_year = datetime.now().year
    year_matches = re.findall(r'\b(19[5-9]\d|20[0-2]\d)\b', text)
    for year_str in year_matches:
        year = int(year_str)
        if 1950 <= year <= current_year:
            years.append(year)
    return years

def extract_earliest_year_from_page(page):
    """Извлекает самый ранний год со страницы игры"""
    try:
        page_content = page.content()
        years = extract_years_from_text(page_content)
        return min(years) if years else None
    except Exception as e:
        log_message(f"❌ Ошибка извлечения года со страницы: {e}")
        return None

def choose_best_candidate(candidates, orig_title, input_year):
    """Выбирает лучшего кандидата согласно логике из logs.py"""
    try:
        if not candidates:
            return None
        
        # Вычисляем схожесть для каждого кандидата
        scored_candidates = []
        for candidate in candidates:
            # Используем SequenceMatcher для более точного расчета схожести
            score = calculate_sequence_similarity(orig_title, candidate["text"])
            
            # Бонус +0.02, если normalized(original) является подстрокой normalized(candidate_text)
            if clean_title_for_comparison(orig_title) in clean_title_for_comparison(candidate["text"]):
                score += 0.02
            
            # earliest_year = min(candidate.years) если есть годы
            earliest_year = min(candidate["years"]) if candidate["years"] else None
            
            scored_candidates.append({
                "candidate": candidate,
                "score": score,
                "earliest_year": earliest_year
            })
        
        # Сортировка: по score desc, tie-break по earliest_year (меньше — лучше)
        scored_candidates.sort(key=lambda x: (-x["score"], x["earliest_year"] if x["earliest_year"] else 9999))
        
        # Определяем ambiguous если больше одного кандидата с score >= 0.80
        high_score_candidates = [c for c in scored_candidates if c["score"] >= 0.80]
        ambiguous = len(high_score_candidates) > 1
        
        # Правила принятия, применяются последовательно
        for candidate_data in scored_candidates:
            score = candidate_data["score"]
            candidate = candidate_data["candidate"]
            earliest_year = candidate_data["earliest_year"]
            
            # Rule 1: score >= 0.95 → принять
            if score >= 0.95:
                return {
                    "candidate": candidate,
                    "score": score,
                    "earliest_year": earliest_year,
                    "reason": "score_very_high"
                }
            
            # Rule 2: score >= 0.88 and candidate_earliest_year <= input_year → принять
            # Применяем годовую логику активнее когда ambiguous == True или когда candidate выглядит как orig: subtitle
            if score >= 0.88 and input_year and earliest_year and earliest_year <= input_year:
                # Проверяем, выглядит ли кандидат как orig: subtitle
                looks_like_subtitle = (":" in candidate["text"] and 
                                     clean_title_for_comparison(orig_title) in clean_title_for_comparison(candidate["text"]))
                
                if ambiguous or looks_like_subtitle:
                    return {
                        "candidate": candidate,
                        "score": score,
                        "earliest_year": earliest_year,
                        "reason": "score_high_and_year_ok"
                    }
            
            # Rule 3: score >= 0.92 and candidate contains orig substring → принять
            if score >= 0.92 and clean_title_for_comparison(orig_title) in clean_title_for_comparison(candidate["text"]):
                return {
                    "candidate": candidate,
                    "score": score,
                    "earliest_year": earliest_year,
                    "reason": "score_high_and_contains_original"
                }
        
        # Rule 4 (fallback): если ничего из выше не сработало — выбрать лучший candidate
        best = scored_candidates[0]
        
        # Если топ-2 почти равны (diff < 0.02) — выбрать того у кого наименьший earliest_year
        if len(scored_candidates) >= 2:
            second_best = scored_candidates[1]
            if abs(best["score"] - second_best["score"]) < 0.02:
                if (best["earliest_year"] and second_best["earliest_year"] and 
                    second_best["earliest_year"] < best["earliest_year"]):
                    best = second_best
        
        return {
            "candidate": best["candidate"],
            "score": best["score"],
            "earliest_year": best["earliest_year"],
            "reason": "fallback_best_score"
        }
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего кандидата: {e}")
        return None

def calculate_sequence_similarity(title1, title2):
    """Вычисляет схожесть используя SequenceMatcher для более точного результата"""
    try:
        from difflib import SequenceMatcher
        
        # Нормализуем названия для сравнения
        normalized1 = normalize_title_for_comparison(title1)
        normalized2 = normalize_title_for_comparison(title2)
        
        # Используем SequenceMatcher для расчета схожести
        similarity = SequenceMatcher(None, normalized1, normalized2).ratio()
        
        return similarity
        
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести SequenceMatcher: {e}")
        # Fallback на старый метод
        return calculate_title_similarity(title1, title2)


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

def generate_alternative_titles(game_title, game_year=None):
    """Генерирует альтернативные варианты названия для поиска согласно логике из logs.py"""
    alternatives = []
    
    # Если есть год, добавляем вариант с годом в начало (сильный вариант)
    if game_year:
        alternatives.append(f"{game_title} {game_year}")
    
    # Добавляем оригинальное название
    alternatives.append(game_title)
    
    # Обрабатываем названия с "/" согласно логике logs.py
    if "/" in game_title:
        # Проверяем, окружен ли слэш пробелами (A / B)
        if " / " in game_title:
            parts = [part.strip() for part in game_title.split(" / ")]
            
            # Для каждой части генерируем sub-variants
            for part in parts:
                if part:
                    # 1. part (оригинал)
                    if part not in alternatives:
                        alternatives.append(part)
                    
                    # 2. part_without_parentheses
                    part_without_parens = remove_parentheses(part)
                    if part_without_parens != part and part_without_parens not in alternatives:
                        alternatives.append(part_without_parens)
                    
                    # 3. римско↔арабские варианты
                    roman_variants = generate_roman_arabic_variants(part)
                    for variant in roman_variants:
                        if variant not in alternatives:
                            alternatives.append(variant)
                    
                    # 4. ampersand-variants
                    ampersand_variants = generate_ampersand_variants(part)
                    for variant in ampersand_variants:
                        if variant not in alternatives:
                            alternatives.append(variant)
            
            # В конце добавляем full-title-variants
            full_variants = generate_full_title_variants(game_title)
            for variant in full_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
        
        else:
            # Слэш без пробелов (A/B/C)
            parts = [part.strip() for part in game_title.split("/")]
            if len(parts) >= 2:
                # Определяем Base (префикс)
                base = determine_base_from_parts(parts)
                
                # 1. Парные: Base A and B, Base B and C, ...
                for i in range(len(parts) - 1):
                    for j in range(i + 1, len(parts)):
                        pair_variant = f"{base} {parts[i]} and {parts[j]}"
                        if pair_variant not in alternatives:
                            alternatives.append(pair_variant)
                
                # 2. Одиночные: Base A, Base B, Base C
                for part in parts:
                    single_variant = f"{base} {part}"
                    if single_variant not in alternatives:
                        alternatives.append(single_variant)
                
                # 3. Тройные / все вместе
                if len(parts) >= 3:
                    all_together = f"{base} {' and '.join(parts)}"
                    if all_together not in alternatives:
                        alternatives.append(all_together)
                
                # 4. Затем without_parentheses, римские/ampersand для полного title
                full_variants = generate_full_title_variants(game_title)
                for variant in full_variants:
                    if variant not in alternatives:
                        alternatives.append(variant)
    
    else:
        # Если слэша нет: title, title_without_parentheses, римские↔арабские, ampersand-variants
        # 1. title_without_parentheses
        without_parens = remove_parentheses(game_title)
        if without_parens != game_title and without_parens not in alternatives:
            alternatives.append(without_parens)
        
        # 2. римско↔арабские варианты
        roman_variants = generate_roman_arabic_variants(game_title)
        for variant in roman_variants:
            if variant not in alternatives:
                alternatives.append(variant)
        
        # 3. ampersand-variants
        ampersand_variants = generate_ampersand_variants(game_title)
        for variant in ampersand_variants:
            if variant not in alternatives:
                alternatives.append(variant)
    
    # Убираем дубликаты, сохраняя порядок
    seen = set()
    unique_alternatives = []
    for alt in alternatives:
        if alt not in seen:
            seen.add(alt)
            unique_alternatives.append(alt)
    
    return unique_alternatives

def remove_parentheses(text):
    """Удаляет скобки и их содержимое"""
    import re
    return re.sub(r'\s*\([^)]*\)', '', text).strip()

def generate_roman_arabic_variants(text):
    """Генерирует варианты с римскими и арабскими цифрами"""
    variants = []
    import re
    
    # Арабские -> римские
    arabic_pattern = r'(\b\d+\b)'
    matches = re.findall(arabic_pattern, text)
    
    for match in matches:
        # Проверяем, что это не часть дробного числа
        context_pattern = r'(\b' + match + r'\b)'
        context_matches = re.finditer(context_pattern, text)
        
        for context_match in context_matches:
            start_pos = context_match.start()
            end_pos = context_match.end()
            
            before_char = text[start_pos - 1] if start_pos > 0 else ''
            after_char = text[end_pos] if end_pos < len(text) else ''
            
            if before_char != '.' and after_char != '.':
                roman = convert_arabic_to_roman(match)
                if roman != match:
                    variant = re.sub(r'\b' + match + r'\b', roman, text)
                    variants.append(variant)
                break
    
    # Римские -> арабские
    roman_to_arabic = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
        'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
    }
    
    for roman, arabic in roman_to_arabic.items():
        pattern = r'\b' + roman + r'\b'
        if re.search(pattern, text):
            variant = re.sub(pattern, arabic, text)
            variants.append(variant)
    
    return variants

def generate_ampersand_variants(text):
    """Генерирует варианты с амперсандом"""
    variants = []
        import re
        
    if "&" in text and "(" in text and ")" in text:
        # Вариант 1: убираем скобки полностью
        without_brackets = re.sub(r'\s*\([^)]*\)', '', text).strip()
        if without_brackets and without_brackets != text:
            variants.append(without_brackets)
        
        # Вариант 2: заменяем "&" на "and" в скобках
        with_and = re.sub(r'\([^)]*&([^)]*)\)', r'(and\1)', text)
        if with_and != text:
            variants.append(with_and)
        
        # Вариант 3: убираем скобки и заменяем "&" на "and"
        with_and_no_brackets = re.sub(r'\s*\([^)]*&([^)]*)\)', r' and\1', text).strip()
        if with_and_no_brackets and with_and_no_brackets != text:
            variants.append(with_and_no_brackets)
        
        # Вариант 4: убираем скобки и заменяем "&" на "&" (без скобок)
        with_ampersand_no_brackets = re.sub(r'\s*\(([^)]*&[^)]*)\)', r' \1', text).strip()
        if with_ampersand_no_brackets and with_ampersand_no_brackets != text:
            variants.append(with_ampersand_no_brackets)
    
    elif "(" in text and ")" in text:
    # Для названий с любыми скобками (без "&") добавляем вариант без скобок
        without_brackets = re.sub(r'\s*\([^)]*\)', '', text).strip()
        if without_brackets and without_brackets != text:
            variants.append(without_brackets)
    
    return variants

def determine_base_from_parts(parts):
    """Определяет базовую часть из списка частей"""
    if not parts:
        return ""
    
    # Берем первую часть и убираем последнее слово как базовую часть
    first_part = parts[0]
    if " " in first_part:
        words = first_part.split()
        if len(words) >= 2:
            return " ".join(words[:-1])
    
    return first_part

def generate_full_title_variants(title):
    """Генерирует варианты для полного названия"""
    variants = []
    
    # without_parentheses
    without_parens = remove_parentheses(title)
    if without_parens != title:
        variants.append(without_parens)
    
    # римские/арабские
    roman_variants = generate_roman_arabic_variants(title)
    variants.extend(roman_variants)
    
    # ampersand
    ampersand_variants = generate_ampersand_variants(title)
    variants.extend(ampersand_variants)
    
    return variants

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
        
        # Штраф за большую разницу в длине названий
        words1_count = len(words1)
        words2_count = len(words2)
        if words1_count > 0 and words2_count > 0:
            length_ratio = min(words1_count, words2_count) / max(words1_count, words2_count)
            if length_ratio < 0.8:  # Если одно название короче другого
                word_similarity *= 0.6  # Уменьшаем схожесть на 40%
            elif length_ratio < 0.9:  # Если одно название немного короче
                word_similarity *= 0.8  # Уменьшаем схожесть на 20%
        
        # Бонус за включение одного в другое (но не полный)
        if normalized1 in normalized2 or normalized2 in normalized1:
            # Если одно название является подстрокой другого, но не равно ему
            if normalized1 != normalized2:
                # Проверяем, насколько одно название короче другого
                shorter = min(len(normalized1), len(normalized2))
                longer = max(len(normalized1), len(normalized2))
                ratio = shorter / longer
                
                # Бонус зависит от соотношения длин
                if ratio >= 0.8:  # Почти одинаковые по длине
                    word_similarity += 0.05
                elif ratio >= 0.6:  # Среднее соотношение
                    word_similarity += 0.03
                else:  # Одно намного короче другого
                    word_similarity += 0.01
            else:
                word_similarity += 0.2  # Полный бонус для точных совпадений
        
        # Бонус за общие длинные слова (более 4 символов) - уменьшен
        long_common = [w for w in common_words if len(w) > 4]
        if long_common:
            word_similarity += 0.02 * len(long_common)  # Уменьшен с 0.1 до 0.02
        
        return min(word_similarity, 1.0)
        
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

def extract_gamestats_blocks(page):
    """Извлекает данные из GameStats блоков (приоритетный источник)"""
    try:
        # Ищем GameStats блоки
        gamestats_selectors = [
            'div[class*="GameStats_game_times"]',
            'div[class*="GameStats"]',
            '.GameStats_game_times',
            '.GameStats'
        ]
        
        for selector in gamestats_selectors:
            try:
                blocks = page.locator(selector)
                count = blocks.count()
                
                if count > 0:
                    for i in range(count):
                        block = blocks.nth(i)
                        block_text = block.inner_text()
                        
                        # Ищем ul/li структуру
                        ul_elements = block.locator('ul')
                        if ul_elements.count() > 0:
                            li_elements = ul_elements.locator('li')
                            
                            for j in range(li_elements.count()):
                                li = li_elements.nth(j)
                                li_text = li.inner_text()
                                
                                # Ищем h4 (категория) и h5 (время)
                                h4 = li.locator('h4')
                                h5 = li.locator('h5')
                                
                                if h4.count() > 0 and h5.count() > 0:
                                    category = h4.inner_text().strip()
                                    time_value = h5.inner_text().strip()
                                    
                                    # Парсим категорию и время
                                    parsed_data = parse_gamestats_pair(category, time_value)
                                    if parsed_data:
                                        return parsed_data
                        
            except Exception as e:
                continue
        
        return None
        
    except Exception as e:
        return None

def parse_gamestats_pair(category, time_value):
    """Парсит пару категория-время из GameStats"""
    try:
        category_lower = category.lower()
        
        # Определяем тип категории
        if 'co-op' in category_lower or 'coop' in category_lower:
            category_key = 'coop'
        elif 'vs' in category_lower or 'competitive' in category_lower:
            category_key = 'vs'
        elif 'single' in category_lower and 'player' in category_lower:
            category_key = 'ms'  # Main Story для single-player
        else:
            return None
        
        # Парсим время
        if time_value == '--' or time_value == 'N/A' or not time_value:
            parsed_time = 'N/A'
        else:
            # Обрабатываем "634 Hours" -> "634h"
            if 'Hours' in time_value:
                hours_match = re.search(r'(\d+(?:\.\d+)?)', time_value)
                if hours_match:
                    hours = float(hours_match.group(1))
                    parsed_time = f"{int(hours)}h" if hours == int(hours) else f"{hours}h"
                else:
                    parsed_time = time_value
            else:
                parsed_time = time_value
        
        result = {category_key: {"t": parsed_time}}
        return result
        
    except Exception as e:
        return None

def extract_hltb_data_from_page(page):
    """Извлекает данные HLTB со страницы игры"""
    try:
        hltb_data = {}
        
        # ПРИОРИТЕТ 1: Ищем GameStats блоки (самый надежный источник)
        gamestats_data = extract_gamestats_blocks(page)
        if gamestats_data:
            hltb_data.update(gamestats_data)
            log_message(f"✅ Найдены GameStats данные: {gamestats_data}")
        
        # ПРИОРИТЕТ 2: Ищем все таблицы на странице
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
        result["t"] = final_time if final_time else None
        
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
        
        # Применяем умное округление через round_time
        if hours >= 1:
            return round_time(f"{hours:.1f}h")
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
        
        end = content.find('];', start) + 2
        if end == 1:
            raise ValueError("Не найден конец массива gamesList")
        
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
                game_title = game["title"]
                game_year = game.get("year")
                
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title}" + (f" ({game_year})" if game_year else ""))
                
                # Ищем данные на HLTB
                hltb_data = search_game_on_hltb(page, game_title, game_year)
                
                if hltb_data:
                    # Сохраняем в формате согласно logs.py
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
                
                # Случайная задержка между играми для GitHub Actions
                random_delay()
                
                # Длинные паузы каждые LONG_PAUSE_EVERY игр для GitHub Actions
                if (i + 1) % LONG_PAUSE_EVERY == 0:
                    min_pause, max_pause = LONG_PAUSE_DURATION
                    pause_duration = random.uniform(min_pause, max_pause)
                    log_message(f"⏸️ Длинная пауза {pause_duration:.1f} секунд после {i + 1} игр...")
                    time.sleep(pause_duration)
                
                # Сохраняем прогресс каждые 50 игр
                if (i + 1) % 50 == 0:
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
