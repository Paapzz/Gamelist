#!/usr/bin/env python3

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
PROGRESS_FILE = "progress.json"

# Задержки (убрана вежливая задержка между играми)
BREAK_INTERVAL_MIN = 8 * 60  # 8 минут в секундах
BREAK_INTERVAL_MAX = 10 * 60  # 10 минут в секундах
BREAK_DURATION_MIN = 40  # 40 секунд
BREAK_DURATION_MAX = 80  # 80 секунд

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

def random_delay(min_seconds, max_seconds):
    """Случайная задержка в указанном диапазоне"""
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def check_break_time(start_time, games_processed):
    """Проверяет, нужен ли перерыв"""
    elapsed_seconds = time.time() - start_time
    
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
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]  # Паузы между попытками в секундах
    
    # Инициализируем переменные для хранения лучшего результата
    good_result = None
    good_score = 0
    good_title = None
    
    # Сначала пробуем оригинальное название
    log_message(f"🔍 Ищем оригинальное название: '{game_title}' (год: {game_year})")
    result_data = search_game_single_attempt(page, game_title, game_year)
    
    if result_data is not None:
        hltb_data, found_title = result_data
        score = calculate_title_similarity(game_title, found_title) if found_title else 0
        
        # Если найдено идеальное совпадение (1.0), сразу возвращаем результат
        if score >= 1.0:
            log_message(f"🎯 Найдено идеальное совпадение: '{found_title}' (схожесть: {score:.2f})")
            log_message("🚀 Идеальное совпадение найдено - прекращаем поиск!")
            return hltb_data
        
        # Сохраняем результат как кандидата независимо от схожести
        log_message(f"📝 Сохраняем результат: '{found_title}' (схожесть: {score:.2f})")
        if score >= 0.6:
            log_message("🔄 Продолжаем поиск для лучшего результата...")
        else:
            log_message("🔄 Продолжаем поиск альтернатив...")
        
        # Сохраняем результат, но не возвращаем сразу
        good_result = hltb_data
        good_score = score
        good_title = found_title
    else:
        log_message("❌ Оригинальное название не найдено, пробуем альтернативы...")
    
    # Генерируем альтернативы для поиска
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
            
            # Пробуем альтернативные названия и выбираем лучший результат
            best_result = good_result  # Начинаем с хорошего результата из оригинального поиска
            best_score = good_score
            best_title = good_title
            best_found_title = good_title
            
            for alt_title in alternative_titles:
                # Пропускаем оригинальное название, так как уже пробовали
                if alt_title == game_title:
                    continue
                    
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
                        log_message(f"🎯 Найдено идеальное совпадение в альтернативах! Прекращаем поиск.")
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
        page.goto(search_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
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
        random_delay(3, 5)  # Случайная задержка 3-5 секунд
        
        # Ищем все ссылки на игры
        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()
        
        # Если результатов нет, ждем еще немного
        if found_count == 0:
            random_delay(2, 4)  # Случайная задержка 2-4 секунды
            found_count = game_links.count()
        
        # Если много результатов, ждем дольше для полной загрузки
        if found_count > 10:
            random_delay(5, 8)  # Дополнительная задержка для большого количества результатов
            found_count = game_links.count()  # Пересчитываем после ожидания
            
            # Если все еще много результатов, это может быть проблема с поиском
            if found_count > 30:
                log_message(f"⚠️  Слишком много результатов ({found_count}), пробуем точный поиск")
                # Попробуем более точный поиск с кавычками
                quoted_title = f'"{game_title}"'
                safe_quoted = quote(quoted_title, safe="")
                quoted_url = f"{BASE_URL}/?q={safe_quoted}"
                page.goto(quoted_url, timeout=20000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                random_delay(3, 5)
                game_links = page.locator('a[href^="/game/"]')
                found_count = game_links.count()
        
        if found_count == 0:
            return None
        
        # Выбираем наиболее подходящий результат с учетом года
        best_match, best_title, similarity = find_best_match_with_year(page, game_links, game_title, game_year)
        if not best_match:
            return None
        
        # Сохраняем данные выбранной игры
        best_url = best_match.get_attribute("href")
        
        # Логируем выбор
        log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {similarity:.2f})")
        
        # Если схожесть меньше 0.6, но есть год для проверки, все равно переходим на страницу
        if similarity < 0.6:
            if game_year:
                log_message(f"⚠️  Низкая схожесть ({similarity:.2f}), но есть год для проверки - продолжаем")
            else:
                log_message(f"⚠️  Низкая схожесть ({similarity:.2f}), пробуем альтернативное название")
                return None
        
        # Переходим на страницу выбранной игры
        full_url = f"{BASE_URL}{best_url}"
        
        page.goto(full_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
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
        random_delay(3, 5)  # Увеличена задержка для стабильности
        
        # Извлекаем данные из таблицы
        hltb_data = extract_hltb_data_from_page(page)
        return (hltb_data, best_title) if hltb_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def find_best_match_with_year(page, game_links, original_title, game_year=None):
    """Находит наиболее подходящий результат из списка найденных игр с учетом года"""
    try:
        best_match = None
        best_score = 0
        best_title = ""
        best_year_score = 0
        
        # Очищаем оригинальное название для сравнения
        original_clean = clean_title_for_comparison(original_title)
        
        # Собираем все результаты с их годами
        candidates = []
        for i in range(min(game_links.count(), 10)):  # Проверяем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            
            if link_text:
                # Очищаем найденное название
                found_clean = clean_title_for_comparison(link_text)
                
                # Вычисляем схожесть названий
                title_score = calculate_title_similarity(original_clean, found_clean)
                
                # Извлекаем год из ссылки (переходим на страницу игры)
                hltb_year = extract_year_from_game_page(page, link)
                year_score = calculate_year_similarity(game_year, hltb_year) if game_year and hltb_year else 0
                
                candidates.append({
                    'link': link,
                    'title': link_text,
                    'title_score': title_score,
                    'year_score': year_score,
                    'hltb_year': hltb_year
                })
        
        # Сортируем кандидатов по комбинированному скору
        for candidate in candidates:
            # Комбинированный скор: 70% схожесть названия + 30% схожесть года
            combined_score = candidate['title_score'] * 0.7 + candidate['year_score'] * 0.3
            
            if combined_score > best_score:
                best_score = combined_score
                best_match = candidate['link']
                best_title = candidate['title']
                best_year_score = candidate['year_score']
        
        # Логируем выбор только при наличии года
        if best_match and game_year and candidates:
            log_message(f"🎯 Выбрано: '{best_title}' (схожесть: {best_score:.2f}, год: {candidates[0]['hltb_year']})")
        
        # Возвращаем кортеж с результатом и схожестью
        if best_score >= 0.3:
            return best_match, best_title, best_score
        else:
            return None, "", 0
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return game_links.first if game_links.count() > 0 else None, "", 0

def find_best_match(page, game_links, original_title):
    """Находит наиболее подходящий результат из списка найденных игр (старая версия для совместимости)"""
    return find_best_match_with_year(page, game_links, original_title, None)

def extract_year_from_game_page(page, link):
    """Извлекает год релиза со страницы игры на HLTB"""
    try:
        # Получаем URL игры
        game_url = link.get_attribute("href")
        if not game_url:
            return None
        
        # Сохраняем текущую страницу
        current_url = page.url
        
        # Переходим на страницу игры
        full_url = f"{BASE_URL}{game_url}"
        page.goto(full_url, timeout=15000)
        page.wait_for_load_state("domcontentloaded", timeout=10000)
        
        # Ищем год релиза в различных форматах
        year = None
        
        # Паттерн 1: Ищем даты в формате "November 2nd, 1992"
        date_patterns = [
            r'(?:NA|EU|JP):\s*[A-Za-z]+\s+\d+(?:st|nd|rd|th)?,\s*(\d{4})',
            r'[A-Za-z]+\s+\d+(?:st|nd|rd|th)?,\s*(\d{4})',
            r'(\d{4})'  # Просто год
        ]
        
        page_content = page.content()
        for pattern in date_patterns:
            matches = re.findall(pattern, page_content)
            if matches:
                # Берем самый ранний год
                years = [int(year) for year in matches if year.isdigit()]
                if years:
                    year = min(years)
                    break
        
        # Возвращаемся на исходную страницу
        page.goto(current_url, timeout=15000)
        page.wait_for_load_state("domcontentloaded", timeout=10000)
        
        return year
        
    except Exception as e:
        log_message(f"⚠️ Ошибка извлечения года: {e}")
        return None

def calculate_year_similarity(target_year, hltb_year):
    """Вычисляет схожесть годов (чем ближе, тем выше скор)"""
    if not target_year or not hltb_year:
        return 0.0
    
    # Точное совпадение
    if target_year == hltb_year:
        return 1.0
    
    # Разница в годах
    year_diff = abs(target_year - hltb_year)
    
    # Если разница больше 10 лет, скор очень низкий
    if year_diff > 10:
        return 0.1
    
    # Если разница 1-2 года, скор высокий
    if year_diff <= 2:
        return 0.8
    
    # Если разница 3-5 лет, скор средний
    if year_diff <= 5:
        return 0.6
    
    # Если разница 6-10 лет, скор низкий
    return 0.3

def clean_title_for_comparison(title):
    """Очищает название игры для сравнения"""
    # Убираем лишние символы, приводим к нижнему регистру
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    # Убираем лишние пробелы
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_primary_title(game_title):
    """Извлекает основное название игры из названия с альтернативными вариантами"""
    if not game_title:
        return game_title
    
    # Если есть "/", обрабатываем по-разному
    if "/" in game_title:
        parts = [part.strip() for part in game_title.split("/")]
        
        # Если части без пробелов (например "Gold/Silver/Crystal"), объединяем с "and"
        if all(" " not in part for part in parts):
            primary = f"{parts[0]} and {parts[1]}"
            log_message(f"📝 Объединяем части: '{game_title}' -> '{primary}'")
            return primary
        else:
            # Если есть пробелы, берем только первую часть
            primary = parts[0]
            log_message(f"📝 Извлекаем основное название: '{game_title}' -> '{primary}'")
            return primary
    
    return game_title

def extract_alternative_title(game_title):
    """Извлекает альтернативное название для поиска"""
    if not game_title or "/" not in game_title:
        return None
    
    parts = [part.strip() for part in game_title.split("/")]
    
    # Если части без пробелов, возвращаем вторую часть
    if len(parts) >= 2 and all(" " not in part for part in parts):
        alternative = parts[1]
        log_message(f"📝 Альтернативное название: '{game_title}' -> '{alternative}'")
        return alternative
    
    return None

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

def convert_roman_to_arabic(roman_str):
    """Конвертирует римские цифры в арабские"""
    try:
        roman_to_arabic_map = {
            'I': 1, 'II': 2, 'III': 3, 'IV': 4, 'V': 5,
            'VI': 6, 'VII': 7, 'VIII': 8, 'IX': 9, 'X': 10
        }
        return str(roman_to_arabic_map.get(roman_str, roman_str))
    except:
        return roman_str

def generate_alternative_titles(game_title):
    """Генерирует альтернативные варианты названия для поиска"""
    alternatives = [game_title]
    
    # Добавляем основное название без подзаголовка (до двоеточия)
    if ":" in game_title:
        main_title = game_title.split(":")[0].strip()
        if main_title and main_title not in alternatives:
            alternatives.append(main_title)
    
    # Добавляем варианты без скобок и с заменой (& на and)
    if "(" in game_title and ")" in game_title:
        # Убираем скобки полностью
        no_parens = re.sub(r'\([^)]*\)', '', game_title).strip()
        no_parens = re.sub(r'\s+', ' ', no_parens)  # Убираем лишние пробелы
        if no_parens and no_parens not in alternatives:
            alternatives.append(no_parens)
        
        # Заменяем (& на and
        with_and = re.sub(r'\(\s*&\s*', 'and ', game_title)
        with_and = re.sub(r'\s*\)', '', with_and)
        with_and = re.sub(r'\s+', ' ', with_and).strip()
        if with_and and with_and not in alternatives:
            alternatives.append(with_and)
        
        # Заменяем (& на &
        with_amp = re.sub(r'\(\s*&\s*', '& ', game_title)
        with_amp = re.sub(r'\s*\)', '', with_amp)
        with_amp = re.sub(r'\s+', ' ', with_amp).strip()
        if with_amp and with_amp not in alternatives:
            alternatives.append(with_amp)
    
    # Добавляем варианты с римскими цифрами (только для целых чисел)
    # Ищем арабские цифры в конце названия или после пробела, но НЕ в составе дробных чисел
    arabic_pattern = r'(\b\d+\b)'
    matches = re.findall(arabic_pattern, game_title)
    
    for match in matches:
        # Проверяем, что это не часть дробного числа (например, "1.6")
        # Ищем контекст вокруг цифры
        context_pattern = r'(\b' + match + r'\b)'
        context_matches = re.finditer(context_pattern, game_title)
        
        for context_match in context_matches:
            start_pos = context_match.start()
            end_pos = context_match.end()
            
            # Проверяем, что перед и после цифры нет точки
            before_char = game_title[start_pos - 1] if start_pos > 0 else ''
            after_char = game_title[end_pos] if end_pos < len(game_title) else ''
            
            # Если это не часть дробного числа, преобразуем в римские
            if before_char != '.' and after_char != '.':
                roman = convert_arabic_to_roman(match)
                if roman != match:
                    # Заменяем арабскую цифру на римскую
                    alt_title = re.sub(r'\b' + match + r'\b', roman, game_title)
                    alternatives.append(alt_title)
                break  # Прерываем после первого подходящего совпадения
    
    # Добавляем обратные варианты (римские -> арабские)
    roman_pattern = r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b'
    roman_matches = re.findall(roman_pattern, game_title)
    for roman_match in roman_matches:
        arabic = convert_roman_to_arabic(roman_match)
        if arabic != roman_match:
            alt_title = re.sub(r'\b' + roman_match + r'\b', arabic, game_title)
            alternatives.append(alt_title)
    
    # Обрабатываем слэши - разделяем на отдельные названия
    if "/" in game_title:
        # Разделяем по слэшу (с пробелами или без)
        if " / " in game_title:
            parts = [part.strip() for part in game_title.split(" / ")]
        else:
            parts = [part.strip() for part in game_title.split("/")]
        
        # Добавляем каждую часть как отдельное название
        for part in parts:
            if part and part not in alternatives:
                alternatives.append(part)
                
                # Если часть содержит двоеточие, добавляем основную часть
                if ":" in part:
                    main_part = part.split(":")[0].strip()
                    if main_part and main_part not in alternatives:
                        alternatives.append(main_part)
    
    
    # Убираем дубликаты и сортируем по правильному приоритету
    unique_alternatives = []
    seen = set()
    
    # Проверяем, есть ли два названия (разделенные " / ")
    has_two_titles = " / " in game_title
    
    # Для случаев с двумя названиями НЕ добавляем оригинал в начало
    if not has_two_titles:
        # Сначала добавляем оригинальное название (только для обычных названий)
        if game_title and game_title not in seen:
            unique_alternatives.append(game_title)
            seen.add(game_title)
    
    if has_two_titles:
        # Для случаев с двумя названиями: сначала части, потом переворот цифр к каждой части
        parts = [part.strip() for part in game_title.split(" / ")]
        
        # Добавляем части в правильном порядке
        for part in parts:
            if part and part not in seen:
                unique_alternatives.append(part)
                seen.add(part)
                
                # Если часть содержит двоеточие, добавляем основную часть
                if ":" in part:
                    main_part = part.split(":")[0].strip()
                    if main_part and main_part not in seen:
                        unique_alternatives.append(main_part)
                        seen.add(main_part)
        
        # Затем применяем переворот цифр к каждой части отдельно
        for part in parts:
            if part:
                # Конвертируем арабские цифры в римские для этой части
                arabic_pattern = r'(\b\d+\b)'
                matches = re.findall(arabic_pattern, part)
                for match in matches:
                    roman = convert_arabic_to_roman(match)
                    if roman != match:
                        alt_part = re.sub(r'\b' + match + r'\b', roman, part)
                        if alt_part and alt_part not in seen:
                            unique_alternatives.append(alt_part)
                            seen.add(alt_part)
                
                # Конвертируем римские цифры в арабские для этой части
                roman_pattern = r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b'
                roman_matches = re.findall(roman_pattern, part)
                for roman_match in roman_matches:
                    arabic = convert_roman_to_arabic(roman_match)
                    if arabic != roman_match:
                        alt_part = re.sub(r'\b' + roman_match + r'\b', arabic, part)
                        if alt_part and alt_part not in seen:
                            unique_alternatives.append(alt_part)
                            seen.add(alt_part)
        
        # В САМОМ КОНЦЕ добавляем варианты с конвертацией цифр для полного названия
        # Генерируем их прямо здесь, а не ищем в alternatives
        # Конвертируем арабские цифры в римские для полного названия
        arabic_pattern = r'(\b\d+\b)'
        matches = re.findall(arabic_pattern, game_title)
        for match in matches:
            roman = convert_arabic_to_roman(match)
            if roman != match:
                alt_title = re.sub(r'\b' + match + r'\b', roman, game_title)
                if alt_title and alt_title not in seen:
                    unique_alternatives.append(alt_title)
                    seen.add(alt_title)
        
        # Конвертируем римские цифры в арабские для полного названия
        roman_pattern = r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b'
        roman_matches = re.findall(roman_pattern, game_title)
        for roman_match in roman_matches:
            arabic = convert_roman_to_arabic(roman_match)
            if arabic != roman_match:
                alt_title = re.sub(r'\b' + roman_match + r'\b', arabic, game_title)
                if alt_title and alt_title not in seen:
                    unique_alternatives.append(alt_title)
                    seen.add(alt_title)
        
        # В САМЫЙ КОНЕЦ добавляем оригинальное полное название
        if game_title and game_title not in seen:
            unique_alternatives.append(game_title)
            seen.add(game_title)
    else:
        # Для обычных названий (без " / "): сначала переворот цифр, потом остальные варианты
        for alt in alternatives:
            if alt and alt not in seen:
                # Проверяем, что это вариант с конвертацией цифр
                original_clean = re.sub(r'\b\d+\b', '', game_title.lower())
                alt_clean = re.sub(r'\b\d+\b', '', alt.lower())
                roman_clean = re.sub(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', '', game_title.lower())
                alt_roman_clean = re.sub(r'\b(I{1,3}|IV|V|VI{0,3}|IX|X)\b', '', alt.lower())
                
                # Если это конвертация цифр (остальное одинаково)
                if (original_clean == alt_clean or roman_clean == alt_roman_clean) and alt != game_title:
                    unique_alternatives.append(alt)
                    seen.add(alt)
    
    # Затем добавляем остальные варианты (без скобок, с and, etc.)
    for alt in alternatives:
        if alt and alt not in seen:
            # Для случаев с двумя названиями исключаем варианты с слэшем в "остальных"
            if has_two_titles and "/" in alt:
                continue
            unique_alternatives.append(alt)
            seen.add(alt)
    
    return unique_alternatives

def calculate_title_similarity(title1, title2):
    """Вычисляет схожесть между двумя названиями игр с весовой системой для последовательности"""
    try:
        # Нормализуем названия для сравнения (конвертируем римские цифры в арабские)
        normalized1 = normalize_title_for_comparison(title1)
        normalized2 = normalize_title_for_comparison(title2)
        
        # Очищаем названия
        clean1 = clean_title_for_comparison(normalized1)
        clean2 = clean_title_for_comparison(normalized2)
        
        # Простой алгоритм схожести на основе общих слов
        words1 = set(clean1.split())
        words2 = set(clean2.split())
        
        if not words1 or not words2:
            return 0.0
        
        # Вычисляем пересечение слов
        common_words = words1.intersection(words2)
        total_words = words1.union(words2)
        
        # Базовая схожесть по словам
        word_similarity = len(common_words) / len(total_words)
        
        # Бонус за точное совпадение
        if clean1 == clean2:
            return 1.0
        
        # Бонус за включение одного в другое
        if clean1 in clean2 or clean2 in clean1:
            word_similarity += 0.2
        
        # Бонус за общие длинные слова (более 4 символов)
        long_common = [w for w in common_words if len(w) > 4]
        if long_common:
            word_similarity += 0.1 * len(long_common)
        
        # КЛЮЧЕВОЕ УЛУЧШЕНИЕ: Весовая система для последовательности
        if '/' in title1:
            # Извлекаем отдельные слова из слэш-разделенного названия
            slash_parts = [part.strip() for part in title1.split('/')]
            slash_words = []
            for part in slash_parts:
                part_clean = clean_title_for_comparison(part)
                slash_words.extend(part_clean.split())
            
            # Создаем весовую систему: чем ближе к началу, тем больше вес
            word_weights = {}
            for i, word in enumerate(slash_words):
                # Вес уменьшается с расстоянием от начала (1.0, 0.8, 0.6, 0.4, ...)
                weight = max(0.2, 1.0 - (i * 0.2))
                word_weights[word] = weight
            
            # Проверяем, сколько слов из слэш-частей есть в title2
            words2_list = clean2.split()
            total_weight = 0
            matched_weight = 0
            
            for word, weight in word_weights.items():
                total_weight += weight
                if word in words2_list:
                    matched_weight += weight
            
            # Вычисляем взвешенное покрытие
            if total_weight > 0:
                weighted_coverage = matched_weight / total_weight
                
                # Если title2 содержит "and" и хорошее взвешенное покрытие
                if 'and' in clean2 and weighted_coverage >= 0.5:  # 50% взвешенное покрытие
                    word_similarity += 0.3 * weighted_coverage  # Бонус пропорциональный покрытию
                
                # Дополнительный бонус за отличное покрытие
                if weighted_coverage >= 0.8:
                    word_similarity += 0.2
        
        return min(word_similarity, 1.0)
        
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести: {e}")
        return 0.0

def normalize_title_for_comparison(title):
    """Нормализует название для сравнения, конвертируя римские цифры в арабские"""
    try:
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

def extract_hltb_data_from_page(page):
    """Извлекает данные HLTB со страницы игры"""
    try:
        hltb_data = {}
        
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
        
        # Ищем отдельные блоки с мультиплеерными данными (не в таблицах)
        try:
            # Ищем Vs., Co-Op, Single-Player блоки
            multiplayer_elements = page.locator('text="Vs.", text="Co-Op", text="Single-Player"')
            element_count = multiplayer_elements.count()
            if element_count > 0:
                for i in range(min(5, element_count)):  # Проверяем первые 5 вхождений
                    try:
                        element = multiplayer_elements.nth(i)
                        element_text = element.inner_text().strip()
                        surrounding_text = element.evaluate("(e) => (e.closest('div')||e.parentElement||e).innerText")
                        
                        # Если это не таблица и содержит время, извлекаем данные
                        if "Hours" in surrounding_text and "table" not in str(element.locator("..").get_attribute("tagName")).lower():
                            # Определяем тип данных по тексту элемента
                            if "Vs." in element_text and "vs" not in hltb_data:
                                vs_data = extract_vs_data_from_text(surrounding_text)
                                if vs_data:
                                    hltb_data["vs"] = vs_data
                                    log_message(f"🎯 Найдены Vs. данные в отдельном блоке: {vs_data}")
                            elif "Co-Op" in element_text and "coop" not in hltb_data:
                                coop_data = extract_coop_data_from_text(surrounding_text)
                                if coop_data:
                                    hltb_data["coop"] = coop_data
                                    log_message(f"🎯 Найдены Co-Op данные в отдельном блоке: {coop_data}")
                            elif "Single-Player" in element_text and "ms" not in hltb_data:
                                sp_data = extract_single_player_data_from_text(surrounding_text)
                                if sp_data:
                                    hltb_data["ms"] = sp_data
                                    log_message(f"🎯 Найдены Single-Player данные в отдельном блоке: {sp_data}")
                    except Exception as e:
                        log_message(f"⚠️ Ошибка обработки мультиплеерного блока {i}: {e}")
                        continue
        except Exception as e:
            log_message(f"⚠️ Ошибка поиска мультиплеерных блоков: {e}")
        
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
        # Применяем округление к результату
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
                hours_match = re.search(r'(\d+)h', time_str)
                minutes_match = re.search(r'(\d+)m', time_str)
                
                if hours_match:
                    total_minutes += int(hours_match.group(1)) * 60
                if minutes_match:
                    total_minutes += int(minutes_match.group(1))
                    
            elif "h" in time_str:
                # Только часы "660h"
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
        
        # Возвращаем время в формате "Xh Ym" для дальнейшего округления
        if hours >= 1:
            if hours == int(hours):
                return f"{int(hours)}h"
            else:
                # Конвертируем дробные часы в часы и минуты
                int_hours = int(hours)
                remaining_minutes = int((hours - int_hours) * 60)
                if remaining_minutes > 0:
                    return f"{int_hours}h {remaining_minutes}m"
                else:
                    return f"{int_hours}h"
        else:
            return f"{int(avg_minutes)}m"
            
    except Exception as e:
        log_message(f"❌ Ошибка вычисления среднего времени: {e}")
        return time1_str or time2_str

def extract_vs_data_from_text(text):
    """Извлекает Vs. данные из текста"""
    try:
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

def extract_coop_data_from_text(text):
    """Извлекает Co-Op данные из текста"""
    try:
        # Убираем переносы строк для читаемого лога
        clean_text = text.replace('\n', ' ').replace('\r', ' ')
        log_message(f"🔍 Ищем Co-Op данные в тексте: '{clean_text[:200]}...'")
        
        # Ищем различные форматы Co-Op данных
        patterns = [
            r'Co-Op\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',  # "Co-Op | 634 Hours"
            r'Co-Op\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Co-Op 634 Hours"
            r'Co-Op\s*(\d+(?:\.\d+)?)\s*Hours?',        # "Co-Op634 Hours"
            r'Co-Op\s*(\d+(?:\.\d+)?[½]?)\s*Hours?',    # "Co-Op 634½ Hours"
        ]
        
        for pattern in patterns:
            coop_match = re.search(pattern, text)
            if coop_match:
                time_str = coop_match.group(1)
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
                
                log_message(f"✅ Найдены Co-Op данные: {formatted_time}")
                return {"t": formatted_time}
        
        log_message("❌ Co-Op данные не найдены")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Co-Op данных: {e}")
        return None

def extract_single_player_data_from_text(text):
    """Извлекает Single-Player данные из текста"""
    try:
        # Убираем переносы строк для читаемого лога
        clean_text = text.replace('\n', ' ').replace('\r', ' ')
        log_message(f"🔍 Ищем Single-Player данные в тексте: '{clean_text[:200]}...'")
        
        # Ищем различные форматы Single-Player данных
        patterns = [
            r'Single-Player\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',  # "Single-Player | 50 Hours"
            r'Single-Player\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Single-Player 50 Hours"
            r'Single-Player\s*(\d+(?:\.\d+)?)\s*Hours?',        # "Single-Player50 Hours"
            r'Single-Player\s*(\d+(?:\.\d+)?[½]?)\s*Hours?',    # "Single-Player 50½ Hours"
        ]
        
        for pattern in patterns:
            sp_match = re.search(pattern, text)
            if sp_match:
                time_str = sp_match.group(1)
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
                
                log_message(f"✅ Найдены Single-Player данные: {formatted_time}")
                return {"t": formatted_time}
        
        log_message("❌ Single-Player данные не найдены")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения Single-Player данных: {e}")
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

def extract_time_from_row(row_text):
    """Извлекает только время из строки таблицы (для обратной совместимости)"""
    try:
        # Ищем время в формате "Xh Ym"
        time_match = re.search(r'(\d+h\s*\d*m)', row_text)
        if time_match:
            time_str = time_match.group(1)
            return round_time(time_str)
        return None
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
                game_title = game["title"]
                game_year = game.get("year")
                
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title} ({game_year})")
                
                # Ищем данные на HLTB
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
                
                # Вежливая задержка убрана - достаточно задержек в процессе поиска
                
                # Проверяем перерыв
                start_time = check_break_time(start_time, i + 1)
                
                # Сохраняем прогресс каждые 50 игр
                if (i + 1) % 50 == 0:
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
