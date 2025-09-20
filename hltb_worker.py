#!/usr/bin/env python3

print("🚀 HLTB Worker запускается...")
print("📦 Импортируем модули...")

import json
print("✅ json импортирован")

import time
print("✅ time импортирован")

import random
print("✅ random импортирован")

import re
print("✅ re импортирован")

import os
print("✅ os импортирован")

from datetime import datetime
print("✅ datetime импортирован")

from urllib.parse import quote
print("✅ urllib.parse импортирован")

print("📦 Импортируем Playwright...")
from playwright.sync_api import sync_playwright
print("✅ Playwright импортирован")

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
    
    # Генерируем все альтернативные названия
    alternative_titles = generate_alternative_titles(game_title)
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
                    
                    # Проверяем, что найденное название точно соответствует тому, что мы ищем
                    if found_title and found_title.lower() == alt_title.lower():
                        # Точное соответствие - это лучший результат
                        log_message(f"🎯 Найдено точное соответствие: '{found_title}'")
                        return hltb_data
                    elif score > best_score:
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
            log_message(f"📊 Найдено {found_count} результатов, ждем дополнительную загрузку...")
            random_delay(5, 8)  # Дополнительная задержка для большого количества результатов
            found_count = game_links.count()  # Пересчитываем после ожидания
        
        if found_count == 0:
            return None
        
        # Перебираем все результаты по порядку, начиная с лучшего по схожести
        matches_with_scores = find_all_matches_with_scores(page, game_links, game_title)
        if not matches_with_scores:
            return None
        
        # Сортируем по схожести (лучшие сначала)
        matches_with_scores.sort(key=lambda x: x[2], reverse=True)
        
        # Если указан год, сначала ищем точное совпадение, потом ближайший в меньшую сторону
        if game_year is not None:
            best_match = find_best_match_by_year(page, matches_with_scores, game_title, game_year)
            if best_match:
                match, title, similarity = best_match
                return process_single_match(page, match, title, similarity, game_title, game_year)
        
        # Перебираем результаты по порядку (если год не указан или не найден подходящий)
        for match, title, similarity in matches_with_scores:
            # Если схожесть меньше 0.6, пропускаем
            if similarity < 0.6:
                log_message(f"⚠️ Низкая схожесть ({similarity:.2f}) для '{title}', пропускаем")
                continue
            
            # Проверяем точное соответствие названия
            if similarity < 1.0:
                # Если схожесть не идеальная, проверяем, не является ли это более короткой версией
                if len(title) < len(game_title) and title.lower() in game_title.lower():
                    log_message(f"⚠️ Найдено более короткое название '{title}' вместо '{game_title}', пропускаем")
                    continue
                # Также проверяем, не является ли это более длинной версией
                elif len(title) > len(game_title) and game_title.lower() in title.lower():
                    log_message(f"⚠️ Найдено более длинное название '{title}' вместо '{game_title}', пропускаем")
                    continue
            
            # Обрабатываем результат
            result = process_single_match(page, match, title, similarity, game_title, game_year)
            if result:
                return result
        
        # Если дошли сюда, значит ни один результат не подошел
        log_message("❌ Ни один результат не подошел по критериям")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def find_best_match_by_year(page, matches_with_scores, game_title, game_year):
    """Находит лучший результат по году (точное совпадение или ближайший в меньшую сторону)"""
    try:
        exact_matches = []
        earlier_matches = []
        
        for match, title, similarity in matches_with_scores:
            # Если схожесть меньше 0.6, пропускаем
            if similarity < 0.6:
                continue
            
            # Проверяем точное соответствие названия
            if similarity < 1.0:
                if len(title) < len(game_title) and title.lower() in game_title.lower():
                    continue
                elif len(title) > len(game_title) and game_title.lower() in title.lower():
                    continue
            
            # Получаем URL и переходим на страницу для проверки года
            try:
                game_url = match.get_attribute("href")
                if not game_url:
                    continue
                
                full_url = f"{BASE_URL}{game_url}"
                page.goto(full_url, timeout=20000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                
                # Проверяем на блокировку
                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    continue
                
                # Извлекаем год
                hltb_year = extract_year_from_hltb_page(page)
                if hltb_year is not None:
                    if hltb_year == game_year:
                        # Точное совпадение года
                        exact_matches.append((match, title, similarity, hltb_year))
                        log_message(f"✅ Точное совпадение года: '{title}' ({hltb_year})")
                    elif hltb_year < game_year:
                        # Год меньше ожидаемого (ближе к оригиналу)
                        earlier_matches.append((match, title, similarity, hltb_year))
                        log_message(f"📅 Год меньше ожидаемого: '{title}' ({hltb_year} < {game_year})")
                
            except Exception as e:
                log_message(f"⚠️ Ошибка проверки года для '{title}': {e}")
                continue
        
        # Возвращаем лучший результат
        if exact_matches:
            # Сортируем по схожести и возвращаем лучший
            exact_matches.sort(key=lambda x: x[2], reverse=True)
            return exact_matches[0][:3]  # Возвращаем только match, title, similarity
        
        if earlier_matches:
            # Сортируем по году (ближайший к ожидаемому) и схожести
            earlier_matches.sort(key=lambda x: (x[3], x[2]), reverse=True)
            log_message(f"🎯 Выбран ближайший год в меньшую сторону: '{earlier_matches[0][1]}' ({earlier_matches[0][3]})")
            return earlier_matches[0][:3]  # Возвращаем только match, title, similarity
        
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска по году: {e}")
        return None

def process_single_match(page, match, title, similarity, game_title, game_year):
    """Обрабатывает один результат поиска"""
    try:
        # Логируем попытку
        log_message(f"🎯 Проверяем: '{title}' (схожесть: {similarity:.2f})")
        
        # Сохраняем данные выбранной игры
        try:
            game_url = match.get_attribute("href")
            if not game_url:
                log_message(f"⚠️ Не удалось получить URL для '{title}', пропускаем")
                return None
        except Exception as e:
            log_message(f"⚠️ Ошибка получения URL для '{title}': {e}, пропускаем")
            return None
        
        # Переходим на страницу выбранной игры
        full_url = f"{BASE_URL}{game_url}"
        
        page.goto(full_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
        # Проверяем на блокировку на странице игры
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры, пробуем следующий результат")
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message("⚠️ Cloudflare проверка на странице игры - ждем...")
            time.sleep(5)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует страницу игры, пробуем следующий результат")
                return None
        
        # Ждем загрузки данных игры (React контент)
        random_delay(3, 5)  # Увеличена задержка для стабильности
        
        # Проверяем год игры, если он указан
        if game_year is not None:
            hltb_year = extract_year_from_hltb_page(page)
            if hltb_year is not None and hltb_year != game_year:
                log_message(f"⚠️ Несоответствие года: ожидался {game_year}, найден {hltb_year}")
                return None
            elif hltb_year is not None:
                log_message(f"✅ Год совпадает: {hltb_year}")
        
        # Извлекаем данные из таблицы
        hltb_data = extract_hltb_data_from_page(page)
        if hltb_data:
            log_message(f"✅ Найдены данные для: '{title}'")
            return (hltb_data, title)
        else:
            log_message(f"⚠️ Данные не найдены для: '{title}', пробуем следующий результат")
            return None
            
    except Exception as e:
        log_message(f"❌ Ошибка обработки результата '{title}': {e}")
        return None

def find_all_matches_with_scores(page, game_links, original_title):
    """Находит все результаты с их схожестью"""
    try:
        matches_with_scores = []
        
        # Очищаем оригинальное название для сравнения
        original_clean = clean_title_for_comparison(original_title)
        
        for i in range(min(game_links.count(), 10)):  # Проверяем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            
            if link_text:
                # Очищаем найденное название
                found_clean = clean_title_for_comparison(link_text)
                
                # Вычисляем схожесть
                score = calculate_title_similarity(original_clean, found_clean)
                
                matches_with_scores.append((link, link_text, score))
        
        return matches_with_scores
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска совпадений: {e}")
        return []

def find_best_match(page, game_links, original_title):
    """Находит наиболее подходящий результат из списка найденных игр"""
    try:
        best_match = None
        best_score = 0
        best_title = ""
        
        # Очищаем оригинальное название для сравнения
        original_clean = clean_title_for_comparison(original_title)
        
        for i in range(min(game_links.count(), 10)):  # Проверяем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            
            if link_text:
                # Очищаем найденное название
                found_clean = clean_title_for_comparison(link_text)
                
                # Вычисляем схожесть
                score = calculate_title_similarity(original_clean, found_clean)
                
                if score > best_score:
                    best_score = score
                    best_match = link
                    best_title = link_text
                
                # Если нашли очень хорошее совпадение, останавливаемся
                if score >= 0.9:
                    break
        
        # Возвращаем кортеж с результатом и схожестью
        if best_score >= 0.3:
            return best_match, best_title, best_score
        else:
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

def generate_alternative_titles(game_title):
    """Генерирует альтернативные варианты названия для поиска"""
    alternatives = [game_title]
    
    # Добавляем варианты с римскими цифрами (только для целых чисел)
    import re
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
    
    # Для названий с "/" различаем два случая:
    # 1. "game 123 / game 123v1" (с пробелом) = две разные игры
    # 2. "Pokémon Red/Blue/Yellow" (без пробела) = варианты одного названия
    if "/" in game_title:
        parts = [part.strip() for part in game_title.split("/")]
        
        # Проверяем, есть ли пробел вокруг слэша (разные игры)
        has_space_around_slash = " / " in game_title or game_title.count(" /") > 0 or game_title.count("/ ") > 0
        
        if has_space_around_slash:
            # Случай 1: Разные игры - добавляем каждую часть отдельно
            for part in parts:
                if part and len(part) > 3 and part not in alternatives:
                    alternatives.append(part)
        else:
            # Случай 2: Варианты одного названия - создаем комбинированные варианты
            # Добавляем первую часть (основное название), только если она достаточно длинная
            if parts[0] and len(parts[0]) > 3 and parts[0] not in alternatives:
                alternatives.append(parts[0])
            
            # Создаем варианты с "and" и пробелами
            if len(parts) >= 2:
                first_part = parts[0]
                if " " in first_part:
                    # Берем все слова кроме последнего
                    words = first_part.split()
                    if len(words) >= 2:
                        base = " ".join(words[:-1])
                        last_word = words[-1]
                        
                        # Вариант 1: с "and" между всеми частями
                        # "Pokémon Red/Blue/Yellow" -> "Pokémon Red and Blue and Yellow"
                        all_parts_with_and = [last_word] + [part.split()[0] if " " in part else part for part in parts[1:]]
                        combined_with_and = f"{base} {' and '.join(all_parts_with_and)}"
                        if combined_with_and not in alternatives:
                            alternatives.append(combined_with_and)
                        
                        # Вариант 2: без "and", просто с пробелами
                        # "Pokémon Red/Blue/Yellow" -> "Pokémon Red Blue Yellow"
                        all_parts_with_spaces = [last_word] + [part.split()[0] if " " in part else part for part in parts[1:]]
                        combined_with_spaces = f"{base} {' '.join(all_parts_with_spaces)}"
                        if combined_with_spaces not in alternatives:
                            alternatives.append(combined_with_spaces)
    
    # Добавляем короткие названия (до двоеточия)
    if ":" in game_title:
        short_name = game_title.split(":")[0].strip()
        if short_name and short_name not in alternatives:
            alternatives.append(short_name)
    
    # Добавляем варианты без скобок
    if "(" in game_title and ")" in game_title:
        # Убираем содержимое в скобках
        no_parentheses = re.sub(r'\s*\([^)]*\)', '', game_title).strip()
        if no_parentheses and no_parentheses not in alternatives:
            alternatives.append(no_parentheses)
        
        # Заменяем скобки на "&" для случаев типа "(& Knuckles)"
        with_ampersand = re.sub(r'\s*\(\s*&\s*([^)]+)\s*\)', r' & \1', game_title)
        if with_ampersand != game_title and with_ampersand not in alternatives:
            alternatives.append(with_ampersand)
    
    # Добавляем варианты с подзаголовками для игр с римскими цифрами
    # Например: "Doom II" -> "Doom II: Hell on Earth"
    if re.search(r'\b(II|III|IV|V|VI|VII|VIII|IX|X)\b', game_title):
        # Добавляем варианты с подзаголовками
        base_name = re.sub(r'\s+(II|III|IV|V|VI|VII|VIII|IX|X)\b', '', game_title)
        roman_num = re.search(r'\b(II|III|IV|V|VI|VII|VIII|IX|X)\b', game_title)
        if roman_num:
            roman = roman_num.group(1)
            # Добавляем варианты с подзаголовками
            subtitle_variants = [
                f"{base_name} {roman}: Hell on Earth",
                f"{base_name} {roman}: The Reckoning",
                f"{base_name} {roman}: Resurrection",
                f"{base_name} {roman}: Final Doom"
            ]
            for variant in subtitle_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
    
    return alternatives

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
        
        # Бонус за включение одного в другое (только если разница в длине не слишком большая)
        if normalized1 in normalized2 or normalized2 in normalized1:
            # Проверяем, что разница в длине не слишком большая
            len_diff = abs(len(normalized1) - len(normalized2))
            shorter_len = min(len(normalized1), len(normalized2))
            
            # Если разница в длине меньше 50% от более короткого названия, даем бонус
            if len_diff < shorter_len * 0.5:
                word_similarity += 0.2
            else:
                # Если разница большая, это скорее всего разные игры
                word_similarity -= 0.1
        
        # Бонус за общие длинные слова (более 4 символов)
        long_common = [w for w in common_words if len(w) > 4]
        if long_common:
            word_similarity += 0.1 * len(long_common)
        
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

def extract_year_from_hltb_page(page):
    """Извлекает минимальный год игры со страницы HLTB"""
    try:
        import re
        
        # Получаем весь текст страницы
        page_text = page.content()
        
        # Ищем все годы в тексте
        year_patterns = [
            r'\b(19|20)\d{2}\b',  # Простые годы
            r'(?:NA|EU|JP|US|UK|Worldwide)[:\s]*\w+\s+\d{1,2}(?:st|nd|rd|th)?,\s*(19|20)\d{2}',  # Даты с регионами
            r'(?:Release|Published|Launched)[:\s]*\w+\s+\d{1,2}(?:st|nd|rd|th)?,\s*(19|20)\d{2}',  # Даты с Release/Published
            r'\w+\s+\d{1,2}(?:st|nd|rd|th)?,\s*(19|20)\d{2}',  # Месяц день, год
        ]
        
        all_years = []
        
        for pattern in year_patterns:
            matches = re.findall(pattern, page_text, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    year_str = match[1] if match[1] else match[0]
                else:
                    year_str = match
                
                try:
                    year = int(year_str)
                    if 1900 <= year <= 2030:  # Разумные границы
                        all_years.append(year)
                except ValueError:
                    continue
        
        if all_years:
            min_year = min(all_years)
            log_message(f"📅 Найден минимальный год на HLTB: {min_year} (из {sorted(set(all_years))})")
            return min_year
        
        log_message("⚠️ Год не найден на странице HLTB")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения года: {e}")
        return None

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
                            if vs_data:
                                # Проверяем, какой тип данных найден
                                if "category" in vs_data:
                                    category = vs_data["category"]
                                    data = vs_data["data"]
                                    
                                    # Записываем в правильную категорию
                                    if category == "coop" and "coop" not in hltb_data:
                                        hltb_data["coop"] = data
                                        log_message(f"🎯 Найдены Co-Op данные в отдельном блоке: {data}")
                                    elif category == "single_player" and "ms" not in hltb_data:
                                        hltb_data["ms"] = data
                                        log_message(f"🎯 Найдены Single-Player данные в отдельном блоке: {data}")
                                    elif category == "multi_player" and "vs" not in hltb_data:
                                        hltb_data["vs"] = data
                                        log_message(f"🎯 Найдены Multi-Player данные в отдельном блоке: {data}")
                                else:
                                    # Старый формат (только Vs. данные)
                                    if "vs" not in hltb_data:
                                        hltb_data["vs"] = vs_data
                                        log_message(f"🎯 Найдены Vs. данные в отдельном блоке: {vs_data}")
                    except Exception as e:
                        log_message(f"⚠️ Ошибка обработки Vs. блока {i}: {e}")
                        continue
        except Exception as e:
            log_message(f"⚠️ Ошибка поиска Vs. блоков: {e}")
        
        # Если найдены только альтернативные данные (чисто мультиплеерные игры), добавляем их как основную категорию
        if hltb_data and len(hltb_data) == 1:
            if "vs" in hltb_data:
                log_message("🎮 Обнаружена чисто мультиплеерная игра, добавляем Vs. как основную категорию")
            elif "coop" in hltb_data:
                log_message("🎮 Обнаружена чисто кооперативная игра, добавляем Co-Op как основную категорию")
            elif "ms" in hltb_data:
                log_message("🎮 Обнаружена чисто одиночная игра, добавляем Single-Player как основную категорию")
        elif hltb_data and len(hltb_data) == 2 and "stores" in hltb_data:
            if "vs" in hltb_data:
                log_message("🎮 Обнаружена чисто мультиплеерная игра с магазинами")
            elif "coop" in hltb_data:
                log_message("🎮 Обнаружена чисто кооперативная игра с магазинами")
            elif "ms" in hltb_data:
                log_message("🎮 Обнаружена чисто одиночная игра с магазинами")
        
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
    """Извлекает Vs. данные из текста или альтернативные данные если Vs. нет"""
    try:
        import re
        
        # Убираем переносы строк для читаемого лога
        clean_text = text.replace('\n', ' ').replace('\r', ' ')
        log_message(f"🔍 Ищем Vs. данные в тексте: '{clean_text[:200]}...'")
        
        # Проверяем, есть ли "Vs. --" (нет данных)
        if re.search(r'Vs\.\s*--', text):
            log_message("ℹ️ Vs. данные отсутствуют (Vs. --), ищем альтернативные данные...")
            # Не возвращаем None, продолжаем поиск альтернативных данных
        
        # Ищем различные форматы Vs. данных
        vs_patterns = [
            r'Vs\.\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',  # "Vs. | 1767 Hours"
            r'Vs\.\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Vs. 1767 Hours"
            r'Vs\.\s*(\d+(?:\.\d+)?)\s*Hours?',        # "Vs.1767 Hours"
            r'Vs\.\s*(\d+(?:\.\d+)?[½]?)\s*Hours?',    # "Vs. 1767½ Hours"
        ]
        
        for pattern in vs_patterns:
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
        
        # Если Vs. данных нет, ищем альтернативные данные (Co-Op, Single-Player и т.п.)
        log_message("🔍 Vs. данных нет, ищем альтернативные данные...")
        
        # Ищем Co-Op данные
        coop_patterns = [
            r'Co-Op\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Co-Op 634 Hours"
            r'Co-Op\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',   # "Co-Op | 634 Hours"
        ]
        
        for pattern in coop_patterns:
            coop_match = re.search(pattern, text)
            if coop_match:
                time_str = coop_match.group(1)
                hours = float(time_str)
                
                if hours >= 1:
                    if hours == int(hours):
                        formatted_time = f"{int(hours)}h"
                    else:
                        formatted_time = f"{hours:.1f}h"
                else:
                    formatted_time = f"{int(hours * 60)}m"
                
                log_message(f"✅ Найдены Co-Op данные: {formatted_time}")
                return {"category": "coop", "data": {"t": formatted_time}}
        
        # Ищем Single-Player данные
        sp_patterns = [
            r'Single-Player\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Single-Player 480 Hours"
            r'Single-Player\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',   # "Single-Player | 480 Hours"
        ]
        
        for pattern in sp_patterns:
            sp_match = re.search(pattern, text)
            if sp_match:
                time_str = sp_match.group(1)
                hours = float(time_str)
                
                if hours >= 1:
                    if hours == int(hours):
                        formatted_time = f"{int(hours)}h"
                    else:
                        formatted_time = f"{hours:.1f}h"
                else:
                    formatted_time = f"{int(hours * 60)}m"
                
                log_message(f"✅ Найдены Single-Player данные: {formatted_time}")
                return {"category": "single_player", "data": {"t": formatted_time}}
        
        # Ищем Multi-Player данные
        mp_patterns = [
            r'Multi-Player\s+(\d+(?:\.\d+)?)\s*Hours?',        # "Multi-Player 480 Hours"
            r'Multi-Player\s*\|\s*(\d+(?:\.\d+)?)\s*Hours?',   # "Multi-Player | 480 Hours"
        ]
        
        for pattern in mp_patterns:
            mp_match = re.search(pattern, text)
            if mp_match:
                time_str = mp_match.group(1)
                hours = float(time_str)
                
                if hours >= 1:
                    if hours == int(hours):
                        formatted_time = f"{int(hours)}h"
                    else:
                        formatted_time = f"{hours:.1f}h"
                else:
                    formatted_time = f"{int(hours * 60)}m"
                
                log_message(f"✅ Найдены Multi-Player данные: {formatted_time}")
                return {"category": "multi_player", "data": {"t": formatted_time}}
        
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
                
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title}")
                
                # Извлекаем год из данных игры
                game_year = game.get("year")
                if game_year:
                    log_message(f"📅 Год игры: {game_year}")
                
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
        log_message(f"📝 Записываем {len(games_list)} игр с HLTB данными в {GAMES_LIST_FILE}")
        if update_html_with_hltb(GAMES_LIST_FILE, games_list):
            log_message("✅ HTML файл успешно обновлен!")
            log_message("🎯 Индекс теперь содержит HLTB данные для всех игр")
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
