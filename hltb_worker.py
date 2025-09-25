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
    categories = {"ms": 0, "mpe": 0, "comp": 0, "coop": 0, "vs": 0}
    total_polled = {"ms": 0, "mpe": 0, "comp": 0, "coop": 0, "vs": 0}
    na_count = 0
    
    for game in hltb_data:
        if "hltb" in game:
            # Проверяем, не является ли это N/A записью
            if (isinstance(game["hltb"], dict) and 
                game["hltb"].get("ms") == "N/A" and 
                game["hltb"].get("mpe") == "N/A" and 
                game["hltb"].get("comp") == "N/A"):
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
    """Ищет игру на HLTB и возвращает данные с повторными попытками, учитывая год релиза"""
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
            
            # Пробуем все альтернативные названия и собираем все результаты
            all_results = []
            
            for alt_title in alternative_titles:
                # Ищем только ссылки, не переходя на страницу
                game_links = search_game_links_only(page, alt_title)
                if game_links:
                    # Вычисляем схожесть между оригинальным названием и альтернативным
                    score = calculate_title_similarity(
                        clean_title_for_comparison(game_title),
                        clean_title_for_comparison(alt_title)
                    )
                    
                    all_results.append({
                        'game_links': game_links,
                        'score': score,
                        'title': alt_title
                    })
            
            # Если есть результаты, выбираем лучший с учетом года
            if all_results:
                best_result = find_best_result_with_year(page, all_results, game_title, game_year)
                if best_result:
                    log_message(f"🏆 Лучший результат: '{best_result['title']}' (схожесть: {best_result['score']:.2f})")
                    # Теперь извлекаем данные с выбранной страницы
                    return extract_data_from_selected_game(page, best_result['selected_link'])
            
        except Exception as e:
            log_message(f"❌ Ошибка попытки {attempt + 1} для '{game_title}': {e}")
            if attempt == max_attempts - 1:
                log_message(f"💥 Все попытки исчерпаны для '{game_title}'")
                return None
    
    return None

def search_game_links_only(page, game_title):
    """Ищет только ссылки на игры без перехода на страницу"""
    try:
        log_message(f"🔍 Ищем ссылки для: '{game_title}'")
        
        # Кодируем название для URL
        safe_title = quote(game_title, safe="")
        search_url = f"{BASE_URL}/?q={safe_title}"
        
        # Переходим на страницу поиска
        page.goto(search_url, timeout=20000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
        # Проверяем на блокировку
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
        
        # Ждем загрузки результатов поиска
        random_delay(3, 5)
        
        # Ищем все ссылки на игры
        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()
        
        if found_count == 0:
            random_delay(2, 4)
            found_count = game_links.count()
        
        if found_count > 10:
            log_message(f"📊 Найдено {found_count} результатов, ждем дополнительную загрузку...")
            random_delay(5, 8)
            found_count = game_links.count()
        
        if found_count == 0:
            return None
        
        # Возвращаем все найденные ссылки
        links_data = []
        for i in range(min(found_count, 10)):  # Берем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            link_href = link.get_attribute("href")
            
            if link_text and link_href:
                links_data.append({
                    'text': link_text,
                    'href': link_href,
                    'element': link
                })
        
        return links_data
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска ссылок для '{game_title}': {e}")
        return None

def extract_data_from_selected_game(page, selected_link):
    """Извлекает данные с выбранной страницы игры"""
    try:
        # Переходим на страницу выбранной игры
        full_url = f"{BASE_URL}{selected_link['href']}"
        
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
        
        # Ждем загрузки страницы
        random_delay(3, 5)
        
        # Извлекаем данные HLTB
        return extract_hltb_data_from_page(page)
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных с страницы игры: {e}")
        return None

def find_best_result_with_year(page, all_results, original_title, original_year):
    """Выбирает лучший результат из всех найденных с учетом года релиза"""
    try:
        if not all_results:
            return None
        
        # Если год не указан, используем старую логику
        if original_year is None:
            best_result = max(all_results, key=lambda x: x['score'])
            # Выбираем лучшую ссылку из этого результата
            best_link = find_best_link_in_result(best_result['game_links'], original_title)
            return {
                'title': best_result['title'],
                'score': best_result['score'],
                'selected_link': best_link
            }
        
        # Сначала собираем всех кандидатов без года
        all_candidates = []
        for result in all_results:
            for link in result['game_links']:
                # Вычисляем схожесть между альтернативным названием (которое искали) и найденным на сайте
                link_similarity = calculate_title_similarity(
                    clean_title_for_comparison(result['title']),  # Используем альтернативное название, которое искали
                    clean_title_for_comparison(link['text'])
                )
                
                all_candidates.append({
                    'title': result['title'],
                    'score': link_similarity,
                    'link': link,
                    'year': None
                })
        
        # Сортируем по схожести и берем только топ-3 для извлечения года
        all_candidates.sort(key=lambda x: -x['score'])
        
        # Определяем количество кандидатов для извлечения года
        if all_candidates and all_candidates[0]['score'] >= 0.99:
            # Для точных совпадений проверяем, есть ли несколько кандидатов с одинаковой схожестью
            same_score_count = sum(1 for c in all_candidates if c['score'] >= 0.99)
            if same_score_count > 1:
                # Если несколько кандидатов с одинаковой схожестью, берем их все (до 3)
                top_candidates = all_candidates[:min(3, same_score_count)]
                log_message(f"🎯 Найдено {same_score_count} точных совпадений, извлекаем год для топ-{len(top_candidates)} кандидатов")
            else:
                # Если только один точный кандидат, берем его + еще 2 лучших
                top_candidates = all_candidates[:3]
                log_message(f"🎯 Найдено точное совпадение, извлекаем год для топ-3 кандидатов")
        else:
            top_candidates = all_candidates[:3]  # Топ-3 кандидата
        
        # Извлекаем год только для выбранных кандидатов
        for candidate in top_candidates:
            game_year = extract_year_from_game_page(page, candidate['link'])
            candidate['year'] = game_year
            log_message(f"🔍 Кандидат: '{candidate['link']['text']}' (схожесть: {candidate['score']:.3f}, год: {game_year})")
            
            # Небольшая пауза между запросами для снижения нагрузки
            if len(top_candidates) > 1:
                time.sleep(random.uniform(0.5, 1.5))  # 0.5-1.5 секунды между запросами
        
        # Добавляем остальных кандидатов без года
        candidates_with_years = top_candidates + all_candidates[len(top_candidates):]
        
        # Сортируем по приоритетам
        candidates_with_years.sort(key=lambda x: (
            -x['score'],  # Сначала по схожести (убывание)
            abs(x['year'] - original_year) if x['year'] is not None else 999  # Потом по разнице в годах
        ))
        
        # Логируем всех кандидатов
        log_message(f"📊 Всего кандидатов: {len(candidates_with_years)}")
        for i, candidate in enumerate(candidates_with_years[:10], 1):  # Показываем первые 10
            log_message(f"📊 {i}. {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
        
        # Приоритет 1: название >= 0.8 + год идентичный
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.8 and candidate['year'] == original_year:
                log_message(f"✅ ПРИОРИТЕТ 1: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        # Приоритет 2: название >= 0.8 + год ближайший в меньшую сторону
        log_message(f"🔍 Ищем приоритет 2: схожесть >= 0.8 и год < {original_year}")
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.8 and candidate['year'] is not None and candidate['year'] < original_year:
                log_message(f"✅ ПРИОРИТЕТ 2: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        # Приоритет 3: название >= 0.8 + год ближайший в любую сторону
        # Но только если нет кандидатов с более высокой схожестью без года
        best_score_without_year = max([c['score'] for c in candidates_with_years if c['year'] is None], default=0)
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.8 and candidate['year'] is not None and candidate['score'] >= best_score_without_year:
                log_message(f"✅ ПРИОРИТЕТ 3: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        # Приоритет 4: название >= 0.6 + год идентичный
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.6 and candidate['year'] == original_year:
                log_message(f"✅ ПРИОРИТЕТ 4: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        # Если ничего не подошло, возвращаем лучший по схожести
        best_candidate = candidates_with_years[0] if candidates_with_years else None
        if best_candidate:
            log_message(f"✅ Лучший по схожести: {best_candidate['link']['text']} (схожесть: {best_candidate['score']:.3f}, год: {best_candidate['year']})")
            return {
                'title': best_candidate['title'],
                'score': best_candidate['score'],
                'selected_link': best_candidate['link']
            }
        
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего результата: {e}")
        return None

def find_best_link_in_result(game_links, original_title):
    """Находит лучшую ссылку в результате поиска"""
    if not game_links:
        return None
    
    best_link = None
    best_score = 0
    
    for link in game_links:
        score = calculate_title_similarity(
            clean_title_for_comparison(original_title),
            clean_title_for_comparison(link['text'])
        )
        
        if score > best_score:
            best_score = score
            best_link = link
    
    return best_link

def extract_year_from_game_page(page, link):
    """Извлекает год релиза со страницы игры"""
    try:
        # Кэш для часто встречающихся игр
        if not hasattr(extract_year_from_game_page, 'url_cache'):
            extract_year_from_game_page.url_cache = {}
        
        full_url = f"{BASE_URL}{link['href']}"
        
        # Проверяем кэш
        if full_url in extract_year_from_game_page.url_cache:
            cached_year = extract_year_from_game_page.url_cache[full_url]
            log_message(f"📅 Год из кэша для '{link['text']}': {cached_year}")
            return cached_year
        
        # Переходим на страницу игры
        page.goto(full_url, timeout=15000)  # Уменьшаем таймаут до 15 секунд
        page.wait_for_load_state("domcontentloaded", timeout=10000)  # Уменьшаем таймаут до 10 секунд
        
        # Извлекаем год
        year = extract_release_year_from_page(page)
        
        # Сохраняем в кэш
        extract_year_from_game_page.url_cache[full_url] = year
        
        log_message(f"📅 Извлечен год для '{link['text']}': {year}")
        return year
        
    except Exception as e:
        log_message(f"⚠️ Ошибка извлечения года для {link['text']}: {e}")
        # Пробуем еще раз с меньшим таймаутом
        try:
            log_message(f"🔄 Повторная попытка извлечения года для '{link['text']}'...")
            page.goto(full_url, timeout=8000)  # Еще меньше таймаут
            page.wait_for_load_state("domcontentloaded", timeout=5000)  # Еще меньше таймаут
            year = extract_release_year_from_page(page)
            log_message(f"📅 Извлечен год для '{link['text']}' (повторно): {year}")
            return year
        except Exception as e2:
            log_message(f"⚠️ Повторная ошибка извлечения года для {link['text']}: {e2}")
            return None

def search_game_single_attempt(page, game_title):
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
        
        # Выбираем наиболее подходящий результат
        best_match, best_title, similarity = find_best_match(page, game_links, game_title)
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
        return hltb_data
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

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
    alternatives = []
    
    # Проверяем, есть ли слеш в названии
    if " / " in game_title:
        # Слеш с пробелами - два отдельных названия (НЕ включаем оригинал)
        parts = [part.strip() for part in game_title.split(" / ")]
        log_message(f"📝 Обрабатываем слеш с пробелами: {parts}")
        
        # Порядок: A, B, A римские, B римские, A амперсанд, B амперсанд, A без скобок, B без скобок
        for part in parts:
            if part and part not in alternatives:
                alternatives.append(part)
        
        # Римские/арабские варианты для каждой части
        for part in parts:
            roman_variants = generate_roman_variants(part)
            for variant in roman_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
        
        # Амперсанд варианты для каждой части
        for part in parts:
            ampersand_variants = generate_ampersand_variants(part)
            for variant in ampersand_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
        
        # Без скобок для каждой части
        for part in parts:
            no_parens = remove_parentheses(part)
            if no_parens and no_parens not in alternatives:
                alternatives.append(no_parens)
                
    elif "/" in game_title and " / " not in game_title:
        # Слеш без пробелов - определяем базовую часть
        parts = [part.strip() for part in game_title.split("/")]
        log_message(f"📝 Обрабатываем слеш без пробелов: {parts}")
        
        # Добавляем оригинал
        alternatives.append(game_title)
        
        # Определяем базовую часть (префикс)
        base = determine_base_part(parts)
        log_message(f"📝 Базовая часть: '{base}'")
        
        if base:
            # Новый порядок: все вместе, парные, одиночные
            
            # 1. Все части вместе
            if len(parts) > 2:
                non_base_parts = []
                for p in parts:
                    if p != base:
                        # Убираем базу из части, если она там есть
                        clean_part = p.replace(base, "").strip()
                        if clean_part:
                            non_base_parts.append(clean_part)
                
                if len(non_base_parts) > 2:
                    all_parts_title = f"{base} {' and '.join(non_base_parts)}"
                    if all_parts_title not in alternatives:
                        alternatives.append(all_parts_title)
            
            # 2. Парные варианты
            for i in range(len(parts)):
                for j in range(i + 1, len(parts)):
                    if parts[i] != base and parts[j] != base:
                        # Убираем базу из частей, если она там есть
                        part1 = parts[i].replace(base, "").strip()
                        part2 = parts[j].replace(base, "").strip()
                        if part1 and part2:
                            pair_title = f"{base} {part1} and {part2}"
                            if pair_title not in alternatives:
                                alternatives.append(pair_title)
            
            # 3. Одиночные варианты
            for part in parts:
                if part and part != base:
                    # Если часть уже содержит базу, не дублируем
                    if part.startswith(base):
                        if part not in alternatives:
                            alternatives.append(part)
                    else:
                        # Проверяем, что часть не начинается с базы
                        if not part.startswith(base + " "):
                            full_title = f"{base} {part}"
                            if full_title not in alternatives:
                                alternatives.append(full_title)
        else:
            # Если базу не определили, обрабатываем как обычные части
            for part in parts:
                if part and part not in alternatives:
                    alternatives.append(part)
    else:
        # Обычное название без слешей
        log_message(f"📝 Обрабатываем обычное название: {game_title}")
        
        # Добавляем оригинал
        alternatives.append(game_title)
        
        # Римские/арабские варианты
        roman_variants = generate_roman_variants(game_title)
        alternatives.extend(roman_variants)
        
        # Амперсанд варианты
        ampersand_variants = generate_ampersand_variants(game_title)
        alternatives.extend(ampersand_variants)
        
        # Без скобок
        no_parens = remove_parentheses(game_title)
        if no_parens and no_parens not in alternatives:
            alternatives.append(no_parens)
    
    # Убираем дубликаты, сохраняя порядок
    unique_alternatives = []
    for alt in alternatives:
        if alt and alt not in unique_alternatives:
            unique_alternatives.append(alt)
    
    log_message(f"🔄 Сгенерировано {len(unique_alternatives)} альтернативных названий")
    return unique_alternatives

def generate_roman_variants(title):
    """Генерирует варианты с римскими/арабскими цифрами"""
    variants = []
    import re
    
    # Ищем арабские цифры
    arabic_pattern = r'(\b\d+\b)'
    matches = re.findall(arabic_pattern, title)
    
    for match in matches:
        roman = convert_arabic_to_roman(match)
        if roman != match:
            alt_title = re.sub(r'\b' + match + r'\b', roman, title)
            if alt_title not in variants:
                variants.append(alt_title)
    
    # Ищем римские цифры
    roman_pattern = r'\b([IVX]+)\b'
    roman_matches = re.findall(roman_pattern, title)
    
    for match in roman_matches:
        arabic = convert_roman_to_arabic(match)
        if arabic != match:
            alt_title = re.sub(r'\b' + match + r'\b', arabic, title)
            if alt_title not in variants:
                variants.append(alt_title)
    
    return variants

def generate_ampersand_variants(title):
    """Генерирует варианты с амперсандом"""
    variants = []
    
    # & -> and
    if "&" in title:
        and_variant = title.replace("&", "and")
        if and_variant not in variants:
            variants.append(and_variant)
    
    # ( & Something) -> & Something (убираем скобки вокруг &)
    import re
    if "(&" in title:
        no_parens_amp = re.sub(r'\(\s*&\s*([^)]+)\)', r'& \1', title)
        if no_parens_amp and no_parens_amp != title and no_parens_amp not in variants:
            variants.append(no_parens_amp)
    
    # Убираем & часть полностью
    if "&" in title:
        # Ищем паттерн "& Something" или "(& Something)"
        no_ampersand = re.sub(r'\s*\(?&\s*[^)]+\)?', '', title).strip()
        if no_ampersand and no_ampersand != title and no_ampersand not in variants:
            variants.append(no_ampersand)
        
        # Убираем только &, оставляя скобки
        no_amp_only = re.sub(r'\s*&\s*', ' ', title).strip()
        if no_amp_only and no_amp_only != title and no_amp_only not in variants:
            variants.append(no_amp_only)
    
    # Упрощаем длинные названия (убираем "the", "of", "and")
    simplified = simplify_title(title)
    if simplified and simplified != title and simplified not in variants:
        variants.append(simplified)
    
    return variants

def remove_parentheses(title):
    """Убирает содержимое в скобках"""
    import re
    no_parens = re.sub(r'\([^)]*\)', '', title).strip()
    # Убираем лишние пробелы
    no_parens = re.sub(r'\s+', ' ', no_parens).strip()
    return no_parens if no_parens != title else None

def determine_base_part(parts):
    """Определяет базовую часть для названий со слешем без пробелов"""
    if not parts or len(parts) < 2:
        return None
    
    # Ищем общий префикс
    first_part = parts[0]
    if " " not in first_part:
        return None
    
    words = first_part.split()
    if len(words) < 2:
        return None
    
    # Пробуем разные варианты базовой части
    for i in range(1, len(words)):
        potential_base = " ".join(words[:i])
        
        # Проверяем, есть ли эта база в других частях
        base_found = True
        for part in parts[1:]:
            if not part.startswith(potential_base):
                base_found = False
                break
        
        if base_found:
            return potential_base
    
    # Если не нашли общий префикс, пробуем найти базовую часть по-другому
    # Для случаев типа "Pokémon Red/Blue/Dark" - база это "Pokémon"
    if len(parts) >= 2:
        # Берем первое слово из первой части как потенциальную базу
        first_word = words[0]
        # Для случаев типа "Pokémon Red/Blue/Dark" - база это "Pokémon"
        # Проверяем, что другие части короткие (скорее всего это варианты)
        short_parts = all(len(part.split()) <= 2 for part in parts[1:])
        if short_parts:
            return first_word
    
    return None

def simplify_title(title):
    """Упрощает название, убирая лишние слова"""
    import re
    # Убираем "the", "of", "and" в начале и конце
    simplified = re.sub(r'\b(the|of|and)\b', '', title, flags=re.IGNORECASE)
    # Убираем лишние пробелы
    simplified = re.sub(r'\s+', ' ', simplified).strip()
    return simplified if simplified != title else None

def convert_roman_to_arabic(roman):
    """Конвертирует римские цифры в арабские"""
    roman_to_arabic = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
        'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
    }
    return roman_to_arabic.get(roman, roman)

def jaro_distance(s1, s2):
    """Вычисляет Jaro-схожесть (базовая метрика для Jaro-Winkler)."""
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    max_dist = (max(len1, len2) // 2) - 1
    match = 0
    hash_s1 = [0] * len1
    hash_s2 = [0] * len2
    # Поиск совпадений в окне
    for i in range(len1):
        for j in range(max(0, i - max_dist), min(len2, i + max_dist + 1)):
            if s1[i] == s2[j] and hash_s2[j] == 0:
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break
    if match == 0:
        return 0.0
    # Подсчёт транспозиций
    t = 0
    point = 0
    for i in range(len1):
        if hash_s1[i]:
            while hash_s2[point] == 0:
                point += 1
            if s1[i] != s2[point]:
                t += 1
            point += 1
    t //= 2  # Транспозиции считаются парно
    return (match / len1 + match / len2 + (match - t) / match) / 3.0

def jaro_winkler_similarity(s1, s2):
    """Jaro-Winkler: Jaro + бонус за префикс (до 4 символов)."""
    jaro = jaro_distance(s1, s2)
    if jaro > 0.7:  # Только если базовая схожесть высокая
        prefix = 0
        min_len = min(len(s1), len(s2))
        for i in range(min_len):
            if s1[i] == s2[i]:
                prefix += 1
            else:
                break
        prefix = min(4, prefix)
        jaro += 0.1 * prefix * (1 - jaro)
    return jaro

def calculate_title_similarity(title1, title2, year1=None, year2=None):
    """Вычисляет схожесть между двумя названиями игр используя Jaro-Winkler с учетом года"""
    try:
        # Нормализуем названия для сравнения (конвертируем римские цифры в арабские)
        normalized1 = normalize_title_for_comparison(title1)
        normalized2 = normalize_title_for_comparison(title2)
        
        # Используем Jaro-Winkler для вычисления схожести
        similarity = jaro_winkler_similarity(normalized1.lower(), normalized2.lower())
        
        # Бонус за точное совпадение
        if normalized1.lower() == normalized2.lower():
            similarity = 1.0
        
        # Штраф за значительную разницу в длине (более короткое название весит меньше)
        len1, len2 = len(normalized1), len(normalized2)
        if len1 > 0 and len2 > 0:
            length_ratio = min(len1, len2) / max(len1, len2)
            # Если одно название значительно короче другого, снижаем схожесть
            if length_ratio < 0.7:  # Если разница в длине больше 30%
                similarity *= length_ratio
        
        # Штраф за разницу в годах (если годы предоставлены)
        if year1 is not None and year2 is not None:
            year_diff = abs(year1 - year2)
            if year_diff == 0:
                year_penalty = 0  # Точное совпадение года
            elif year_diff <= 1:
                year_penalty = 0.01  # Разница в 1 год
            elif year_diff <= 2:
                year_penalty = 0.05  # Разница в 2 года
            elif year_diff <= 5:
                year_penalty = 0.1   # Разница в 3-5 лет
            else:
                year_penalty = 0.2   # Большая разница в годах
            
            similarity -= year_penalty
        
        return max(0.0, similarity)  # Не даем отрицательных значений
        
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести: {e}")
        return 0.0
        
def extract_release_year_from_page(page):
    """Извлекает год релиза со страницы игры HLTB"""
    try:
        # Кэш для хранения извлеченных годов
        if not hasattr(extract_release_year_from_page, 'year_cache'):
            extract_release_year_from_page.year_cache = {}
        
        # Проверяем кэш
        page_url = page.url
        if page_url in extract_release_year_from_page.year_cache:
            return extract_release_year_from_page.year_cache[page_url]
        
        # Пытаемся извлечь год из JSON данных
        try:
            # Ищем JSON данные в script теге
            json_script = page.locator('script#__NEXT_DATA__').first
            if json_script.count() > 0:
                json_text = json_script.text_content()
                import json
                data = json.loads(json_text)
                
                # Ищем год в структуре данных
                games = data.get('props', {}).get('pageProps', {}).get('game', {}).get('data', {}).get('game', [])
                if games:
                    # Берем первую игру из списка
                    game_data = games[0]
                    
                    # Ищем различные поля с датой
                    year_fields = ['game_name_date', 'release_date', 'date', 'year']
                    for field in year_fields:
                        if field in game_data and game_data[field]:
                            year = game_data[field]
                            if isinstance(year, (int, str)) and str(year).isdigit():
                                year_int = int(year)
                                if 1950 <= year_int <= 2030:  # Разумный диапазон годов
                                    extract_release_year_from_page.year_cache[page_url] = year_int
                                    return year_int
        except Exception as e:
            log_message(f"⚠️ Ошибка извлечения года из JSON: {e}")
        
        # Если JSON не сработал, ищем в HTML тексте
        try:
            # Ищем паттерны типа "NA: September 24th, 1997" или "2016"
            page_text = page.content()
            
            # Паттерн для дат типа "September 24th, 1997"
            date_pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,\s+(\d{4})'
            matches = re.findall(date_pattern, page_text, re.IGNORECASE)
            if matches:
                years = [int(year) for year in matches if 1950 <= int(year) <= 2030]
                if years:
                    # Берем самый ранний год
                    earliest_year = min(years)
                    extract_release_year_from_page.year_cache[page_url] = earliest_year
                    return earliest_year
            
            # Паттерн для простых годов
            year_pattern = r'\b(19\d{2}|20\d{2})\b'
            matches = re.findall(year_pattern, page_text)
            if matches:
                years = [int(match) for match in matches if 1950 <= int(match) <= 2030]
                if years:
                    # Берем самый ранний год
                    earliest_year = min(years)
                    extract_release_year_from_page.year_cache[page_url] = earliest_year
                    return earliest_year
        except Exception as e:
            log_message(f"⚠️ Ошибка извлечения года из HTML: {e}")
        
        # Если ничего не найдено
        extract_release_year_from_page.year_cache[page_url] = None
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения года релиза: {e}")
        return None

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

def extract_hltb_data_from_page(page):
    """Извлекает данные HLTB со страницы игры"""
    try:
        hltb_data = {}
        
        # Сначала ищем данные в верхних блоках (Single, Co-Op, Vs.)
        top_block_data = extract_top_block_data(page)
        if top_block_data:
            hltb_data.update(top_block_data)
            log_message(f"📊 Найдены данные в верхних блоках: {list(top_block_data.keys())}")
        
        # Затем ищем данные в таблицах
        table_data = extract_table_data(page)
        if table_data:
            hltb_data.update(table_data)
            log_message(f"📊 Найдены данные в таблицах: {list(table_data.keys())}")
        
        # Если есть только верхние блоки (без таблиц с ms/mpe/comp), используем только их
        if top_block_data and not table_data:
            log_message("🎮 Обнаружена игра только с верхними блоками (без детальных таблиц)")
            # Оставляем только данные из верхних блоков
        elif top_block_data and table_data:
            # Проверяем, есть ли в таблицах данные о single player (ms/mpe/comp)
            has_single_player_data = any(key in table_data for key in ["ms", "mpe", "comp"])
            if not has_single_player_data:
                log_message("🎮 В таблицах нет single player данных, используем только верхние блоки")
                # Удаляем данные из таблиц, оставляем только верхние блоки
                hltb_data = top_block_data.copy()
        
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
                log_message(f"📊 Итоговые категории: {', '.join(categories)}")
        
        return hltb_data if hltb_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных со страницы: {e}")
        return None

def extract_top_block_data(page):
    """Извлекает данные из верхних блоков (Single, Co-Op, Vs.)"""
    try:
        top_data = {}
        
        # Ищем блок с игровыми статистиками
        game_stats = page.locator('.GameStats_game_times__KHrRY')
        if game_stats.count() == 0:
            log_message("❌ Блок GameStats не найден")
            return None
        
        # Получаем все элементы списка
        stats_items = game_stats.locator('li')
        item_count = stats_items.count()
        
        log_message(f"📊 Найдено {item_count} элементов статистики")
        
        for i in range(item_count):
            try:
                item = stats_items.nth(i)
                
                # Получаем название категории (h4) и время (h5)
                category_element = item.locator('h4')
                time_element = item.locator('h5')
                
                if category_element.count() > 0 and time_element.count() > 0:
                    category = category_element.inner_text().strip()
                    time_text = time_element.inner_text().strip()
                    
                    log_message(f"📊 Категория: '{category}', Время: '{time_text}'")
                    
                    # Пропускаем пустые значения
                    if time_text == "--" or not time_text or "Hours" not in time_text:
                        log_message(f"⚠️ Пропускаем пустое значение для {category}")
                        continue
                    
                    # Обрабатываем данные в зависимости от категории
                    if category == "Co-Op":
                        coop_data = extract_time_from_h5(time_text)
                        if coop_data and "coop" not in top_data:
                            top_data["coop"] = coop_data
                            log_message(f"🎯 Найдены Co-Op данные: {coop_data}")
                    elif category == "Vs.":
                        vs_data = extract_time_from_h5(time_text)
                        if vs_data and "vs" not in top_data:
                            top_data["vs"] = vs_data
                            log_message(f"🎯 Найдены Vs. данные: {vs_data}")
                    elif category in ["Single-Player", "Single Player"]:
                        single_data = extract_time_from_h5(time_text)
                        if single_data and "ms" not in top_data:
                            top_data["ms"] = single_data
                            log_message(f"🎯 Найдены Single данные: {single_data}")
                            
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки элемента статистики {i}: {e}")
                continue
        
        return top_data if top_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из верхних блоков: {e}")
        return None

def extract_time_from_h5(time_text):
    """Извлекает время из текста h5 элемента"""
    try:
        import re
        
        log_message(f"🔍 Обрабатываем время: '{time_text}'")
        
        # Ищем число и "Hours"
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*Hours?', time_text)
        if time_match:
            hours = float(time_match.group(1))
            
            # Форматируем время
            if hours >= 1:
                if hours == int(hours):
                    formatted_time = f"{int(hours)}h"
                else:
                    formatted_time = f"{hours:.1f}h"
            else:
                formatted_time = f"{int(hours * 60)}m"
            
            log_message(f"✅ Извлечено время: {formatted_time}")
            return {"t": formatted_time}
        
        # Ищем число и "Minutes" или "Mins" или просто "m"
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:Minutes?|Mins?|m)\b', time_text)
        if time_match:
            minutes = float(time_match.group(1))
            formatted_time = f"{int(minutes)}m"
            
            log_message(f"✅ Извлечено время: {formatted_time}")
            return {"t": formatted_time}
        
        # Ищем число и "h" (часы)
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*h\b', time_text)
        if time_match:
            hours = float(time_match.group(1))
            if hours == int(hours):
                formatted_time = f"{int(hours)}h"
            else:
                formatted_time = f"{hours:.1f}h"
            
            log_message(f"✅ Извлечено время: {formatted_time}")
            return {"t": formatted_time}
        
        log_message("❌ Время не найдено")
        return None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения времени: {e}")
        return None

def extract_table_data(page):
    """Извлекает данные из таблиц"""
    try:
        table_data = {}
        
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
                            if "Main Story" in row_text and "ms" not in table_data:
                                table_data["ms"] = extract_hltb_row_data(row_text)
                            elif "Main + Extras" in row_text and "mpe" not in table_data:
                                table_data["mpe"] = extract_hltb_row_data(row_text)
                            elif "Completionist" in row_text and "comp" not in table_data:
                                table_data["comp"] = extract_hltb_row_data(row_text)
                            elif "Co-Op" in row_text and "coop" not in table_data:
                                table_data["coop"] = extract_hltb_row_data(row_text)
                            elif "Competitive" in row_text and "vs" not in table_data:
                                table_data["vs"] = extract_hltb_row_data(row_text)
                                
                        except Exception as e:
                            log_message(f"⚠️ Ошибка обработки строки {row_idx}: {e}")
                            continue
                            
            except Exception as e:
                log_message(f"⚠️ Ошибка обработки таблицы {table_idx}: {e}")
                continue
        
        return table_data if table_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из таблиц: {e}")
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


def determine_error_type(page, game_title):
    """Определяет тип ошибки при поиске игры"""
    try:
        page_content = page.content().lower()
        
        # Проверяем на блокировку IP
        if "blocked" in page_content or "access denied" in page_content:
            return "IP блокировка"
        
        # Проверяем на Cloudflare
        if "cloudflare" in page_content and "checking your browser" in page_content:
            return "Cloudflare блокировка"
        
        # Проверяем на таймаут
        if "timeout" in page_content or "timed out" in page_content:
            return "Таймаут запроса"
        
        # Проверяем на ошибку сети
        if "network error" in page_content or "connection error" in page_content:
            return "Ошибка сети"
        
        # Проверяем на отсутствие результатов поиска
        search_results = page.locator('a[href^="/game/"]')
        if search_results.count() == 0:
            return "Игра не найдена в поиске"
        
        # Проверяем на отсутствие данных на странице игры
        tables = page.locator("table")
        if tables.count() == 0:
            return "Нет таблиц с данными на странице"
        
        # Если ничего не подошло
        return "Неизвестная ошибка"
        
    except Exception as e:
        log_message(f"❌ Ошибка определения типа ошибки: {e}")
        return "Ошибка анализа страницы"

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
        log_message(f"📊 Completionist: {categories['comp']} ({total_polled['comp']} голосов)")
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
            for i in range(0, total_games):
                game = games_list[i]
                game_title = game["title"]
                game_year = game.get("year")  # Получаем год из данных игры
                
                log_message(f"🎮 Обрабатываю {i+1}/{total_games}: {game_title} ({game_year})")
                
                # Ищем данные на HLTB
                hltb_data = search_game_on_hltb(page, game_title, game_year)
                
                if hltb_data:
                    game["hltb"] = hltb_data
                    processed_count += 1
                    blocked_count = 0  # Сбрасываем счетчик блокировок при успехе
                    log_message(f"✅ Найдены данные: {hltb_data}")
                else:
                    # Определяем тип ошибки
                    error_type = determine_error_type(page, game_title)
                    
                    # Записываем N/A если данные не найдены
                    game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A"}
                    log_message(f"⚠️  {error_type} для: {game_title} - записано N/A")
                    
                    # Проверяем, не было ли блокировки
                    if error_type == "IP блокировка":
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
                
                # Логируем прогресс каждые 50 игр
                if (i + 1) % 50 == 0:
                    log_progress(i + 1, total_games, start_time)
            
            browser.close()
        
        # Сохраняем финальные результаты
        save_results(games_list)
        
        # Финальная статистика
        successful = len([g for g in games_list if "hltb" in g])
        log_message(f"🎉 Завершено! Обработано {successful}/{total_games} игр ({successful/total_games*100:.1f}%)")
        
        # HTML файл не обновляется - только сохраняем данные в JSON
        log_message("📄 Данные сохранены в JSON файл, HTML не обновляется")
        
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
