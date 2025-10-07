import json
import time
import random
import re
import os
from datetime import datetime
from urllib.parse import quote
from playwright.sync_api import sync_playwright

BASE_URL = "https://howlongtobeat.com"
GAMES_LIST_FILE = "index.html"
OUTPUT_DIR = "hltb_data"
OUTPUT_FILE = f"{OUTPUT_DIR}/hltb_data.json"

CHUNK_INDEX = int(os.environ.get('CHUNK_INDEX', '0'))
CHUNK_SIZE = 350


def get_chunk_games(games_list):
    total_games = len(games_list)
    
    start_index = CHUNK_INDEX * CHUNK_SIZE
    end_index = min(start_index + CHUNK_SIZE, total_games)
    
    if start_index >= total_games:
        log_message(f"⚠ Чанк {CHUNK_INDEX} выходит за границы списка")
        return [], 0, 0
    
    chunk_games = games_list[start_index:end_index]
    log_message(f" Чанк {CHUNK_INDEX}: игры {start_index+1}-{end_index} из {total_games}")
    
    return chunk_games, start_index, end_index

def setup_directories():
    print(f" Создаем директорию: {OUTPUT_DIR}")
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f" Директория {OUTPUT_DIR} создана/существует")
    except Exception as e:
        print(f"❌ Ошибка создания директории: {e}")
        raise
    
def log_message(message):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
    except Exception as e:
        print(f"Ошибка логирования: {e}")
        print(f"Сообщение: {message}")

def count_hltb_data(hltb_data):
    categories = {"ms": 0, "mpe": 0, "comp": 0, "coop": 0, "vs": 0}
    total_polled = {"ms": 0, "mpe": 0, "comp": 0, "coop": 0, "vs": 0}
    na_count = 0
    
    for game in hltb_data:
        if "hltb" in game:
            if (isinstance(game["hltb"], dict) and 
                game["hltb"].get("ms") == "N/A" and 
                game["hltb"].get("mpe") == "N/A" and 
                game["hltb"].get("comp") == "N/A"):
                na_count += 1
                continue
            
            for category in categories:
                if category in game["hltb"] and game["hltb"][category] and game["hltb"][category] != "N/A":
                    categories[category] += 1
                    if isinstance(game["hltb"][category], dict) and "p" in game["hltb"][category]:
                        total_polled[category] += game["hltb"][category]["p"]
    
    return categories, total_polled, na_count

def extract_games_list(html_file):
    try:
        log_message(f" Читаем файл {html_file}...")
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        log_message(f" Файл прочитан, размер: {len(content)} символов")
        
        log_message(" Ищем 'const gamesList = ['...")
        start = content.find('const gamesList = [')
        if start == -1:
            raise ValueError("Не найден const gamesList в HTML файле")
        
        log_message(f" Найден const gamesList на позиции {start}")
        
        log_message(" Ищем закрывающую скобку массива...")
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
        
        log_message(f" Найден конец массива на позиции {end}")
        
        log_message(" Извлекаем JSON...")
        games_json = content[start:end]
        games_json = games_json.replace('const gamesList = ', '')
        
        log_message(f" JSON извлечен, размер: {len(games_json)} символов")
        log_message(" Парсим JSON...")
        
        games_list = json.loads(games_json)
        log_message(f" Извлечено {len(games_list)} игр из HTML файла")
        return games_list
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения списка игр: {e}")
        raise

def parse_time_to_hours(time_str):
    if not time_str or time_str == "N/A":
        return 0, 0
    
    time_str = time_str.replace("Hours", "").strip()
    
    hours_match = re.search(r'(\d+(?:\.\d+)?)h', time_str)
    minutes_match = re.search(r'(\d+)m', time_str)
    
    hours = float(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    
    if hours == 0 and minutes == 0:
        number_match = re.search(r'(\d+(?:\.\d+)?)', time_str)
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
    if not time_str or time_str == "N/A":
        return None
    
    if time_str.endswith('m') and 'h' not in time_str:
        return time_str
    
    hours, minutes = parse_time_to_hours(time_str)
    
    hours = int(hours)
    
    if minutes <= 14:
        return f"{hours}h"
    elif minutes <= 44:
        return f"{hours}.5h"
    else:
        return f"{hours + 1}h"

def random_delay(min_seconds, max_seconds):
    delay = random.uniform(min_seconds, max_seconds)
    time.sleep(delay)

def is_valid_gog_link(page, gog_url):
    try:
        original_url = page.url
        
        page.goto(gog_url, timeout=10000, wait_until="domcontentloaded")
        
        time.sleep(2)
        
        try:
            age_verification = page.locator('text=Age verification').first
            if age_verification.count() > 0:
                page.goto(original_url, timeout=10000, wait_until="domcontentloaded")
                return True
        except:
            pass
        
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except:
            pass
        
        time.sleep(3)
        
        current_url = page.url
        
        try:
            page_title = page.title()
            if "Browse" in page_title or "Games" in page_title or "Catalog" in page_title:
                page.goto(original_url, timeout=10000, wait_until="domcontentloaded")
                return False
            
            game_title = page.locator('h1').first
            if game_title.count() > 0:
                title_text = game_title.text_content()
                if "Browse" in title_text or "Games" in title_text or "All games" in title_text:
                    page.goto(original_url, timeout=10000, wait_until="domcontentloaded")
                    return False
            
            buy_buttons = page.locator('text=Buy now, text=Add to cart, text=Install, text=Play').first
            if buy_buttons.count() == 0:
                catalog_elements = page.locator('text=Browse games, text=All games, text=New releases').first
                if catalog_elements.count() > 0:
                    page.goto(original_url, timeout=10000, wait_until="domcontentloaded")
                    return False
        except:
            pass
        
        page.goto(original_url, timeout=10000, wait_until="domcontentloaded")
        
        invalid_urls = [
            "https://www.gog.com/",
            "https://www.gog.com/en", 
            "https://www.gog.com/en/",
            "https://www.gog.com/en/games",
            "https://www.gog.com/en/games/"
        ]
        
        if current_url in invalid_urls:
            return False
        
        if (current_url == "https://www.gog.com/" or 
            current_url.endswith("/en") or 
            current_url.endswith("/en/") or
            current_url.endswith("/games") or
            current_url.endswith("/games/")):
            return False
        
        if "/game/" in current_url or "/en/game/" in current_url:
            return True
        
        if current_url != gog_url and "gog.com" in current_url:
            return True
        
        return False
        
    except Exception as e:
        return False

def progressive_delay_for_blocking():
    delays = [30, 60, 120, 180, 300]
    delay = random.choice(delays)
    log_message(f"Задержка при блокировке: {delay} секунд")
    time.sleep(delay)



def extract_data_by_hltb_id(page, hltb_id):
    try:
        log_message(f" -Извлекаем данные по ID {hltb_id}...")
        
        game_url = f"{BASE_URL}/game/{hltb_id}"
        
        max_attempts = 3
        timeouts = [12000, 15000, 18000]
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    log_message(f" Повторная попытка {attempt + 1}/{max_attempts} извлечения данных по ID {hltb_id}...")
                
                random_delay(1, 3)
                
                page.goto(game_url, wait_until="domcontentloaded", timeout=timeouts[attempt])
                
                page.wait_for_selector('table', timeout=8000)
                
                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при прямом доступе!")
                    progressive_delay_for_blocking()
                    if attempt < max_attempts - 1:
                        log_message(f" Повторная попытка после блокировки (попытка {attempt + 2})...")
                        continue
                    else:
                        log_message("❌ Блокировка IP - все попытки исчерпаны")
                        return None
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message(" Cloudflare проверка при прямом доступе - ждем...")
                    time.sleep(10)
                    page_content = page.content()
                    if "checking your browser" in page_content.lower():
                        log_message("❌ Cloudflare блокирует прямой доступ")
                        if attempt < max_attempts - 1:
                            log_message(f" Повторная попытка после Cloudflare (попытка {attempt + 2})...")
                            continue
                        else:
                            return None
                
                random_delay(3, 5)
                
                hltb_data = extract_hltb_data_from_page(page)
                
                if hltb_data:
                    hltb_data["hltb_id"] = hltb_id
                    if attempt > 0:
                        log_message(f"✅ Данные извлечены по ID {hltb_id} с попытки {attempt + 1}")
                    else:
                        log_message(f"✅ Данные извлечены по ID {hltb_id}")
                    return hltb_data
                else:
                    if attempt == max_attempts - 1:
                        log_message(f"⚠️ Не удалось извлечь данные по ID {hltb_id} после всех попыток")
                        return None
                    else:
                        log_message(f"⚠️ Не удалось извлечь данные по ID {hltb_id} (попытка {attempt + 1})")
                        continue
                
            except Exception as e:
                error_msg = str(e)
                if "Timeout" in error_msg:
                    log_message(f" Таймаут при извлечении данных по ID {hltb_id} (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        log_message(f"❌ Все {max_attempts} попыток исчерпаны для извлечения данных по ID {hltb_id} - таймауты")
                        return None
                else:
                    log_message(f"❌ Ошибка извлечения данных по ID {hltb_id} (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        return None
        
        return None
            
    except Exception as e:
        log_message(f"❌ Критическая ошибка извлечения данных по ID {hltb_id}: {e}")
        return None

def retry_game_with_blocking_handling(page, game_title, game_year, max_retries=5):
    delays = [30, 60, 120, 180, 300]
    
    for retry in range(max_retries):
        try:
            log_message(f" -Попытка {retry + 1}/{max_retries} для '{game_title}'")
            
            hltb_data = search_game_on_hltb(page, game_title, game_year)
            
            if hltb_data:
                log_message(f"✅ Успешно получены данные для '{game_title}'")
                return hltb_data
            else:
                page_content = page.content().lower()
                if "blocked" in page_content or "access denied" in page_content:
                    log_message(f"🚫 Блокировка при попытке {retry + 1} для '{game_title}'")
                    
                    if retry < max_retries - 1:
                        delay = delays[retry]
                        log_message(f" Повторная попытка через {delay} секунд...")
                        time.sleep(delay)
                        continue
                    else:
                        log_message(f"❌ Все {max_retries} попыток исчерпаны для '{game_title}' - блокировка")
                        return None
                else:
                    log_message(f"⚠️ Данные не найдены для '{game_title}' (попытка {retry + 1})")
                    return None
                    
        except Exception as e:
            log_message(f"❌ Ошибка при попытке {retry + 1} для '{game_title}': {e}")
            if retry < max_retries - 1:
                delay = delays[retry]
                log_message(f" Повторная попытка через {delay} секунд...")
                time.sleep(delay)
            else:
                log_message(f"❌ Все {max_retries} попыток исчерпаны для '{game_title}' - ошибка")
                return None
    
    return None


def search_game_on_hltb(page, game_title, game_year=None):
    max_attempts = 3
    delays = [0, (15, 18), (65, 70)]
    
    
    alternative_titles = generate_alternative_titles(game_title)
    
    for attempt in range(max_attempts):
        try:
            if attempt > 0:
                log_message(f" Попытка {attempt + 1}/{max_attempts} для '{game_title}'")
                if isinstance(delays[attempt], tuple):
                    min_delay, max_delay = delays[attempt]
                    log_message(f" Пауза {min_delay}-{max_delay} секунд...")
                    random_delay(min_delay, max_delay)
                else:
                    log_message(f" Пауза {delays[attempt]} секунд...")
                    time.sleep(delays[attempt])
            
            all_results = []
            
            for alt_title in alternative_titles:
                game_links = search_game_links_only(page, alt_title)
                if game_links:
                    score = calculate_title_similarity(
                        clean_title_for_comparison(game_title),
                        clean_title_for_comparison(alt_title)
                    )
                    
                    all_results.append({
                        'game_links': game_links,
                        'score': score,
                        'title': alt_title
                    })
                else:
                    log_message(f" -Не найдено результатов для '{alt_title}'")
            
            if all_results:
                best_result = find_best_result_with_year(page, all_results, game_title, game_year)
                if best_result:
                    pass
                    return extract_data_from_selected_game(page, best_result['selected_link'])
            
        except Exception as e:
            log_message(f" Ошибка попытки {attempt + 1} для '{game_title}': {e}")
            if attempt == max_attempts - 1:
                log_message(f" Все попытки исчерпаны для '{game_title}'")
                return None
    
    return None

def search_game_links_only(page, game_title):
    try:
        safe_title = quote(game_title, safe="")
        if "%25" in safe_title:
            safe_title = safe_title.replace("%25", "%")
        search_url = f"{BASE_URL}/?q={safe_title}"
        
        max_attempts = 3
        timeouts = [10000, 12000, 15000]
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    pass
                
                page.goto(search_url, wait_until="domcontentloaded", timeout=timeouts[attempt])
                
                page_content = page.content()
                
                if "captcha" in page_content.lower():
                    log_message(f" Найдено слово 'captcha' на странице, проверяем контекст...")
                    import re
                    captcha_context = re.findall(r'.{0,50}captcha.{0,50}', page_content.lower())
                    for context in captcha_context[:3]:  # Показываем первые 3 вхождения
                        log_message(f" Контекст: '{context.strip()}'")
                    
                    log_message(" ВРЕМЕННО: пропускаем проверку на капчу для диагностики")
                
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при поиске!")
                    progressive_delay_for_blocking()
                    if attempt < max_attempts - 1:
                        log_message(f" Повторная попытка после блокировки (попытка {attempt + 2})...")
                        continue
                    else:
                        return None
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message(" Cloudflare проверка при поиске - ждем...")
                    time.sleep(10)
                    page_content = page.content()
                    if "checking your browser" in page_content.lower():
                        log_message("❌ Cloudflare блокирует поиск")
                        if attempt < max_attempts - 1:
                            log_message(f" Повторная попытка после Cloudflare (попытка {attempt + 2})...")
                            continue
                        else:
                            return None
                captcha_elements = page.locator('[class*="captcha"], [id*="captcha"], iframe[src*="captcha"], .g-recaptcha, .hcaptcha')
                has_captcha_element = captcha_elements.count() > 0
                
                has_captcha_text = ("captcha" in page_content.lower() and ("solve" in page_content.lower() or "verify" in page_content.lower() or "challenge" in page_content.lower())) or ("robot" in page_content.lower() and "detected" in page_content.lower())
                
                
                if has_captcha_element:
                    log_message(f" Найдено {captcha_elements.count()} элементов капчи на странице (игнорируем)")
                if has_captcha_text:
                    log_message(" Найдены текстовые признаки капчи (игнорируем)")
                elif "rate limit" in page_content.lower() or "too many requests" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНО ОГРАНИЧЕНИЕ СКОРОСТИ при поиске!")
                    if attempt < max_attempts - 1:
                        log_message(f" Повторная попытка после ограничения скорости (попытка {attempt + 2})...")
                        time.sleep(60)
                        continue
                    else:
                        return None
                
                random_delay(1, 2)
                
                js_errors = page.evaluate("() => { return window.console && window.console.error ? 'Есть JS ошибки' : 'Нет JS ошибок'; }")
                
                if js_errors == "Есть JS ошибки":
                    time.sleep(2)
                    
                    if attempt < max_attempts - 1:
                        page.reload(wait_until="domcontentloaded", timeout=timeouts[attempt])
                        time.sleep(1)
                
                max_wait_attempts = 2
                found_count = 0
                game_links = None
                
                for wait_attempt in range(max_wait_attempts):
                    game_links = page.locator('a[href^="/game/"]')
                    found_count = game_links.count()
                    
                    if found_count > 0:
                        break
                    
                    if wait_attempt < max_wait_attempts - 1:
                        pass
                        if js_errors == "Есть JS ошибки":
                            random_delay(2, 4)
                        else:
                            random_delay(1, 3)
                
                if found_count == 0:
                    random_delay(2, 4)
                    found_count = game_links.count()
                
                if found_count == 0:
                    pass
                    
                    error_selectors = [
                        '.error', '.no-results', '.not-found', 
                        '[class*="error"]', '[class*="no-results"]'
                    ]
                    for error_selector in error_selectors:
                        error_elements = page.locator(error_selector)
                        if error_elements.count() > 0:
                            error_text = error_elements.first.inner_text()
                            log_message(f" Найдено сообщение об ошибке: {error_text}")
                    
                    alternative_selectors = [
                        'a[href*="/game/"]',
                        '.search-result a',
                        '.game-link',
                        'a[href^="/game"]',
                        'a[href*="game"]',
                        '.result a',
                        '[data-testid*="game"] a',
                        'a[href*="howlongtobeat.com/game/"]',
                        '.game-title a',
                        '.search-item a',
                        'a[href*="/game"]'
                    ]
                    
                    for selector in alternative_selectors:
                        alt_links = page.locator(selector)
                        alt_count = alt_links.count()
                        if alt_count > 0:
                            log_message(f" Найдено {alt_count} результатов с селектором '{selector}'")
                            game_links = alt_links
                            found_count = alt_count
                            break
                
                if found_count > 10:
                    random_delay(3, 5)
                    found_count = game_links.count()
                
                if found_count == 0:
                    if attempt == max_attempts - 1:
                        return None
                    else:
                        pass
                        continue
                
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
                
                if attempt > 0:
                    log_message(f"✅ Поиск успешен с попытки {attempt + 1}")
                
                return links_data
                
            except Exception as e:
                error_msg = str(e)
                if "Timeout" in error_msg:
                    log_message(f" Таймаут при поиске '{game_title}' (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        log_message(f"❌ Все {max_attempts} попыток исчерпаны для поиска '{game_title}' - таймауты")
                        return None
                else:
                    log_message(f"❌ Ошибка поиска '{game_title}' (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        return None
        
        return None
        
    except Exception as e:
        log_message(f"❌ Критическая ошибка поиска ссылок для '{game_title}': {e}")
        return None

def extract_data_from_selected_game(page, selected_link):
    try:
        full_url = f"{BASE_URL}{selected_link['href']}"
        
        max_attempts = 3
        timeouts = [12000, 15000, 18000]
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    log_message(f" Повторная попытка {attempt + 1}/{max_attempts} извлечения данных с страницы игры...")
                
                random_delay(1, 3)
                
                page.goto(full_url, wait_until="domcontentloaded", timeout=timeouts[attempt])
                
                page.wait_for_selector('table', timeout=8000)
                
                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры!")
                    progressive_delay_for_blocking()
                    if attempt < max_attempts - 1:
                        log_message(f" Повторная попытка после блокировки (попытка {attempt + 2})...")
                        continue
                    else:
                        log_message("❌ Блокировка IP - все попытки исчерпаны")
                        return None
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message(" Cloudflare проверка на странице игры - ждем...")
                    time.sleep(10)
                    page_content = page.content()
                    if "checking your browser" in page_content.lower():
                        log_message("❌ Cloudflare блокирует страницу игры")
                        if attempt < max_attempts - 1:
                            log_message(f" Повторная попытка после Cloudflare (попытка {attempt + 2})...")
                            continue
                        else:
                            return None
                
                random_delay(5, 8)
                
                hltb_data = extract_hltb_data_from_page(page)
                
                if attempt > 0 and hltb_data:
                    log_message(f"✅ Успешно извлечены данные с попытки {attempt + 1}")
                
                return hltb_data
                
            except Exception as e:
                error_msg = str(e)
                if "Timeout" in error_msg:
                    log_message(f" Таймаут при извлечении данных с страницы игры (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        log_message(f"❌ Все {max_attempts} попыток исчерпаны для извлечения данных с страницы игры - таймауты")
                        return None
                else:
                    log_message(f"❌ Ошибка извлечения данных с страницы игры (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        return None
        
        return None
        
    except Exception as e:
        log_message(f"❌ Критическая ошибка извлечения данных с страницы игры: {e}")
        return None

def find_best_result_with_year(page, all_results, original_title, original_year):
    try:
        if not all_results:
            return None
        
        if original_year is None:
            best_result = max(all_results, key=lambda x: x['score'])
            best_link = find_best_link_in_result(best_result['game_links'], original_title)
            return {
                'title': best_result['title'],
                'score': best_result['score'],
                'selected_link': best_link
            }
        
        all_candidates = []
        for result in all_results:
            for link in result['game_links']:
                link_similarity = calculate_title_similarity(
                    clean_title_for_comparison(result['title']),
                    clean_title_for_comparison(link['text'])
                )
                
                all_candidates.append({
                    'title': result['title'],
                    'score': link_similarity,
                    'link': link,
                    'year': None
                })
        
        all_candidates.sort(key=lambda x: -x['score'])
        
        if all_candidates and all_candidates[0]['score'] >= 0.99:
            same_score_count = sum(1 for c in all_candidates if c['score'] >= 0.99)
            if same_score_count > 1:
                top_candidates = all_candidates[:min(5, same_score_count)]
                log_message(f" -Найдено {same_score_count} точных совпадений, извлекаем год для топ-{len(top_candidates)} кандидатов")
            else:
                top_candidates = all_candidates[:5]
                pass
        else:
            top_candidates = all_candidates[:5]
        
        for i, candidate in enumerate(top_candidates):
            game_year = extract_year_from_game_page(page, candidate['link'])
            candidate['year'] = game_year
            log_message(f" Кандидат: '{candidate['link']['text']}' (схожесть: {candidate['score']:.3f}, год: {game_year})")
            
            if i < len(top_candidates) - 1:  # Не делаем паузу после последнего кандидата
                time.sleep(random.uniform(2, 3))
        
        if original_year is not None:
            top_candidates.sort(key=lambda x: abs(x.get('year', 9999) - original_year) if x.get('year') is not None else 9999)
        
        candidates_with_years = top_candidates + all_candidates[len(top_candidates):]
        
        candidates_with_years.sort(key=lambda x: (
            -x['score'],
            abs(x['year'] - original_year) if x['year'] is not None else 999
        ))
        
        
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.8 and candidate['year'] == original_year:
                log_message(f" -ПРИОРИТЕТ 1: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        early_year_candidates = [c for c in candidates_with_years if c['score'] >= 0.8 and c['year'] is not None and c['year'] < original_year]
        if early_year_candidates:
            early_year_candidates.sort(key=lambda x: x['year'])
            best_early = early_year_candidates[0]
            log_message(f" -ПРИОРИТЕТ 2: {best_early['link']['text']} (схожесть: {best_early['score']:.3f}, год: {best_early['year']})")
            return {
                'title': best_early['title'],
                'score': best_early['score'],
                'selected_link': best_early['link']
            }
        
        best_score_without_year = max([c['score'] for c in candidates_with_years if c['year'] is None], default=0)
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.8 and candidate['year'] is not None and candidate['score'] >= best_score_without_year:
                log_message(f" -ПРИОРИТЕТ 3: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        for candidate in candidates_with_years:
            if candidate['score'] >= 0.6 and candidate['year'] == original_year:
                log_message(f" -ПРИОРИТЕТ 4: {candidate['link']['text']} (схожесть: {candidate['score']:.3f}, год: {candidate['year']})")
                return {
                    'title': candidate['title'],
                    'score': candidate['score'],
                    'selected_link': candidate['link']
                }
        
        best_candidate = candidates_with_years[0] if candidates_with_years else None
        if best_candidate:
            log_message(f" -Лучший по схожести: {best_candidate['link']['text']} (схожесть: {best_candidate['score']:.3f}, год: {best_candidate['year']})")
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
    try:
        if not hasattr(extract_year_from_game_page, 'url_cache'):
            extract_year_from_game_page.url_cache = {}
        
        if not hasattr(extract_year_from_game_page, 'quick_cache'):
            extract_year_from_game_page.quick_cache = {}
        
        full_url = f"{BASE_URL}{link['href']}"
        
        if full_url in extract_year_from_game_page.quick_cache:
            return extract_year_from_game_page.quick_cache[full_url]
        
        if full_url in extract_year_from_game_page.url_cache:
            cached_year = extract_year_from_game_page.url_cache[full_url]
            extract_year_from_game_page.quick_cache[full_url] = cached_year
            return cached_year
        
        max_attempts = 3
        timeouts = [10000, 12000, 15000]
        
        for attempt in range(max_attempts):
            try:
                if attempt > 0:
                    log_message(f" Повторная попытка {attempt + 1}/{max_attempts} извлечения года для '{link['text']}'...")
                
                random_delay(1, 3)
                
                page.goto(full_url, wait_until="domcontentloaded", timeout=timeouts[attempt])
                
                page.wait_for_selector('table', timeout=8000)
                
                year = extract_release_year_from_page(page)
                
                extract_year_from_game_page.url_cache[full_url] = year
                extract_year_from_game_page.quick_cache[full_url] = year
                
                if attempt > 0:
                    log_message(f"✅ Успешно извлечен год для '{link['text']}' с попытки {attempt + 1}")
                
                return year
                
            except Exception as e:
                error_msg = str(e)
                if "Timeout" in error_msg:
                    log_message(f" Таймаут при извлечении года для '{link['text']}' (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        log_message(f"❌ Все {max_attempts} попыток исчерпаны для извлечения года '{link['text']}' - таймауты")
                        return None
                else:
                    log_message(f"❌ Ошибка извлечения года для '{link['text']}' (попытка {attempt + 1}): {e}")
                    if attempt == max_attempts - 1:
                        return None
        
        return None
        
    except Exception as e:
        log_message(f"❌ Критическая ошибка извлечения года для {link['text']}: {e}")
        return None

def search_game_single_attempt(page, game_title):
    try:
        log_message(f" -Ищем: '{game_title}'")
        
        safe_title = quote(game_title, safe="")
        if "%25" in safe_title:
            safe_title = safe_title.replace("%25", "%")
        search_url = f"{BASE_URL}/?q={safe_title}"
        
        page.goto(search_url, timeout=10000)
        page.wait_for_load_state("domcontentloaded", timeout=10000)
        
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP при поиске!")
            progressive_delay_for_blocking()
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message(" Cloudflare проверка при поиске - ждем...")
            time.sleep(10)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует поиск")
                return None
        
        random_delay(5, 8)
        
        game_links = page.locator('a[href^="/game/"]')
        found_count = game_links.count()
        
        if found_count == 0:
            random_delay(2, 4)
            found_count = game_links.count()
        
        if found_count > 10:
            random_delay(5, 8)
            found_count = game_links.count()
        
        if found_count == 0:
            return None
        
        best_match, best_title, similarity = find_best_match(page, game_links, game_title)
        if not best_match:
            return None
        
        best_url = best_match.get_attribute("href")
        
        log_message(f" Выбрано: '{best_title}' (схожесть: {similarity:.2f})")
        
        if similarity < 0.6:
            log_message(f"  Низкая схожесть ({similarity:.2f}), пробуем альтернативное название")
            return None
        
        full_url = f"{BASE_URL}{best_url}"
        
        page.goto(full_url, timeout=15000)
        page.wait_for_load_state("domcontentloaded", timeout=15000)
        
        page_content = page.content()
        if "blocked" in page_content.lower() or "access denied" in page_content.lower():
            log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP на странице игры!")
            progressive_delay_for_blocking()
            return None
        elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
            log_message(" Cloudflare проверка на странице игры - ждем...")
            time.sleep(10)
            page_content = page.content()
            if "checking your browser" in page_content.lower():
                log_message("❌ Cloudflare блокирует страницу игры")
                return None
        
        random_delay(5, 8)
        
        hltb_data = extract_hltb_data_from_page(page)
        return hltb_data
        
    except Exception as e:
        log_message(f"❌ Ошибка поиска игры '{game_title}': {e}")
        return None

def find_best_match(page, game_links, original_title):
    try:
        best_match = None
        best_score = 0
        best_title = ""
        
        original_clean = clean_title_for_comparison(original_title)
        
        for i in range(min(game_links.count(), 10)):  # Проверяем первые 10 результатов
            link = game_links.nth(i)
            link_text = link.inner_text().strip()
            
            if link_text:
                found_clean = clean_title_for_comparison(link_text)
                
                score = calculate_title_similarity(original_clean, found_clean)
                
                if score > best_score:
                    best_score = score
                    best_match = link
                    best_title = link_text
                
                if score >= 0.9:
                    break
        
        if best_score >= 0.3:
            return best_match, best_title, best_score
        else:
            return None, "", 0
        
    except Exception as e:
        log_message(f"❌ Ошибка выбора лучшего совпадения: {e}")
        return game_links.first if game_links.count() > 0 else None, "", 0

def clean_title_for_comparison(title):
    import re
    cleaned = re.sub(r'[^\w\s]', '', title.lower())
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    return cleaned

def extract_primary_title(game_title):
    if not game_title:
        return game_title
    
    if "/" in game_title:
        parts = [part.strip() for part in game_title.split("/")]
        
        if all(" " not in part for part in parts):
            primary = f"{parts[0]} and {parts[1]}"
            return primary
        else:
            primary = parts[0]
            return primary
    
    return game_title

def extract_alternative_title(game_title)
    if not game_title or "/" not in game_title:
        return None
    
    parts = [part.strip() for part in game_title.split("/")]
    
    if len(parts) >= 2 and all(" " not in part for part in parts):
        alternative = parts[1]
        return alternative
    
    return None

def convert_arabic_to_roman(num_str):
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
    alternatives = []
    
    if " / " in game_title:
        parts = [part.strip() for part in game_title.split(" / ")]
        
        for part in parts:
            if part and part not in alternatives:
                alternatives.append(part)
        
        for part in parts:
            roman_variants = generate_roman_variants(part)
            for variant in roman_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
        
        for part in parts:
            ampersand_variants = generate_ampersand_variants(part)
            for variant in ampersand_variants:
                if variant not in alternatives:
                    alternatives.append(variant)
        
        for part in parts:
            no_parens = remove_parentheses(part)
            if no_parens and no_parens not in alternatives:
                alternatives.append(no_parens)
        
        for part in parts:
            if ":" in part:
                simplified = part.split(":")[0].strip()
                if simplified and simplified not in alternatives:
                    alternatives.append(simplified)
        
        for part in parts:
            if "(" in part and ")" in part:
                no_subtitle = part.split("(")[0].strip()
                if no_subtitle and no_subtitle not in alternatives:
                    alternatives.append(no_subtitle)
                
    elif "/" in game_title and " / " not in game_title:
        parts = [part.strip() for part in game_title.split("/")]
        
        alternatives.append(game_title)
        
        base = determine_base_part_new(parts)
        
        if base:
            
            if len(parts) > 2:
                non_base_parts = []
                for p in parts:
                    if p != base:
                        clean_part = p.replace(base, "").strip()
                        if clean_part:
                            non_base_parts.append(clean_part)
                
                if len(non_base_parts) > 2:
                    all_parts_title = f"{base} {' and '.join(non_base_parts)}"
                    if all_parts_title not in alternatives:
                        alternatives.append(all_parts_title)
            
            for i in range(len(parts)):
                for j in range(i + 1, len(parts)):
                    if parts[i] != base and parts[j] != base:
                        part1 = parts[i].replace(base, "").strip()
                        part2 = parts[j].replace(base, "").strip()
                        if part1 and part2:
                            pair_title = f"{base} {part1} and {part2}"
                            if pair_title not in alternatives:
                                alternatives.append(pair_title)
            
            for part in parts:
                if part and part != base:
                    if part.startswith(base):
                        if part not in alternatives:
                            alternatives.append(part)
                    else:
                        if not part.startswith(base + " "):
                            full_title = f"{base} {part}"
                            if full_title not in alternatives:
                                alternatives.append(full_title)
        else:
            for part in parts:
                if part and part not in alternatives:
                    alternatives.append(part)
    else:
        
        alternatives.append(game_title)
        
        roman_variants = generate_roman_variants(game_title)
        alternatives.extend(roman_variants)
        
        ampersand_variants = generate_ampersand_variants(game_title)
        alternatives.extend(ampersand_variants)
        
        no_parens = remove_parentheses(game_title)
        if no_parens and no_parens not in alternatives:
            alternatives.append(no_parens)
        
        if ":" in game_title:
            simplified = game_title.split(":")[0].strip()
            if simplified and simplified not in alternatives:
                alternatives.append(simplified)
        
        if "(" in game_title and ")" in game_title:
            no_subtitle = game_title.split("(")[0].strip()
            if no_subtitle and no_subtitle not in alternatives:
                alternatives.append(no_subtitle)
        
        abbreviation_variants = generate_abbreviation_variants(game_title)
        for variant in abbreviation_variants:
            if variant and variant not in alternatives:
                alternatives.append(variant)
    
    unique_alternatives = []
    for alt in alternatives:
        if alt and alt not in unique_alternatives:
            unique_alternatives.append(alt)
    
    return unique_alternatives

def generate_abbreviation_variants(title):
    variants = []
    import re
    
    abbreviation_pattern = r'^([A-Z](?:\.[A-Z]\.?)*)\s+(.+)$'
    match = re.match(abbreviation_pattern, title)
    
    if match:
        abbreviation = match.group(1)
        expansion = match.group(2)
        
        variants.append(abbreviation)
        
        no_dots = abbreviation.replace('.', '')
        if no_dots != abbreviation:
            variants.append(no_dots)
        
        with_spaces = abbreviation.replace('.', ' ')
        if with_spaces != abbreviation:
            variants.append(with_spaces)
        
        first_word = expansion.split()[0] if expansion.split() else ""
        if first_word and first_word != abbreviation:
            variants.append(first_word)
    
    dash_pattern = r'^([A-Z](?:\.[A-Z]\.?)*)\s*-\s*(.+)$'
    match = re.match(dash_pattern, title)
    
    if match:
        abbreviation = match.group(1)
        expansion = match.group(2)
        
        variants.append(abbreviation)
        
        no_dots = abbreviation.replace('.', '')
        if no_dots != abbreviation:
            variants.append(no_dots)
    
    return variants

def generate_roman_variants(title):
    variants = []
    import re
    
    arabic_pattern = r'(\b\d+\b)'
    matches = re.findall(arabic_pattern, title)
    
    for match in matches:
        roman = convert_arabic_to_roman(match)
        if roman != match:
            alt_title = re.sub(r'\b' + match + r'\b', roman, title)
            if alt_title not in variants:
                variants.append(alt_title)
    
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
    variants = []
    
    if "&" in title:
        and_variant = title.replace("&", "and")
        if and_variant not in variants:
            variants.append(and_variant)
    
    import re
    if "(&" in title:
        no_parens_amp = re.sub(r'\(\s*&\s*([^)]+)\)', r'& \1', title)
        if no_parens_amp and no_parens_amp != title and no_parens_amp not in variants:
            variants.append(no_parens_amp)
    
    if "&" in title:
        no_ampersand = re.sub(r'\s*\(?&\s*[^)]+\)?', '', title).strip()
        if no_ampersand and no_ampersand != title and no_ampersand not in variants:
            variants.append(no_ampersand)
        
        no_amp_only = re.sub(r'\s*&\s*', ' ', title).strip()
        if no_amp_only and no_amp_only != title and no_amp_only not in variants:
            variants.append(no_amp_only)
    
    simplified = simplify_title(title)
    if simplified and simplified != title and simplified not in variants:
        variants.append(simplified)
    
    return variants

def remove_parentheses(title):
    import re
    no_parens = re.sub(r'\([^)]*\)', '', title).strip()
    no_parens = re.sub(r'\s+', ' ', no_parens).strip()
    return no_parens if no_parens != title else None

def determine_base_part_new(parts):
    if not parts or len(parts) < 2:
        return None
    
    first_part = parts[0]
    if " " not in first_part:
        return None
    
    words = first_part.split()
    if len(words) < 2:
        return None
    
    return words[0]

def simplify_title(title):
    import re
    simplified = re.sub(r'\b(the|of|and)\b', '', title, flags=re.IGNORECASE)
    simplified = re.sub(r'\s+', ' ', simplified).strip()
    return simplified if simplified != title else None

def convert_roman_to_arabic(roman):
    roman_to_arabic = {
        'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
        'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
    }
    return roman_to_arabic.get(roman, roman)

def jaro_distance(s1, s2):
    if s1 == s2:
        return 1.0
    len1, len2 = len(s1), len(s2)
    if len1 == 0 or len2 == 0:
        return 0.0
    max_dist = (max(len1, len2) // 2) - 1
    match = 0
    hash_s1 = [0] * len1
    hash_s2 = [0] * len2
    for i in range(len1):
        for j in range(max(0, i - max_dist), min(len2, i + max_dist + 1)):
            if s1[i] == s2[j] and hash_s2[j] == 0:
                hash_s1[i] = 1
                hash_s2[j] = 1
                match += 1
                break
    if match == 0:
        return 0.0
    t = 0
    point = 0
    for i in range(len1):
        if hash_s1[i]:
            while hash_s2[point] == 0:
                point += 1
            if s1[i] != s2[point]:
                t += 1
            point += 1
    t //= 2
    return (match / len1 + match / len2 + (match - t) / match) / 3.0

def jaro_winkler_similarity(s1, s2):
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
    try:
        normalized1 = normalize_title_for_comparison(title1)
        normalized2 = normalize_title_for_comparison(title2)
        
        similarity = jaro_winkler_similarity(normalized1.lower(), normalized2.lower())
        
        if normalized1.lower() == normalized2.lower():
            similarity = 1.0
        
        len1, len2 = len(normalized1), len(normalized2)
        if len1 > 0 and len2 > 0:
            length_ratio = min(len1, len2) / max(len1, len2)
            if length_ratio < 0.7:  # Если разница в длине больше 30%
                similarity *= length_ratio
        
        if year1 is not None and year2 is not None:
            year_diff = abs(year1 - year2)
            if year_diff == 0:
                year_penalty = 0
            elif year_diff <= 1:
                year_penalty = 0.01
            elif year_diff <= 2:
                year_penalty = 0.05
            elif year_diff <= 5:
                year_penalty = 0.1
            else:
                year_penalty = 0.2
            
            similarity -= year_penalty
        
        return max(0.0, similarity)
        
    except Exception as e:
        log_message(f"❌ Ошибка вычисления схожести: {e}")
        return 0.0
        
def extract_release_year_from_page(page):
    import re
    try:
        if not hasattr(extract_release_year_from_page, 'year_cache'):
            extract_release_year_from_page.year_cache = {}
        
        if not hasattr(extract_release_year_from_page, 'quick_cache'):
            extract_release_year_from_page.quick_cache = {}
        
        page_url = page.url
        
        if page_url in extract_release_year_from_page.quick_cache:
            return extract_release_year_from_page.quick_cache[page_url]
        
        if page_url in extract_release_year_from_page.year_cache:
            year = extract_release_year_from_page.year_cache[page_url]
            extract_release_year_from_page.quick_cache[page_url] = year
            return year
        
        try:
            json_script = page.locator('script
            if json_script.count() > 0:
                json_text = json_script.text_content()
                import json
                data = json.loads(json_text)
                
                games = data.get('props', {}).get('pageProps', {}).get('game', {}).get('data', {}).get('game', [])
                if games:
                    game_data = games[0]
                    
                    year_fields = ['game_name_date', 'release_date', 'date', 'year']
                    for field in year_fields:
                        if field in game_data and game_data[field]:
                            year = game_data[field]
                            if isinstance(year, (int, str)) and str(year).isdigit():
                                year_int = int(year)
                                if 1950 <= year_int <= 2030:  # Разумный диапазон годов
                                    extract_release_year_from_page.year_cache[page_url] = year_int
                                    extract_release_year_from_page.quick_cache[page_url] = year_int
                                    return year_int
        except Exception as e:
            log_message(f" Ошибка извлечения года из JSON: {e}")
        
        try:
            page_text = page.content()
            
            date_pattern = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2}(?:st|nd|rd|th)?,\s+(\d{4})'
            matches = re.findall(date_pattern, page_text, re.IGNORECASE)
            if matches:
                years = [int(year) for year in matches if 1950 <= int(year) <= 2030]
                if years:
                    earliest_year = min(years)
                    extract_release_year_from_page.year_cache[page_url] = earliest_year
                    extract_release_year_from_page.quick_cache[page_url] = earliest_year
                    return earliest_year
            
            year_pattern = r'\b(19\d{2}|20\d{2})\b'
            matches = re.findall(year_pattern, page_text)
            if matches:
                years = [int(match) for match in matches if 1950 <= int(match) <= 2030]
                if years:
                    earliest_year = min(years)
                    extract_release_year_from_page.year_cache[page_url] = earliest_year
                    extract_release_year_from_page.quick_cache[page_url] = earliest_year
                    return earliest_year
        except Exception as e:
            log_message(f"⚠️ Ошибка извлечения года из JSON: {e}")
        
        try:
            years = []
            
            year_selectors = [
                'span[data-testid="release-date"]',
                '.release-date',
                '.game-release-date',
                'time[datetime]',
                '[data-testid="game-release-year"]'
            ]
            
            for selector in year_selectors:
                elements = page.locator(selector).all()
                for element in elements:
                    text = element.text_content()
                    if text:
                        year_match = re.search(r'\b(19|20)\d{2}\b', text)
                        if year_match:
                            year = int(year_match.group())
                            if 1950 <= year <= 2030:
                                years.append(year)
            
            if years:
                earliest_year = min(years)
                extract_release_year_from_page.year_cache[page_url] = earliest_year
                return earliest_year
        except Exception as e:
            log_message(f" Ошибка извлечения года из HTML: {e}")
        
        extract_release_year_from_page.year_cache[page_url] = None
        extract_release_year_from_page.quick_cache[page_url] = None
        return None
        
    except Exception as e:
        log_message(f" Ошибка извлечения года релиза: {e}")
        return None

def normalize_title_for_comparison(title):
    try:
        import re
        
        normalized = re.sub(r'\([^)]*\)', '', title).strip()
        
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        
        roman_to_arabic = {
            'I': '1', 'II': '2', 'III': '3', 'IV': '4', 'V': '5',
            'VI': '6', 'VII': '7', 'VIII': '8', 'IX': '9', 'X': '10'
        }
        
        for roman, arabic in roman_to_arabic.items():
            pattern = r'\b' + roman + r'\b'
            normalized = re.sub(pattern, arabic, normalized)
        
        return normalized
        
    except Exception as e:
        log_message(f" Ошибка нормализации названия: {e}")
        return title

def extract_hltb_data_from_page(page):
    try:
        hltb_data = {}
        
        top_block_data = extract_top_block_data(page)
        if top_block_data:
            hltb_data.update(top_block_data)
        
        table_data = extract_table_data(page)
        if table_data:
            hltb_data.update(table_data)
        
        if top_block_data and not table_data:
            pass
        elif top_block_data and table_data:
            has_single_player_data = any(key in table_data for key in ["ms", "mpe", "comp"])
            if not has_single_player_data:
                log_message(" В таблицах нет single player данных, используем только верхние блоки")
                hltb_data = top_block_data.copy()
        
        store_links = extract_store_links(page)
        if store_links:
            hltb_data["stores"] = store_links
        
        import re
        url_match = re.search(r'/game/(\d+)', page.url)
        if url_match:
            hltb_data["hltb_id"] = url_match.group(1)
        
        return hltb_data if hltb_data else None
        
    except Exception as e:
        log_message(f" Ошибка извлечения данных со страницы: {e}")
        return None

def extract_top_block_data(page):
    try:
        top_data = {}
        
        game_stats = page.locator('.GameStats_game_times__KHrRY')
        if game_stats.count() == 0:
            log_message(" Блок GameStats не найден")
            return None
        
        stats_items = game_stats.locator('li')
        item_count = stats_items.count()
        
        
        for i in range(item_count):
            try:
                item = stats_items.nth(i)
                
                category_element = item.locator('h4')
                time_element = item.locator('h5')
                
                if category_element.count() > 0 and time_element.count() > 0:
                    category = category_element.inner_text().strip()
                    time_text = time_element.inner_text().strip()
                    
                    
                    if time_text == "--" or not time_text:
                        continue
                    
                    if category == "Co-Op":
                        coop_data = extract_time_from_h5(time_text)
                        if coop_data and "coop" not in top_data:
                            top_data["coop"] = coop_data
                    elif category == "Vs.":
                        vs_data = extract_time_from_h5(time_text)
                        if vs_data and "vs" not in top_data:
                            top_data["vs"] = vs_data
                    elif category in ["Single-Player", "Single Player"]:
                        single_data = extract_time_from_h5(time_text)
                        if single_data and "ms" not in top_data:
                            top_data["ms"] = single_data
                            
            except Exception as e:
                log_message(f" Ошибка обработки элемента статистики {i}: {e}")
                continue
        
        return top_data if top_data else None
        
    except Exception as e:
        log_message(f" Ошибка извлечения данных из верхних блоков: {e}")
        return None

def extract_time_from_h5(time_text):
    try:
        import re
        
        
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*Hours?', time_text)
        if time_match:
            hours = float(time_match.group(1))
            
            if hours >= 1:
                if hours == int(hours):
                    formatted_time = f"{int(hours)}h"
                else:
                    formatted_time = f"{hours:.1f}h"
            else:
                formatted_time = f"{int(hours * 60)}m"
            
            return {"t": formatted_time}
        
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:Minutes?|Mins?|m)\b', time_text)
        if time_match:
            minutes = float(time_match.group(1))
            formatted_time = f"{int(minutes)}m"
            
            return {"t": formatted_time}
        
        time_match = re.search(r'(\d+(?:\.\d+)?)\s*h\b', time_text)
        if time_match:
            hours = float(time_match.group(1))
            if hours == int(hours):
                formatted_time = f"{int(hours)}h"
            else:
                formatted_time = f"{hours:.1f}h"
            
            return {"t": formatted_time}
        
        log_message(" Время не найдено")
        return None
        
    except Exception as e:
        log_message(f" Ошибка извлечения времени: {e}")
        return None

def extract_table_data(page):
    try:
        table_data = {}
        
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
                            log_message(f" Ошибка обработки строки {row_idx}: {e}")
                            continue
                            
            except Exception as e:
                log_message(f" Ошибка обработки таблицы {table_idx}: {e}")
                continue
        
        return table_data if table_data else None
        
    except Exception as e:
        log_message(f"❌ Ошибка извлечения данных из таблиц: {e}")
        return None

def extract_store_links(page):
    try:
        store_links = {}
        
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
                        if store_name in ["steam", "epic", "gog", "humble", "itch", "origin", "uplay", "battlenet", "psn", "xbox", "nintendo"]:
                            price_element = link_element.locator('.StoreButton_price__agxuh')
                            if price_element.count() > 0:
                                price_text = price_element.inner_text().strip()
                                if price_text == "N/A":
                                    pass
                                    continue
                        
                        if store_name == "gog" and "adtraction.com" in href:
                            import re
                            match = re.search(r'url=([^&]+)', href)
                            if match:
                                href = match.group(1)
                                from urllib.parse import unquote
                                href = unquote(href)
                        
                        if store_name == "gog":
                            if not is_valid_gog_link(page, href):
                                continue
                        
                        store_links[store_name] = href
            except:
                continue
        
        return store_links if store_links else None
        
    except Exception as e:
        log_message(f" Ошибка извлечения ссылок на магазины: {e}")
        return None

def extract_hltb_row_data(row_text):
    try:
        import re
        
        polled_match = re.search(r'^[A-Za-z\s/\+]+\s+(\d+(?:\.\d+)?[Kk]?)\s+', row_text)
        if not polled_match:
            polled_match = re.search(r'^[A-Za-z\s/\+]+\t+(\d+(?:\.\d+)?[Kk]?)\t+', row_text)
        if not polled_match:
            polled_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s+(?:\d+h|\d+\s*Hours?)', row_text)
        
        polled = None
        if polled_match:
            polled_str = polled_match.group(1)
            if 'K' in polled_str.upper():
                number = float(polled_str.replace('K', '').replace('k', ''))
                polled = int(number * 1000)
            else:
                polled = int(float(polled_str))
        
        times = []
        
        time_part = re.sub(r'^[A-Za-z\s/\+]+\s+\d+(?:\.\d+)?[Kk]?\s+', '', row_text)
        if time_part == row_text:  # Если не сработало, пробуем с табами
            time_part = re.sub(r'^[A-Za-z\s/\+]+\t+\d+(?:\.\d+)?[Kk]?\t+', '', row_text)
        
        
        
        combined_pattern = r'(\d+h\s*\d+m|\d+(?:\.\d+)?[½]?\s*Hours?|\d+h|\d+m)'
        
        matches = re.findall(combined_pattern, time_part)
        for match in matches:
            clean_match = re.sub(r'\s+', ' ', match.strip())
            times.append(clean_match)
        
        if len(times) < 1:
            return None
        
        is_single_player = any(keyword in row_text for keyword in ["Main Story", "Main + Extras", "Completionist"])
        is_multi_player = any(keyword in row_text for keyword in ["Co-Op", "Competitive"])
        
        result = {}
        
        average_time = times[0] if len(times) > 0 else None
        median_time = times[1] if len(times) > 1 else None
        
        final_time = calculate_average_time(average_time, median_time)
        result["t"] = round_time(final_time) if final_time else None
        
        if polled:
            result["p"] = polled
        
        if is_single_player and len(times) >= 4:
            result["r"] = round_time(times[2])
            result["l"] = round_time(times[3])
            
        elif is_multi_player and len(times) >= 4:
            result["min"] = round_time(times[2])
            result["max"] = round_time(times[3])
            
        return result
        
    except Exception as e:
        log_message(f" Ошибка извлечения данных из строки: {e}")
        return None

def calculate_average_time(time1_str, time2_str):
    try:
        def parse_time_to_minutes(time_str):
            if not time_str:
                return 0
            
            time_str = time_str.replace("Hours", "").strip()
            
            total_minutes = 0
            
            if "h" in time_str and "m" in time_str:
                import re
                hours_match = re.search(r'(\d+)h', time_str)
                minutes_match = re.search(r'(\d+)m', time_str)
                
                if hours_match:
                    total_minutes += int(hours_match.group(1)) * 60
                if minutes_match:
                    total_minutes += int(minutes_match.group(1))
                    
            elif "h" in time_str:
                import re
                hours_match = re.search(r'(\d+)h', time_str)
                if hours_match:
                    total_minutes = int(hours_match.group(1)) * 60
                    
            elif time_str.replace(".", "").isdigit():
                total_minutes = float(time_str) * 60
                
            return total_minutes
        
        minutes1 = parse_time_to_minutes(time1_str)
        minutes2 = parse_time_to_minutes(time2_str)
        
        if minutes1 == 0 and minutes2 == 0:
            return round_time(time1_str or time2_str) if (time1_str or time2_str) else None
        
        if minutes2 == 0:
            return round_time(time1_str) if time1_str else None
        
        avg_minutes = (minutes1 + minutes2) / 2
        
        hours = avg_minutes / 60
        
        if hours >= 1:
            if hours == int(hours):
                return f"{int(hours)}h"
            else:
                return f"{hours:.1f}h"
        else:
            return f"{int(avg_minutes)}m"
            
    except Exception as e:
        log_message(f" Ошибка вычисления среднего времени: {e}")
        return time1_str or time2_str


def determine_error_type(page, game_title):
    try:
        page_content = page.content().lower()
        
        if "blocked" in page_content or "access denied" in page_content:
            return "IP блокировка"
        
        if "cloudflare" in page_content and "checking your browser" in page_content:
            return "Cloudflare блокировка"
        
        if "timeout" in page_content or "timed out" in page_content:
            return "Таймаут запроса"
        
        if "network error" in page_content or "connection error" in page_content:
            return "Ошибка сети"
        
        search_results = page.locator('a[href^="/game/"]')
        if search_results.count() == 0:
            return "Игра не найдена в поиске"
        
        tables = page.locator("table")
        if tables.count() == 0:
            return "Нет таблиц с данными на странице"
        
        return "Неизвестная ошибка"
        
    except Exception as e:
        log_message(f" Ошибка определения типа ошибки: {e}")
        return "Ошибка анализа страницы"

def extract_time_and_polled_from_row(row_text):
    try:
        time_match = re.search(r'(\d+h\s*\d*m)', row_text)
        if time_match:
            time_str = time_match.group(1)
            rounded_time = round_time(time_str)
            
            polled_count = None
            
            polled_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)\s*(?:Polled|polled)', row_text, re.IGNORECASE)
            if polled_match:
                polled_str = polled_match.group(1)
                polled_count = parse_polled_number(polled_str)
            
            if not polled_count:
                first_number_match = re.search(r'^(\d+(?:\.\d+)?[Kk]?)', row_text.strip())
                if first_number_match:
                    polled_str = first_number_match.group(1)
                    polled_count = parse_polled_number(polled_str)
            
            if not polled_count:
                any_number_match = re.search(r'(\d+(?:\.\d+)?[Kk]?)', row_text)
                if any_number_match:
                    polled_str = any_number_match.group(1)
                    polled_count = parse_polled_number(polled_str)
            
            result = {"t": rounded_time}
            if polled_count:
                result["p"] = polled_count
            
            return result
        return None
    except Exception as e:
        log_message(f" Ошибка извлечения данных из строки: {e}")
        return None

def parse_polled_number(polled_str):
    try:
        if 'K' in polled_str.upper():
            return int(float(polled_str.upper().replace('K', '')) * 1000)
        else:
            return int(float(polled_str))
    except:
        return None

def extract_time_from_row(row_text):
    try:
        time_match = re.search(r'(\d+h\s*\d*m)', row_text)
        if time_match:
            time_str = time_match.group(1)
            return round_time(time_str)
        return None
    except:
        return None


def save_results(games_data):
    try:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            for i, game in enumerate(games_data):
                if i > 0:
                    f.write("\n")
                json.dump(game, f, separators=(',', ':'), ensure_ascii=False)
        
        categories, total_polled, na_count = count_hltb_data(games_data)
        successful = len([g for g in games_data if "hltb" in g])
        
        log_message(f" Результаты сохранены в {OUTPUT_FILE}")
        log_message(f" Статистика: {successful}/{len(games_data)} игр с данными HLTB")
        log_message(f" Main Story: {categories['ms']} ({total_polled['ms']} голосов), Main+Extras: {categories['mpe']} ({total_polled['mpe']} голосов)")
        log_message(f" Completionist: {categories['comp']} ({total_polled['comp']} голосов)")
        log_message(f" Co-Op: {categories['coop']} ({total_polled['coop']} голосов), Vs: {categories['vs']} ({total_polled['vs']} голосов)")
        log_message(f" N/A (не найдено): {na_count} игр")
        
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
        
        formatted_games = []
        for game in hltb_data:
            game_json = json.dumps(game, separators=(',', ':'), ensure_ascii=False)
            formatted_games.append(f'    {game_json}')
        
        new_games_list = '[\n' + ',\n'.join(formatted_games) + '\n]'
        new_content = content[:start] + f'const gamesList = {new_games_list}' + content[end:]
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(new_content)
        
        log_message(f"[OK] HTML файл обновлен: {html_file}")
        return True
        
    except Exception as e:
        log_message(f"[ERROR] Ошибка обновления HTML: {e}")
        return False

def main():
    print("🔧 Функция main() вызвана")
    log_message(" Запуск HLTB Worker")
    log_message(f" Рабочая директория: {os.getcwd()}")
    log_message(f" Ищем файл: {GAMES_LIST_FILE}")
    log_message(f" Настройки чанков: CHUNK_INDEX={CHUNK_INDEX}, CHUNK_SIZE={CHUNK_SIZE}")
    
    if not os.path.exists(GAMES_LIST_FILE):
        log_message(f"❌ Файл {GAMES_LIST_FILE} не найден!")
        return
    
    log_message(f" Файл {GAMES_LIST_FILE} найден, размер: {os.path.getsize(GAMES_LIST_FILE)} байт")
    
    setup_directories()
    log_message(" Директории настроены")
    
    try:
        log_message(" Начинаем извлечение списка игр...")
        all_games = extract_games_list(GAMES_LIST_FILE)
        total_games = len(all_games)
        log_message(f"✅ Извлечено {total_games} игр")
        
        games_list, start_index, end_index = get_chunk_games(all_games)
        chunk_games_count = len(games_list)
        
        if chunk_games_count == 0:
            log_message("⚠️ Нет игр для обработки в этом чанке")
            return
        
        log_message(f" Обрабатываем чанк {CHUNK_INDEX}: {chunk_games_count} игр")
        
        log_message(" Запускаем Playwright...")
        with sync_playwright() as p:
            log_message(" Запускаем Chromium...")
            browser = p.chromium.launch(headless=True)
            log_message(" Chromium запущен")
            
            log_message(" Создаем контекст браузера...")
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                locale="en-US"
            )
            log_message(" Контекст создан")
            
            log_message(" Создаем новую страницу...")
            page = context.new_page()
            log_message(" Страница создана")
            
            log_message(" Проверяем доступность HowLongToBeat.com...")
            try:
                page.goto(BASE_URL, timeout=15000)
                page.wait_for_load_state("domcontentloaded", timeout=15000)
                
                title = page.title()
                log_message(f" Заголовок страницы: {title}")
                
                search_box = page.locator('input[type="search"], input[name="q"]')
                if search_box.count() > 0:
                    log_message(" Поисковая строка найдена - сайт доступен")
                else:
                    log_message(" Поисковая строка не найдена - возможны проблемы")
                
                page_content = page.content()
                if "blocked" in page_content.lower() or "access denied" in page_content.lower():
                    log_message("❌ ОБНАРУЖЕНА БЛОКИРОВКА IP! Сайт заблокировал доступ")
                    progressive_delay_for_blocking()
                    return
                elif "cloudflare" in page_content.lower() and "checking your browser" in page_content.lower():
                    log_message(" Cloudflare проверка браузера - ждем...")
                    time.sleep(10)
                    page_content = page.content()
                    if "checking your browser" in page_content.lower():
                        log_message("❌ Cloudflare блокирует доступ")
                        return
                
                log_message(" Сайт доступен, начинаем обработку игр")
                
            except Exception as e:
                log_message(f" Ошибка проверки доступности сайта: {e}")
                log_message(" Продолжаем работу, но возможны проблемы...")
            
            start_time = time.time()
            processed_count = 0
            direct_id_count = 0
            search_count = 0
            
            for i in range(0, chunk_games_count):
                game = games_list[i]
                game_title = game["title"]
                game_year = game.get("year")
                
                global_game_number = start_index + i + 1
                log_message(f"🎮🎮🎮 Обрабатываю {global_game_number}/{total_games}: {game_title} ({game_year})")
                
                existing_hltb = game.get("hltb")
                hltb_id = None
                
                if existing_hltb and isinstance(existing_hltb, dict):
                    hltb_id = existing_hltb.get("hltb_id")
                
                if hltb_id:
                    log_message(f" -Найден существующий HLTB ID {hltb_id}, обновляем актуальные данные...")
                    hltb_data = extract_data_by_hltb_id(page, hltb_id)
                    
                    if hltb_data:
                        game["hltb"] = hltb_data
                        processed_count += 1
                        direct_id_count += 1
                        log_message(f"✅✅✅ Данные обновлены по ID: {hltb_data}")
                    else:
                        log_message(f"⚠️ Не удалось извлечь по ID {hltb_id}, пробуем обычный поиск...")
                        hltb_data = retry_game_with_blocking_handling(page, game_title, game_year, max_retries=5)
                        
                        if hltb_data:
                            game["hltb"] = hltb_data
                            processed_count += 1
                            search_count += 1
                            log_message(f"✅✅✅ Данные обновлены через поиск: {hltb_data}")
                        else:
                            error_type = determine_error_type(page, game_title)
                            
                            game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A", "hltb_id": hltb_id}
                            log_message(f"⚠️  {error_type} для: {game_title} - записано N/A (сохранен ID {hltb_id})")
                else:
                    log_message(f" -Ищем новые HLTB данные для '{game_title}'...")
                    hltb_data = retry_game_with_blocking_handling(page, game_title, game_year, max_retries=5)
                    
                    if hltb_data:
                        game["hltb"] = hltb_data
                        processed_count += 1
                        search_count += 1
                        log_message(f"✅✅✅ Найдены новые данные: {hltb_data}")
                    else:
                        error_type = determine_error_type(page, game_title)
                        
                        game["hltb"] = {"ms": "N/A", "mpe": "N/A", "comp": "N/A"}
                        log_message(f"⚠️  {error_type} для: {game_title} - записано N/A")
                
                
                
                if (i + 1) % 50 == 0:
                    log_progress(i + 1, chunk_games_count, start_time)
            
            browser.close()
        
        save_results(games_list)
        
        log_message("🔄 Обновляем HTML файл с данными HLTB...")
        
        updated_all_games = all_games.copy()
        
        updated_count = 0
        skipped_count = 0
        for i, processed_game in enumerate(games_list):
            global_index = start_index + i
            if global_index < len(updated_all_games):
                if "hltb" in processed_game:
                    old_hltb = updated_all_games[global_index].get("hltb")
                    updated_all_games[global_index]["hltb"] = processed_game["hltb"]
                    
                    if old_hltb:
                        log_message(f"🔄 Игра {global_index+1} обновлена актуальными HLTB данными")
                    else:
                        log_message(f"➕ Игра {global_index+1} получила новые HLTB данные")
                    
                    updated_count += 1
        
        log_message(f" Обновлено {updated_count} игр, пропущено {skipped_count} игр в полном списке (позиции {start_index+1}-{start_index+len(games_list)})")
        
        html_updated = update_html_with_hltb(GAMES_LIST_FILE, updated_all_games)
        if html_updated:
            log_message("✅ HTML файл успешно обновлен")
        else:
            log_message("⚠️ Не удалось обновить HTML файл")
        
        successful = len([g for g in games_list if "hltb" in g])
        log_message(f" Завершено! Обработано {successful}/{chunk_games_count} игр в чанке {CHUNK_INDEX} ({successful/chunk_games_count*100:.1f}%)")
        log_message(f" Оптимизация: {direct_id_count} игр обработано по ID, {search_count} игр через поиск")
        if direct_id_count > 0:
            optimization_percent = (direct_id_count / (direct_id_count + search_count)) * 100
            log_message(f" Экономия времени: {optimization_percent:.1f}% игр обработано без поиска")
        
    except Exception as e:
        log_message(f" Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    print(" Запускаем main()...")
    try:
        main()
        print("✅ main() завершен успешно")
    except Exception as e:
        print(f" Критическая ошибка в main(): {e}")
        import traceback
        traceback.print_exc()
        raise
