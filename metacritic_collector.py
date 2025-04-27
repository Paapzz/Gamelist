import os
import json
import time
import glob
import requests
from datetime import datetime
import re
import random
import logging
import sys
from bs4 import BeautifulSoup

# Глобальный кэш для хранения результатов поиска
search_cache = {}

GAMES_PER_FILE = 5000
METACRITIC_DATA_FILE = 'meta_data/metacritic_ratings.json'
REQUEST_DELAY = 2.0
MAX_REQUESTS_PER_RUN = 4000
LOG_FILE = 'metacritic_collector.log'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_metacritic_data():
    """Загружает существующие данные Metacritic из файла."""
    if os.path.exists(METACRITIC_DATA_FILE):
        try:
            with open(METACRITIC_DATA_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Проверяем наличие поля last_processed_index
                if 'last_processed_index' not in data:
                    data['last_processed_index'] = 0
                return data
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных Metacritic: {e}")
            return {"games": {}, "last_updated": "", "total_games": 0, "last_processed_index": 0}
    else:
        logging.info(f"Файл с данными Metacritic не найден: {METACRITIC_DATA_FILE}. Создаем новый.")
        return {"games": {}, "last_updated": "", "total_games": 0, "last_processed_index": 0}

def save_metacritic_data(data):
    """Сохраняет данные Metacritic в файл."""
    os.makedirs(os.path.dirname(METACRITIC_DATA_FILE), exist_ok=True)
    try:
        with open(METACRITIC_DATA_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logging.info(f"Данные Metacritic сохранены в {METACRITIC_DATA_FILE}")
    except Exception as e:
        logging.error(f"Ошибка при сохранении данных Metacritic: {e}")

def load_all_games():
    """Загружает все игры из файлов games_*.json."""
    all_games = []
    game_files = sorted(glob.glob('data/games_*.json'))

    if not game_files:
        logging.error("Ошибка: файлы с играми не найдены в директории data!")
        return []

    logging.info(f"Найдено {len(game_files)} файлов с играми.")

    for file_path in game_files:
        logging.info(f"Загружаем файл {file_path}...")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                games = json.load(f)
                all_games.extend(games)
        except Exception as e:
            logging.error(f"Ошибка при загрузке файла {file_path}: {e}")

    logging.info(f"Всего загружено {len(all_games)} игр.")
    return all_games

def get_metacritic_data(game_name, platform=None):
    """
    Получает данные о рейтинге игры с Metacritic.

    Args:
        game_name: Название игры
        platform: Платформа (опционально)

    Returns:
        dict: Данные о рейтинге игры или None в случае ошибки
    """
    if not isinstance(game_name, str):
        logging.error(f"Название игры должно быть строкой, получено: {type(game_name)}")
        return None

    if platform is not None and not isinstance(platform, str):
        logging.error(f"Платформа должна быть строкой, получено: {type(platform)}")
        return None

    # Используем глобальный кэш для хранения результатов поиска
    global search_cache

    # Создаем ключ для кэша
    cache_key = f"{game_name}_{platform}" if platform else game_name

    # Проверяем, есть ли результат в кэше
    if cache_key in search_cache:
        logging.info(f"Используем кэшированный результат для игры {game_name}")
        return search_cache[cache_key]

    # Metacritic изменил формат URL - теперь они не содержат платформу
    url = "https://www.metacritic.com/game/"

    # Сохраняем информацию о платформе для логирования, но не используем в URL
    platform_name = platform
    if platform:
        platform_map = {
            # PC
            "PC": "PC",
            "Windows": "PC",
            "Win": "PC",

            # PlayStation
            "PlayStation 5": "PlayStation 5",
            "PS5": "PlayStation 5",
            "PlayStation 4": "PlayStation 4",
            "PS4": "PlayStation 4",
            "PlayStation 3": "PlayStation 3",
            "PS3": "PlayStation 3",
            "PlayStation 2": "PlayStation 2",
            "PS2": "PlayStation 2",
            "PlayStation": "PlayStation",
            "PS1": "PlayStation",
            "PSX": "PlayStation",

            # Xbox
            "Xbox Series X": "Xbox Series X",
            "Xbox Series S": "Xbox Series X",
            "XSX": "Xbox Series X",
            "Xbox One": "Xbox One",
            "XONE": "Xbox One",
            "Xbox 360": "Xbox 360",
            "X360": "Xbox 360",
            "Xbox": "Xbox",

            # Nintendo
            "Nintendo Switch": "Nintendo Switch",
            "Switch": "Nintendo Switch",
            "NSW": "Nintendo Switch",
            "Wii U": "Wii U",
            "Wii": "Wii",
            "Nintendo 3DS": "Nintendo 3DS",
            "3DS": "Nintendo 3DS",
            "Nintendo DS": "Nintendo DS",
            "NDS": "Nintendo DS",
            "GameCube": "GameCube",
            "GC": "GameCube",
            "Nintendo 64": "Nintendo 64",
            "N64": "Nintendo 64",

            # Handhelds
            "Game Boy Advance": "Game Boy Advance",
            "GBA": "Game Boy Advance",
            "PlayStation Vita": "PlayStation Vita",
            "PS Vita": "PlayStation Vita",
            "PSV": "PlayStation Vita",
            "PSP": "PSP",

            # Mobile
            "iOS": "iOS",
            "iPhone": "iOS",
            "iPad": "iOS",
            "Android": "Android",

            # Other
            "Dreamcast": "Dreamcast",
            "DC": "Dreamcast",
            "Stadia": "Stadia",
            "Linux": "PC",
            "Mac": "Mac",
            "macOS": "Mac",
            "Apple Macintosh": "Mac"
        }
        platform_name = platform_map.get(platform, platform)

    # Предварительная обработка названия игры
    original_name = game_name
    game_name = game_name.lower()

    # Специальная обработка для известных префиксов
    prefixes_to_handle = {
        "marvel's": "marvels",
        "tom clancy's": "tom-clancys",
        "sid meier's": "sid-meiers"
    }

    for prefix, replacement in prefixes_to_handle.items():
        if game_name.startswith(prefix):
            game_name = game_name.replace(prefix, replacement)
            break

    # Обработка специальных символов
    game_name = game_name.replace("ō", "o").replace("ū", "u").replace("ā", "a")
    game_name = game_name.replace("é", "e").replace("è", "e").replace("ê", "e")
    game_name = game_name.replace("ü", "u").replace("ö", "o").replace("ä", "a")
    game_name = game_name.replace("ñ", "n").replace("ç", "c").replace("ß", "ss")
    game_name = game_name.replace("í", "i").replace("ì", "i").replace("î", "i")
    game_name = game_name.replace("ó", "o").replace("ò", "o").replace("ô", "o")
    game_name = game_name.replace("ú", "u").replace("ù", "u").replace("û", "u")
    game_name = game_name.replace("ý", "y").replace("ÿ", "y")

    # Удаление специальных символов, но сохранение дефисов
    game_name = re.sub(r'[^a-z0-9\s\-]', '', game_name)

    # Замена пробелов на дефисы
    game_name = re.sub(r'\s+', '-', game_name)

    # Удаление лишних дефисов
    game_name = re.sub(r'-+', '-', game_name)

    # Удаление дефисов в начале и конце
    game_name = game_name.strip('-')

    # Только самые критические специальные случаи
    special_cases = {
        # Римские цифры
        "hades-2": "hades-ii",
        "red-dead-redemption-2": "red-dead-redemption-ii",

        # Особые случаи с дефисами
        "marvels-spiderman": "marvels-spider-man",
        "marvels-spiderman-2": "marvels-spider-man-2",
        "your-turn-to-die": "Your Turn To Die -Death Game By Majority-",

        # Особые случаи с сокращениями
        "nier-replicant-ver122474487139": "nier-replicant",
        "dragon-quest-xi-s-echoes-of-an-elusive-age-definitive-edition": "dragon-quest-xi-s"
    }

    # Автоматическая обработка длинных названий
    if len(game_name.split('-')) > 4:  # Если название содержит более 4 частей
        # Для известных серий игр, оставляем только первые 3-4 части
        known_series = ["the-witcher", "the-elder-scrolls", "metal-gear-solid",
                        "final-fantasy", "assassins-creed", "star-wars", "call-of-duty"]

        for series in known_series:
            if game_name.startswith(series):
                parts = game_name.split('-')
                series_parts = series.split('-')
                # Оставляем части серии + 1-2 части названия
                game_name = '-'.join(parts[:len(series_parts) + 2])
                logging.debug(f"Сокращено длинное название серии: {game_name}")
                break

    if game_name in special_cases:
        game_name = special_cases[game_name]

    logging.debug(f"Преобразовано название игры: '{original_name}' -> '{game_name}'")

    url += game_name

    delay = REQUEST_DELAY + random.uniform(0.0, 2.0)  # Случайная задержка от 2 до 4 секунд
    time.sleep(delay)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0'
    }

    def try_fetch_metacritic(url, game_name, platform_name=None):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Проверяем, что страница действительно содержит информацию об игре
                # Metacritic обновил свой сайт в 2025 году, поэтому используем новые селекторы
                game_title_elem = soup.select_one('div.c-productHero_title h1, h1.c-productHero_title, div.product_title h1, h1')

                # Если не нашли заголовок через селекторы, проверяем наличие названия игры в заголовке страницы
                if not game_title_elem:
                    page_title = soup.title.text if soup.title else ""
                    if game_name.lower() in page_title.lower() and "metacritic" in page_title.lower():
                        # Страница существует, но заголовок не найден через селекторы
                        logging.info(f"Страница существует, но заголовок не найден через селекторы: {url}")
                        # Создаем фиктивный элемент для продолжения
                        game_title_elem = True

                        # Пробуем найти оценки в заголовке страницы
                        # Например, "Thief II: The Metal Age Reviews - Metacritic"
                        # Ищем оценки в тексте страницы
                        page_text = soup.text

                        # Ищем Metascore и User Score в тексте страницы
                        metascore_match = re.search(r'Metascore.*?(\d{2,3}).*?Based on \d+ Critic Reviews', page_text, re.DOTALL)
                        if metascore_match:
                            try:
                                metascore = int(metascore_match.group(1))
                                logging.info(f"Найден Metascore в заголовке страницы: {metascore}")
                            except ValueError:
                                pass

                        userscore_match = re.search(r'User Score.*?(\d+\.\d+).*?Based on \d+ User Ratings', page_text, re.DOTALL)
                        if userscore_match:
                            try:
                                userscore = float(userscore_match.group(1))
                                logging.info(f"Найден User Score в заголовке страницы: {userscore}")
                            except ValueError:
                                pass
                    else:
                        logging.warning(f"Страница не содержит информацию об игре: {url}")
                        return None

                # Пробуем разные селекторы для metascore (новейший дизайн 2025 года)
                metascore = None

                # Прямой поиск оценок в тексте страницы
                # Это самый надежный метод для новейшего дизайна Metacritic 2025 года
                html_text = str(soup)

                # Ищем Metascore
                metascore_patterns = [
                    r'Metascore\s+Generally\s+Favorable.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                    r'Metascore\s+Universal\s+Acclaim.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                    r'Metascore\s+Mixed.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                    r'Metascore\s+Generally\s+Unfavorable.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                    r'Metascore.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)'
                ]

                for pattern in metascore_patterns:
                    match = re.search(pattern, html_text, re.DOTALL | re.IGNORECASE)
                    if match:
                        try:
                            metascore = int(match.group(1))
                            logging.info(f"Найден Metascore в тексте страницы: {metascore}")
                            break
                        except ValueError:
                            pass

                # Ищем User Score
                userscore_patterns = [
                    r'User\s+Score\s+Generally\s+Favorable.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)',
                    r'User\s+Score\s+Universal\s+Acclaim.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)',
                    r'User\s+Score\s+Mixed.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)',
                    r'User\s+Score\s+Generally\s+Unfavorable.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)',
                    r'User\s+Score.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)'
                ]

                for pattern in userscore_patterns:
                    match = re.search(pattern, html_text, re.DOTALL | re.IGNORECASE)
                    if match:
                        try:
                            userscore = float(match.group(1))
                            logging.info(f"Найден User Score в тексте страницы: {userscore}")
                            break
                        except ValueError:
                            pass

                # Если не нашли оценки через прямой поиск, пробуем найти их в структуре страницы
                if metascore is None or userscore is None:
                    # Ищем блоки с оценками
                    score_blocks = soup.select('div[class*="metascore"], div[class*="userscore"], div[class*="score"], div[class*="Score"]')

                    for block in score_blocks:
                        block_text = block.text

                        # Ищем Metascore
                        if metascore is None and 'Metascore' in block_text:
                            # Ищем числа от 0 до 100
                            metascore_matches = re.findall(r'\b(\d{2,3})\b', block_text)
                            for match in metascore_matches:
                                try:
                                    score = int(match)
                                    if 60 <= score <= 100 and not (match.startswith('19') or match.startswith('20')):
                                        metascore = score
                                        logging.info(f"Найден Metascore в блоке: {metascore}")
                                        break
                                except ValueError:
                                    continue

                        # Ищем User Score
                        if userscore is None and 'User Score' in block_text:
                            # Ищем десятичные числа от 0 до 10
                            userscore_matches = re.findall(r'\b(\d+\.\d+)\b', block_text)
                            for match in userscore_matches:
                                try:
                                    score = float(match)
                                    if 6.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден User Score в блоке: {userscore}")
                                        break
                                except ValueError:
                                    continue

                # Если не нашли оценки рядом с заголовком, продолжаем поиск
                # Новейший дизайн 2025 года
                metascore_text = None

                # Ищем Metascore в тексте страницы
                metascore_patterns = [
                    r'Metascore\s+(\d+)',
                    r'Metascore.*?(\d+)',
                    r'Metascore.*?Based on \d+ Critic Reviews.*?(\d+)',
                    r'Metascore\s+Generally Favorable.*?(\d+)',
                    r'Metascore\s+Universal Acclaim.*?(\d+)',
                    r'Metascore\s+Mixed.*?(\d+)',
                    r'Metascore\s+Generally Unfavorable.*?(\d+)',
                    r'Metascore\s+Overwhelming Dislike.*?(\d+)',
                    r'Metascore.*?Based on.*?(\d+)'
                ]

                for pattern in metascore_patterns:
                    match = re.search(pattern, soup.text)
                    if match:
                        metascore_text = match.group(1)
                        break

                if metascore_text:
                    try:
                        # Проверяем, содержит ли текст десятичную точку
                        if '.' in metascore_text:
                            # Конвертируем из десятичного формата (например, 8.9) в целочисленный из 100 (89)
                            metascore = int(float(metascore_text) * 10)
                        else:
                            metascore = int(metascore_text)
                    except ValueError:
                        pass

                # Если не нашли через текст, пробуем через селекторы
                if metascore is None:
                    metascore_selectors = [
                        # Новейший дизайн 2025
                        'div[class*="metascore"]',
                        'span[class*="metascore"]',
                        'div.c-productHero_metascore',
                        'span.c-productHero_metascore',
                        # Специфичные селекторы для новейшего дизайна 2025
                        'div.c-productHero_metascoreContainer',
                        'span.c-productHero_metascoreContainer',
                        'div.c-productHero_metascoreNumber',
                        'span.c-productHero_metascoreNumber',
                        # Новый дизайн
                        'div.c-productScoreInfo span.c-metascore, div.c-productScoreInfo div.c-metascore',
                        'span.c-metascore, div.c-metascore',
                        # Старый дизайн
                        'div.metascore_w.game span',
                        'div.metascore_w span',
                        'div.metascore_w.xlarge span',
                        'div.metascore_w.large span'
                    ]

                    for selector in metascore_selectors:
                        metascore_elems = soup.select(selector)
                        for metascore_elem in metascore_elems:
                            text = metascore_elem.text.strip()
                            if text and text != 'tbd':
                                try:
                                    # Проверяем, содержит ли текст десятичную точку
                                    if '.' in text:
                                        # Удаляем все нецифровые символы, кроме точки
                                        text = re.sub(r'[^\d\.]', '', text)
                                        if text:
                                            # Конвертируем из десятичного формата (например, 8.9) в целочисленный из 100 (89)
                                            metascore = int(float(text) * 10)
                                            break
                                    else:
                                        # Удаляем все нецифровые символы
                                        text = re.sub(r'[^\d]', '', text)
                                        if text:
                                            metascore = int(text)
                                            break
                                except ValueError:
                                    continue
                        if metascore is not None:
                            break

                # Пробуем разные селекторы для userscore (новейший дизайн 2025 года)
                userscore = None

                # Ищем User Score в тексте страницы
                userscore_patterns = [
                    r'User Score\s+(\d+\.\d+)',
                    r'User Score.*?(\d+\.\d+)',
                    r'User Score.*?Based on \d+ User Ratings.*?(\d+\.\d+)',
                    r'User Score\s+Generally Favorable.*?(\d+\.\d+)',
                    r'User Score\s+Universal Acclaim.*?(\d+\.\d+)',
                    r'User Score\s+Mixed.*?(\d+\.\d+)',
                    r'User Score\s+Generally Unfavorable.*?(\d+\.\d+)',
                    r'User Score\s+Overwhelming Dislike.*?(\d+\.\d+)',
                    r'User Score.*?Based on.*?(\d+\.\d+)'
                ]

                for pattern in userscore_patterns:
                    match = re.search(pattern, soup.text)
                    if match:
                        userscore_text = match.group(1)
                        try:
                            userscore = float(userscore_text)
                            break
                        except ValueError:
                            pass

                # Если не нашли через текст, пробуем через селекторы
                if userscore is None:
                    userscore_selectors = [
                        # Новейший дизайн 2025
                        'div[class*="userscore"]',
                        'span[class*="userscore"]',
                        'div.c-productHero_userscore',
                        'span.c-productHero_userscore',
                        # Специфичные селекторы для новейшего дизайна 2025
                        'div.c-productHero_userscoreContainer',
                        'span.c-productHero_userscoreContainer',
                        'div.c-productHero_userscoreNumber',
                        'span.c-productHero_userscoreNumber',
                        # Новый дизайн
                        'div.c-productScoreInfo span.c-userscore, div.c-productScoreInfo div.c-userscore',
                        'span.c-userscore, div.c-userscore',
                        # Старый дизайн
                        'div.metascore_w.user.large.game',
                        'div.metascore_w.user',
                        'div.userscore_wrap div.metascore_w'
                    ]

                    for selector in userscore_selectors:
                        userscore_elems = soup.select(selector)
                        for userscore_elem in userscore_elems:
                            text = userscore_elem.text.strip()
                            if text and text != 'tbd':
                                try:
                                    # Удаляем все нецифровые символы, кроме точки
                                    text = re.sub(r'[^\d\.]', '', text)
                                    if text:
                                        userscore = float(text)
                                        break
                                except ValueError:
                                    continue
                        if userscore is not None:
                            break

                # Ищем блоки с категориями оценок
                if metascore is None or userscore is None:
                    # Ищем блоки с категориями оценок
                    category_blocks = soup.select('div.c-productScoreInfo, div.c-productHero_scoreInfo, div[class*="score"], div[class*="Score"]')

                    for block in category_blocks:
                        block_text = block.text

                        # Ищем Metascore
                        if metascore is None and ('Metascore' in block_text or 'Critics' in block_text):
                            # Ищем числа от 0 до 100
                            metascore_matches = re.findall(r'\b(\d{2,3})\b', block_text)
                            for match in metascore_matches:
                                try:
                                    score = int(match)
                                    if 60 <= score <= 100 and not (match.startswith('19') or match.startswith('20')):
                                        metascore = score
                                        logging.info(f"Найден Metascore в блоке категорий: {metascore}")
                                        break
                                except ValueError:
                                    continue

                        # Ищем User Score
                        if userscore is None and ('User Score' in block_text or 'User' in block_text and 'Rating' in block_text):
                            # Ищем десятичные числа от 0 до 10
                            userscore_matches = re.findall(r'\b(\d+\.\d+)\b', block_text)
                            for match in userscore_matches:
                                try:
                                    score = float(match)
                                    if 6.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден User Score в блоке категорий: {userscore}")
                                        break
                                except ValueError:
                                    continue

                # Если не нашли оценки через предыдущие методы,
                # пробуем найти их в HTML-коде страницы
                if metascore is None:
                    # Ищем конкретные HTML-структуры, которые содержат оценки
                    # Например, ищем элементы с классами, содержащими "metascore" и числовое значение
                    metascore_elements = soup.select('[class*="metascore"]')
                    for elem in metascore_elements:
                        # Проверяем, содержит ли элемент число
                        elem_text = elem.text.strip()
                        if re.match(r'^\d+$', elem_text) and 60 <= int(elem_text) <= 100:
                            metascore = int(elem_text)
                            logging.info(f"Найден Metascore в HTML-элементе: {metascore}")
                            break

                if userscore is None:
                    # Ищем конкретные HTML-структуры, которые содержат оценки пользователей
                    userscore_elements = soup.select('[class*="userscore"]')
                    for elem in userscore_elements:
                        # Проверяем, содержит ли элемент десятичное число
                        elem_text = elem.text.strip()
                        if re.match(r'^\d+\.\d+$', elem_text) and 6.0 <= float(elem_text) <= 10.0:
                            userscore = float(elem_text)
                            logging.info(f"Найден User Score в HTML-элементе: {userscore}")
                            break

                # Если все еще не нашли оценки, пробуем найти их в HTML-коде с более точными шаблонами
                if metascore is None:
                    html_metascore_patterns = [
                        # Очень специфичные шаблоны для Metacritic 2025
                        r'Metascore.*?(\d+).*?Based on \d+ Critic Reviews',
                        r'Metascore.*?Generally Favorable.*?(\d+)',
                        r'Metascore.*?Universal Acclaim.*?(\d+)',
                        r'Metascore.*?Mixed.*?(\d+)',
                        r'Metascore.*?Generally Unfavorable.*?(\d+)',
                        r'Metascore.*?Overwhelming Dislike.*?(\d+)',
                        # Более общие шаблоны
                        r'Metascore.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                        r'Metascore.*?Based\s+on.*?(\d+)',
                        r'Metascore.*?(\d+)'
                    ]

                    # Сначала ищем с более точными шаблонами
                    for pattern in html_metascore_patterns:
                        matches = re.findall(pattern, str(soup), re.IGNORECASE)
                        if matches:
                            for match in matches:
                                try:
                                    score = int(match)
                                    if 0 <= score <= 100:
                                        # Проверяем, что это не процент или другое число
                                        # Ищем контекст вокруг числа
                                        context_pattern = r'[^>]*' + re.escape(match) + r'[^<]*'
                                        context_matches = re.findall(context_pattern, str(soup))

                                        # Проверяем, что контекст связан с Metascore
                                        is_valid = False
                                        for context in context_matches:
                                            if ('metascore' in context.lower() or
                                                'critic' in context.lower() or
                                                'review' in context.lower()):
                                                is_valid = True
                                                break

                                        if is_valid:
                                            metascore = score
                                            logging.info(f"Найден Metascore в HTML-коде: {metascore}")
                                            break
                                except ValueError:
                                    continue
                        if metascore is not None:
                            break

                if userscore is None:
                    # Ищем User Score в HTML-коде с более точными шаблонами
                    html_userscore_patterns = [
                        r'User\s+Score\s+Generally\s+Favorable.*?(\d+\.\d+)',
                        r'User\s+Score\s+Universal\s+Acclaim.*?(\d+\.\d+)',
                        r'User\s+Score\s+Mixed.*?(\d+\.\d+)',
                        r'User\s+Score\s+Generally\s+Unfavorable.*?(\d+\.\d+)',
                        r'User\s+Score\s+Overwhelming\s+Dislike.*?(\d+\.\d+)',
                        r'User\s+Score.*?Based\s+on\s+\d+\s+User\s+Ratings.*?(\d+\.\d+)',
                        r'User\s+Score.*?Based\s+on.*?(\d+\.\d+)',
                        r'User\s+Score.*?(\d+\.\d+)'
                    ]

                    # Сначала ищем с более точными шаблонами
                    for pattern in html_userscore_patterns:
                        matches = re.findall(pattern, str(soup), re.IGNORECASE)
                        if matches:
                            for match in matches:
                                try:
                                    score = float(match)
                                    if 0 <= score <= 10:
                                        # Проверяем, что это не процент или другое число
                                        # Ищем контекст вокруг числа
                                        context_pattern = r'[^>]*' + re.escape(match) + r'[^<]*'
                                        context_matches = re.findall(context_pattern, str(soup))

                                        # Проверяем, что контекст связан с User Score
                                        is_valid = False
                                        for context in context_matches:
                                            if ('userscore' in context.lower() or
                                                'user score' in context.lower() or
                                                'user' in context.lower() and 'rating' in context.lower()):
                                                is_valid = True
                                                break

                                        if is_valid:
                                            userscore = score
                                            logging.info(f"Найден User Score в HTML-коде: {userscore}")
                                            break
                                except ValueError:
                                    continue
                        if userscore is not None:
                            break

                # Ищем оценки в блоках с обзорами критиков и пользователей
                if metascore is None or userscore is None:
                    # Ищем блоки с обзорами
                    review_blocks = soup.select('div.c-reviewsSection, div[class*="review"], div[class*="Review"], section[class*="review"], section[class*="Review"]')

                    for block in review_blocks:
                        block_text = block.text

                        # Ищем заголовки разделов с обзорами
                        critic_headers = ['Critic Reviews', 'Critics', 'Critic', 'Professional Reviews']
                        user_headers = ['User Reviews', 'Users', 'User', 'Player Reviews']

                        # Ищем Metascore
                        if metascore is None and any(header in block_text for header in critic_headers):
                            # Ищем числа от 0 до 100
                            metascore_matches = re.findall(r'\b(\d{2,3})\b', block_text)
                            for match in metascore_matches:
                                try:
                                    score = int(match)
                                    if 60 <= score <= 100 and not (match.startswith('19') or match.startswith('20')):
                                        metascore = score
                                        logging.info(f"Найден Metascore в блоке обзоров: {metascore}")
                                        break
                                except ValueError:
                                    continue

                        # Ищем User Score
                        if userscore is None and any(header in block_text for header in user_headers):
                            # Ищем десятичные числа от 0 до 10
                            userscore_matches = re.findall(r'\b(\d+\.\d+)\b', block_text)
                            for match in userscore_matches:
                                try:
                                    score = float(match)
                                    if 6.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден User Score в блоке обзоров: {userscore}")
                                        break
                                except ValueError:
                                    continue

                # Последняя попытка - поиск в тексте страницы с использованием более гибких регулярных выражений
                if metascore is None:
                    # Ищем любые числа от 0 до 100 рядом с ключевыми словами
                    text = soup.text

                    # Ищем числа рядом с "Metascore", "Critic", "Critics", "Reviews"
                    metascore_keywords = [
                        "Metascore Generally Favorable",
                        "Metascore Universal Acclaim",
                        "Metascore Mixed",
                        "Metascore Generally Unfavorable",
                        "Metascore Based on",
                        "Based on Critic Reviews",
                        "Metascore"
                    ]

                    # Сортируем ключевые слова по длине (от самых длинных к самым коротким)
                    # Это позволяет сначала искать более специфичные контексты
                    metascore_keywords.sort(key=len, reverse=True)

                    for keyword in metascore_keywords:
                        # Ищем в окрестности 100 символов от ключевого слова
                        keyword_pos = text.find(keyword)
                        if keyword_pos != -1:
                            # Берем больше контекста после ключевого слова, так как оценка обычно идет после
                            context = text[max(0, keyword_pos - 20):min(len(text), keyword_pos + 200)]

                            # Ищем числа от 0 до 100
                            # Используем более точный шаблон для поиска чисел
                            # Ищем числа, которые стоят отдельно или в начале строки
                            number_matches = re.findall(r'(?:^|\s)(\d{1,3})(?:\s|$)', context)

                            # Сортируем найденные числа по близости к ключевому слову
                            # (предполагаем, что оценка находится ближе к ключевому слову)
                            number_matches.sort(key=lambda x: abs(context.find(x) - context.find(keyword)))

                            for match in number_matches:
                                try:
                                    score = int(match)
                                    # Более строгая проверка диапазона для Metascore
                                    # Большинство игр имеют оценки от 60 до 95
                                    if 60 <= score <= 100:
                                        # Дополнительная проверка: убедимся, что это не год и не количество обзоров
                                        # Годы обычно начинаются с 19 или 20
                                        if not (match.startswith('19') or match.startswith('20')):
                                            metascore = score
                                            logging.info(f"Найден вероятный Metascore в тексте: {metascore} (контекст: {keyword})")
                                            break
                                except ValueError:
                                    continue
                        if metascore is not None:
                            break

                if userscore is None:
                    # Ищем десятичные числа от 0 до 10 рядом с ключевыми словами
                    text = soup.text

                    # Ищем числа рядом с "User Score", "User", "Users", "Ratings"
                    userscore_keywords = [
                        "User Score Generally Favorable",
                        "User Score Universal Acclaim",
                        "User Score Mixed",
                        "User Score Generally Unfavorable",
                        "User Score Based on",
                        "Based on User Ratings",
                        "User Score"
                    ]

                    # Сортируем ключевые слова по длине (от самых длинных к самым коротким)
                    # Это позволяет сначала искать более специфичные контексты
                    userscore_keywords.sort(key=len, reverse=True)

                    for keyword in userscore_keywords:
                        # Ищем в окрестности 200 символов от ключевого слова
                        keyword_pos = text.find(keyword)
                        if keyword_pos != -1:
                            # Берем больше контекста после ключевого слова, так как оценка обычно идет после
                            context = text[max(0, keyword_pos - 20):min(len(text), keyword_pos + 200)]

                            # Ищем десятичные числа от 0 до 10
                            # Используем более точный шаблон для поиска десятичных чисел
                            number_matches = re.findall(r'(?:^|\s)(\d+\.\d+)(?:\s|$)', context)

                            # Сортируем найденные числа по близости к ключевому слову
                            # (предполагаем, что оценка находится ближе к ключевому слову)
                            number_matches.sort(key=lambda x: abs(context.find(x) - context.find(keyword)))

                            for match in number_matches:
                                try:
                                    score = float(match)
                                    # Более строгая проверка диапазона для User Score
                                    # Большинство игр имеют оценки от 6.0 до 9.5
                                    if 6.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден вероятный User Score в тексте: {userscore} (контекст: {keyword})")
                                        break
                                except ValueError:
                                    continue
                        if userscore is not None:
                            break

                # Проверяем, что хотя бы одна оценка найдена
                if metascore is None and userscore is None:
                    # Проверяем, является ли игра предстоящей (без оценок)
                    # Пробуем разные селекторы для даты релиза (новейший дизайн 2025 года)
                    release_date = None

                    # Ищем дату релиза в тексте страницы
                    release_date_patterns = [
                        r'Released On:\s*([A-Za-z0-9,\s]+)',
                        r'Release Date:\s*([A-Za-z0-9,\s]+)',
                        r'Release:\s*([A-Za-z0-9,\s]+)'
                    ]

                    for pattern in release_date_patterns:
                        match = re.search(pattern, soup.text)
                        if match:
                            release_date = match.group(1).strip()
                            break

                    # Если не нашли через текст, пробуем через селекторы
                    if not release_date:
                        release_date_selectors = [
                            # Новейший дизайн 2025
                            'div[class*="releaseDate"]',
                            'span[class*="releaseDate"]',
                            'div.c-gameDetails_releaseDate',
                            'span.c-gameDetails_releaseDate',
                            # Новый дизайн
                            'div.c-gameDetails_ReleaseDate, div.c-gameDetails_releaseDate',
                            'span.c-gameDetails_ReleaseDate, span.c-gameDetails_releaseDate',
                            # Старый дизайн
                            'li.summary_detail.release_data div.data',
                            'div.release_data'
                        ]

                        for selector in release_date_selectors:
                            release_date_elem = soup.select_one(selector)
                            if release_date_elem:
                                release_date = release_date_elem.text.strip()
                                break

                    if release_date:
                        # Если дата релиза в будущем или содержит "TBA" (To Be Announced)
                        if "TBA" in release_date or "Coming" in release_date or "TBC" in release_date or "Announced" in release_date:
                            logging.info(f"Игра {game_name} еще не вышла: {release_date}")
                            result = {
                                "name": original_name,
                                "metascore": None,
                                "userscore": None,
                                "url": url,
                                "timestamp": datetime.now().isoformat(),
                                "platform": platform_name,
                                "note": f"Игра еще не вышла: {release_date}"
                            }
                            return result

                    logging.warning(f"Не удалось найти оценки для игры {game_name} на странице {url}")
                    # Возвращаем результат с пустыми оценками, но с пометкой
                    result = {
                        "name": original_name,
                        "metascore": None,
                        "userscore": None,
                        "url": url,
                        "timestamp": datetime.now().isoformat(),
                        "platform": platform_name,
                        "note": "Страница существует, но оценки не найдены"
                    }
                    return result

                # Проверяем, что оценки находятся в правильном диапазоне
                if metascore is not None:
                    # Если оценка меньше 10, возможно, это десятичный формат (например, 8.9 вместо 89)
                    if metascore < 10:
                        metascore = metascore * 10
                    # Если оценка больше 100, возможно, это ошибка парсинга
                    elif metascore > 100:
                        metascore = 100

                result = {
                    "name": original_name,
                    "metascore": metascore,
                    "userscore": userscore,
                    "url": url,
                    "timestamp": datetime.now().isoformat(),
                    "platform": platform_name
                }

                return result
            else:
                logging.warning(f"Ошибка при запросе к Metacritic: {response.status_code} для игры {game_name}")
                return None
        except Exception as e:
            logging.error(f"Ошибка при получении данных с Metacritic для игры {game_name}: {e}")
            return None

    # Функция для генерации альтернативных вариантов названия
    def generate_name_variants(name, original):
        variants = []

        # Основной вариант
        variants.append((name, "основной вариант"))

        # Для игр с двоеточием в названии, пробуем без части после двоеточия
        if ":" in original:
            base_name = original.split(":")[0].strip()
            base_name = base_name.lower()
            base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
            base_name = re.sub(r'\s+', '-', base_name)
            variants.append((base_name, "без части после двоеточия"))

        # Для длинных названий, пробуем первые 2-3 слова
        words = original.split()
        if len(words) > 2:
            short_name = " ".join(words[:2])
            short_name = short_name.lower()
            short_name = re.sub(r'[^a-z0-9\s\-]', '', short_name)
            short_name = re.sub(r'\s+', '-', short_name)
            variants.append((short_name, "первые 2 слова"))

            if len(words) > 3:
                short_name = " ".join(words[:3])
                short_name = short_name.lower()
                short_name = re.sub(r'[^a-z0-9\s\-]', '', short_name)
                short_name = re.sub(r'\s+', '-', short_name)
                variants.append((short_name, "первые 3 слова"))

        # Для названий с тире, пробуем часть до первого тире
        if "-" in name:
            jp_name = name.split("-")[0].strip()
            variants.append((jp_name, "до первого тире"))

        # Для очень длинных названий, пробуем только первое слово
        if len(words) > 1:
            first_word = words[0].lower()
            first_word = re.sub(r'[^a-z0-9\-]', '', first_word)
            variants.append((first_word, "только первое слово"))

        # Для игр с "Spider-Man" в названии
        if "spider" in original.lower() or "spiderman" in original.lower():
            if "spider-man" in name:
                variants.append((name.replace("spider-man", "spiderman"), "замена spider-man на spiderman"))
            elif "spiderman" in name:
                variants.append((name.replace("spiderman", "spider-man"), "замена spiderman на spider-man"))

        # Для DLC и расширений
        dlc_indicators = ["dlc", "expansion", "addon", "add-on", "shadow of", "part ii", "part 2"]
        if any(indicator in original.lower() for indicator in dlc_indicators):
            base_name = re.split(r'[:\-]', original)[0].strip()
            base_name = base_name.lower()
            base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
            base_name = re.sub(r'\s+', '-', base_name)
            variants.append((base_name, "базовая игра для DLC"))

        # Удаляем дубликаты, сохраняя порядок
        unique_variants = []
        seen = set()
        for variant, desc in variants:
            if variant not in seen and variant:
                seen.add(variant)
                unique_variants.append((variant, desc))

        return unique_variants

    # Пробуем все варианты названий
    variants = generate_name_variants(game_name, original_name)

    for variant_name, variant_desc in variants:
        # Новый формат URL без платформы
        variant_url = f"https://www.metacritic.com/game/{variant_name}/"

        logging.info(f"Пробуем вариант '{variant_desc}': {variant_url}")

        # Добавляем небольшую задержку между запросами
        if variant_name != game_name:  # Не добавляем задержку для первого запроса
            time.sleep(1)

        result = try_fetch_metacritic(variant_url, variant_name, platform_name)
        if result:
            if variant_desc != "основной вариант":
                result["note"] = f"Найдено по варианту: {variant_desc}"
            # Сохраняем результат в кэш
            search_cache[cache_key] = result
            return result

    # Для игр на PS5/Xbox Series X, пробуем найти их на PS4/Xbox One
    if platform in ["PlayStation 5", "PS5", "Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
        prev_gen_platform = None
        if platform in ["PlayStation 5", "PS5"]:
            prev_gen_platform = "PlayStation 4"
        elif platform in ["Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
            prev_gen_platform = "Xbox One"

        if prev_gen_platform:
            # Новый формат URL без платформы
            alt_url = f"https://www.metacritic.com/game/{game_name}/"

            logging.info(f"Пробуем найти игру на предыдущем поколении консолей ({prev_gen_platform}): {alt_url}")

            # Добавляем небольшую задержку перед повторным запросом
            time.sleep(1)

            result = try_fetch_metacritic(alt_url, game_name, prev_gen_platform)
            if result:
                # Отмечаем, что это оценка для предыдущего поколения
                result["note"] = f"Оценка для {prev_gen_platform}"
                # Сохраняем результат в кэш
                search_cache[cache_key] = result
                return result

    # Все дополнительные проверки уже включены в функцию generate_name_variants
    # и обрабатываются в цикле выше, поэтому здесь они не нужны

    # Если все попытки не удались, сохраняем отрицательный результат в кэш
    # Это позволит избежать повторных запросов для игр, которые не найдены на Metacritic
    search_cache[cache_key] = None
    return None

def update_metacritic_data():
    """Обновляет данные Metacritic для игр."""
    metacritic_data = load_metacritic_data()

    # Выводим статистику текущей базы данных
    if metacritic_data['games']:
        games_with_metascore = 0
        games_with_userscore = 0
        games_with_both_scores = 0
        games_not_released = 0
        games_no_scores = 0
        games_not_found = 0

        for game_data in metacritic_data['games'].values():
            has_metascore = game_data.get('metascore') is not None
            has_userscore = game_data.get('userscore') is not None
            note = game_data.get('note', '')

            if has_metascore:
                games_with_metascore += 1
            if has_userscore:
                games_with_userscore += 1
            if has_metascore and has_userscore:
                games_with_both_scores += 1

            if "Игра еще не вышла" in note:
                games_not_released += 1
            elif note == "Страница существует, но оценки не найдены":
                games_no_scores += 1
            elif note == "Не найдено на Metacritic":
                games_not_found += 1

        logging.info(f"Текущая статистика базы данных Metacritic:")
        logging.info(f"  - Всего игр в базе: {len(metacritic_data['games'])}")
        logging.info(f"  - Игр с оценкой критиков: {games_with_metascore}")
        logging.info(f"  - Игр с оценкой пользователей: {games_with_userscore}")
        logging.info(f"  - Игр с обеими оценками: {games_with_both_scores}")
        logging.info(f"  - Игр, которые еще не вышли: {games_not_released}")
        logging.info(f"  - Игр без оценок: {games_no_scores}")
        logging.info(f"  - Игр, не найденных на Metacritic: {games_not_found}")
        logging.info(f"")

    all_games = load_all_games()

    total_games = len(all_games)
    processed_games = 0
    updated_games = 0
    skipped_games = 0
    error_games = 0
    requests_count = 0

    # Получаем индекс последней обработанной игры
    last_processed_index = metacritic_data.get('last_processed_index', 0)

    # Проверяем, нужно ли продолжить с определенного места
    if last_processed_index > 0 and last_processed_index < total_games:
        logging.info(f"Продолжаем обновление данных Metacritic с индекса {last_processed_index} (всего игр: {total_games})")
    else:
        last_processed_index = 0
        logging.info(f"Начинаем обновление данных Metacritic для {total_games} игр...")

    # Сортируем игры по ID для обеспечения стабильного порядка
    all_games.sort(key=lambda g: g.get('id', 0))

    for i, game in enumerate(all_games[last_processed_index:], start=last_processed_index):
        game_id = str(game.get('id'))
        game_name = game.get('name')

        if not isinstance(game_name, str):
            logging.warning(f"Пропускаем игру с ID {game_id}: название игры не является строкой ({type(game_name)})")
            processed_games += 1
            error_games += 1
            continue

        if game_id in metacritic_data['games']:
            game_data = metacritic_data['games'][game_id]
            last_updated = game_data.get('timestamp', '')

            # Проверяем примечание к игре
            note = game_data.get('note', '')

            # Если у игры есть примечание "Не найдено на Metacritic", проверяем реже (раз в 90 дней)
            if note == "Не найдено на Metacritic" and last_updated:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    if days_since_update < 90:
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), неудачный поиск {days_since_update} дней назад")
                        continue
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")

            # Если у игры есть примечание "Игра еще не вышла", проверяем раз в 30 дней
            elif "Игра еще не вышла" in note and last_updated:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    if days_since_update < 30:
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), {note}, проверено {days_since_update} дней назад")
                        continue
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")

            # Если у игры есть примечание "Страница существует, но оценки не найдены", проверяем раз в 60 дней
            elif note == "Страница существует, но оценки не найдены" and last_updated:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    if days_since_update < 60:
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), страница без оценок, проверено {days_since_update} дней назад")
                        continue
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")

            # Для обычных игр с данными проверяем каждые 30 дней
            elif last_updated and (game_data.get('metascore') is not None or game_data.get('userscore') is not None):
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    if days_since_update < 30:
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), данные обновлены {days_since_update} дней назад")
                        continue
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")
                    pass

        if requests_count >= MAX_REQUESTS_PER_RUN:
            logging.warning(f"Достигнуто максимальное количество запросов ({MAX_REQUESTS_PER_RUN}). Прерываем обновление.")
            break

        logging.info(f"Получаем данные Metacritic для игры: {game_name} (ID: {game_id})")

        platforms = game.get('platforms', [])
        if not isinstance(platforms, list):
            logging.warning(f"Поле platforms для игры {game_name} (ID: {game_id}) не является списком: {type(platforms)}. Используем пустой список.")
            platforms = []

        metacritic_result = None

        if platforms:
            priority_platform_groups = [
                ["PC", "Windows", "Win"],
                ["Nintendo Switch 2", "Switch 2", "NSW2"],
                ["PlayStation 5", "PS5"],
                ["Xbox Series X", "Xbox Series S", "XSX", "XSS"],
                ["PlayStation 4", "PS4"],
                ["Xbox One", "XONE"],
                ["Nintendo Switch", "Switch", "NSW"],
                ["PlayStation 3", "PS3"],
                ["Xbox 360", "X360"],
                ["Wii U"],
                ["PlayStation 2", "playstation-2", "PS2"],
                ["Wii"],
                ["PlayStation", "playstation", "PS1", "PSX"],
                ["Xbox", "xbox"],
                ["Nintendo 3DS", "3ds", "Game Boy Advance", "game-boy-advance", "GBA", "game-boy-advance", "PlayStation Vita", "playstation-vita", "PS Vita", "playstation-vita", "PSV", "playstation-vita", "PSP", "psp"],
                ["Nintendo DS", "ds", "NDS", "ds", "GameCube", "gamecube", "GC", "gamecube", "Nintendo 64", "nintendo-64", "N64", "nintendo-64"],
                ["iOS", "iPhone", "iPad"],
                ["Android",  "dreamcast", "dreamcast", "stadia", "mac", "Linux"]
            ]

            priority_platforms = []
            for group in priority_platform_groups:
                priority_platforms.extend(group)
            valid_platforms = []
            for p in platforms:
                if isinstance(p, str):
                    valid_platforms.append(p)
                elif isinstance(p, dict):
                    if 'name' in p and isinstance(p['name'], str):
                        valid_platforms.append(p['name'])
                    elif 'abbreviation' in p and isinstance(p['abbreviation'], str):
                        abbr_map = {
                            'PC': 'PC',
                            'PS4': 'PlayStation 4',
                            'PS5': 'PlayStation 5',
                            'XONE': 'Xbox One',
                            'XSX': 'Xbox Series X',
                            'NSW': 'Nintendo Switch'
                        }
                        abbr = p['abbreviation']
                        if abbr in abbr_map:
                            valid_platforms.append(abbr_map[abbr])
                        else:
                            valid_platforms.append(abbr)
                    elif 'slug' in p and isinstance(p['slug'], str):
                        slug_map = {
                            'pc': 'PC',
                            'win': 'PC',
                            'windows': 'PC',
                            'ps4': 'PlayStation 4',
                            'playstation4': 'PlayStation 4',
                            'ps5': 'PlayStation 5',
                            'playstation5': 'PlayStation 5',
                            'xboxone': 'Xbox One',
                            'xbox-one': 'Xbox One',
                            'xboxseriesx': 'Xbox Series X',
                            'xbox-series-x': 'Xbox Series X',
                            'switch': 'Nintendo Switch',
                            'nintendo-switch': 'Nintendo Switch'
                        }
                        slug = p['slug'].lower()
                        if slug in slug_map:
                            valid_platforms.append(slug_map[slug])
                        else:
                            platform_name = slug.replace('-', ' ').title()
                            valid_platforms.append(platform_name)
                    elif 'id' in p and isinstance(p['id'], int):
                        platform_id_map = {
                            6: 'PC',
                            48: 'PlayStation 4',
                            167: 'PlayStation 5',
                            49: 'Xbox One',
                            169: 'Xbox Series X',
                            130: 'Nintendo Switch'
                        }
                        platform_id = p['id']
                        if platform_id in platform_id_map:
                            valid_platforms.append(platform_id_map[platform_id])
                        else:
                            logging.debug(f"Неизвестный ID платформы: {platform_id} для игры {game_name}")
                    else:
                        logging.warning(f"Словарь платформы не содержит известных полей: {p.keys()} для игры {game_name}")
                else:
                    logging.warning(f"Пропускаем платформу неверного типа: {type(p)} для игры {game_name}")

            sorted_platforms = []

            for platform_group in priority_platform_groups:
                for platform in platform_group:
                    if platform in valid_platforms:
                        sorted_platforms.append(platform)
                        break
                if sorted_platforms:
                    break

            if not sorted_platforms and valid_platforms:
                sorted_platforms.append(valid_platforms[0])
            if not sorted_platforms:
                sorted_platforms.append("PC")
            if sorted_platforms:
                highest_priority_platform = sorted_platforms[0]

                priority_group = 0
                for i, group in enumerate(priority_platform_groups):
                    if highest_priority_platform in group:
                        priority_group = i + 1
                        break

                logging.info(f"Проверяем только платформу с наивысшим приоритетом: {highest_priority_platform} (группа приоритета: {priority_group}) для игры {game_name}")
                logging.debug(f"Доступные платформы для игры {game_name}: {valid_platforms}")

                metacritic_result = get_metacritic_data(game_name, highest_priority_platform)
                requests_count += 1

                if metacritic_result:
                    # Платформа теперь всегда добавляется в результат в функции try_fetch_metacritic
                    platform_info = metacritic_result.get('platform', highest_priority_platform)
                    metascore = metacritic_result.get('metascore')
                    userscore = metacritic_result.get('userscore')

                    # Формируем сообщение с информацией об оценках
                    scores_info = []
                    if metascore is not None:
                        scores_info.append(f"Metascore: {metascore}")
                    if userscore is not None:
                        scores_info.append(f"Userscore: {userscore}")

                    scores_text = ", ".join(scores_info) if scores_info else "без оценок"

                    logging.info(f"Найдены данные Metacritic для игры {game_name} на платформе {platform_info} ({scores_text})")
                    if 'note' in metacritic_result:
                        logging.info(f"Примечание: {metacritic_result['note']}")
                else:
                    logging.warning(f"Не удалось найти данные Metacritic для игры {game_name} на платформе {highest_priority_platform}")

        if not metacritic_result:
            metacritic_result = get_metacritic_data(game_name)
            requests_count += 1

        if metacritic_result:
            # Проверяем, есть ли у результата примечание о том, что страница существует, но оценок нет
            has_note = 'note' in metacritic_result
            has_scores = metacritic_result.get('metascore') is not None or metacritic_result.get('userscore') is not None

            metacritic_data['games'][game_id] = metacritic_result

            # Если есть оценки или есть примечание (игра еще не вышла, страница без оценок и т.д.),
            # считаем это успешным обновлением
            if has_scores or has_note:
                updated_games += 1
                if has_scores:
                    logging.info(f"Данные Metacritic для игры {game_name} успешно обновлены")
                else:
                    logging.info(f"Данные Metacritic для игры {game_name} обновлены: {metacritic_result.get('note')}")
            else:
                # Если нет ни оценок, ни примечания, считаем это ошибкой
                error_games += 1
                logging.warning(f"Получены пустые данные для игры {game_name}")
        else:
            error_games += 1
            # Добавляем запись о неудачном поиске, чтобы не искать эту игру снова в ближайшее время
            metacritic_data['games'][game_id] = {
                "name": game_name,
                "metascore": None,
                "userscore": None,
                "url": None,
                "timestamp": datetime.now().isoformat(),
                "note": "Не найдено на Metacritic"
            }
            logging.info(f"Добавлена запись о неудачном поиске для игры {game_name}")

        processed_games += 1

        if processed_games % 10 == 0:
            metacritic_data['last_updated'] = datetime.now().isoformat()
            metacritic_data['total_games'] = len(metacritic_data['games'])
            # Сохраняем индекс текущей игры
            metacritic_data['last_processed_index'] = i
            save_metacritic_data(metacritic_data)

            # Подсчитываем статистику по собранным данным
            games_with_metascore = 0
            games_with_userscore = 0
            games_with_both_scores = 0
            games_not_released = 0
            games_no_scores = 0
            games_not_found = 0

            for game_data in metacritic_data['games'].values():
                has_metascore = game_data.get('metascore') is not None
                has_userscore = game_data.get('userscore') is not None
                note = game_data.get('note', '')

                if has_metascore:
                    games_with_metascore += 1
                if has_userscore:
                    games_with_userscore += 1
                if has_metascore and has_userscore:
                    games_with_both_scores += 1

                if "Игра еще не вышла" in note:
                    games_not_released += 1
                elif note == "Страница существует, но оценки не найдены":
                    games_no_scores += 1
                elif note == "Не найдено на Metacritic":
                    games_not_found += 1

            # Выводим краткую сводку
            logging.info(f"Промежуточное сохранение: обработано {processed_games}/{total_games} игр")
            logging.info(f"Статистика собранных данных:")
            logging.info(f"  - Всего игр в базе: {len(metacritic_data['games'])}")
            logging.info(f"  - Игр с оценкой критиков: {games_with_metascore}")
            logging.info(f"  - Игр с оценкой пользователей: {games_with_userscore}")
            logging.info(f"  - Игр с обеими оценками: {games_with_both_scores}")
            logging.info(f"  - Игр, которые еще не вышли: {games_not_released}")
            logging.info(f"  - Игр без оценок: {games_no_scores}")
            logging.info(f"  - Игр, не найденных на Metacritic: {games_not_found}")

    metacritic_data['last_updated'] = datetime.now().isoformat()
    metacritic_data['total_games'] = len(metacritic_data['games'])

    # Если обработаны все игры, сбрасываем индекс
    if processed_games >= total_games or requests_count >= MAX_REQUESTS_PER_RUN:
        metacritic_data['last_processed_index'] = 0
        logging.info("Обработка завершена или достигнут лимит запросов. Сбрасываем индекс для следующего запуска.")
    else:
        # Иначе сохраняем текущий индекс для продолжения в следующий раз
        metacritic_data['last_processed_index'] = last_processed_index + processed_games
        logging.info(f"Сохраняем индекс {metacritic_data['last_processed_index']} для продолжения в следующий раз.")

    save_metacritic_data(metacritic_data)

    # Подсчитываем финальную статистику по собранным данным
    games_with_metascore = 0
    games_with_userscore = 0
    games_with_both_scores = 0
    games_not_released = 0
    games_no_scores = 0
    games_not_found = 0

    for game_data in metacritic_data['games'].values():
        has_metascore = game_data.get('metascore') is not None
        has_userscore = game_data.get('userscore') is not None
        note = game_data.get('note', '')

        if has_metascore:
            games_with_metascore += 1
        if has_userscore:
            games_with_userscore += 1
        if has_metascore and has_userscore:
            games_with_both_scores += 1

        if "Игра еще не вышла" in note:
            games_not_released += 1
        elif note == "Страница существует, но оценки не найдены":
            games_no_scores += 1
        elif note == "Не найдено на Metacritic":
            games_not_found += 1

    # Выводим финальную сводку
    logging.info(f"Обновление данных Metacritic завершено:")
    logging.info(f"Всего игр в IGDB: {total_games}")
    logging.info(f"Обработано игр: {processed_games}")
    logging.info(f"Обновлено игр: {updated_games}")
    logging.info(f"Пропущено игр: {skipped_games}")
    logging.info(f"Ошибок: {error_games}")
    logging.info(f"")
    logging.info(f"Статистика базы данных Metacritic:")
    logging.info(f"  - Всего игр в базе: {len(metacritic_data['games'])}")
    logging.info(f"  - Игр с оценкой критиков: {games_with_metascore}")
    logging.info(f"  - Игр с оценкой пользователей: {games_with_userscore}")
    logging.info(f"  - Игр с обеими оценками: {games_with_both_scores}")
    logging.info(f"  - Игр, которые еще не вышли: {games_not_released}")
    logging.info(f"  - Игр без оценок: {games_no_scores}")
    logging.info(f"  - Игр, не найденных на Metacritic: {games_not_found}")

def main():
    """Основная функция скрипта."""
    logging.info("Начинаем сбор данных с Metacritic...")
    try:
        update_metacritic_data()
        logging.info("Сбор данных с Metacritic завершен успешно.")
    except Exception as e:
        logging.error(f"Произошла ошибка при сборе данных с Metacritic: {e}")
        raise

if __name__ == "__main__":
    main()
