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

    global search_cache

    cache_key = f"{game_name}_{platform}" if platform else game_name

    if cache_key in search_cache:
        logging.info(f"Используем кэшированный результат для игры {game_name}")
        return search_cache[cache_key]

    url = "https://www.metacritic.com/game/"

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

    original_name = game_name
    game_name = game_name.lower()
    prefixes_to_handle = {
        "marvel's": "marvels",
        "tom clancy's": "tom-clancys",
        "sid meier's": "sid-meiers"
    }

    for prefix, replacement in prefixes_to_handle.items():
        if game_name.startswith(prefix):
            game_name = game_name.replace(prefix, replacement)
            break

    game_name = game_name.replace("ō", "o").replace("ū", "u").replace("ā", "a")
    game_name = game_name.replace("é", "e").replace("è", "e").replace("ê", "e")
    game_name = game_name.replace("ü", "u").replace("ö", "o").replace("ä", "a")
    game_name = game_name.replace("ñ", "n").replace("ç", "c").replace("ß", "ss")
    game_name = game_name.replace("í", "i").replace("ì", "i").replace("î", "i")
    game_name = game_name.replace("ó", "o").replace("ò", "o").replace("ô", "o")
    game_name = game_name.replace("ú", "u").replace("ù", "u").replace("û", "u")
    game_name = game_name.replace("ý", "y").replace("ÿ", "y")

    game_name = re.sub(r'[^a-z0-9\s\-]', '', game_name)
    game_name = re.sub(r'\s+', '-', game_name)
    game_name = re.sub(r'-+', '-', game_name)
    game_name = game_name.strip('-')

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

    if len(game_name.split('-')) > 4:
        known_series = ["the-witcher", "the-elder-scrolls", "metal-gear-solid",
                        "final-fantasy", "assassins-creed", "star-wars", "call-of-duty"]

        for series in known_series:
            if game_name.startswith(series):
                parts = game_name.split('-')
                series_parts = series.split('-')
                game_name = '-'.join(parts[:len(series_parts) + 2])
                logging.debug(f"Сокращено длинное название серии: {game_name}")
                break

    if game_name in special_cases:
        game_name = special_cases[game_name]

    logging.debug(f"Преобразовано название игры: '{original_name}' -> '{game_name}'")

    url += game_name

    delay = REQUEST_DELAY + random.uniform(0.0, 2.0)
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

                game_title_elem = soup.select_one('div.c-productHero_title h1, h1.c-productHero_title, div.product_title h1, h1')

                if not game_title_elem:
                    page_title = soup.title.text if soup.title else ""
                    if game_name.lower() in page_title.lower() and "metacritic" in page_title.lower():
                        logging.info(f"Страница существует, но заголовок не найден через селекторы: {url}")
                        game_title_elem = True
                        page_text = soup.text

                        metascore_match = re.search(r'Metascore\s+(\d{1,2}|100)\s', page_text)
                        if metascore_match:
                            try:
                                metascore = int(metascore_match.group(1))
                                if 0 <= metascore <= 100:
                                    logging.info(f"Найден Metascore в заголовке страницы: {metascore}")
                            except ValueError:
                                pass

                        userscore_match = re.search(r'User\s+Score\s+(\d\.\d)\s', page_text)
                        if userscore_match:
                            try:
                                userscore = float(userscore_match.group(1))
                                if 0.0 <= userscore <= 10.0:
                                    logging.info(f"Найден User Score в заголовке страницы: {userscore}")
                            except ValueError:
                                pass
                    else:
                        logging.warning(f"Страница не содержит информацию об игре: {url}")
                        return None

                metascore = None
                html_text = str(soup)
                metascore_patterns = [
                    r'Metascore\s+(\d{1,2}|100)\s',
                    r'Metascore\s+Generally\s+Favorable\s+\[Based\s+on\s+\d+\s+Critic\s+Reviews\]\s+(\d{1,2}|100)',
                    r'Metascore\s+Universal\s+Acclaim\s+\[Based\s+on\s+\d+\s+Critic\s+Reviews\]\s+(\d{1,2}|100)',
                    r'Metascore\s+Mixed\s+\[Based\s+on\s+\d+\s+Critic\s+Reviews\]\s+(\d{1,2}|100)',
                    r'Metascore\s+Generally\s+Unfavorable\s+\[Based\s+on\s+\d+\s+Critic\s+Reviews\]\s+(\d{1,2}|100)',
                    r'Metascore\s+Overwhelming\s+Dislike\s+\[Based\s+on\s+\d+\s+Critic\s+Reviews\]\s+(\d{1,2}|100)'
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

                userscore_patterns = [
                    r'User\s+Score\s+(\d\.\d)\s',
                    r'User\s+Score\s+Generally\s+Favorable\s+\[Based\s+on\s+\d+\s+User\s+Ratings\]\s+(\d\.\d)',
                    r'User\s+Score\s+Universal\s+Acclaim\s+\[Based\s+on\s+\d+\s+User\s+Ratings\]\s+(\d\.\d)',
                    r'User\s+Score\s+Mixed\s+\[Based\s+on\s+\d+\s+User\s+Ratings\]\s+(\d\.\d)',
                    r'User\s+Score\s+Generally\s+Unfavorable\s+\[Based\s+on\s+\d+\s+User\s+Ratings\]\s+(\d\.\d)',
                    r'User\s+Score\s+Overwhelming\s+Dislike\s+\[Based\s+on\s+\d+\s+User\s+Ratings\]\s+(\d\.\d)'
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

                if metascore is None or userscore is None:
                    metascore_blocks = soup.select('div[class*="metascore"], span[class*="metascore"], div.c-metascore, span.c-metascore')
                    userscore_blocks = soup.select('div[class*="userscore"], span[class*="userscore"], div.c-userscore, span.c-userscore')
                    if metascore is None:
                        for block in metascore_blocks:
                            block_text = block.text.strip()
                            if re.match(r'^\d{1,2}$|^100$', block_text):
                                try:
                                    score = int(block_text)
                                    if 0 <= score <= 100:
                                        metascore = score
                                        logging.info(f"Найден Metascore в блоке: {metascore}")
                                        break
                                except ValueError:
                                    continue
                    if userscore is None:
                        for block in userscore_blocks:
                            block_text = block.text.strip()
                            if re.match(r'^\d\.\d$', block_text):
                                try:
                                    score = float(block_text)
                                    if 0.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден User Score в блоке: {userscore}")
                                        break
                                except ValueError:
                                    continue

                metascore_text = None
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
                        if '.' in metascore_text:
                            metascore = int(float(metascore_text) * 10)
                        else:
                            metascore = int(metascore_text)
                    except ValueError:
                        pass

                if metascore is None:
                    metascore_selectors = [
                        # Дизайн 2025
                        'div[class*="metascore"]',
                        'span[class*="metascore"]',
                        'div.c-productHero_metascore',
                        'span.c-productHero_metascore',
                        # Специфичные селекторы для дизайна 2025
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
                                    if '.' in text:
                                        text = re.sub(r'[^\d\.]', '', text)
                                        if text:
                                            metascore = int(float(text) * 10)
                                            break
                                    else:
                                        text = re.sub(r'[^\d]', '', text)
                                        if text:
                                            metascore = int(text)
                                            break
                                except ValueError:
                                    continue
                        if metascore is not None:
                            break

                userscore = None
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

                if userscore is None:
                    userscore_selectors = [
                        # Дизайн 2025
                        'div[class*="userscore"]',
                        'span[class*="userscore"]',
                        'div.c-productHero_userscore',
                        'span.c-productHero_userscore',
                        # Специфичные селекторы для дизайна 2025
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
                                    text = re.sub(r'[^\d\.]', '', text)
                                    if text:
                                        userscore = float(text)
                                        break
                                except ValueError:
                                    continue
                        if userscore is not None:
                            break

                if metascore is None or userscore is None:
                    category_blocks = soup.select('div.c-productScoreInfo, div.c-productHero_scoreInfo, div[class*="score"], div[class*="Score"]')
                    for block in category_blocks:
                        block_text = block.text
                        if metascore is None and ('Metascore' in block_text or 'Critics' in block_text):
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
                        if userscore is None and ('User Score' in block_text or 'User' in block_text and 'Rating' in block_text):
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

                if metascore is None:
                    metascore_elements = soup.select('[class*="metascore"]')
                    for elem in metascore_elements:
                        elem_text = elem.text.strip()
                        if re.match(r'^\d+$', elem_text) and 60 <= int(elem_text) <= 100:
                            metascore = int(elem_text)
                            logging.info(f"Найден Metascore в HTML-элементе: {metascore}")
                            break

                if userscore is None:
                    userscore_elements = soup.select('[class*="userscore"]')
                    for elem in userscore_elements:
                        elem_text = elem.text.strip()
                        if re.match(r'^\d+\.\d+$', elem_text) and 6.0 <= float(elem_text) <= 10.0:
                            userscore = float(elem_text)
                            logging.info(f"Найден User Score в HTML-элементе: {userscore}")
                            break

                if metascore is None:
                    html_metascore_patterns = [
                        r'Metascore.*?(\d+).*?Based on \d+ Critic Reviews',
                        r'Metascore.*?Generally Favorable.*?(\d+)',
                        r'Metascore.*?Universal Acclaim.*?(\d+)',
                        r'Metascore.*?Mixed.*?(\d+)',
                        r'Metascore.*?Generally Unfavorable.*?(\d+)',
                        r'Metascore.*?Overwhelming Dislike.*?(\d+)',
                        r'Metascore.*?Based\s+on\s+\d+\s+Critic\s+Reviews.*?(\d+)',
                        r'Metascore.*?Based\s+on.*?(\d+)',
                        r'Metascore.*?(\d+)'
                    ]

                    for pattern in html_metascore_patterns:
                        matches = re.findall(pattern, str(soup), re.IGNORECASE)
                        if matches:
                            for match in matches:
                                try:
                                    score = int(match)
                                    if 0 <= score <= 100:
                                        context_pattern = r'[^>]*' + re.escape(match) + r'[^<]*'
                                        context_matches = re.findall(context_pattern, str(soup))

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

                    for pattern in html_userscore_patterns:
                        matches = re.findall(pattern, str(soup), re.IGNORECASE)
                        if matches:
                            for match in matches:
                                try:
                                    score = float(match)
                                    if 0 <= score <= 10:
                                        context_pattern = r'[^>]*' + re.escape(match) + r'[^<]*'
                                        context_matches = re.findall(context_pattern, str(soup))

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

                if metascore is None or userscore is None:
                    review_blocks = soup.select('div.c-reviewsSection, div[class*="review"], div[class*="Review"], section[class*="review"], section[class*="Review"]')

                    for block in review_blocks:
                        block_text = block.text

                        critic_headers = ['Critic Reviews', 'Critics', 'Critic', 'Professional Reviews']
                        user_headers = ['User Reviews', 'Users', 'User', 'Player Reviews']

                        if metascore is None and any(header in block_text for header in critic_headers):
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

                        if userscore is None and any(header in block_text for header in user_headers):
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

                if metascore is None:
                    text = soup.text

                    metascore_keywords = [
                        "Metascore Generally Favorable",
                        "Metascore Universal Acclaim",
                        "Metascore Mixed",
                        "Metascore Generally Unfavorable",
                        "Metascore Based on",
                        "Based on Critic Reviews",
                        "Metascore"
                    ]

                    metascore_keywords.sort(key=len, reverse=True)

                    for keyword in metascore_keywords:
                        keyword_pos = text.find(keyword)
                        if keyword_pos != -1:
                            context = text[max(0, keyword_pos - 20):min(len(text), keyword_pos + 200)]

                            number_matches = re.findall(r'(?:^|\s)(\d{1,3})(?:\s|$)', context)
                            number_matches.sort(key=lambda x: abs(context.find(x) - context.find(keyword)))

                            for match in number_matches:
                                try:
                                    score = int(match)
                                    if 60 <= score <= 100:
                                        if not (match.startswith('19') or match.startswith('20')):
                                            metascore = score
                                            logging.info(f"Найден вероятный Metascore в тексте: {metascore} (контекст: {keyword})")
                                            break
                                except ValueError:
                                    continue
                        if metascore is not None:
                            break

                if userscore is None:
                    text = soup.text
                    userscore_keywords = [
                        "User Score Generally Favorable",
                        "User Score Universal Acclaim",
                        "User Score Mixed",
                        "User Score Generally Unfavorable",
                        "User Score Based on",
                        "Based on User Ratings",
                        "User Score"
                    ]

                    userscore_keywords.sort(key=len, reverse=True)

                    for keyword in userscore_keywords:
                        keyword_pos = text.find(keyword)
                        if keyword_pos != -1:
                            context = text[max(0, keyword_pos - 20):min(len(text), keyword_pos + 200)]
                            number_matches = re.findall(r'(?:^|\s)(\d+\.\d+)(?:\s|$)', context)
                            number_matches.sort(key=lambda x: abs(context.find(x) - context.find(keyword)))

                            for match in number_matches:
                                try:
                                    score = float(match)
                                    if 6.0 <= score <= 10.0:
                                        userscore = score
                                        logging.info(f"Найден вероятный User Score в тексте: {userscore} (контекст: {keyword})")
                                        break
                                except ValueError:
                                    continue
                        if userscore is not None:
                            break

                if metascore is None and userscore is None:
                    release_date = None
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

                    if not release_date:
                        release_date_selectors = [
                            # Дизайн 2025
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

                if metascore is not None:
                    if metascore < 10:
                        metascore = metascore * 10
                    elif metascore > 100:
                        logging.warning(f"Найден некорректный Metascore: {metascore}. Игнорируем.")
                        metascore = None

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

    def generate_name_variants(name, original):
        variants = []
        variants.append((name, "основной вариант"))
        if ":" in original:
            base_name = original.split(":")[0].strip()
            base_name = base_name.lower()
            base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
            base_name = re.sub(r'\s+', '-', base_name)
            variants.append((base_name, "без части после двоеточия"))

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

        if "-" in name:
            jp_name = name.split("-")[0].strip()
            variants.append((jp_name, "до первого тире"))

        if len(words) > 1:
            first_word = words[0].lower()
            first_word = re.sub(r'[^a-z0-9\-]', '', first_word)
            variants.append((first_word, "только первое слово"))

        # Определяем индикаторы DLC вне условия
        dlc_indicators = ["dlc", "expansion", "addon", "add-on", "shadow of", "part ii", "part 2"]
        if any(indicator in original.lower() for indicator in dlc_indicators):
            base_name = re.split(r'[:\-]', original)[0].strip()
            base_name = base_name.lower()
            base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
            base_name = re.sub(r'\s+', '-', base_name)
            variants.append((base_name, "базовая игра для DLC"))

        unique_variants = []
        seen = set()
        for variant, desc in variants:
            if variant not in seen and variant:
                seen.add(variant)
                unique_variants.append((variant, desc))

        return unique_variants

    variants = generate_name_variants(game_name, original_name)

    for variant_name, variant_desc in variants:
        variant_url = f"https://www.metacritic.com/game/{variant_name}/"

        logging.info(f"Пробуем вариант '{variant_desc}': {variant_url}")

        if variant_name != game_name:
            time.sleep(1)

        result = try_fetch_metacritic(variant_url, variant_name, platform_name)
        if result:
            if variant_desc != "основной вариант":
                result["note"] = f"Найдено по варианту: {variant_desc}"
            search_cache[cache_key] = result
            return result

    if platform in ["PlayStation 5", "PS5", "Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
        prev_gen_platform = None
        if platform in ["PlayStation 5", "PS5"]:
            prev_gen_platform = "PlayStation 4"
        elif platform in ["Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
            prev_gen_platform = "Xbox One"

        if prev_gen_platform:
            alt_url = f"https://www.metacritic.com/game/{game_name}/"

            logging.info(f"Пробуем найти игру на предыдущем поколении консолей ({prev_gen_platform}): {alt_url}")
            time.sleep(1)

            result = try_fetch_metacritic(alt_url, game_name, prev_gen_platform)
            if result:
                result["note"] = f"Оценка для {prev_gen_platform}"
                search_cache[cache_key] = result
                return result

    search_cache[cache_key] = None
    return None

def update_metacritic_data(reset_index=False):
    """Обновляет данные Metacritic для игр."""
    metacritic_data = load_metacritic_data()

    # Если указан флаг reset_index, сбрасываем индекс
    if reset_index:
        metacritic_data['last_processed_index'] = 0
        logging.info("Сбрасываем индекс последней обработанной игры на 0 (начинаем сначала)")

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
    last_processed_index = metacritic_data.get('last_processed_index', 0)

    if last_processed_index > 0 and last_processed_index < total_games:
        logging.info(f"Продолжаем обновление данных Metacritic с индекса {last_processed_index} (всего игр: {total_games})")
    else:
        last_processed_index = 0
        logging.info(f"Начинаем обновление данных Metacritic для {total_games} игр...")

    # all_games.sort(key=lambda g: g.get('id', 0))
    # Обрабатываем игры в том порядке, в котором они находятся в файлах

    for i, game in enumerate(all_games[last_processed_index:], start=last_processed_index):
        game_id = str(game.get('id'))
        game_name = game.get('name')

        if not isinstance(game_name, str):
            logging.warning(f"Пропускаем игру с ID {game_id}: название игры не является строкой ({type(game_name)})")
            processed_games += 1
            error_games += 1
            continue

        # Определяем, завершен ли полный цикл сбора данных
        is_first_run = not metacritic_data.get('full_cycle_complete', False)

        # Если это первый запуск, пропускаем проверку на старые/не старые игры
        if is_first_run and processed_games == 0 and game_id in metacritic_data['games']:
            logging.info(f"Полный цикл сбора данных еще не завершен. Пропускаем проверку на старые/не старые игры.")

        if game_id in metacritic_data['games'] and not is_first_run:
            game_data = metacritic_data['games'][game_id]
            last_updated = game_data.get('timestamp', '')
            note = game_data.get('note', '')
            release_date = None
            if 'release_date' in game:
                try:
                    release_date = datetime.fromisoformat(game['release_date'].replace('Z', '+00:00'))
                except Exception as e:
                    logging.debug(f"Не удалось распарсить дату релиза для игры {game_name}: {e}")

            first_updated = game_data.get('first_updated')

            if (game_data.get('metascore') is not None or game_data.get('userscore') is not None) and last_updated:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days

                    if not first_updated:
                        if release_date and (datetime.now() - release_date).days >= 60:
                            game_data['first_updated'] = last_updated
                            game_data['no_more_updates'] = True
                            metacritic_data['games'][game_id] = game_data
                            skipped_games += 1
                            processed_games += 1
                            logging.info(f"Игра {game_name} (ID: {game_id}) вышла более 60 дней назад, больше не будем проверять")
                            continue
                        elif release_date and (datetime.now() - release_date).days < 59:
                            if days_since_update < 30:
                                skipped_games += 1
                                processed_games += 1
                                logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), данные обновлены {days_since_update} дней назад")
                                continue
                            else:
                                game_data['first_updated'] = datetime.now().isoformat()
                        else:
                            game_data['first_updated'] = last_updated
                            game_data['no_more_updates'] = True
                            metacritic_data['games'][game_id] = game_data
                            skipped_games += 1
                            processed_games += 1
                            logging.info(f"Игра {game_name} (ID: {game_id}) вышла 59-60 дней назад, больше не будем проверять")
                            continue
                    elif game_data.get('no_more_updates'):
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), больше не требует обновлений")
                        continue
                    else:
                        if days_since_update < 30:
                            skipped_games += 1
                            processed_games += 1
                            logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), данные обновлены {days_since_update} дней назад")
                            continue
                        else:
                            game_data['no_more_updates'] = True
                            metacritic_data['games'][game_id] = game_data
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")

            elif note == "Не найдено на Metacritic" and last_updated and not is_first_run:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    check_count = game_data.get('check_count', 0)

                    if check_count >= 3:
                        game_data['no_more_updates'] = True
                        metacritic_data['games'][game_id] = game_data
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), не найдена на Metacritic после 3 проверок")
                        continue
                    elif check_count == 2:
                        if days_since_update < 60:
                            skipped_games += 1
                            processed_games += 1
                            logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), не найдена на Metacritic, ждем 60 дней для 3-й проверки, прошло {days_since_update} дней")
                            continue
                        else:
                            game_data['check_count'] = check_count + 1
                            metacritic_data['games'][game_id] = game_data
                            logging.info(f"Выполняем 3-ю проверку для игры {game_name} (ID: {game_id}), не найденной на Metacritic")
                    elif check_count == 1:
                        if days_since_update < 30:
                            skipped_games += 1
                            processed_games += 1
                            logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), не найдена на Metacritic, ждем 30 дней для 2-й проверки, прошло {days_since_update} дней")
                            continue
                        else:
                            game_data['check_count'] = check_count + 1
                            metacritic_data['games'][game_id] = game_data
                            logging.info(f"Выполняем 2-ю проверку для игры {game_name} (ID: {game_id}), не найденной на Metacritic")
                    else:
                        if days_since_update < 30:
                            skipped_games += 1
                            processed_games += 1
                            logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), не найдена на Metacritic, ждем 30 дней для 1-й проверки, прошло {days_since_update} дней")
                            continue
                        else:
                            game_data['check_count'] = check_count + 1
                            metacritic_data['games'][game_id] = game_data
                            logging.info(f"Выполняем 1-ю проверку для игры {game_name} (ID: {game_id}), не найденной на Metacritic")
                except Exception as e:
                    logging.warning(f"Не удалось распарсить дату обновления для игры {game_name}: {e}")

            elif "Игра еще не вышла" in note and last_updated and not is_first_run:
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

            elif note == "Страница существует, но оценки не найдены" and last_updated and not is_first_run:
                try:
                    last_updated_date = datetime.fromisoformat(last_updated)
                    days_since_update = (datetime.now() - last_updated_date).days
                    if days_since_update < 30:
                        skipped_games += 1
                        processed_games += 1
                        logging.debug(f"Пропускаем игру {game_name} (ID: {game_id}), страница без оценок, проверено {days_since_update} дней назад")
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
                    platform_info = metacritic_result.get('platform', highest_priority_platform)
                    metascore = metacritic_result.get('metascore')
                    userscore = metacritic_result.get('userscore')
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
            has_note = 'note' in metacritic_result
            has_scores = metacritic_result.get('metascore') is not None or metacritic_result.get('userscore') is not None
            if game_id in metacritic_data['games'] and metacritic_data['games'][game_id].get('no_more_updates'):
                metacritic_result['no_more_updates'] = True
            if game_id in metacritic_data['games'] and metacritic_data['games'][game_id].get('first_updated'):
                metacritic_result['first_updated'] = metacritic_data['games'][game_id]['first_updated']
            if game_id in metacritic_data['games'] and metacritic_data['games'][game_id].get('check_count') is not None:
                metacritic_result['check_count'] = metacritic_data['games'][game_id]['check_count']

            metacritic_data['games'][game_id] = metacritic_result

            if has_scores or has_note:
                updated_games += 1
                if has_scores:
                    logging.info(f"Данные Metacritic для игры {game_name} успешно обновлены")
                else:
                    logging.info(f"Данные Metacritic для игры {game_name} обновлены: {metacritic_result.get('note')}")
            else:
                error_games += 1
                logging.warning(f"Получены пустые данные для игры {game_name}")
        else:
            error_games += 1
            new_data = {
                "name": game_name,
                "metascore": None,
                "userscore": None,
                "url": None,
                "timestamp": datetime.now().isoformat(),
                "note": "Не найдено на Metacritic",
                "check_count": 0
            }

            if game_id in metacritic_data['games'] and metacritic_data['games'][game_id].get('first_updated'):
                new_data['first_updated'] = metacritic_data['games'][game_id]['first_updated']

            metacritic_data['games'][game_id] = new_data
            logging.info(f"Добавлена запись о неудачном поиске для игры {game_name}")

        processed_games += 1

        if processed_games % 10 == 0:
            metacritic_data['last_updated'] = datetime.now().isoformat()
            metacritic_data['total_games'] = len(metacritic_data['games'])
            metacritic_data['last_processed_index'] = i
            save_metacritic_data(metacritic_data)

            games_with_metascore = 0
            games_with_userscore = 0
            games_with_both_scores = 0
            games_not_released = 0
            games_no_scores = 0
            games_not_found = 0
            games_no_more_updates = 0

            for game_data in metacritic_data['games'].values():
                has_metascore = game_data.get('metascore') is not None
                has_userscore = game_data.get('userscore') is not None
                note = game_data.get('note', '')
                no_more_updates = game_data.get('no_more_updates', False)

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

                if no_more_updates:
                    games_no_more_updates += 1

            logging.info(f"Промежуточное сохранение: обработано {processed_games}/{total_games} игр")
            logging.info(f"Статистика собранных данных:")
            logging.info(f"  - Всего игр в базе: {len(metacritic_data['games'])}")
            logging.info(f"  - Игр с оценкой критиков: {games_with_metascore}")
            logging.info(f"  - Игр с оценкой пользователей: {games_with_userscore}")
            logging.info(f"  - Игр с обеими оценками: {games_with_both_scores}")
            logging.info(f"  - Игр, которые еще не вышли: {games_not_released}")
            logging.info(f"  - Игр без оценок: {games_no_scores}")
            logging.info(f"  - Игр, не найденных на Metacritic: {games_not_found}")
            logging.info(f"  - Игр, которые больше не требуют обновлений: {games_no_more_updates}")

    metacritic_data['last_updated'] = datetime.now().isoformat()
    metacritic_data['total_games'] = len(metacritic_data['games'])

    # Проверяем, завершен ли полный цикл сбора данных
    if last_processed_index + processed_games >= total_games:
        logging.info(f"Завершен полный цикл сбора данных. Можно начинать проверку на старые/не старые игры.")
        # Добавляем флаг, что завершен полный цикл сбора данных
        metacritic_data['full_cycle_complete'] = True
    elif not metacritic_data.get('full_cycle_complete', False):
        logging.info(f"Полный цикл сбора данных еще не завершен: обработано {last_processed_index + processed_games}/{total_games} игр. Продолжаем сбор данных.")
        # Не меняем флаг, если он уже установлен в True
        if 'full_cycle_complete' not in metacritic_data:
            metacritic_data['full_cycle_complete'] = False

    if processed_games >= total_games or requests_count >= MAX_REQUESTS_PER_RUN:
        metacritic_data['last_processed_index'] = 0
        logging.info("Обработка завершена или достигнут лимит запросов. Сбрасываем индекс для следующего запуска.")
    else:
        metacritic_data['last_processed_index'] = last_processed_index + processed_games
        logging.info(f"Сохраняем индекс {metacritic_data['last_processed_index']} для продолжения в следующий раз.")

    save_metacritic_data(metacritic_data)

    games_with_metascore = 0
    games_with_userscore = 0
    games_with_both_scores = 0
    games_not_released = 0
    games_no_scores = 0
    games_not_found = 0
    games_no_more_updates = 0

    for game_data in metacritic_data['games'].values():
        has_metascore = game_data.get('metascore') is not None
        has_userscore = game_data.get('userscore') is not None
        note = game_data.get('note', '')
        no_more_updates = game_data.get('no_more_updates', False)

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

        if no_more_updates:
            games_no_more_updates += 1

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
    logging.info(f"  - Игр, которые больше не требуют обновлений: {games_no_more_updates}")

def main():
    """Основная функция скрипта."""
    # Проверяем аргументы командной строки
    if len(sys.argv) > 1 and sys.argv[1] == "--reset":
        # Удаляем файл с данными Metacritic, если он существует
        if os.path.exists(METACRITIC_DATA_FILE):
            try:
                os.remove(METACRITIC_DATA_FILE)
                logging.info(f"Файл {METACRITIC_DATA_FILE} успешно удален. Данные будут собраны заново.")
            except Exception as e:
                logging.error(f"Не удалось удалить файл {METACRITIC_DATA_FILE}: {e}")
                sys.exit(1)
        else:
            logging.info(f"Файл {METACRITIC_DATA_FILE} не существует. Данные будут собраны заново.")

    logging.info("Начинаем сбор данных с Metacritic...")
    try:
        # Если был указан флаг --reset, передаем его в функцию update_metacritic_data
        reset_index = len(sys.argv) > 1 and sys.argv[1] == "--reset"
        update_metacritic_data(reset_index=reset_index)
        logging.info("Сбор данных с Metacritic завершен успешно.")
    except Exception as e:
        logging.error(f"Произошла ошибка при сборе данных с Metacritic: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
