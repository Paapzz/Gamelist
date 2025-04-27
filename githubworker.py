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
                return json.load(f)
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных Metacritic: {e}")
            return {"games": {}, "last_updated": "", "total_games": 0}
    else:
        logging.info(f"Файл с данными Metacritic не найден: {METACRITIC_DATA_FILE}. Создаем новый.")
        return {"games": {}, "last_updated": "", "total_games": 0}

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

    url = "http://www.metacritic.com/game/"

    if platform:
        platform_map = {
            # PC
            "PC": "pc",
            "Windows": "pc",
            "Win": "pc",

            # PlayStation
            "PlayStation 5": "playstation-5",
            "PS5": "playstation-5",
            "PlayStation 4": "playstation-4",
            "PS4": "playstation-4",
            "PlayStation 3": "playstation-3",
            "PS3": "playstation-3",
            "PlayStation 2": "playstation-2",
            "PS2": "playstation-2",
            "PlayStation": "playstation",
            "PS1": "playstation",
            "PSX": "playstation",

            # Xbox
            "Xbox Series X": "xbox-series-x",
            "Xbox Series S": "xbox-series-x",
            "XSX": "xbox-series-x",
            "Xbox One": "xbox-one",
            "XONE": "xbox-one",
            "Xbox 360": "xbox-360",
            "X360": "xbox-360",
            "Xbox": "xbox",

            # Nintendo
            "Nintendo Switch": "switch",
            "Switch": "switch",
            "NSW": "switch",
            "Wii U": "wii-u",
            "Wii": "wii",
            "Nintendo 3DS": "3ds",
            "3DS": "3ds",
            "Nintendo DS": "ds",
            "NDS": "ds",
            "GameCube": "gamecube",
            "GC": "gamecube",
            "Nintendo 64": "nintendo-64",
            "N64": "nintendo-64",

            # Handhelds
            "Game Boy Advance": "game-boy-advance",
            "GBA": "game-boy-advance",
            "PlayStation Vita": "playstation-vita",
            "PS Vita": "playstation-vita",
            "PSV": "playstation-vita",
            "PSP": "psp",

            # Mobile
            "iOS": "ios",
            "iPhone": "ios",
            "iPad": "ios",
            "Android": "android",

            # Other
            "Dreamcast": "dreamcast",
            "DC": "dreamcast",
            "Stadia": "stadia",
            "Linux": "pc",
            "Mac": "mac",
            "macOS": "mac",
            "Apple Macintosh": "mac"
        }
        platform_url = platform_map.get(platform, "pc")
        url += f"{platform_url}/"
    else:
        url += "pc/"

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

    # Удаление специальных символов, но сохранение дефисов
    game_name = re.sub(r'[^a-z0-9\s\-]', '', game_name)

    # Замена пробелов на дефисы
    game_name = re.sub(r'\s+', '-', game_name)

    # Удаление лишних дефисов
    game_name = re.sub(r'-+', '-', game_name)

    # Удаление дефисов в начале и конце
    game_name = game_name.strip('-')

    # Специальная обработка для известных игр с проблемами в URL
    special_cases = {
        "marvels-spiderman-2": "marvels-spider-man-2",
        "marvels-spiderman": "marvels-spider-man",
        "okami-hd": "okami-hd",
        "metal-gear-solid-v-the-phantom-pain": "metal-gear-solid-v",
        "the-witcher-3-wild-hunt": "the-witcher-3",
        "the-elder-scrolls-v-skyrim": "the-elder-scrolls-v",
        "grand-theft-auto-v": "grand-theft-auto-v",
        "red-dead-redemption-2": "red-dead-redemption-ii",
        "the-last-of-us-part-ii": "the-last-of-us-part-ii",
        "the-last-of-us-part-2": "the-last-of-us-part-ii",
        "god-of-war-ragnarok": "god-of-war-ragnaroek",
        "elden-ring-shadow-of-the-erdtree": "elden-ring"
    }

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

    def try_fetch_metacritic(url, game_name):
        try:
            response = requests.get(url, headers=headers, timeout=10)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                metascore_elem = soup.select_one('div.metascore_w.game span')
                metascore = int(metascore_elem.text) if metascore_elem else None

                userscore_elem = soup.select_one('div.metascore_w.user.large.game')
                userscore = float(userscore_elem.text) if userscore_elem else None

                result = {
                    "name": original_name,
                    "metascore": metascore,
                    "userscore": userscore,
                    "url": url,
                    "timestamp": datetime.now().isoformat()
                }

                return result
            else:
                logging.warning(f"Ошибка при запросе к Metacritic: {response.status_code} для игры {game_name}")
                return None
        except Exception as e:
            logging.error(f"Ошибка при получении данных с Metacritic для игры {game_name}: {e}")
            return None

    # Пробуем основной вариант URL
    result = try_fetch_metacritic(url, game_name)
    if result:
        return result

    # Если основной вариант не сработал, пробуем альтернативные варианты
    # Для игр с "Spider-Man" в названии
    if "spider" in original_name.lower() or "spiderman" in original_name.lower():
        # Пробуем разные варианты написания
        variants = [
            "spider-man",
            "spiderman"
        ]

        for variant in variants:
            alt_game_name = game_name
            if "spider-man" in alt_game_name:
                alt_game_name = alt_game_name.replace("spider-man", variant)
            elif "spiderman" in alt_game_name:
                alt_game_name = alt_game_name.replace("spiderman", variant)

            if alt_game_name != game_name:
                alt_url = url.replace(game_name, alt_game_name)
                logging.info(f"Пробуем альтернативный URL: {alt_url}")

                # Добавляем небольшую задержку перед повторным запросом
                time.sleep(1)

                result = try_fetch_metacritic(alt_url, alt_game_name)
                if result:
                    return result

    # Для игр с двоеточием в названии, пробуем без части после двоеточия
    if ":" in original_name:
        base_name = original_name.split(":")[0].strip()
        base_name = base_name.lower()
        base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
        base_name = re.sub(r'\s+', '-', base_name)

        if platform:
            platform_url = platform_map.get(platform, "pc")
            alt_url = f"http://www.metacritic.com/game/{platform_url}/{base_name}"
        else:
            alt_url = f"http://www.metacritic.com/game/pc/{base_name}"

        logging.info(f"Пробуем URL без части после двоеточия: {alt_url}")

        # Добавляем небольшую задержку перед повторным запросом
        time.sleep(1)

        result = try_fetch_metacritic(alt_url, base_name)
        if result:
            return result

    # Для игр на PS5/Xbox Series X, пробуем найти их на PS4/Xbox One
    if platform in ["PlayStation 5", "PS5", "Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
        prev_gen_platform = None
        if platform in ["PlayStation 5", "PS5"]:
            prev_gen_platform = "PlayStation 4"
        elif platform in ["Xbox Series X", "Xbox Series S", "XSX", "XSS"]:
            prev_gen_platform = "Xbox One"

        if prev_gen_platform:
            platform_url = platform_map.get(prev_gen_platform, "pc")
            alt_url = f"http://www.metacritic.com/game/{platform_url}/{game_name}"

            logging.info(f"Пробуем найти игру на предыдущем поколении консолей ({prev_gen_platform}): {alt_url}")

            # Добавляем небольшую задержку перед повторным запросом
            time.sleep(1)

            result = try_fetch_metacritic(alt_url, game_name)
            if result:
                # Отмечаем, что это оценка для предыдущего поколения
                result["note"] = f"Оценка для {prev_gen_platform}"
                return result

    # Для DLC и расширений, пробуем найти базовую игру
    dlc_indicators = ["dlc", "expansion", "addon", "add-on", "shadow of", "part ii", "part 2"]
    if any(indicator in original_name.lower() for indicator in dlc_indicators):
        # Попробуем найти базовую игру, удалив все после первого двоеточия или тире
        base_name = re.split(r'[:\-]', original_name)[0].strip()
        base_name = base_name.lower()
        base_name = re.sub(r'[^a-z0-9\s\-]', '', base_name)
        base_name = re.sub(r'\s+', '-', base_name)

        if platform:
            platform_url = platform_map.get(platform, "pc")
            alt_url = f"http://www.metacritic.com/game/{platform_url}/{base_name}"
        else:
            alt_url = f"http://www.metacritic.com/game/pc/{base_name}"

        logging.info(f"Пробуем найти базовую игру для DLC: {alt_url}")

        # Добавляем небольшую задержку перед повторным запросом
        time.sleep(1)

        result = try_fetch_metacritic(alt_url, base_name)
        if result:
            # Отмечаем, что это оценка базовой игры
            result["note"] = f"Оценка для базовой игры '{base_name}'"
            return result

    # Если все попытки не удались, возвращаем None
    return None

def update_metacritic_data():
    """Обновляет данные Metacritic для игр."""
    metacritic_data = load_metacritic_data()

    all_games = load_all_games()

    total_games = len(all_games)
    processed_games = 0
    updated_games = 0
    skipped_games = 0
    error_games = 0
    requests_count = 0

    logging.info(f"Начинаем обновление данных Metacritic для {total_games} игр...")

    for game in all_games:
        game_id = str(game.get('id'))
        game_name = game.get('name')

        if not isinstance(game_name, str):
            logging.warning(f"Пропускаем игру с ID {game_id}: название игры не является строкой ({type(game_name)})")
            processed_games += 1
            error_games += 1
            continue

        if game_id in metacritic_data['games']:
            last_updated = metacritic_data['games'][game_id].get('timestamp', '')
            if last_updated:
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
                    if 'platform' not in metacritic_result:
                        metacritic_result['platform'] = highest_priority_platform
                    logging.info(f"Найдены данные Metacritic для игры {game_name} на платформе {metacritic_result.get('platform', highest_priority_platform)}")
                    if 'note' in metacritic_result:
                        logging.info(f"Примечание: {metacritic_result['note']}")
                else:
                    logging.warning(f"Не удалось найти данные Metacritic для игры {game_name} на платформе {highest_priority_platform}")

        if not metacritic_result:
            metacritic_result = get_metacritic_data(game_name)
            requests_count += 1

        if metacritic_result:
            metacritic_data['games'][game_id] = metacritic_result
            updated_games += 1
        else:
            error_games += 1

        processed_games += 1

        if processed_games % 10 == 0:
            metacritic_data['last_updated'] = datetime.now().isoformat()
            metacritic_data['total_games'] = len(metacritic_data['games'])
            save_metacritic_data(metacritic_data)
            logging.info(f"Промежуточное сохранение: обработано {processed_games}/{total_games} игр")

    metacritic_data['last_updated'] = datetime.now().isoformat()
    metacritic_data['total_games'] = len(metacritic_data['games'])

    save_metacritic_data(metacritic_data)

    logging.info(f"Обновление данных Metacritic завершено:")
    logging.info(f"Всего игр: {total_games}")
    logging.info(f"Обработано игр: {processed_games}")
    logging.info(f"Обновлено игр: {updated_games}")
    logging.info(f"Пропущено игр: {skipped_games}")
    logging.info(f"Ошибок: {error_games}")

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
