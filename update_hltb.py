import logging
from howlongtobeatpy import HowLongToBeat
import asyncio
import json
import aiohttp
from bs4 import BeautifulSoup
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def get_game_info(game_name):
    try:
        results = await HowLongToBeat().async_search(game_name)
        if results and len(results) > 0:
            game = max(results, key=lambda element: element.similarity)
            return {
                "title": game_name,
                "hltb_id": game.game_id,
                "main_story": game.main_story,
                "main_extra": game.main_extra,
                "completionist": game.completionist,
                "solo": game.solo,
                "coop": game.coop,
                "vs": game.vs
            }
    except Exception as e:
        logger.error(f"Ошибка при получении информации для {game_name}: {str(e)}")
    return None

async def read_games_from_html(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        
        soup = BeautifulSoup(content, 'html.parser')
        
        script_tags = soup.find_all('script')
        games_data = None
        for script in script_tags:
            if script.string and 'const gamesList = [' in script.string:
                games_data = script.string
                break
        
        if games_data is None:
            logger.error(f"Не удалось найти данные игр в {file_path}")
            return []
        
        json_str = re.search(r'const gamesList = (\[.*?\]);', games_data, re.DOTALL)
        if json_str:
            games_list = json.loads(json_str.group(1))
            return [game['title'] for game in games_list]
        else:
            logger.error(f"Не удалось извлечь JSON данные из {file_path}")
            return []
    except Exception as e:
        logger.error(f"Ошибка при чтении {file_path}: {str(e)}")
        return []

async def load_cache():
    try:
        with open('games_cache.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.info("Файл кэша не найден. Создаем новый.")
        return {}
    except json.JSONDecodeError:
        logger.warning("Ошибка декодирования JSON в файле кэша. Создаем новый.")
        return {}

async def save_cache(cache):
    with open('games_cache.json', 'w') as f:
        json.dump(cache, f, indent=2)

async def update_games_list():
    games = await read_games_from_html('index.html')
    logger.info(f"Прочитано {len(games)} игр из index.html")
    
    if not games:
        logger.error("Список игр пуст. Завершаем выполнение.")
        return
    
    cache = await load_cache()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for game in games:
            if game not in cache:
                tasks.append(get_game_info(game))
            else:
                logger.info(f"Используем кэшированные данные для {game}")
        
        results = await asyncio.gather(*tasks)
    
    updated_games = []
    for game, result in zip(games, results):
        if result:
            cache[game] = result
            updated_games.append(result)
        elif game in cache:
            updated_games.append(cache[game])
    
    await save_cache(cache)
    
    with open('games_list.json', 'w') as f:
        json.dump(updated_games, f, indent=2)
    
    logger.info(f"Обновлено {len(updated_games)} игр в games_list.json")

if __name__ == "__main__":
    asyncio.run(update_games_list())
