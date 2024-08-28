from howlongtobeatpy import HowLongToBeat
import asyncio
import json
import aiohttp
from bs4 import BeautifulSoup

async def get_game_info(game_name):
    results = await HowLongToBeat().async_search(game_name)
    if results and len(results) > 0:
        best_match = max(results, key=lambda element: element.similarity)
        return {
            "name": best_match.game_name,
            "image_url": best_match.game_image_url,
            "main_story": best_match.main_story,
            "main_extra": best_match.main_extra,
            "completionist": best_match.completionist
        }
    return None

async def read_games_from_html(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    soup = BeautifulSoup(content, 'html.parser')
    games_data = soup.find_all('script')[1].string
    games_list = json.loads(games_data)
    return [game['title'].replace('-', ' ') for game in games_list]

async def load_cache():
    try:
        with open('games_cache.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

async def save_cache(cache):
    with open('games_cache.json', 'w') as f:
        json.dump(cache, f, indent=2)

async def update_games_list():
    games = await read_games_from_html('index.html')
    cache = await load_cache()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for game in games:
            if game not in cache:
                tasks.append(get_game_info(game))
            else:
                print(f"Using cached data for {game}")
        
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

if __name__ == "__main__":
    asyncio.run(update_games_list())