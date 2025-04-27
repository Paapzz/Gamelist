import os
import json
import math
import requests
import time

ALLOWED_PLATFORMS = [6, 167, 169, 130, 48, 49, 41, 9, 12, 5, 37, 46, 8, 11, 20, 7, 14, 3, 162, 165]
BATCH_SIZE = 500
GAMES_PER_FILE = 5000

def get_access_token():
    client_id = os.environ['IGDB_CLIENT_ID']
    client_secret = os.environ['IGDB_CLIENT_SECRET']
    url = f"https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials"
    resp = requests.post(url)
    resp.raise_for_status()
    return resp.json()['access_token']

def fetch_games(token, offset):
    url = "https://api.igdb.com/v4/games"
    headers = {
        "Client-ID": os.environ['IGDB_CLIENT_ID'],
        "Authorization": f"Bearer {token}"
    }
    body = f"""
        fields id,name,cover.url,first_release_date,genres.name,platforms.id,aggregated_rating,total_rating,total_rating_count,rating,rating_count,category,status;
        where platforms != null & id >= {offset} & id < {offset + BATCH_SIZE};
        limit {BATCH_SIZE};
    """
    resp = requests.post(url, headers=headers, data=body)
    if resp.status_code == 429:
        print("Rate limit! Waiting 10 seconds...")
        time.sleep(10)
        return fetch_games(token, offset)
    resp.raise_for_status()
    return resp.json()

def allowed_platforms(game):
    if not game.get('platforms'):
        return False
    return any(pid in ALLOWED_PLATFORMS for pid in [p['id'] if isinstance(p, dict) else p for p in game['platforms']])

def is_main_game(game):
    return (
        game.get('category') == 0 and
        game.get('status') in (0, 4) and
        (game.get('rating_count', 0) >= 12 or game.get('total_rating_count', 0) >= 12)
    )

def get_average_rating(game):
    agg = game.get('aggregated_rating')
    rat = game.get('rating')

    if agg is not None:
        agg = round(agg)
    if rat is not None:
        rat = round(rat)

    if agg and rat:
        return (agg + rat) / 2
    return agg or rat or 0

def load_metacritic_data():
    """Загружает данные Metacritic из файла."""
    metacritic_file = 'meta_data/metacritic_ratings.json'
    if os.path.exists(metacritic_file):
        try:
            with open(metacritic_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Ошибка при загрузке данных Metacritic: {e}")
            return {"games": {}, "last_updated": "", "total_games": 0}
    else:
        print(f"Файл с данными Metacritic не найден: {metacritic_file}")
        return {"games": {}, "last_updated": "", "total_games": 0}

def get_metacritic_score(game, metacritic_data):
    """Получает оценку Metacritic для игры."""
    game_id = str(game.get('id'))
    if game_id in metacritic_data['games']:
        mc_data = metacritic_data['games'][game_id]
        metascore = mc_data.get('metascore')
        userscore = mc_data.get('userscore')

        if metascore is not None or userscore is not None:
            if metascore is not None and userscore is not None:
                return round(metascore * 0.3 + (userscore * 10) * 0.7)
            elif metascore is not None:
                return metascore
            else:
                return round(userscore * 10)
    return None

def get_weighted_rating(game, metacritic_data=None):
    metacritic_score = None
    if metacritic_data:
        metacritic_score = get_metacritic_score(game, metacritic_data)

    if metacritic_score is not None:
        avg_rating = get_average_rating(game)
        if avg_rating > 0:
            combined_rating = round(metacritic_score * 0.7 + avg_rating * 0.3)
        else:
            combined_rating = metacritic_score

        total_count = game.get('total_rating_count', 0) or game.get('rating_count', 0) or 0
        count_factor = math.log10(total_count + 1) * 25
        review_penalty = 60 / (math.sqrt(total_count) + 1)
        rating_bonus = (combined_rating - 60) * 0.4 if combined_rating > 80 else 0
        very_low_ratings_penalty = 15 - total_count if total_count < 10 else 0

        special_adjustment = 0
        if combined_rating >= 80 and total_count >= 500:
            special_adjustment += 5
        if combined_rating >= 90 and total_count < 35:
            special_adjustment -= 5
        metacritic_bonus = 10

        return combined_rating + count_factor + rating_bonus - review_penalty - very_low_ratings_penalty + special_adjustment + metacritic_bonus

    avg_rating = get_average_rating(game)
    total_count = game.get('total_rating_count', 0) or game.get('rating_count', 0) or 0
    base_rating = avg_rating
    count_factor = math.log10(total_count + 1) * 25
    review_penalty = 60 / (math.sqrt(total_count) + 1)
    rating_bonus = (avg_rating - 60) * 0.4 if avg_rating > 80 else 0
    very_low_ratings_penalty = 15 - total_count if total_count < 10 else 0

    special_adjustment = 0
    if avg_rating >= 80 and total_count >= 500:
        special_adjustment += 5
    if avg_rating >= 90 and total_count < 35:
        special_adjustment -= 7

    return base_rating + count_factor + rating_bonus - review_penalty - very_low_ratings_penalty + special_adjustment

def main():
    token = get_access_token()
    print("Access token received.")

    print("Checking for Metacritic data...")
    metacritic_data = load_metacritic_data()
    has_metacritic = bool(metacritic_data and metacritic_data.get('games'))

    if has_metacritic:
        print(f"Found Metacritic data with {metacritic_data.get('total_games', 0)} games.")
    else:
        print("No Metacritic data found. Will use only IGDB ratings.")

    all_games = []
    offset = 0
    max_id = 400000
    while offset < max_id:
        print(f"Fetching games {offset} - {offset+BATCH_SIZE}")
        games = fetch_games(token, offset)
        if not games:
            break
        for game in games:
            if allowed_platforms(game):
                if 'rating' in game and game['rating'] is not None:
                    game['rating'] = round(game['rating'])
                if 'aggregated_rating' in game and game['aggregated_rating'] is not None:
                    game['aggregated_rating'] = round(game['aggregated_rating'])
                if 'total_rating' in game and game['total_rating'] is not None:
                    game['total_rating'] = round(game['total_rating'])

                all_games.append(game)
        offset += BATCH_SIZE
        time.sleep(0.4)

    print(f"Total games fetched: {len(all_games)}")

    main_games = [g for g in all_games if is_main_game(g)]
    other_games = [g for g in all_games if not is_main_game(g)]

    if has_metacritic:
        print("Sorting games using Metacritic ratings...")
        main_games.sort(key=lambda g: (
            -get_weighted_rating(g, metacritic_data),
            g.get('status', 0)
        ))

        other_games.sort(key=lambda g: (
            -get_weighted_rating(g, metacritic_data),
            g.get('status', 0)
        ))
    else:
        print("Sorting games using only IGDB ratings...")
        main_games.sort(key=lambda g: (
            -get_weighted_rating(g),
            g.get('status', 0)
        ))

        other_games.sort(key=lambda g: (
            -get_weighted_rating(g),
            g.get('status', 0)
        ))

    all_sorted = main_games + other_games

    os.makedirs('data', exist_ok=True)
    os.makedirs('meta_data', exist_ok=True)
    total_files = math.ceil(len(all_sorted) / GAMES_PER_FILE)
    for i in range(total_files):
        start = i * GAMES_PER_FILE
        end = min((i + 1) * GAMES_PER_FILE, len(all_sorted))
        with open(f'data/games_{i+1}.json', 'w', encoding='utf-8') as f:
            json.dump(all_sorted[start:end], f, ensure_ascii=False)
        print(f"Saved data/games_{i+1}.json ({end-start} games)")

    search_index = [
        [
            f"id{game['id']}",
            f"name{game['name']}",
            f"date{game.get('first_release_date', 0)}",
            f"file{i // GAMES_PER_FILE + 1}"
        ]
        for i, game in enumerate(all_sorted)
    ]
    with open('data/search_index.json', 'w', encoding='utf-8') as f:
        json.dump(search_index, f, ensure_ascii=False)
    print("Saved data/search_index.json (optimized format)")

    index = {
        "total_games": len(all_sorted),
        "total_files": total_files,
        "last_updated": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        "main_games": len(main_games),
        "other_games": len(other_games),
        "index_format": {
            "0": "id",
            "1": "name",
            "2": "date",
            "3": "file"
        },
        "games_per_file": GAMES_PER_FILE
    }
    with open('data/index.json', 'w', encoding='utf-8') as f:
        json.dump(index, f, ensure_ascii=False)
    print("Saved data/index.json")

if __name__ == "__main__":
    main()
