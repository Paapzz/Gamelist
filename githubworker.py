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

def get_enum_id(field):
    if isinstance(field, dict):
        return field.get('id')
    return field

def is_main_game(game):
    cat_id = get_enum_id(game.get('category'))
    status_id = get_enum_id(game.get('status'))
    return (
        cat_id == 0 and
        status_id in (0, 4) and
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

def create_all_games_file(total_files):
    print(f"Starting to combine {total_files} files into all_games.json")
    all_games = []

    for i in range(1, total_files + 1):
        file_path = f'data/games_{i}.json'
        print(f"Processing file {i}/{total_files}: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                games = json.load(f)
                print(f"Loaded {len(games)} games from {file_path}")
                all_games.extend(games)
        except Exception as e:
            print(f"Error loading file {file_path}: {e}")

    all_games_path = 'data/all_games.json'
    print(f"Saving {len(all_games)} games to {all_games_path}")

    new_all_games = json.dumps(all_games, ensure_ascii=False)
    old_all_games = None
    if os.path.exists(all_games_path):
        with open(all_games_path, 'r', encoding='utf-8') as f:
            old_all_games = f.read()
    if old_all_games != new_all_games:
        with open(all_games_path, 'w', encoding='utf-8') as f:
            f.write(new_all_games)
        print(f"Successfully created {all_games_path} with {len(all_games)} games")
    else:
        print(f"No changes for {all_games_path}, skipping write.")

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
            get_enum_id(g.get('status', 0))
        ))

        other_games.sort(key=lambda g: (
            -get_weighted_rating(g, metacritic_data),
            get_enum_id(g.get('status', 0))
        ))
    else:
        print("Sorting games using only IGDB ratings...")
        main_games.sort(key=lambda g: (
            -get_weighted_rating(g),
            get_enum_id(g.get('status', 0))
        ))

        other_games.sort(key=lambda g: (
            -get_weighted_rating(g),
            get_enum_id(g.get('status', 0))
        ))

    all_sorted = main_games + other_games

    os.makedirs('data', exist_ok=True)
    os.makedirs('meta_data', exist_ok=True)
    total_files = math.ceil(len(all_sorted) / GAMES_PER_FILE)
    for i in range(total_files):
        start = i * GAMES_PER_FILE
        end = min((i + 1) * GAMES_PER_FILE, len(all_sorted))
        file_path = f'data/games_{i+1}.json'
        new_content = json.dumps(all_sorted[start:end], ensure_ascii=False)
        old_content = None
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                old_content = f.read()
        if old_content != new_content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Saved {file_path} ({end-start} games)")
        else:
            print(f"No changes for {file_path}, skipping write.")

    search_index = [
        [
            f"id{game['id']}",
            f"name{game['name']}",
            f"date{game.get('first_release_date', 0)}",
            f"file{i // GAMES_PER_FILE + 1}"
        ]
        for i, game in enumerate(all_sorted)
    ]
    search_index_path = 'data/search_index.json'
    new_search_index = json.dumps(search_index, ensure_ascii=False)
    old_search_index = None
    if os.path.exists(search_index_path):
        with open(search_index_path, 'r', encoding='utf-8') as f:
            old_search_index = f.read()
    if old_search_index != new_search_index:
        with open(search_index_path, 'w', encoding='utf-8') as f:
            f.write(new_search_index)
        print("Saved data/search_index.json (optimized format)")
    else:
        print("No changes for data/search_index.json, skipping write.")

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
        "games_per_file": GAMES_PER_FILE,
        "all_games_file": "all_games.json"
    }
    index_path = 'data/index.json'
    new_index = json.dumps(index, ensure_ascii=False)
    old_index = None
    if os.path.exists(index_path):
        with open(index_path, 'r', encoding='utf-8') as f:
            old_index = f.read()
    if old_index != new_index:
        with open(index_path, 'w', encoding='utf-8') as f:
            f.write(new_index)
        print("Saved data/index.json")
    else:
        print("No changes for data/index.json, skipping write.")

    print("Creating consolidated file with all games...")
    create_all_games_file(total_files)

if __name__ == "__main__":
    main()
