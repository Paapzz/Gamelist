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
    if agg and rat:
        return (agg + rat) / 2
    return agg or rat or 0

def get_weighted_rating(game):
    avg_rating = get_average_rating(game)
    total_count = game.get('total_rating_count', 0) or game.get('rating_count', 0) or 0
    base_rating = avg_rating
    count_factor = math.log10(total_count + 1) * 25
    review_penalty = 60 / (math.sqrt(total_count) + 1)
    rating_bonus = (avg_rating - 60) * 0.4 if avg_rating > 80 else 0
    very_low_ratings_penalty = 15 - total_count if total_count < 10 else 0

    special_adjustment = 0

    if avg_rating >= 80 and total_count >= 500:
        special_adjustment += 2
    if avg_rating >= 95 and total_count < 30:
        special_adjustment -= 5
    return base_rating + count_factor + rating_bonus - review_penalty - very_low_ratings_penalty + special_adjustment

def main():
    token = get_access_token()
    print("Access token received.")

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
                all_games.append(game)
        offset += BATCH_SIZE
        time.sleep(0.4)

    print(f"Total games fetched: {len(all_games)}")

    main_games = [g for g in all_games if is_main_game(g)]
    other_games = [g for g in all_games if not is_main_game(g)]

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
