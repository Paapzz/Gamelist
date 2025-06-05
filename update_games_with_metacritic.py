import os
import json
import glob
import logging
import sys

# Константы
METACRITIC_DATA_FILE = 'meta_data/metacritic_ratings.json'  # Файл с данными Metacritic
LOG_FILE = 'update_games_with_metacritic.log'  # Файл для логирования

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_metacritic_data():
    """Загружает данные Metacritic из файла."""
    if os.path.exists(METACRITIC_DATA_FILE):
        try:
            with open(METACRITIC_DATA_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logging.error(f"Ошибка при загрузке данных Metacritic: {e}")
            return {"games": {}, "last_updated": "", "total_games": 0}
    else:
        logging.error(f"Файл с данными Metacritic не найден: {METACRITIC_DATA_FILE}")
        return {"games": {}, "last_updated": "", "total_games": 0}

def update_games_with_metacritic():
    """Обновляет данные игр с учетом рейтингов Metacritic."""
    # Загружаем данные Metacritic
    metacritic_data = load_metacritic_data()

    if not metacritic_data['games']:
        logging.warning("Нет данных Metacritic для обновления игр.")
        return

    # Получаем список файлов с играми
    game_files = sorted(glob.glob('data/games_*.json'))

    if not game_files:
        logging.error("Ошибка: файлы с играми не найдены в директории data!")
        return

    logging.info(f"Найдено {len(game_files)} файлов с играми.")

    # Счетчики для статистики
    total_games = 0
    updated_games = 0

    # Обновляем каждый файл с играми
    for file_path in game_files:
        logging.info(f"Обновляем файл {file_path}...")

        try:
            # Загружаем игры из файла
            with open(file_path, 'r', encoding='utf-8') as f:
                games = json.load(f)

            total_games += len(games)

            # Обновляем данные игр
            updated = False
            file_updated_games = 0

            for game in games:
                game_id = str(game.get('id'))

                # Если для игры есть данные Metacritic, обновляем их
                if game_id in metacritic_data['games']:
                    mc_data = metacritic_data['games'][game_id]

                    # Добавляем данные Metacritic в игру
                    if 'metacritic' not in game:
                        game['metacritic'] = {}
                        updated = True

                    # Обновляем только оценки и URL
                    game['metacritic']['metascore'] = mc_data.get('metascore')
                    game['metacritic']['userscore'] = mc_data.get('userscore')
                    game['metacritic']['url'] = mc_data.get('url')
                    game['metacritic']['last_updated'] = mc_data.get('timestamp')

                    file_updated_games += 1

            # Если были обновления, сохраняем файл
            if updated:
                new_content = json.dumps(games, ensure_ascii=False)
                old_content = None
                if os.path.exists(file_path):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        old_content = f.read()
                if old_content != new_content:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    logging.info(f"Файл {file_path} обновлен. Обновлено игр: {file_updated_games}")
                    updated_games += file_updated_games
                else:
                    logging.info(f"В файле {file_path} нет изменений (контент совпадает).")
            else:
                logging.info(f"В файле {file_path} нет изменений.")

        except Exception as e:
            logging.error(f"Ошибка при обновлении файла {file_path}: {e}")

    logging.info(f"Обновление данных игр с учетом рейтингов Metacritic завершено.")
    logging.info(f"Всего игр: {total_games}")
    logging.info(f"Обновлено игр: {updated_games}")

def main():
    """Основная функция скрипта."""
    logging.info("Начинаем обновление данных игр с учетом рейтингов Metacritic...")
    try:
        update_games_with_metacritic()
        logging.info("Обновление данных игр завершено успешно.")
    except Exception as e:
        logging.error(f"Произошла ошибка при обновлении данных игр: {e}")
        raise

if __name__ == "__main__":
    main()
