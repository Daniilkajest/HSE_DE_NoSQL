import time
import random
from pymongo import MongoClient, HASHED
from faker import Faker
import matplotlib.pyplot as plt
import concurrent.futures

# Подключение k роутеру (mongos)
client = MongoClient('mongodb://localhost:27017/')
db = client['marathon_db']
fake = Faker('ru_RU')

def setup_sharding():
    """Настраивает шардинг для базы и коллекции workouts"""
    try:
        # Включаем шардинг для БД
        client.admin.command('enableSharding', 'marathon_db')
        # Создаем хешированный индекс для ключа шардирования
        db.workouts.create_index([("runner_id", HASHED)])
        # Указываем, как шардировать коллекцию (по хешу runner_id)
        client.admin.command('shardCollection', 'marathon_db.workouts', key={'runner_id': 'hashed'})
        print("[+] Шардинг для БД 'marathon_db' и коллекции 'workouts' успешно инициализирован.\n")
    except Exception as e:
        # Если скрипт уже запускался, то пропускаем т.к. шардировка прошла

def add_runner():
    name = input("Введите имя марафонца: ")
    distance = input("Целевая дистанция (например, 22 или 42): ")
    runner_id = fake.uuid4()
    
    db.runners.insert_one({
        "runner_id": runner_id,
        "name": name,
        "target_distance_km": int(distance)
    })
    print(f"[+] Бегун {name} добавлен!\n")

def insert_fake_workouts(count):
    """Генерирует и вставляет тренировки пачкой"""
    workouts = []
    for _ in range(count):
        workouts.append({
            "runner_id": fake.uuid4(), # Уникальный ID для хеширования
            "date": fake.date_this_year().isoformat(),
            "distance_km": round(random.uniform(5.0, 30.0), 2),
            "avg_heart_rate": random.randint(120, 180)
        })
    db.workouts.insert_many(workouts)

def run_load_test():
    print("\n--- Запуск нагрузочного тестирования ---")
    batches = [1000, 5000, 10000, 20000] # Количество записей для теста
    times = []

    for batch_size in batches:
        print(f"Генерация и запись {batch_size} тренировок...")
        start_time = time.time()
        
        # вставка в 4 потока для имитации нагрузки
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            chunk_size = batch_size // 4
            futures = [executor.submit(insert_fake_workouts, chunk_size) for _ in range(4)]
            concurrent.futures.wait(futures)
            
        end_time = time.time()
        duration = end_time - start_time
        times.append(duration)
        print(f"✅ Успешно за {duration:.2f} сек.")

    # график
    plt.figure(figsize=(8, 5))
    plt.plot(batches, times, marker='o', linestyle='-', color='b')
    plt.title('Нагрузочное тестирование MongoDB (Шардинг включен)')
    plt.xlabel('Количество записей (шт)')
    plt.ylabel('Время выполнения (сек)')
    plt.grid(True)
    plt.savefig('load_test_results.png')
    print("\n[+] График тестирования сохранен в файл 'load_test_results.png'.\n")

def main():
    setup_sharding()
    
    while True:
        print("=== СИСТЕМА УЧЕТА МАРАФОНЦЕВ ===")
        print("1. Добавить бегуна")
        print("2. Провести нагрузочное тестирование (и получить график)")
        print("3. Выход")
        
        choice = input("Выберите действие (1-3): ")
        
        if choice == '1':
            add_runner()
        elif choice == '2':
            run_load_test()
        elif choice == '3':
            print("Выход из системы...")
            break
        else:
            print("Неверный ввод, попробуйте снова.\n")

if __name__ == "__main__":
    main()