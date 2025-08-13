import datetime
import json
import os

import duckdb
import numpy as np
import pandas as pd
from faker import Faker
from shapely.geometry import Point, shape
from tqdm import tqdm


def load_polygon_from_json(json_data):
    """Загрузка полигона из GeoJSON"""
    try:
        polygon_geom = shape(json_data)
        print(f"Загружен полигон типа: {polygon_geom.geom_type}")
        print(f"Площадь полигона: {polygon_geom.area:.6f} кв. градусов")
        print(f"Периметр полигона: {polygon_geom.length:.6f} градусов")
        return polygon_geom
    except Exception as e:
        print(f"Ошибка при загрузке полигона: {e}")
        return None


def calculate_center_of_polygon(polygon):
    """Вычисление центра полигона"""
    centroid = polygon.centroid
    return centroid.y, centroid.x

def generate_points_in_polygon(polygon, num_points=10000, with_hotspots=True):
    """
    Генерация точек внутри полигона.

    Args:
        polygon: Полигон, в котором нужно генерировать точки
        num_points: Количество точек для генерации
        with_hotspots: Использовать ли центры активности (True)
                      или равномерное распределение (False)

    Returns:
        DataFrame с координатами сгенерированных точек

    """
    print(f"Генерация {num_points} точек внутри полигона...")

    # Получаем ограничивающий прямоугольник полигона
    min_x, min_y, max_x, max_y = polygon.bounds

    # Центр полигона
    center_y, center_x = calculate_center_of_polygon(polygon)

    points = []

    if with_hotspots:
        # Определяем центры активности (хотспоты)
        # Создадим несколько центров активности внутри полигона
        hotspots = [
            # Центр полигона (основной центр)
            (center_y, center_x, 0.025, 0.3),
            # Северная часть
            (min_y + (max_y - min_y) * 0.75, center_x, 0.02, 0.2),
            # Южная часть
            (min_y + (max_y - min_y) * 0.25, center_x, 0.02, 0.15),
            # Восточная часть
            (center_y, min_x + (max_x - min_x) * 0.75, 0.015, 0.2),
            # Западная часть
            (center_y, min_x + (max_x - min_x) * 0.25, 0.015, 0.15),
        ]

        # Процент точек для равномерного распределения
        random_points_percent = 0.4  # 40% точек будут равномерно распределены

        # Количество точек для каждого типа распределения
        hotspot_points = int(num_points * (1 - random_points_percent))
        random_points = num_points - hotspot_points

        # 1. Генерация точек вокруг центров активности
        # Извлекаем веса из хотспотов
        hotspot_weights = [h[3] for h in hotspots]
        total_weight = sum(hotspot_weights)

        # Вычисляем, сколько точек генерировать для каждого хотспота
        hotspot_targets = [int(hotspot_points * w / total_weight) for w in hotspot_weights]

        # Проверка, что сумма целей не превышает общее количество точек для хотспотов
        # Если есть разница из-за округления, добавляем/убираем из самого большого хотспота
        diff = hotspot_points - sum(hotspot_targets)
        if diff != 0:
            max_idx = hotspot_weights.index(max(hotspot_weights))
            hotspot_targets[max_idx] += diff

        for idx, (hot_y, hot_x, radius, _) in enumerate(hotspots):
            target = hotspot_targets[idx]

            with tqdm(total=target, desc=f"Хотспот {idx+1}") as pbar:
                generated = 0
                attempts = 0
                max_attempts = target * 50  # Максимальное количество попыток

                while generated < target and attempts < max_attempts:
                    # Нормальное распределение вокруг центра активности
                    dx = np.random.normal(0, radius)
                    dy = np.random.normal(0, radius)

                    x = hot_x + dx
                    y = hot_y + dy

                    # Проверяем, что точка внутри полигона
                    point = Point(x, y)
                    if polygon.contains(point):
                        points.append((y, x))  # Широта, долгота
                        generated += 1
                        pbar.update(1)

                    attempts += 1

        # 2. Генерация точек с равномерным распределением
        print("\nГенерация точек с равномерным распределением...")

        with tqdm(total=random_points) as pbar:
            generated = 0
            attempts = 0
            max_attempts = random_points * 50

            while generated < random_points and attempts < max_attempts:
                # Равномерное распределение по всему полигону
                x = np.random.uniform(min_x, max_x)
                y = np.random.uniform(min_y, max_y)

                point = Point(x, y)
                if polygon.contains(point):
                    points.append((y, x))  # Широта, долгота
                    generated += 1
                    pbar.update(1)

                attempts += 1
    else:
        # Только равномерное распределение (без хотспотов)
        print("Генерация точек с равномерным распределением...")

        with tqdm(total=num_points) as pbar:
            generated = 0
            attempts = 0
            max_attempts = num_points * 50

            while generated < num_points and attempts < max_attempts:
                x = np.random.uniform(min_x, max_x)
                y = np.random.uniform(min_y, max_y)

                point = Point(x, y)
                if polygon.contains(point):
                    points.append((y, x))  # Широта, долгота
                    generated += 1
                    pbar.update(1)

                attempts += 1

    print(f"Сгенерировано {len(points)} точек")

    # Создаем DataFrame с точками
    df = pd.DataFrame(points, columns=["latitude", "longitude"])

    return df


def enrich_dataframe(df):
    """
    Обогащает DataFrame случайными значениями из заданного словаря.

    Parameters
    ----------
    df : pandas.DataFrame
        Исходный DataFrame для обогащения

    Returns
    -------
    pandas.DataFrame
        DataFrame с добавленными колонками: 'type_user', 'category_name',
        'ship_date', 'type_of_payment', 'price_of_order'

    """
    # Создаем копию DataFrame, чтобы не изменять исходный
    df_enriched = df.copy()

    # Инициализируем Faker для генерации случайных данных
    fake = Faker()

    # Словарь с возможными значениями
    value_dict = {
        "type_user": ("ЮЛ", "ИП", "ФЛ"),
        "category_name": ("Напитки",
                          "Приправы и соусы",
                          "Кондитерские изделия",
                          "Молочные продукты",
                          "Крупы и злаки",
                          "Мясо и птица",
                          "Овощи и фрукты",
                          "Морепродукты",
                          ),
        "type_of_payment": ("Наличные", "Карта", "QR-код", "Кредит", "Счёт"),
    }

    # Генерируем случайные значения для type_user
    df_enriched["type_user"] = np.random.choice(
        value_dict["type_user"],
        size=len(df),
    )

    # Генерируем случайные значения для category_name
    df_enriched["category_name"] = np.random.choice(
        value_dict["category_name"],
        size=len(df),
    )

    # Генерируем случайные даты отгрузки используя fake.date_time_ad
    start_date = datetime.date(year=2024, month=1, day=1)
    end_date = datetime.date(year=2025, month=1, day=1)

    # Генерируем даты для каждой строки и форматируем в YYYY-MM-DD
    df_enriched["ship_date"] = [
        fake.date_time_ad(
            start_datetime=start_date,
            end_datetime=end_date,
        ).strftime("%Y-%m-%d") for _ in range(len(df))
    ]

    # Генерируем случайные значения для price_of_order без ограничения цифр
    df_enriched["price_of_order"] = [fake.random_number() for _ in range(len(df))]

    # Генерируем значения для type_of_payment с учетом условия
    def get_payment_type(user_type):
        if user_type in ("ЮЛ", "ИП"):
            return "Счёт"
        # ФЛ
        return np.random.choice([p for p in value_dict["type_of_payment"] if p != "Счёт"])

    # Применяем условие для каждой строки
    df_enriched["type_of_payment"] = df_enriched["type_user"].apply(get_payment_type)

    return df_enriched


with open("polygon_data_left.json", encoding="utf-8") as file:
    polygon_data = json.load(file)

polygon = load_polygon_from_json(polygon_data)
df_left = generate_points_in_polygon(polygon=polygon)
df_left = enrich_dataframe(df_left)

with open("polygon_data_right.json", encoding="utf-8") as file:
    polygon_data = json.load(file)

polygon = load_polygon_from_json(polygon_data)
df_right = generate_points_in_polygon(polygon=polygon)
df_right = enrich_dataframe(df_right)

with open("polygon_data_up.json", encoding="utf-8") as file:
    polygon_data = json.load(file)

polygon = load_polygon_from_json(polygon_data)
df_up = generate_points_in_polygon(polygon=polygon)
df_up = enrich_dataframe(df_up)


df_left.to_parquet("left.parquet",index=False)
df_right.to_parquet("right.parquet",index=False)
df_up.to_parquet("up.parquet",index=False)


conn = duckdb.connect("data.duckdb")

conn.sql(
    """
    DROP TABLE IF EXISTS orders;
    CREATE TABLE orders (
        latitude DOUBLE,
        longitude DOUBLE,
        type_user VARCHAR,
        category_name VARCHAR,
        ship_date DATE,
        price_of_order BIGINT,
        type_of_payment VARCHAR
    );
    """,
)

conn.sql(
    """
    INSERT INTO orders SELECT * FROM read_parquet('left.parquet');
    INSERT INTO orders SELECT * FROM read_parquet('right.parquet');
    INSERT INTO orders SELECT * FROM read_parquet('up.parquet');
    """,
)


conn.close()

# Список файлов для удаления
files_to_delete = ["left.parquet", "right.parquet", "up.parquet"]

# Цикл по файлам и их удаление
for file in files_to_delete:
    if os.path.exists(file):  # noqa: PTH110
        os.remove(file)  # noqa: PTH107
        print(f"Файл '{file}' успешно удален.")
    else:
        print(f"Файл '{file}' не существует.")
