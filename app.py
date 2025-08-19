import logging
import time
from datetime import datetime

import dash
import duckdb
import folium
import pandas as pd
import plotly.express as px
from dash import Input, Output, dcc, html
from folium.plugins import FastMarkerCluster

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


# Function to load data from DuckDB
def load_data():
    try:
        # Initialize DuckDB connection
        conn = duckdb.connect(database="data.duckdb", read_only=True)

        # Get data into pandas DataFrame for initial setup
        df = conn.execute("SELECT * FROM orders").df()

        count_orders = conn.sql("SELECT count(*) FROM orders").fetchone()[0]

        logging.info(f"Loaded {count_orders} orders from the database.")

        return conn, df
    except Exception as e:
        print(f"Error loading data: {e}")
        # Return an empty connection and DataFrame with expected columns if file doesn't exist
        conn = duckdb.connect(database=":memory:", read_only=False)
        df = pd.DataFrame(columns=["type_user", "category_name", "ship_date",
                                   "price_of_order", "type_of_payment",
                                   "latitude", "longitude"])
        return conn, df


# Initialize the app
app = dash.Dash(
    __name__,
    # Include Google Font 'Poppins' for modern typography
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600&display=swap",
    ],
)

# Set the page title
app.title = "Интерактивная Карта"

# Load the data
conn, df = load_data()

# Initialize the app layout with modern styling
app.layout = html.Div([
    # Main container
    html.Div([
        # Filters card
        html.Div([
            # Filters row
            html.Div([
                # Filter 1: Type User
                html.Div([
                    dcc.Dropdown(
                        id="type-user-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT type_user FROM orders").fetchall()] if "type_user" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Тип пользователя",
                    ),
                ], className="filter-column"),

                # Filter 2: Category Name
                html.Div([
                    dcc.Dropdown(
                        id="category-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT category_name FROM orders").fetchall()] if "category_name" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Категория",
                    ),
                ], className="filter-column"),

                # Filter 3: Ship Date Range
                html.Div([
                    dcc.DatePickerRange(
                        id="date-range",
                        min_date_allowed=conn.execute("SELECT MIN(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                        max_date_allowed=conn.execute("SELECT MAX(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
                        start_date=conn.execute("SELECT MIN(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                        end_date=conn.execute("SELECT MAX(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
                        display_format="YYYY-MM-DD",
                        first_day_of_week=1,
                        start_date_placeholder_text="Начальная дата",
                        end_date_placeholder_text="Конечная дата",
                        className="date-range-picker",
                    ),
                ], className="filter-column date-filter"),

                # Filter 5: Payment Type
                html.Div([
                    dcc.Dropdown(
                        id="payment-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT type_of_payment FROM orders").fetchall()] if "type_of_payment" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Способ оплаты",
                    ),
                ], className="filter-column"),

                # Filter 6: Map Type
                html.Div([
                    dcc.Dropdown(
                        id="map-type-dropdown",
                        options=[
                            {"label": "Точки", "value": "points"},
                            {"label": "Тепловая карта", "value": "heatmap"},
                            {"label": "Кластеры", "value": "clusters"},
                        ],
                        value="clusters",
                        clearable=False,
                        placeholder="Тип отображения карты",
                    ),
                ], className="filter-column"),
            ], className="filter-row"),
        ], className="filter-card"),

        # Map container - Использует и iframe и dcc.Graph в зависимости от типа карты
        html.Div([
            html.Div(id="map-container", style={"height": "800px", "width": "100%"}),
        ], className="map-container"),

        # Hidden div for storing filtered data info
        html.Div(id="filtered-data-info", style={"display": "none"}),
    ], className="container"),
], style={"backgroundColor": "var(--background-color)"})


# Callback to update the map based on filters
@app.callback(
    Output("map-container", "children"),
    [
        Input("type-user-dropdown", "value"),
        Input("category-dropdown", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("payment-dropdown", "value"),
        Input("map-type-dropdown", "value"),
    ],
)
def update_map(selected_users, selected_categories, start_date, end_date, selected_payments, map_type):
    start_time = time.time()

    # Build SQL query based on selected filters
    sql_query = "SELECT * FROM orders WHERE 1=1"

    if selected_users and len(selected_users) > 0:
        placeholders = ", ".join([f"'{user}'" for user in selected_users])
        sql_query += f" AND type_user IN ({placeholders})"

    if selected_categories and len(selected_categories) > 0:
        placeholders = ", ".join([f"'{cat}'" for cat in selected_categories])
        sql_query += f" AND category_name IN ({placeholders})"

    if start_date and end_date:
        sql_query += f" AND ship_date >= '{start_date}' AND ship_date <= '{end_date}'"

    if selected_payments and len(selected_payments) > 0:
        placeholders = ", ".join([f"'{pay}'" for pay in selected_payments])
        sql_query += f" AND type_of_payment IN ({placeholders})"

    # Execute the query and get the filtered data
    try:
        filtered_df = conn.execute(sql_query).fetchdf()
    except Exception as e:
        print(f"SQL Error: {e}")
        filtered_df = pd.DataFrame(columns=["type_user", "category_name", "ship_date",
                                            "price_of_order", "type_of_payment",
                                            "latitude", "longitude"])

    logging.info(f"Query: {sql_query}")
    # Log the number of records returned
    logging.info(f"Query returned {len(filtered_df)} records")

    # Check if we have data with coordinates
    if len(filtered_df) == 0 or "latitude" not in filtered_df.columns or "longitude" not in filtered_df.columns:
        # Return an empty map centered on a default location if no data
        empty_fig = px.scatter_mapbox(
            lat=[52.260853], lon=[104.282274],
            zoom=12, height=800,
        )
        empty_fig.update_layout(
            mapbox_style="carto-positron",
            margin={"r": 0, "t": 0, "l": 0, "b": 0},
            height=800,
            dragmode="pan",
        )
        execution_time = time.time() - start_time
        logging.info(f"Построение пустой карты заняло {execution_time:.4f} секунд")
        return dcc.Graph(
            figure=empty_fig,
            style={"height": "800px"},
            config={"scrollZoom": True, "doubleClick": "reset"},
        )

    # Для кластеров используем folium
    if map_type == "clusters":
        # Создаем карту folium
        center_lat = filtered_df["latitude"].mean()
        center_lon = filtered_df["longitude"].mean()

        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=12,
            tiles="CartoDB positron",
        )

        # Используем FastMarkerCluster для быстрой кластеризации
        # Создаем callback-функцию для JavaScript
        callback = """
        function (row) {
            var lat = row[0];
            var lng = row[1];
            var type = row[2];
            var color = '#3f51b5'; // По умолчанию

            if (type === 'ФЛ') color = '#3f51b5';
            else if (type === 'ЮЛ') color = '#ff7043';
            else if (type === 'ИП') color = '#2e7d32';

            var marker = L.circleMarker(new L.LatLng(lat, lng), {
                radius: 4,
                color: color,
                fillColor: color,
                fillOpacity: 0.7
            });
            return marker;
        };
        """

        # Подготовка данных для кластеризации без сэмплирования
        cluster_data = filtered_df[["latitude", "longitude", "type_user"]].values.tolist()

        FastMarkerCluster(data=cluster_data, callback=callback).add_to(m)

        # Конвертируем карту в HTML и отображаем в iframe
        html_string = m._repr_html_()
        execution_time = time.time() - start_time
        logging.info(f"Построение карты кластеров заняло {execution_time:.4f} секунд")

        return html.Iframe(srcDoc=html_string, style={"width": "100%", "height": "800px", "border": "none"})

    # Для остальных типов карт используем Plotly
    # Format price for hover data
    if "price_of_order" in filtered_df.columns:
        filtered_df["price_formatted"] = filtered_df["price_of_order"].apply(lambda x: f"₽{x:,}".replace(",", " "))

    # Create the map based on the map type
    hover_data = {
        "type_user": True,
        "category_name": True,
        "ship_date": True,
        "price_formatted": True,
        "price_of_order": False,  # Скрываем исходную цену
        "type_of_payment": True,
        "latitude": False,
        "longitude": False,
    }

    if map_type == "points":
        fig = px.scatter_mapbox(
            filtered_df,
            lat="latitude",
            lon="longitude",
            hover_name="category_name",
            hover_data=hover_data,
            color="type_user",
            color_discrete_map={
                "ФЛ": "#5c6ac4",  # Синий
                "ЮЛ": "#ff9800",  # Оранжевый
                "ИП": "#4caf50",   # Зеленый
            },
            opacity=0.7,
            zoom=11,
            height=800,
        )
        fig.update_traces(marker=dict(size=6))

    elif map_type == "heatmap":
        # Для тепловой карты используем px.density_mapbox
        fig = px.density_mapbox(
            filtered_df,
            lat="latitude",
            lon="longitude",
            z="price_of_order" if "price_of_order" in filtered_df.columns else None,
            radius=10,
            zoom=11,
            height=800,
            color_continuous_scale=[
                [0, "blue"],
                [0.4, "blue"],
                [0.65, "lime"],
                [1.0, "red"],
            ],
            opacity=0.8,
        )

    # Общие настройки карты
    fig.update_layout(
        mapbox_style="carto-positron",
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=800,
        legend=dict(
            title="Тип пользователя",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1,
        ),
        dragmode="pan",  # Разрешаем перетаскивание карты
    )

    execution_time = time.time() - start_time
    logging.info(f"Построение карты {map_type} заняло {execution_time:.4f} секунд")

    return dcc.Graph(
        figure=fig,
        style={"height": "800px"},
        config={
            "scrollZoom": True,  # Включаем зум колесом мыши
            "doubleClick": "reset",  # Двойной клик для сброса вида
        },
    )


if __name__ == "__main__":
    app.run(debug=True)
