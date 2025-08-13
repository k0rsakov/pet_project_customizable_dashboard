import logging
from datetime import datetime

import dash
import duckdb
import folium
import pandas as pd
from dash import Input, Output, dcc, html
from folium.plugins import HeatMap, MarkerCluster
import time
import functools
import logging
from dash import html
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)





def timing_decorator(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time

        # Логируем время выполнения
        logging.info(f"Функция {func.__name__} выполнена за {execution_time:.4f} секунд")

        # Добавляем информацию о времени в результат HTML
        if isinstance(result, str) and "</body>" in result:
            timing_info = f"""
            <div style="position:fixed; bottom:10px; right:10px; 
                        background-color:rgba(0,0,0,0.7); color:white; 
                        padding:5px 10px; border-radius:5px; z-index:1000;">
                Время построения: {execution_time:.4f} сек
            </div>
            """
            result = result.replace("</body>", f"{timing_info}</body>")

        return result

    return wrapper

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

        # Map container with increased height
        html.Div([
            html.Iframe(id="map", srcDoc="", width="100%", height="800px", className="map-frame"),
        ], className="map-container"),

        # Hidden div for storing filtered data info
        html.Div(id="filtered-data-info", style={"display": "none"}),
    ], className="container"),
], style={"backgroundColor": "var(--background-color)"})


# Callback to update the map based on filters
@app.callback(
    Output("map", "srcDoc"),
    [
        Input("type-user-dropdown", "value"),
        Input("category-dropdown", "value"),
        Input("date-range", "start_date"),
        Input("date-range", "end_date"),
        Input("payment-dropdown", "value"),
        Input("map-type-dropdown", "value"),
    ],
)
@timing_decorator
def update_map(selected_users, selected_categories, start_date, end_date, selected_payments, map_type):
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

    # Check if we have data with coordinates
    if len(filtered_df) == 0 or "latitude" not in filtered_df.columns or "longitude" not in filtered_df.columns:
        # Return an empty map centered on a default location if no data
        m = folium.Map(
            location=[52.260853, 104.282274],
            zoom_start=12,
            tiles="CartoDB positron",
        )
        return m._repr_html_()

    # Calculate map center and bounds for proper zoom
    if len(filtered_df) > 0:
        center_lat = filtered_df["latitude"].mean()
        center_lon = filtered_df["longitude"].mean()

        # Calculate bounds for auto-zoom
        sw = [filtered_df["latitude"].min(), filtered_df["longitude"].min()]
        ne = [filtered_df["latitude"].max(), filtered_df["longitude"].max()]
    else:
        # Default center if no filtered data
        center_lat = 52.260853
        center_lon = 104.282274
        sw = None
        ne = None

    # Create a base map
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=12,
        tiles="CartoDB positron",
    )

    # Fit map to bounds if we have data
    if sw and ne:
        m.fit_bounds([sw, ne])

    # Add markers based on selected map type
    if map_type == "points":
        # Add individual circle markers
        for idx, row in filtered_df.iterrows():
            popup_text = f"<strong>Тип пользователя:</strong> {row.get('type_user', 'Н/Д')}<br>" \
                         f"<strong>Категория:</strong> {row.get('category_name', 'Н/Д')}<br>" \
                         f"<strong>Дата доставки:</strong> {row.get('ship_date', 'Н/Д')}<br>" \
                         f"<strong>Стоимость:</strong> ₽{row.get('price_of_order', 'Н/Д')}<br>" \
                         f"<strong>Способ оплаты:</strong> {row.get('type_of_payment', 'Н/Д')}"# noqa: ISC002

            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=1.5,  # Smaller size for better visibility with many points
                color="#5c6ac4",  # Using our primary color variable
                fill=True,
                fill_opacity=0.7,
                popup=folium.Popup(popup_text, max_width=300),
            ).add_to(m)

    elif map_type == "heatmap":
        # Create a heatmap layer
        heat_data = [[row["latitude"], row["longitude"]] for _, row in filtered_df.iterrows()]
        HeatMap(heat_data, gradient={0.4: "blue", 0.65: "lime", 1: "red"}, min_opacity=0.5, radius=8).add_to(m)

    elif map_type == "clusters":
        # Create a marker cluster
        marker_cluster = MarkerCluster().add_to(m)

        for idx, row in filtered_df.iterrows():
            popup_text = f"<strong>Тип пользователя:</strong> {row.get('type_user', 'Н/Д')}<br>" \
                         f"<strong>Категория:</strong> {row.get('category_name', 'Н/Д')}<br>" \
                         f"<strong>Дата доставки:</strong> {row.get('ship_date', 'Н/Д')}<br>" \
                         f"<strong>Стоимость:</strong> ₽{row.get('price_of_order', 'Н/Д')}<br>" \
                         f"<strong>Способ оплаты:</strong> {row.get('type_of_payment', 'Н/Д')}"

            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=1.5,
                color="#3f51b5",  # Using our primary-dark color variable
                fill=True,
                fill_opacity=0.7,
                popup=folium.Popup(popup_text, max_width=300),
            ).add_to(marker_cluster)

    # Return the HTML representation of the map
    return m._repr_html_()

if __name__ == "__main__":
    app.run(debug=True)
