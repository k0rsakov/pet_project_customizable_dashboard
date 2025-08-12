from datetime import datetime

import dash
import duckdb
import folium
import pandas as pd
from dash import Input, Output, dcc, html
from folium.plugins import HeatMap, MarkerCluster


# Function to load data from DuckDB
def load_data():
    try:
        # Initialize DuckDB connection
        conn = duckdb.connect(database=":memory:", read_only=False)

        # Read CSV file into DuckDB
        conn.execute("CREATE TABLE IF NOT EXISTS orders AS SELECT * FROM read_csv_auto('data.csv')")

        # Convert ship_date to date type
        conn.execute("ALTER TABLE orders ALTER COLUMN ship_date TYPE DATE")

        # Get data into pandas DataFrame for initial setup
        df = conn.execute("SELECT * FROM orders").fetchdf()

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
app.title = "Interactive Map Dashboard"

# Load the data
conn, df = load_data()

# Get min and max values for price slider
if "price_of_order" in df.columns and len(df) > 0:
    min_price = float(conn.execute("SELECT MIN(price_of_order) FROM orders").fetchone()[0])
    max_price = float(conn.execute("SELECT MAX(price_of_order) FROM orders").fetchone()[0])
else:
    min_price = 0
    max_price = 1000

# Initialize the app layout with modern styling
app.layout = html.Div([
    # Main container
    html.Div([
        # Header
        html.H1("Interactive Map Dashboard", style={"textAlign": "center"}),

        # Filters card
        html.Div([
            # Filters row
            html.Div([
                # Filter 1: Type User
                html.Div([
                    html.Label("User Type"),
                    dcc.Dropdown(
                        id="type-user-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT type_user FROM orders").fetchall()] if "type_user" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Select User Type",
                    ),
                ], className="filter-column"),

                # Filter 2: Category Name
                html.Div([
                    html.Label("Category"),
                    dcc.Dropdown(
                        id="category-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT category_name FROM orders").fetchall()] if "category_name" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Select Category",
                    ),
                ], className="filter-column"),

                # Filter 3: Ship Date Range
                html.Div([
                    html.Label("Ship Date Range"),
                    dcc.DatePickerRange(
                        id="date-range",
                        min_date_allowed=conn.execute("SELECT MIN(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                        max_date_allowed=conn.execute("SELECT MAX(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
                        start_date=conn.execute("SELECT MIN(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                        end_date=conn.execute("SELECT MAX(ship_date) FROM orders").fetchone()[0] if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
                        display_format="MMM DD, YYYY",
                    ),
                ], className="filter-column"),

                # Filter 4: Price Range
                html.Div([
                    html.Label("Price Range"),
                    dcc.RangeSlider(
                        id="price-slider",
                        min=min_price,
                        max=max_price,
                        value=[min_price, max_price],
                        marks={},  # We'll update this dynamically
                        tooltip={"placement": "bottom", "always_visible": True},
                        step=1,
                    ),
                ], className="filter-column"),

                # Filter 5: Payment Type
                html.Div([
                    html.Label("Payment Method"),
                    dcc.Dropdown(
                        id="payment-dropdown",
                        options=[{"label": row[0], "value": row[0]} for row in
                                 conn.execute("SELECT DISTINCT type_of_payment FROM orders").fetchall()] if "type_of_payment" in df.columns and len(df) > 0 else [],
                        multi=True,
                        placeholder="Select Payment Type",
                    ),
                ], className="filter-column"),

                # Filter 6: Map Type
                html.Div([
                    html.Label("Map Display"),
                    dcc.Dropdown(
                        id="map-type-dropdown",
                        options=[
                            {"label": "Points", "value": "points"},
                            {"label": "Heat Map", "value": "heatmap"},
                            {"label": "Clusters", "value": "clusters"},
                        ],
                        value="points",
                        clearable=False,
                    ),
                ], className="filter-column"),
            ], className="filter-row"),
        ], className="filter-card"),

        # Map container with increased height
        html.Div([
            html.Iframe(id="map", srcDoc="", width="100%", height="720px", className="map-frame"),
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
        Input("price-slider", "value"),
        Input("payment-dropdown", "value"),
        Input("map-type-dropdown", "value"),
    ],
)
def update_map(selected_users, selected_categories, start_date, end_date, price_range, selected_payments, map_type):
    # Build SQL query based on selected filters
    sql_query = "SELECT * FROM orders WHERE 1=1"
    params = {}

    if selected_users and len(selected_users) > 0:
        placeholders = ", ".join([f"'{user}'" for user in selected_users])
        sql_query += f" AND type_user IN ({placeholders})"

    if selected_categories and len(selected_categories) > 0:
        placeholders = ", ".join([f"'{cat}'" for cat in selected_categories])
        sql_query += f" AND category_name IN ({placeholders})"

    if start_date and end_date:
        sql_query += f" AND ship_date >= '{start_date}' AND ship_date <= '{end_date}'"

    if price_range:
        sql_query += f" AND price_of_order >= {price_range[0]} AND price_of_order <= {price_range[1]}"

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
            popup_text = f"<strong>User Type:</strong> {row.get('type_user', 'N/A')}<br>" \
                         f"<strong>Category:</strong> {row.get('category_name', 'N/A')}<br>" \
                         f"<strong>Ship Date:</strong> {row.get('ship_date', 'N/A')}<br>" \
                         f"<strong>Price:</strong> ${row.get('price_of_order', 'N/A')}<br>" \
                         f"<strong>Payment:</strong> {row.get('type_of_payment', 'N/A')}"

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
            popup_text = f"<strong>User Type:</strong> {row.get('type_user', 'N/A')}<br>" \
                         f"<strong>Category:</strong> {row.get('category_name', 'N/A')}<br>" \
                         f"<strong>Ship Date:</strong> {row.get('ship_date', 'N/A')}<br>" \
                         f"<strong>Price:</strong> ${row.get('price_of_order', 'N/A')}<br>" \
                         f"<strong>Payment:</strong> {row.get('type_of_payment', 'N/A')}"

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

# Callback to update the price slider marks and shown values
@app.callback(
    Output("price-slider", "marks"),
    [Input("price-slider", "value")],
)
def update_price_slider_marks(value):
    if value is None:
        value = [min_price, max_price]

    return {
        int(value[0]): {"label": f"${int(value[0])}", "style": {"color": "#5c6ac4", "font-weight": "500"}},
        int(value[1]): {"label": f"${int(value[1])}", "style": {"color": "#5c6ac4", "font-weight": "500"}},
    }

if __name__ == "__main__":
    app.run(debug=True)
