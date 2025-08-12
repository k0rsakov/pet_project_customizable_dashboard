from datetime import datetime

import dash
import folium
import pandas as pd
from dash import Input, Output, dcc, html
from folium.plugins import HeatMap, MarkerCluster


# Function to load data - can be replaced with other data sources later
def load_data():
    # This function can be replaced with different data sources (duckdb, postgresql, etc.)
    # For now, it loads data from a CSV file
    try:
        df = pd.read_csv("data.csv")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        # Return an empty DataFrame with expected columns if file doesn't exist
        return pd.DataFrame(columns=["type_user", "category_name", "ship_date",
                                     "price_of_order", "type_of_payment",
                                     "latitude", "longitude"])

# Initialize the app
app = dash.Dash(__name__, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])

# Load the data
df = load_data()

# Convert ship_date to datetime if it exists in the dataframe
if "ship_date" in df.columns and len(df) > 0:
    df["ship_date"] = pd.to_datetime(df["ship_date"])

# Initialize the app layout
app.layout = html.Div([
    html.H1("Dashboard with Folium Map", style={"textAlign": "center"}),

    # Filters row
    html.Div([
        # Filter 1: Type User
        html.Div([
            html.Label("Type User"),
            dcc.Dropdown(
                id="type-user-dropdown",
                options=[{"label": user, "value": user} for user in df["type_user"].unique()] if "type_user" in df.columns and len(df) > 0 else [],
                multi=True,
                placeholder="Select User Type",
            ),
        ], className="two columns"),

        # Filter 2: Category Name
        html.Div([
            html.Label("Category Name"),
            dcc.Dropdown(
                id="category-dropdown",
                options=[{"label": cat, "value": cat} for cat in df["category_name"].unique()] if "category_name" in df.columns and len(df) > 0 else [],
                multi=True,
                placeholder="Select Category",
            ),
        ], className="two columns"),

        # Filter 3: Ship Date Range
        html.Div([
            html.Label("Ship Date Range"),
            dcc.DatePickerRange(
                id="date-range",
                min_date_allowed=df["ship_date"].min() if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                max_date_allowed=df["ship_date"].max() if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
                start_date=df["ship_date"].min() if "ship_date" in df.columns and len(df) > 0 else datetime(2020, 1, 1),
                end_date=df["ship_date"].max() if "ship_date" in df.columns and len(df) > 0 else datetime(2025, 12, 31),
            ),
        ], className="two columns"),

        # Filter 4: Price Range
        html.Div([
            html.Label("Price Range"),
            dcc.RangeSlider(
                id="price-slider",
                min=df["price_of_order"].min() if "price_of_order" in df.columns and len(df) > 0 else 0,
                max=df["price_of_order"].max() if "price_of_order" in df.columns and len(df) > 0 else 1000,
                value=[
                    df["price_of_order"].min() if "price_of_order" in df.columns and len(df) > 0 else 0,
                    df["price_of_order"].max() if "price_of_order" in df.columns and len(df) > 0 else 1000,
                ],
                marks={
                    int(df["price_of_order"].min()) if "price_of_order" in df.columns and len(df) > 0 else 0: {"label": "Min"},
                    int(df["price_of_order"].max()) if "price_of_order" in df.columns and len(df) > 0 else 1000: {"label": "Max"},
                },
            ),
        ], className="two columns"),

        # Filter 5: Payment Type
        html.Div([
            html.Label("Payment Type"),
            dcc.Dropdown(
                id="payment-dropdown",
                options=[{"label": pay, "value": pay} for pay in df["type_of_payment"].unique()] if "type_of_payment" in df.columns and len(df) > 0 else [],
                multi=True,
                placeholder="Select Payment Type",
            ),
        ], className="two columns"),

        # Filter 6: Map Type
        html.Div([
            html.Label("Map Type"),
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
        ], className="two columns"),
    ], className="row", style={"marginBottom": "20px"}),

    # Map container
    html.Div([
        html.Iframe(id="map", srcDoc="", width="100%", height="600"),
    ], className="row"),

    # Hidden div for storing filtered data info
    html.Div(id="filtered-data-info", style={"display": "none"}),
])

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
    # Filter the data based on selected filters
    filtered_df = df.copy()

    if selected_users and "type_user" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["type_user"].isin(selected_users)]

    if selected_categories and "category_name" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["category_name"].isin(selected_categories)]

    if start_date and end_date and "ship_date" in filtered_df.columns:
        filtered_df = filtered_df[(filtered_df["ship_date"] >= start_date) & (filtered_df["ship_date"] <= end_date)]

    if price_range and "price_of_order" in filtered_df.columns:
        filtered_df = filtered_df[(filtered_df["price_of_order"] >= price_range[0]) &
                                  (filtered_df["price_of_order"] <= price_range[1])]

    if selected_payments and "type_of_payment" in filtered_df.columns:
        filtered_df = filtered_df[filtered_df["type_of_payment"].isin(selected_payments)]

    # Check if we have data with coordinates
    if len(filtered_df) == 0 or "latitude" not in filtered_df.columns or "longitude" not in filtered_df.columns:
        # Return an empty map centered on a default location if no data
        m = folium.Map(location=[0, 0], zoom_start=2)
        return m._repr_html_()

    # Calculate map center
    center_lat = filtered_df["latitude"].mean()
    center_lon = filtered_df["longitude"].mean()

    # Create a base map
    m = folium.Map(location=[center_lat, center_lon], zoom_start=5)

    # Add markers based on selected map type
    if map_type == "points":
        # Add individual markers
        for idx, row in filtered_df.iterrows():
            popup_text = f"User Type: {row.get('type_user', 'N/A')}<br>" \
                         f"Category: {row.get('category_name', 'N/A')}<br>" \
                         f"Ship Date: {row.get('ship_date', 'N/A')}<br>" \
                         f"Price: {row.get('price_of_order', 'N/A')}<br>" \
                         f"Payment: {row.get('type_of_payment', 'N/A')}"

            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=folium.Popup(popup_text, max_width=300),
            ).add_to(m)

    elif map_type == "heatmap":
        # Create a heatmap layer
        heat_data = [[row["latitude"], row["longitude"]] for _, row in filtered_df.iterrows()]
        HeatMap(heat_data).add_to(m)

    elif map_type == "clusters":
        # Create a marker cluster
        marker_cluster = MarkerCluster().add_to(m)

        for idx, row in filtered_df.iterrows():
            popup_text = f"User Type: {row.get('type_user', 'N/A')}<br>" \
                         f"Category: {row.get('category_name', 'N/A')}<br>" \
                         f"Ship Date: {row.get('ship_date', 'N/A')}<br>" \
                         f"Price: {row.get('price_of_order', 'N/A')}<br>" \
                         f"Payment: {row.get('type_of_payment', 'N/A')}"

            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=folium.Popup(popup_text, max_width=300),
            ).add_to(marker_cluster)

    # Return the HTML representation of the map
    return m._repr_html_()

if __name__ == "__main__":
    app.run(debug=True)  # Changed from app.run_server() to app.run()
