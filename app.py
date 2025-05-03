import dash
from dash import Dash, html
import dash_bootstrap_components as dbc
from modules.navbar import navbar

app = Dash(__name__, use_pages=True, external_stylesheets=[dbc.themes.MINTY, dbc.icons.FONT_AWESOME])

app.layout = html.Div([
    navbar,
    dash.page_container
])

if __name__ == "__main__":
    print("Registered pages:", dash.page_registry.keys())  # Optional debug
    app.run(port=8051, debug=True, host='0.0.0.0')