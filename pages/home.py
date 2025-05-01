from dash import html, register_page

register_page(__name__, path='/')

layout = html.Div([
    html.Br(),
    html.H2("Em construção...",  className="text-center") #bootstrap class
])