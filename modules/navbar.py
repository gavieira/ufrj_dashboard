from dash import dcc, html, callback, Output, Input
import dash_bootstrap_components as dbc

navbar = html.Div([
    dcc.Location(id="url"),  # Tracks the current path
    dbc.NavbarSimple(
        children=[
            dbc.DropdownMenu(
                id="page-menu",
                label="Painéis",
                nav=True,
                in_navbar=True,
                align_end=True,
                className="custom-dropdown-label"  # Apply your custom class here
            )
        ],
        brand= [
            html.Img(
                src="assets/ufrj_logo.png", 
                #src="https://ufrj.br/wp-content/uploads/2024/01/ufrj-horizontal-cor-rgb-completa-telas.png", #original link
                height='80px',
                style={"padding": "0px"}
                ), 
            "Painel de dados da PR2 - UFRJ"
        ],
        brand_href="/",
        color="info",
        dark=True,
        style={"padding": "3px"}  # Reduce navbar padding
    )
])

@callback(
    Output("page-menu", "children"),
    Input("url", "pathname")
)
def update_menu(pathname):
    return [
        dbc.DropdownMenuItem(
            "Produção científica", href='/producao',
            active= pathname == "/producao"
        ),
        dbc.DropdownMenuItem(
            "Pós-Graduação", href="/posgrad",
            active= pathname == "/pos_grad"
        ),
        dbc.DropdownMenuItem(
            "Desempenho em rankings", href="/rankings",
            active= pathname == "/rankings"
        ),
        dbc.DropdownMenuItem(
            "Pessoal", href="/pessoal",
            active= pathname == "/pessoal"
        ),
        dbc.DropdownMenuItem(
            "Infraestrutura", href="/infraestrutura",
            active= pathname == "/infraestrutura"
        )
    ]