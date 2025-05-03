from dash import html, register_page, dcc
import dash_bootstrap_components as dbc

register_page(__name__, path='/')

test = dcc.Markdown('''
Este é um parágrafo com um [link](https://www.example.com) no meio.
''')


content = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2("Painel de monitoramento da PR-2", className="text-center text-black"),
            html.Br(),
            dcc.Markdown(''' 
                         A Pró-Reitoria de Pesquisa e Pós-Graduação ([PR-2](https://pr2.ufrj.br/)) da UFRJ
                         desenvolveu este painel com o intuito de promover acesso perene às mais diversas 
                         métricas e dados institucionais, em especial àqueles associados à produção 
                         científica e atuação dos PPGs (Programas de Pós-Graduação).

                         Este painel é dividido em vários subpainéis, que detalham vários aspectos da
                         pesquisa e pós-graduação da UFRJ. Esperamos que estes proverão informações valiosas
                         tanto para uso institucional interno (em auditoria e gerenciamento, por exemplo) quanto 
                         para o consumo pelo público geral.

                         O painel de produção científica faz uso de dados obtidos da [OpenAlex](https://openalex.org/),
                         uma base bibliográfica aberta que conta com um grande número de publicações indexadas. Os dados 
                         analisados aqui correspondem aos documentos encontrados na base onde pelo menos um autor é afiliado
                         à UFRJ, os quais são recuperados e atualizados automaticamente.
                         
                         Os outros painéis são atualizados manualmente com dados levantados pelo Escritório de Gestão de 
                         Indicadores de Desempenho ([GID](https://pr2.ufrj.br/gid)). Este órgão, vinculado à PR-2, tem
                         como um de seus principais objetivos o levantamento de informações para preenchimento de rankings
                         institucionais. A manutenção deste painel depende diretamente da atuação do GID enquanto fonte 
                         primária de dados.

                         Esta ferramenta foi desenvolvida em Python, utiliza exclusivamente bibliotecas abertas e 
                         seu código-fonte está disponível na íntegra no [GitHub](https://github.com/gavieira/ufrj_dashboard). 
                         Essas medidas foram tomadas no sentido de tornar este painel tão acessível e transparente quanto possível.
                ''', className="text-black markdown-container"
            ),
            html.Br(),
            html.H3("Painéis disponíveis:", className="text-center text-dark")
        ], width=8)  # Change this (1-12) to control column width
    ], justify='center')
])

def create_card(icon_class, label, link):
    return dcc.Link(
            dbc.Card(
        [
            # Top icon area
            dbc.CardBody(
                html.I(className=f"fa {icon_class} fa-3x text-info"),
                className="bg-white text-center",
            ),
            # Fixed-height footer
            dbc.CardFooter(
                html.Div(label, className="w-100 text-center"),
                className="bg-info text-white fw-bold mt-auto fixed-footer",
            )
        ],
        className="d-flex flex-column h-100 shadow border-info rounded card-hover",  # ❌ remove 'border' classes
    style={
        "border": "3px solid",  # or use a hex color / named color
        "overflow": "hidden"
    }
    ), href=link
    )

cards = dbc.Container([
    dbc.Row([
        dbc.Col(create_card("fa-file", "Produção científica", "/producao"), width=2),
        dbc.Col(create_card("fa-graduation-cap", "Pós-Graduação", "/posgrad"), width=2),
        dbc.Col(create_card("fa-ranking-star", "Desempenho em rankings", "/rankings"), width=2),
    ], justify="center", className="p-4"),
    dbc.Row([
        dbc.Col(create_card("fa-users", "Pessoal", "/pessoal"), width=2),
        dbc.Col(create_card("fa-building", "Infraestrutura", "/infraestrutura"), width=2),
    ], justify = "center")
], fluid=True, className="p-4")

layout = html.Div([
    html.Br(),
    content,
    cards
])