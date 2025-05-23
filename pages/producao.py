from dash import html, register_page, dcc, dash_table, callback, Output, Input
import plotly.express as px
import dash_bootstrap_components as dbc
from sqlalchemy import select, case, func, text 
from sqlalchemy.sql.expression import and_, any_
import pandas as pd
import sys
import numpy as np
import requests

register_page(__name__)


# Add the project root directory to sys.path
sys.path.append('/config/workspace/Dropbox/repos/ufrj_dashboard')
sys.path.append('/home/gabriel/Dropbox/repos/ufrj_dashboard')

from modules.prod import iso2_to_iso3
from database.db_handlers import OpenAlexDatabaseHandler

db_url = "postgresql+psycopg2://gid_admin:dashboard@postgres:5432/gid_admin" 
db_url = "postgresql+psycopg2://gid_admin:dashboard@127.0.0.1:5432/openalex_db"

handler = OpenAlexDatabaseHandler(db_url)


#stmt = select(handler.works)


#stmt = text(
#    '''SELECT
#    w.work_id,
#    w.publication_year,
#	w.work_type,
#	w.is_oa,
#	w.oa_status,
#	CASE
#		WHEN array_agg(flattened.institution_id) = '{NULL}' THEN 'Undefined'
#        WHEN 'I122140584' = ANY(array_agg(flattened.institution_id)) THEN 'UFRJ'
#        ELSE 'No UFRJ'
#    END AS corresponding
#FROM
#    works w
#LEFT JOIN (
#    SELECT
#        a.work_id,
#        inst.institution_id
#    FROM
#        authorships a,
#        LATERAL unnest(a.institution_id) AS inst(institution_id)
#    WHERE
#        a.is_corresponding = true
#) AS flattened ON w.work_id = flattened.work_id
#GROUP BY
#    w.work_id
#ORDER BY
#    w.work_id;'''
#)




# Step 1: Subquery for corresponding authors with institution_id arrays

corresponding_subq = (
    select(
        handler.authorships.c.work_id,
        handler.authorships.c.institution_id
    )
    .where(handler.authorships.c.is_corresponding == True)
    .subquery()
)

# Step 2: Unnest institution_id arrays via LATERAL join
unnested_subq = select(
    corresponding_subq.c.work_id, 
    func.unnest(corresponding_subq.c.institution_id).label("institution_id")
).lateral("unnested")

# Step 3: Group by work_id and aggregate into flattened array
agg_subq = (
    select(
        unnested_subq.c.work_id,
        func.array_agg(unnested_subq.c.institution_id).label("flattened_insts")
    )
    .group_by(unnested_subq.c.work_id)
    .subquery()
)

# Step 3: Main query with left join and ufrj_corresponding column
target_institution = 'I122140584' #UFRJ openalex id

final_query = (
    select(
        handler.works.c.work_id,
        handler.works.c.publication_year,
        handler.works.c.work_type,
        handler.works.c.is_oa,
        handler.works.c.oa_status,
        agg_subq.c.flattened_insts,
        case(
            ( agg_subq.c.flattened_insts == None, 'Undefined' ), # None if no corresponding author (depicted as)
            ( any_(agg_subq.c.flattened_insts) == target_institution, 'UFRJ'),
            else_ = 'No UFRJ'
        ).label("corresponding")
    )
    .select_from(
        handler.works.outerjoin(agg_subq, handler.works.c.work_id == agg_subq.c.work_id)
    )
)


test = handler.get_query_results(final_query)
#test['indexed_in'] = test['indexed_in'].apply(lambda x: ', '.join(x)) #Converting list to string


query = select(
    handler.authorships.c.work_id,
    handler.institutions.c.institution_id,
    handler.institutions.c.institution_name,
    handler.institutions.c.country_code
).join(
    handler.institutions,
    handler.authorships.c.institution_id.any(handler.institutions.c.institution_id)
)


institutions = handler.get_query_results(query)
collab = institutions[institutions['institution_name'] != 'Universidade Federal do Rio de Janeiro'] \
            .drop_duplicates(subset=['work_id', 'country_code']) \
            .groupby('country_code').size().reset_index(name='n_works') 

collab['country_code'] = collab['country_code'].apply(lambda x: iso2_to_iso3.get(x, 'Unknown'))

#collab = collab[collab['country_code'] != 'BRA'] # Removing Brazil
collab['log_count'] = np.log1p(collab['n_works'])

all_countries = pd.DataFrame({"country_code": [country for country in iso2_to_iso3.values()]})

# Step 2: Merge full country list with the `collab` table
collab_full = all_countries.merge(collab, on="country_code", how="left").fillna({"n_works": 0, "log_count": 0})

# Subquery: count of authors per work
author_count_subq = select(
    handler.authorships.c.work_id,
    func.count(handler.authorships.c.author_id).label("author_count")
).group_by(handler.authorships.c.work_id).subquery()

# Main query with join to the author count subquery
topics_query = select(
    handler.works.c.work_id, 
    handler.works.c.publication_year,
    handler.works.c.work_type,
    handler.works.c.cited_by_count,
    handler.works.c.referenced_works_count,
    handler.topics.c.topic_name,
    handler.topics_by_work.c.score, 
    handler.topics.c.subfield_name,
    handler.topics.c.field_name,
    handler.topics.c.domain_name,
    author_count_subq.c.author_count
).select_from(
    handler.works
    .outerjoin(handler.topics_by_work, handler.works.c.work_id == handler.topics_by_work.c.work_id)
    .outerjoin(handler.topics, handler.topics_by_work.c.topic_id == handler.topics.c.topic_id)
    .outerjoin(author_count_subq, handler.works.c.work_id == author_count_subq.c.work_id)
)


topics_table = handler.get_query_results(topics_query)
topics_works = topics_table.dropna(subset=['domain_name']) #CHANGE TOPICS_WORKS to TOPICS_NO_NAN later

# Keep only rows with the highest score for each work_id group (primary topics)
primary_topics = topics_works.loc[topics_works.groupby("work_id")["score"].idxmax()]\
    .groupby("topic_name", as_index=False)\
    .agg(
        {
            "work_id": "count",  # Count occurrences of each topic_id
            "topic_name": "first",
            "subfield_name": "first",
            "field_name": "first",
            "domain_name": "first",
        }
    )\
    .rename(columns={"work_id": "count"})  # Rename the count column


query_cited_by_year = select(
    handler.cited_by_year
)

cited_by_year_df = handler.get_query_results(query_cited_by_year)

domains_cited_by_year = topics_table[['work_id', 'domain_name']].merge(cited_by_year_df, on="work_id", how="left")  # Use "left" to keep all existing rows
domains_cited_by_year = domains_cited_by_year \
    .drop_duplicates(['work_id', 'domain_name', 'year'], keep='first') \
    .fillna({'domain_name': 'Unknown'}) \
    .groupby(['domain_name', 'year'], as_index=False).agg({'cited_count' : 'sum'})

# Apply cumulative sum per domain, sorted by year
domains_cited_by_year['cumulative_cited_count'] = (
    domains_cited_by_year
    .sort_values(['domain_name', 'year'])  # Ensure correct order
    .groupby('domain_name')['cited_count']
    .cumsum()  # Compute cumulative sum
)

domains_cited_by_year['year']= domains_cited_by_year['year'].astype(int)


#print(domains_cited_by_year)

# Create the base table
domains_n_works_per_pubyear = topics_table[["work_id", "publication_year", 'domain_name']] \
    .drop_duplicates(['work_id', 'domain_name'], keep='first') \
    .fillna({'domain_name': 'Unknown'}) \
    .groupby(['publication_year', 'domain_name']) \
    .size().reset_index(name='n_works')

# Ensure all domain-year combinations exist
all_years = domains_n_works_per_pubyear["publication_year"].unique()
all_domains = domains_n_works_per_pubyear["domain_name"].unique()
full_index = pd.MultiIndex.from_product([all_years, all_domains], names=["publication_year", "domain_name"])

# Reindex the table to include missing combinations
domains_n_works_per_pubyear = (
    domains_n_works_per_pubyear
    .set_index(["publication_year", "domain_name"])
    .reindex(full_index, fill_value=0)  # Fill missing entries with 0
    .reset_index()
)

# Compute cumulative sum per domain
domains_n_works_per_pubyear['cumulative_n_works'] = (
    domains_n_works_per_pubyear
    .sort_values(by=['domain_name', 'publication_year'])
    .groupby('domain_name')['n_works']
    .cumsum()
)

#print(domains_n_works_per_pubyear)    

#print(topics_cited_by_year.head())
#print(topics_cited_by_year.columns)
#print(topics_cited_by_year.query("domain_name == 'Unknown'").shape[0])

def h_index(citations):
    citations = np.sort(citations)[::-1]  # Sort in descending order
    h = np.arange(1, len(citations) + 1)  # Rank values from 1 to N
    return max((h[citations >= h]), default=0)  # Find max h where citations >= rank

#topics_core = topics_works[['work_id', 'work_title', 'publication_year', 'cited_by_count', 
#                          'topic_id', 'subfield_id', 'subfield_name', 'field_id', 'field_name', 
#                          'domain_id', 'domain_name']]
#
#domains_filtered = topics_core.drop_duplicates(subset=['work_id', 'domain_name'], keep='first')
#domains_summary = domains_filtered.groupby('domain_name').size().reset_index(name='n_works')





histogram = dbc.Container([
    dbc.Accordion(
        [
            dbc.AccordionItem(
                [ 
                    dbc.Row([
                        dbc.Col([
                            html.P('Intervalo de ano de publicação:'),
                            dcc.RangeSlider(
                                 min(test['publication_year']), 
                                 max(test['publication_year']), 
                                 1, 
                                 value = [ min(test['publication_year']), max(test['publication_year']) ],
                                 marks={year: str(year) for year in range(test['publication_year'].min(), test['publication_year'].max() + 1, 20)},  # Show labels every 5 years (adjust as needed)
                                 tooltip={"always_visible": False, "placement": "top"},  # Shows year on hover
                                 id='pubyear_range'
                                 )
                        ], width={'size': 5}),
                        dbc.Col([
                            html.P('Dados exibidos:'),
                            dcc.Dropdown(options = [
                                        {'label': 'Total de publicações', 'value': 'none'},
                                        {'label': 'Tipo de documento', 'value': 'work_type'}, 
                                        {'label': 'Publicações em acesso aberto', 'value': 'is_oa'},
                                        {'label': 'Tipo de acesso aberto', 'value': 'oa_status'}, 
                                        {'label': 'Autores correspondentes UFRJ', 'value': 'corresponding'} 
                                        ],
                                        value = 'none', 
                                        clearable=False,
                                        id='pubyear_dropdown'
                                        ),
                        ], width={'size': 3}),
                        dbc.Col([
                            html.P('Tipo de gráfico:'),
                            dcc.Dropdown(options=[
                                        {'label': 'Barra', 'value': 'bar'},
                                        {'label': 'Linha', 'value': 'line'} 
                                        ], 
                                        value = 'bar', 
                                        clearable=False,
                                        id='pubyear_visualization'
                                        ),
                        ], width={'size': 3}),
                    ], justify='around'),
                    dbc.Row([
                        dbc.Col([
                            dcc.Graph(
                            id='pubyear_histogram',
                            figure={}
                            )
                        ])
                    ])
                ],
                title='Histograma da produção científica da UFRJ'
            )
        ], start_collapsed=True
    )
], fluid=True)


maps = dbc.Container([
    dbc.Accordion([
        dbc.AccordionItem([
            dcc.Graph(
                figure=px.choropleth(collab,
                                    locations='country_code',
                                    color='log_count',
                                    #color_continuous_scale=[[0, "lightblue"], [1, "darkblue"]],
                                    hover_name='country_code',
                                    hover_data={
                                        'country_code': True,
                                        'n_works': True,
                                        'log_count': True 
                                        },
                                    color_continuous_midpoint=True).update_layout(
                                        geo = dict(
                                            bgcolor='lightgray',
                                            resolution=110,
                                            showframe=True,
                                            showcoastlines=True,
                                            showlakes=False,
                                            showrivers=False,
                                            showsubunits=False,
                                            projection_type='equirectangular'
                                            #projection_type='orthographic'
                                            #projection_type='natural earth'
                                        )
                                    )
            )
        ], title='Colaborações da UFRJ ao redor do mundo')
    ], start_collapsed=True)
], fluid=True)

topics_data = dbc.Container([
    dbc.Accordion([
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col([
                    html.P('Selecione o nível de classificação:')
                    ], width={'size': 5})
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        options=[
                        {'label': 'Topic', 'value': 'topic_name'},
                        {'label': 'Subfield', 'value': 'subfield_name'}, 
                        {'label': 'Field', 'value': 'field_name'}, 
                        {'label': 'Domain', 'value': 'domain_name'}
                        ], 
                        value = 'topic_name', 
                        clearable=False,
                        id='topics_dropdown'
                        ) 
                    ], width={'size': 2})
            ]),
            html.Br(),
            dbc.Row([
                dbc.Col([
                    html.P(
                        html.B('Tabela de métricas por classificação - Múltiplas classificações por estudo:')
                        )
                    ])
            ]),
            dbc.Row([
                dbc.Col([
                    dash_table.DataTable(data=[], page_size=10, 
                        sort_action='native', id='topics_table')
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.P(
                        html.B('Gráfico de dispersão 3D por classificação - Múltiplas classificações por estudo:')
                        )
                    ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.P('Selecione a variável do eixo z:')
                    ])
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Dropdown(
                        options=[
                        {'label': 'Média de autores', 'value': 'mean_authors'},
                        {'label': 'Média de referências', 'value': 'mean_referenced_works'}
                        ], 
                        value = 'mean_authors', 
                        clearable=False,
                        id='z_axis'
                        ) 
                    ], width={'size': 3})
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure={}, id='topics_3d')
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    html.P(
                        html.B('Hierarquia das clasificações - Classificação principal de cada estudo:')
                        )
                ])
            ]),
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=px.sunburst(primary_topics.sort_values(by=['domain_name']), path=[
                        'domain_name',
                        'field_name', 
                        'subfield_name'
                        ], values = 'count').update_layout(height=800))
                ])
            ])
        ], title='Classificação temática da produção')
    ], start_collapsed=True)
], fluid=True)

animated_plots = dbc.Container([
    dbc.Accordion([
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col([
                    dcc.Graph(figure=px.bar(domains_n_works_per_pubyear, 
                        x='domain_name', 
                        y='cumulative_n_works', 
                        animation_frame='publication_year', 
                        title='Total de trabalhos cumulativos',
                        text_auto='.2s').update_traces(textposition='inside', insidetextanchor="start")),  # Align text at the bottom
                ], width={'size':5}),
                dbc.Col([
                    dcc.Graph(figure=px.bar(domains_cited_by_year, 
                        x='domain_name', 
                        y='cumulative_cited_count', 
                        animation_frame='year', 
                        title='Total de citações cumulativas',
                        text_auto='.2s').update_traces(textposition='inside', insidetextanchor="start")),
                ], width={'size':5})


            ], justify='around')

        ], title='Produção científica por área ao longo dos anos')
    ], start_collapsed=True)
], fluid=True)


layout = html.Div([
    histogram,
    maps,
    topics_data,
    animated_plots
])


@callback(
    Output(component_id='pubyear_histogram', component_property='figure'),
    [Input(component_id='pubyear_range', component_property='value'),
     Input(component_id='pubyear_dropdown', component_property='value'),
     Input(component_id='pubyear_visualization', component_property='value')
    ]
)
def update_pubyear_histogram(year_range, column, plot_type):
    #Getting only timeframe of interest from widget
    min_year, max_year = year_range
    pubs = test[test['publication_year'].between(min_year, max_year)]

    # Group by year and optionally by another column
    group_cols = ['publication_year']
    column = column if column != 'none' else None #Column is None if its value is 'none'
    if column:
        group_cols.append(column)

    grouped = pubs.groupby(group_cols).size().reset_index(name='count')

    # Choose the plot type
    if plot_type == 'bar':
        fig = px.bar(grouped, x='publication_year', y='count',
                     color=column).update_traces(marker_line_color='black', marker_line_width=1.5)
    elif plot_type == 'line':
        fig = px.line(grouped, x='publication_year', y='count',
                      color=column, markers=True).update_traces(
                                        marker=dict(line=dict(width=0)
                                                    )
)

    else:
        fig = px.bar(grouped, x='publication_year', y='count')  # fallback

    fig.update_layout(xaxis_title='Publication Year', yaxis_title='Count')

    return fig


@callback(
    [
     Output(component_id='topics_table', component_property='data'),
     Output(component_id='topics_3d', component_property='figure')
    ],
    Input(component_id='topics_dropdown', component_property='value'),
    Input(component_id='z_axis', component_property='value')
)
def update_topics_table(classification, z_axis):
    work_types = ['review', 'article']  # Work types to be analyzed
    topics_table = topics_works[topics_works['work_type'].isin(work_types)]  # Filtering rows from the desired work types
    topics_table = topics_table.drop_duplicates(['work_id', classification], keep='first')  # Removing duplicates

    # Define aggregation dictionary
    agg_dict = {
        'n_articles': ('work_id', 'count'),
        'h_index': ('cited_by_count', h_index),
        'mean_referenced_works': ('referenced_works_count', lambda x: round(x.mean(), 2)),
        'mean_authors': ('author_count', lambda x: round(x.mean(), 2))
    }

    # Conditionally include 'domain_name' if value is not 'domain_name'
    if classification != 'domain_name':
        agg_dict['domain_name'] = ('domain_name', 'first')

    # Perform aggregation
    agg_topics = topics_table.groupby(classification).agg(**agg_dict).reset_index()

    #Create 3dplot
    fig = px.scatter_3d(agg_topics.sort_values(by='domain_name'), 
                        x='n_articles', 
                        y='h_index', 
                        z=z_axis, 
                        color = 'domain_name',
                        hover_name=classification).update_layout(height=600)
                        
    fig.update_traces(marker=dict(size=5))  # Adjust size as needed

    return agg_topics.to_dict('records'), fig


