# --- 1. IMPORTACIONES ---
from dash import Dash, dcc, html, Output, Input
import polars as pl
import plotly.express as px

# --- 2. CARGA DE DATOS ---
parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

    snib_lazy_df = pl.scan_parquet(parquet_path)

# --- 3. CONFIGURACIÓN Y LIMPIEZA DE DATOS ---
COLUMNAS_INTERES = ["aniocolecta", "paiscoleccion", "procedenciaejemplar", "grupobio"]
VALORES_EXCLUIR = ["NO APLICA", "NO DISPONIBLE", "no disponible", None]

base_limpia_df = (
    snib_lazy_df
    .filter(pl.all_horizontal(pl.col(COLUMNAS_INTERES).is_not_null()))
    .filter(~pl.any_horizontal(pl.col(COLUMNAS_INTERES).is_in(VALORES_EXCLUIR)))
    .with_columns(pl.col("aniocolecta").cast(pl.Int64, strict=False))
    .filter(pl.col("aniocolecta").is_between(1500, 2025))
)

# --- 4. MAPEADO DE CATEGORÍAS (TRADUCCIÓN) ---
mapeo_procedencia = {
    "HumanObservation": "Observación humana",
    "PreservedSpecimen": "Especimen preservado",
    "MachineObservation": "Observación de máquina",
    "LivingSpecimen": "Especimen vivo",
    "FossilSpecimen": "Especimen fósil",
    "Occurrence": "Evidencia",
    "Materialsample": "Muestra registrada",
    "MaterialCitation": "Material citado"
}

mapeo_df = pl.DataFrame({
    "procedenciaejemplar": list(mapeo_procedencia.keys()),
    "procedenciaejemplar_es": list(mapeo_procedencia.values())
})

base_limpia_df = base_limpia_df.join(mapeo_df.lazy(), on="procedenciaejemplar", how="left")
base_limpia_df = base_limpia_df.filter(pl.col("procedenciaejemplar_es").is_not_null())

# --- 5. PREPARACIÓN DE DATOS PARA LOS GRÁFICOS ---
app_df = (
    base_limpia_df
    .group_by(["aniocolecta", "paiscoleccion", "grupobio", "procedenciaejemplar_es"])
    .agg(pl.len().alias("total_registros"))
    .collect()
    .to_pandas()
)

print(f"✓ Total de registros procesados: {len(app_df)}")
print(f"✓ Años disponibles: {app_df['aniocolecta'].min()} - {app_df['aniocolecta'].max()}")
print(f"✓ Países únicos: {app_df['paiscoleccion'].nunique()}")

# --- 5.1. Obtener Listas para los Filtros ---
available_years = sorted(app_df['aniocolecta'].unique())
available_countries = sorted(app_df['paiscoleccion'].unique())

# --- 6. CONSTRUCCIÓN DE LA APLICACIÓN DASH ---
app = Dash(__name__)

app.layout = html.Div(style={'fontFamily': 'Arial, sans-serif', 'padding': '20px'}, children=[

    html.H1("Total de ejemplares por procedencia, país y año",
            style={'textAlign': 'center', 'color': '#2c3e50'}),

    html.P(
        "Seleccione un año y un país para visualizar los resultados.",
        style={'textAlign': 'center', 'color': '#2c3e50', 'marginTop': '-10px'}
    ),

    html.Div(style={'display': 'flex', 'justifyContent': 'center',
                    'gap': '30px', 'padding': '20px',
                    'backgroundColor': '#f8f9fa', 'borderRadius': '10px'}, children=[

        html.Div(children=[
            html.Label("Seleccionar Año:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='year-dropdown',
                options=[{'label': year, 'value': year} for year in available_years],
                value=available_years[-1], clearable=False
            )
        ], style={'width': '300px'}),

        html.Div(children=[
            html.Label("Seleccionar País:", style={'fontWeight': 'bold'}),
            dcc.Dropdown(
                id='country-dropdown',
                options=[{'label': country, 'value': country} for country in available_countries],
                value=available_countries[0]
            )
        ], style={'width': '300px'})
    ]),

    html.Div(style={'display': 'flex', 'flexDirection': 'row', 'gap': '20px', 'marginTop': '20px'}, children=[
        html.Div(dcc.Graph(id='bubble-chart'), style={'flex': '1.5'}),
        html.Div(dcc.Graph(id='bar-chart-aves'), style={'flex': '1'})
    ])
])

# --- 7. CALLBACK PARA ACTUALIZAR LOS GRÁFICOS ---
@app.callback(
    Output('bubble-chart', 'figure'),
    Output('bar-chart-aves', 'figure'),
    Input('year-dropdown', 'value'),
    Input('country-dropdown', 'value')
)
def update_graphs(selected_year, selected_country):

    filtered_df = app_df[
        (app_df['aniocolecta'] == selected_year) &
        (app_df['paiscoleccion'] == selected_country)
    ]

    if filtered_df.empty:
        empty_fig = px.scatter(title=f"Sin datos para {selected_country}, {selected_year}")
        empty_fig.update_layout(
            annotations=[{
                'text': 'No hay datos disponibles para esta combinación',
                'xref': 'paper', 'yref': 'paper',
                'showarrow': False, 'font': {'size': 16}
            }]
        )
        return empty_fig, empty_fig

    # --- 7.1. Gráfico de Burbujas (sin Aves) ---
    df_burbujas = filtered_df[filtered_df['grupobio'] != 'Aves']

    if df_burbujas.empty:
        bubble_fig = px.scatter(title=f"Grupos Biológicos en {selected_country}, {selected_year} (sin Aves)")
        bubble_fig.update_layout(
            annotations=[{
                'text': 'No hay datos de grupos biológicos (sin Aves)',
                'xref': 'paper', 'yref': 'paper',
                'showarrow': False, 'font': {'size': 14}
            }]
        )
    else:
        bubble_fig = px.scatter(
            df_burbujas, x="grupobio", y="total_registros", size="total_registros",
            color="procedenciaejemplar_es", hover_name="grupobio", size_max=80,
            title=f"Grupos Biológicos en {selected_country}, {selected_year} (sin Aves)",
            labels={"grupobio": "Grupo Biológico", "total_registros": "Total de Registros",
                    "procedenciaejemplar_es": "Procedencia"}
        )
        bubble_fig.update_xaxes(tickangle=-45)
        bubble_fig.update_layout(transition_duration=500, yaxis_type="log")

    # --- 7.2. Gráfico de Barras (Aves) ---
    df_aves = filtered_df[filtered_df['grupobio'] == 'Aves']

    if df_aves.empty:
        bar_aves_fig = px.bar(title=f"Registros de Aves en {selected_country}, {selected_year}")
        bar_aves_fig.update_layout(
            annotations=[{
                'text': 'No hay datos de Aves para esta selección',
                'xref': 'paper', 'yref': 'paper',
                'showarrow': False, 'font': {'size': 14}
            }]
        )
    else:
        bar_aves_fig = px.bar(
            df_aves, x="procedenciaejemplar_es", y="total_registros",
            color="procedenciaejemplar_es",
            title=f"Registros de Aves en {selected_country}, {selected_year}",
            labels={"procedenciaejemplar_es": "Tipo de Procedencia", "total_registros": "Total de Registros"}
        )
        bar_aves_fig.update_layout(transition_duration=500)

    bar_aves_fig.update_layout(showlegend=False)

    return bubble_fig, bar_aves_fig


# ======================================================
# EJECUCIÓN
# ======================================================
if __name__ == "__main__":
    app.run(debug=True)
