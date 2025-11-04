import polars as pl
import plotly
import plotly.express as px
import pandas as pd
from dash import Dash, dcc, html, Input, Output

print("Polars versión:", pl.__version__)
print("Plotly versión:", plotly.__version__)
print("Pandas versión:", pd.__version__)
# 1. Definir las rutas de los archivos

#csv_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares_20250710_004212.csv"
parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

#df_lazy = pl.scan_csv(csv_path)
#df_lazy.sink_parquet(parquet_path)
snib_lazy_df = pl.scan_parquet(parquet_path)

snib_lazy_df.collect_schema()

valores_excluir = ["NO APLICA", " ", "NO DISPONIBLE", "", "NaN", "null", None]
N = 10

final_polars_df = (
    snib_lazy_df
    .filter(
        pl.col("aniocolecta").is_not_null() &
        pl.col("coleccion").is_not_null() &
        pl.col("paiscoleccion").is_not_null() &
        ~pl.col("coleccion").is_in(valores_excluir) &
        ~pl.col("paiscoleccion").is_in(valores_excluir)
    )
    .with_columns(
        pl.when(pl.col("aniocolecta").cast(pl.Utf8).str.strip_chars().is_in(["null", "", "0"]))
        .then(None)
        .otherwise(pl.col("aniocolecta"))
        .alias("aniocolecta")
    )
    .with_columns(pl.col("aniocolecta").cast(pl.Int32, strict=False))
    .filter(pl.col("aniocolecta").is_not_null())
    .group_by(["aniocolecta", "paiscoleccion", "coleccion"])
    .agg(pl.len().alias("conteo"))
    .with_columns(
        pl.col("conteo").rank("dense", descending=True).over("aniocolecta").alias("rank")
    )
    .with_columns(
        pl.when(pl.col("rank") <= N)
        .then(pl.col("coleccion"))
        .otherwise(pl.lit("Otras"))
        .alias("coleccion_agrupada")
    )
    .group_by(["aniocolecta", "paiscoleccion", "coleccion_agrupada"])
    .agg(pl.sum("conteo").alias("conteo"))
    .sort(["aniocolecta", "conteo"], descending=[False, True])
    .collect()
)

df = final_polars_df.to_pandas() if not final_polars_df.is_empty() else pd.DataFrame()


# =================================================================
# 2. CREACIÓN DE LA APLICACIÓN DASH (CON MEJORAS VISUALES)
# =================================================================

app = Dash(__name__)

all_years = sorted(df["aniocolecta"].dropna().unique()) if not df.empty else []
all_countries = sorted(df["paiscoleccion"].dropna().unique()) if not df.empty else []

app.layout = html.Div([
    html.H2(f"EJEMPLARES POR COLECCIÓN Y PAÍS EN EL SNIB", style={"textAlign": "center", "marginTop": "20px", "color": "#333"}),
    html.Div([
        html.Div([
            html.Label("Selecciona un año:", style={"fontWeight": "bold"}),
            dcc.Dropdown(id="year-dropdown", options=[{"label": str(y), "value": y} for y in all_years], value=max(all_years) if all_years else None, clearable=False),
        ], style={"width": "48%", "display": "inline-block"}),
        html.Div([
            html.Label("Selecciona un País (o todos):", style={"fontWeight": "bold"}),
            dcc.Dropdown(id="country-dropdown", options=[{'label': 'Todos los Países', 'value': 'all'}] + [{'label': c, 'value': c} for c in all_countries], value='all', clearable=False),
        ], style={"width": "48%", "display": "inline-block", "float": "right"}),
    ], style={"padding": "20px", "borderBottom": "2px solid #ccc"}),
    html.Div([
        dcc.Graph(id="top-collections-chart"),
        dcc.Graph(id="country-collections-chart"),
    ], style={"marginTop": "20px", "display": "flex", "flexWrap": "wrap", "justifyContent": "space-around"})
], style={"fontFamily": "'Open Sans', sans-serif", "backgroundColor": "#f9f9f9"})

# =================================================================
# 3. CALLBACK INTERACTIVO (CON LÓGICA Y ESTÉTICA MEJORADA)
# =================================================================
@app.callback(
    Output("top-collections-chart", "figure"),
    Output("country-collections-chart", "figure"),
    Input("year-dropdown", "value"),
    Input("country-dropdown", "value")
)
def update_charts(selected_year, selected_country):
    if not selected_year or df.empty:
        empty_fig = px.bar(title="Selecciona un año para empezar")
        return empty_fig, empty_fig

    filtered_df = df[df["aniocolecta"] == selected_year]
    if selected_country != 'all':
        filtered_df = filtered_df[filtered_df["paiscoleccion"] == selected_country]

    if filtered_df.empty:
        empty_fig = px.bar(title=f"No hay datos para {selected_country} en {selected_year}")
        return empty_fig, empty_fig

    collections_grouped = filtered_df.groupby("coleccion_agrupada")["conteo"].sum().reset_index()
    collections_grouped['sort_order'] = collections_grouped['coleccion_agrupada'].apply(lambda x: 1 if x == 'Otras' else 0)
    collections_grouped = collections_grouped.sort_values(by=['sort_order', 'conteo'], ascending=[True, False])

    # Gráfico de Top Colecciones (Barras Verticales)
    fig_top_collections = px.bar(
        collections_grouped,
        x="coleccion_agrupada",
        y="conteo",
        color="conteo",
        color_continuous_scale=px.colors.sequential.Viridis,
        title=f"Colecciones en {selected_year} (Top {N})" + (f" de {selected_country}" if selected_country != 'all' else ""),
        labels={"coleccion_agrupada": "Colección", "conteo": "Número de Registros"},
        text_auto=True,
        template="seaborn"
    )
    fig_top_collections.update_layout(
        showlegend=False,
        plot_bgcolor="white",
        xaxis={'categoryorder':'total descending'},
        height=500,  # Aumentar altura
        margin=dict(l=40, r=40, t=80, b=40)
    )
    fig_top_collections.update_xaxes(tickangle=45)

    top_collections_names = collections_grouped[collections_grouped['coleccion_agrupada'] != 'Otras']['coleccion_agrupada'].head(N).tolist()
    top_collections_df = filtered_df[filtered_df['coleccion_agrupada'].isin(top_collections_names)]
    country_grouped = top_collections_df.groupby("paiscoleccion")["conteo"].sum().reset_index().sort_values("conteo", ascending=False) # Orden descendente

    # Gráfico de Países (Barras Horizontales)
    fig_country_collections = px.bar(
        country_grouped,
        y="paiscoleccion",
        x="conteo",
        orientation='h',
        title=f"Paises (Top {len(top_collections_names)})",
        labels={"paiscoleccion": "País"}, # Se elimina la etiqueta 'conteo' para evitar la leyenda 'Número de Registros'
        text_auto=True,
        template="seaborn"
    )
    # Aplicar un color uniforme a las barras y asegurar que no haya leyenda de color.
    fig_country_collections.update_traces(marker_color='#636EFA') # Ejemplo de color azul por defecto

    fig_country_collections.update_layout(
        plot_bgcolor="white",
        yaxis_title=None,
        yaxis=dict(autorange="reversed"), # Invertir el eje para que la barra más grande esté arriba
        height=500, # Aumentar altura
        margin=dict(l=40, r=40, t=80, b=40),
        coloraxis_showscale=False, # Asegurarse de que no se muestre la escala de color si se hubiera definido
        showlegend=False # Asegurarse de que no se muestre ninguna leyenda
    )

    fig_top_collections.update_layout(width=700) # Ajustar ancho para que quepan lado a lado
    fig_country_collections.update_layout(width=500) # Ajustar ancho para que quepan lado a lado
    return fig_top_collections, fig_country_collections

    # =================================================================
# 4. EJECUTAR EL SERVIDOR LOCAL
# =================================================================
if __name__ == "__main__":
    app.run(debug=True)