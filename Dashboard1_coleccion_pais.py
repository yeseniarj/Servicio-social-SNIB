# ============================================================
# DASHBOARD SNIB — 
# ============================================================

import polars as pl
import pandas as pd
from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import warnings

warnings.simplefilter(action="ignore", category=pd.errors.SettingWithCopyWarning)

# ============================================================
# 1. CARGA Y TRANSFORMACIÓN (POLARS)
# ============================================================

parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

snib_lazy_df = pl.scan_parquet(parquet_path)

# ============================================================
# 2. Configuracion y limpieza de datos
# ============================================================

valores_excluir = ["NO APLICA", " ", "NO DISPONIBLE", "", "NaN", "null", None]
N = 10

snib_lazy_df = pl.scan_parquet(parquet_path)

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

df = final_polars_df.to_pandas()

# ============================================================
# 3. DROPDOWNS
# ============================================================

years = sorted(df["aniocolecta"].dropna().unique().tolist())
year_options = [{"label": "Todos", "value": "Todos"}] + [
    {"label": int(y), "value": int(y)} for y in years
]

pais_totales = (
    df.groupby("paiscoleccion")["conteo"].sum()
    .sort_values(ascending=False)
    .reset_index()
)

country_options = [{"label": "Todos", "value": "Todos"}] + [
    {"label": row["paiscoleccion"], "value": row["paiscoleccion"]}
    for _, row in pais_totales.iterrows()
]

# ============================================================
# 4. LAYOUT — DOS GRÁFICAS LADO A LADO
# ============================================================

app = Dash(__name__)

app.layout = html.Div([

    html.H1("EJEMPLARES POR COLECCIÓN Y PAÍS EN EL SNIB", 
            style={"textAlign": "center"}),

    html.Div([
        html.Div([
            html.Label("Seleccionar Año:"),
            dcc.Dropdown(id="dropdown_anio", options=year_options, value="Todos", clearable=False)
        ], style={"width": "48%", "display": "inline-block"}),

        html.Div([
            html.Label("Seleccionar País:"),
            dcc.Dropdown(id="dropdown_pais", options=country_options, value="Todos", clearable=False)
        ], style={"width": "48%", "display": "inline-block"})
    ], style={"padding": "10px 20px"}),

    html.Div([
        html.Div([
            dcc.Graph(id="graf_top_colecciones")
        ], style={"width": "50%", "display": "inline-block"}),

        html.Div([
            dcc.Graph(id="graf_top_paises")
        ], style={"width": "50%", "display": "inline-block"})
    ])

])

# ============================================================
# 5. CALLBACK
# ============================================================

@app.callback(
    Output("graf_top_colecciones", "figure"),
    Output("graf_top_paises", "figure"),
    Input("dropdown_anio", "value"),
    Input("dropdown_pais", "value")
)
def update_charts(selected_year, selected_country):

    df_filtrado = df.copy()

    if selected_year != "Todos":
        df_filtrado = df_filtrado[df_filtrado["aniocolecta"] == selected_year]

    if selected_country != "Todos":
        df_filtrado = df_filtrado[df_filtrado["paiscoleccion"] == selected_country]

    # ===================== TOP COLECCIONES =====================

    top_colecciones = (
        df_filtrado.groupby("coleccion_agrupada")["conteo"].sum()
        .reset_index()
        .sort_values("conteo", ascending=False)
        .head(N)
    )

    # obtener país asociado (modo)
    top_colecciones["País asociado"] = top_colecciones["coleccion_agrupada"].apply(
        lambda x: (
            df_filtrado[df_filtrado["coleccion_agrupada"] == x]["paiscoleccion"].mode()[0]
            if x != "Otras" and not df_filtrado[df_filtrado["coleccion_agrupada"] == x].empty
            else ""
        )
    )

    # Año en título
    titulo_anio = selected_year if selected_year != "Todos" else "Todos los años"

    fig_colecciones = px.bar(
        top_colecciones,
        x="coleccion_agrupada",
        y="conteo",
        text="País asociado",         # <-- vuelve a aparecer
        title=f"Colecciones en {titulo_anio} (Top 10)",
        labels={"conteo": "Número de registros", "coleccion_agrupada": "Colección"},
        color="conteo",
        template="seaborn",
        height=420
    )
    fig_colecciones.update_traces(textposition="outside")
    fig_colecciones.update_layout(showlegend=False, plot_bgcolor="white")
    fig_colecciones.update_xaxes(tickangle=45)

    # ===================== TOP PAISES =====================

    top_paises = (
        df_filtrado.groupby("paiscoleccion")["conteo"].sum()
        .sort_values(ascending=False)
        .reset_index()
        .head(10)
    )

    fig_paises = px.bar(
        top_paises,
        y="paiscoleccion",
        x="conteo",
        orientation="h",
        title="Países (conteo total por país)",
        labels={"conteo": "Número de registros", "paiscoleccion": ""},
        color="conteo",
        template="seaborn",
        height=420
    )
    fig_paises.update_layout(showlegend=False, plot_bgcolor="white")
    fig_paises.update_yaxes(autorange="reversed")  # mantiene orden descendente

    return fig_colecciones, fig_paises


# ============================================================
# 6. EJECUCIÓN DEL SERVIDOR
# ============================================================

if __name__ == "__main__":
    app.run(debug=True)