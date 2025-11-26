# ============================================================
# Sunburst Taxonómico 
# ============================================================

import polars as pl
import plotly.express as px
import plotly.io as pio
import pandas as pd

# -------------------------------
# 1. Ruta al archivo parquet
# -------------------------------
parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

snib_lazy_df = pl.scan_parquet(parquet_path)

print("Mostrando primeras filas del archivo:")
print(snib_lazy_df.head().collect())

# -------------------------------
# 2. Selección de columnas taxonómicas
# -------------------------------
grupobio_a_especie = (
    snib_lazy_df.select([
        "reinovalido",
        "phylumdivisionvalido",
        "clasevalida",
        "ordenvalido",
        "familiavalida",
        "generovalido"
    ])
)

# -------------------------------
# 3. Filtrar registros vacíos
# -------------------------------
df_reducido = grupobio_a_especie.filter(
    (pl.col("reinovalido").is_not_null()) & (pl.col("reinovalido") != "")
)

# -------------------------------
# 4. Agrupar por nivel taxonómico
# -------------------------------
resultado_lazy = (
    df_reducido
    .group_by([
        "reinovalido",
        "phylumdivisionvalido",
        "clasevalida",
        "ordenvalido",
        "familiavalida",
        "generovalido"
    ])
    .agg(pl.len().alias("cantidad"))
)

# EXTRAER SOLO LOS 1000 GÉNEROS MÁS GRANDES
top_generos = (
    resultado_lazy
    .sort("cantidad", descending=True)
    .limit(1000)
    .collect()
)

print("\nTop 1000 géneros procesados para el Sunburst:")
print(top_generos.head())

# -------------------------------
# 5. Convertir Polars → Pandas para Plotly
# -------------------------------
df_pandas = top_generos.to_pandas()

# -------------------------------
# 6. Gráfico Sunburst
# -------------------------------
fig = px.sunburst(
    df_pandas,
    path=[
        "reinovalido",
        "phylumdivisionvalido",
        "clasevalida",
        "ordenvalido",
        "familiavalida",
        "generovalido"
    ],
    values="cantidad",
    title="Distribución taxonómica SNIB (Reino → Género, Top 1000)",
    width=1200,
    height=850,
)

# Personalización del hover (usando customdata)
fig.update_traces(
    textinfo="label+value",
    hovertemplate=(
        "<b>Reino:</b> %{customdata[0]}<br>"
        "<b>Phylum/División:</b> %{customdata[1]}<br>"
        "<b>Clase:</b> %{customdata[2]}<br>"
        "<b>Orden:</b> %{customdata[3]}<br>"
        "<b>Familia:</b> %{customdata[4]}<br>"
        "<b>Género:</b> %{customdata[5]}<br>"
        "<b>Cantidad:</b> %{value}<extra></extra>"
    ),
    customdata=df_pandas[
        [
            "reinovalido",
            "phylumdivisionvalido",
            "clasevalida",
            "ordenvalido",
            "familiavalida",
            "generovalido"
        ]
    ].values
)

# Estética
fig.update_layout(
    title_font_size=24,
    uniformtext=dict(minsize=12, mode="hide")
)

# -------------------------------
# 7. Mostrar gráfico en navegador
# -------------------------------
pio.renderers.default = "browser"
fig.show()