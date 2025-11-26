# ============================================================
# Top 10 de colecciones por año 
# ============================================================

import polars as pl
import plotly.express as px
import plotly.io as pio
import pandas as pd

# -------------------------------
# 1. Ruta del archivo
# -------------------------------
parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

snib_lazy_df = pl.scan_parquet(parquet_path)

print("Esquema del archivo:")
print(snib_lazy_df.collect_schema())

# -------------------------------
# 2. Preparación de datos
# -------------------------------

# Valor de N (Top N colecciones por año)
N = 10

# Conteo de registros por año y colección
conteos = (
    snib_lazy_df
    .select(["aniocolecta", "coleccion"])
    .group_by(["aniocolecta", "coleccion"])
    .agg(pl.len().alias("conteo"))
)

# Ranking por año
ranked = conteos.with_columns(
    pl.col("conteo")
    .rank(method="dense", descending=True)
    .over("aniocolecta")
    .alias("rank")
)

# --- Parte A: Top N ---
top_n = ranked.filter(pl.col("rank") <= N)

# --- Parte B: Otras ---
otras = (
    ranked.filter(pl.col("rank") > N)
    .group_by("aniocolecta")
    .agg(pl.col("conteo").sum().alias("conteo"))
    .with_columns(pl.lit("Otras").alias("coleccion"))
)

# Unir Top N + Otras
top_n_clean = top_n.select(["aniocolecta", "coleccion", "conteo"])
otras_clean = otras.select(["aniocolecta", "coleccion", "conteo"])

final_df = (
    pl.concat([top_n_clean, otras_clean])
    .sort(["aniocolecta", "conteo"], descending=[False, True])
    .collect()
)

# -------------------------------
# 3. Orden para el gráfico
# -------------------------------
years_in_descending_order = (
    sorted(final_df["aniocolecta"].drop_nulls().unique().to_list(), reverse=True)
)

collections_in_order = final_df["coleccion"].unique().to_list()

# -------------------------------
# 4. Gráfico Plotly
# -------------------------------
fig = px.bar(
    final_df,
    x="aniocolecta",
    y="conteo",
    color="coleccion",
    title=f'Top {N} Colecciones por Año - SNIB (incluye grupo "Otras")',
    labels={
        "aniocolecta": "Año de Colecta",
        "conteo": "Número de Registros",
        "coleccion": "Colección"
    },
    category_orders={
        "aniocolecta": years_in_descending_order,
        "coleccion": collections_in_order
    },
    color_discrete_sequence=px.colors.qualitative.Alphabet,
    template="plotly_white"
)

# -------------------------------
# 5. Mostrar gráfico en navegador
# -------------------------------
pio.renderers.default = "browser"
fig.show()
