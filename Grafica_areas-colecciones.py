
import polars as pl
import plotly.graph_objects as go


# -------------------------------------------------------
# 1. Cargar datos en modo lazy 
# -------------------------------------------------------

SNIB_DATA_PARQUET = "./data/SNIBEjemplares_20250803_233426.parquet"
snib_lazy_df = pl.scan_parquet(SNIB_DATA_PARQUET)

# -------------------------------------------------------
# 2. Contar ejemplares por año y colección 
# -------------------------------------------------------
valores_excluir = ["NO APLICA", "NO DISPONIBLE", "", " "]

conteo_df = (
    snib_lazy_df
    .select(["aniocolecta", "coleccion"])
    .filter(
        pl.col("coleccion").is_not_null() &
        pl.col("aniocolecta").is_not_null()
    )
    .filter(~pl.col("aniocolecta").cast(pl.Utf8).str.contains("null"))  
    .filter(~pl.col("coleccion").is_in(valores_excluir))
    .group_by(["aniocolecta", "coleccion"])
    .agg(pl.len().alias("conteo"))
    .collect()  
)

print("conteo_df shape:", conteo_df.shape)
print("conteo_df columns:", conteo_df.columns)
print("conteo_df dtypes:", conteo_df.dtypes)

# -------------------------------------------------------
# 3. Obtener las 9 colecciones más grandes
# -------------------------------------------------------
top9_colecciones = (
    conteo_df
    .group_by("coleccion")
    .agg(pl.col("conteo").sum().alias("total"))
    .sort("total", descending=True)
    .limit(9)
    .select("coleccion")
    .to_series()
    .to_list()
)

print("top9_colecciones:", top9_colecciones)

# -------------------------------------------------------
# 4. Agrupar colecciones pequeñas en "otras"
# -------------------------------------------------------
conteo_final_df = (
    conteo_df
    .with_columns(
        pl.when(pl.col("coleccion").is_in(top9_colecciones))
        .then(pl.col("coleccion"))
        .otherwise(pl.lit("otras"))  
        .alias("coleccion_agrupada")
    )
    .group_by(["aniocolecta", "coleccion_agrupada"])
    .agg(pl.col("conteo").sum().alias("conteo"))
    .sort(["aniocolecta", "coleccion_agrupada"])
)

print("conteo_final_df shape:", conteo_final_df.shape)
print("conteo_final_df columns:", conteo_final_df.columns)
print("conteo_final_df dtypes:", conteo_final_df.dtypes)


conteo_final_df = conteo_final_df.with_columns(
    pl.col("aniocolecta").cast(pl.Utf8).cast(pl.Int32)  
)

# -------------------------------------------------------
# 5. Pivotar tabla a formato ancho
# -------------------------------------------------------
tabla_wide = (
    conteo_final_df.pivot(
        values="conteo",
        index="aniocolecta",
        on="coleccion_agrupada",
        aggregate_function="first"
    )
    .fill_null(0)
)

print("tabla_wide shape:", tabla_wide.shape)
print("tabla_wide columns:", tabla_wide.columns)

# -------------------------------------------------------
# 6. Construir gráfica de áreas acumuladas
# -------------------------------------------------------
df_wide = tabla_wide
x = df_wide["aniocolecta"].to_list()

fig = go.Figure()

for col in df_wide.columns:
    if col != "aniocolecta":
        fig.add_trace(go.Scatter(
            x=x,
            y=df_wide[col].to_list(),
            stackgroup='one',
            name=col
        ))

fig.update_layout(
    title="Tendencia temporal de ejemplares en las 9 colecciones principales y otras",
    xaxis_title="Año de colecta",
    yaxis_title="Número de ejemplares",
    legend_title="Colección"
)

fig.update_xaxes(rangeslider_visible=True)

fig.show()

