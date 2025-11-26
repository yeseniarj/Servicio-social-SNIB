
import polars as pl
import plotly.express as px
import plotly.io as pio

parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

snib_lazy_df = pl.scan_parquet(parquet_path)

# =====================================================
# 1) FILTRO BÁSICO
# =====================================================
ejemplares_anio_reino1 = (
    snib_lazy_df
    .select(pl.col(["grupobio", "procedenciaejemplar", "aniocolecta"]))
    .filter(
        (pl.col("grupobio").is_not_null()) &
        (pl.col("procedenciaejemplar").is_not_null()) &
        (pl.col("grupobio").str.strip_chars() != "") &
        (pl.col("procedenciaejemplar").str.strip_chars() != "")
    )
)

# =====================================================
# 2) AGRUPACIÓN
# =====================================================
conteo_por_reinoprocanio_lazy = (
    ejemplares_anio_reino1
    .group_by("grupobio", "procedenciaejemplar")
    .agg(pl.len().alias("conteo"))
    .sort("conteo", descending=True)
)

df_resultado_polars1 = conteo_por_reinoprocanio_lazy.collect()

print("DataFrame generado para gráficas:")
print(df_resultado_polars1)

# =====================================================
# 3) MAPEAR PROCEDENCIA AL ESPAÑOL
# =====================================================
mapeo_procedencia = {
    "HumanObservation": "Observación humana",
    "PreservedSpecimen": "Especimen preservado",
    "MachineObservation": "Observación de máquina",
    "LivingSpecimen": "Especimen vivo",
    "FossilSpecimen": "Especimen fósil",
    "Occurrence": "Evidencia",
    "Materialsample": "Muestra de material",
    "MaterialCitation": "Material citado"
}

mapeo_df = pl.DataFrame({
    "procedenciaejemplar": list(mapeo_procedencia.keys()),
    "procedenciaejemplar_es": list(mapeo_procedencia.values())
})

ejemplares_df = (
    snib_lazy_df.select(pl.col(["grupobio", "procedenciaejemplar"])).collect()
)

ejemplares_df = ejemplares_df.filter(
    (pl.col("grupobio").is_not_null()) &
    (pl.col("procedenciaejemplar").is_not_null()) &
    (pl.col("grupobio").str.strip_chars() != "") &
    (pl.col("procedenciaejemplar").str.strip_chars() != "")
)

ejemplares_mapeados = (
    ejemplares_df.join(mapeo_df, on="procedenciaejemplar", how="left")
    .with_columns(
        pl.when(pl.col("procedenciaejemplar_es").is_not_null())
        .then(pl.col("procedenciaejemplar_es"))
        .otherwise(pl.col("procedenciaejemplar"))
        .alias("procedenciaejemplar_mapeado")
    )
)

conteo_df = ejemplares_mapeados.group_by(
    ["grupobio", "procedenciaejemplar_mapeado"]
).agg(pl.len().alias("conteo"))

# =====================================================
# 4) BARRAS DE TEXTO EN CONSOLA 
# =====================================================
grupos = conteo_df.select("grupobio").unique().to_series().to_list()
procedencias = conteo_df.select("procedenciaejemplar_mapeado").unique().to_series().to_list()

data = {}
for grp, proc, cnt in conteo_df.iter_rows():
    data.setdefault(grp, {})[proc] = cnt

max_bar_length = 40
print("\nCantidad de Ejemplares por Grupo Biológico y Procedencia\n")

for grupo in grupos:
    print(f"Grupo: {grupo}")
    total = sum(data.get(grupo, {}).values())

    for procedencia in procedencias:
        count = data.get(grupo, {}).get(procedencia, 0)
        bar_len = int((count / total) * max_bar_length) if total > 0 else 0
        bar = "█" * bar_len
        print(f"  {procedencia[:20]:20} | {bar} {count}")

    print()

# =====================================================
# 5) GRÁFICA HORIZONTAL 
# =====================================================
totales = (
    conteo_df.group_by("grupobio")
    .agg(pl.col("conteo").sum().alias("total"))
    .sort("total", descending=True)
)

orden_grupos = totales["grupobio"].to_list()

fig = px.bar(
    conteo_df,
    y="grupobio",
    x="conteo",
    color="procedenciaejemplar_mapeado",
    barmode="stack",
    category_orders={"grupobio": orden_grupos},
    labels={
        "grupobio": "Grupo Biológico",
        "conteo": "Número de Ejemplares",
        "procedenciaejemplar_mapeado": "Procedencia"
    },
    title="Procedencia de grupos biológicos (ordenados de mayor a menor)"
)

fig.update_layout(
    xaxis_title="Número de ejemplares",
    yaxis_title="Grupo biológico",
)

pio.renderers.default = "browser"
fig.show()