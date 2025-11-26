# Grafica matriz de calor, muestra el procentaje del estatus taxónomico de los grupos biológicos

import polars as pl
import plotly.express as px

parquet_path = r"C:\Users\danti\Downloads\SNIBEjemplares_20250710_004212\SNIBEjemplares.parquet"

snib_lazy_df = pl.scan_parquet(parquet_path)


# Mostrar el esquema del dataframe 
print(snib_lazy_df.collect_schema())

# Paso 1: Selección y filtrado inicial
valores_a_excluir = ["NO APLICA", "", "NO DISPONIBLE", " "]

ejemplares_especies_tax = (
    snib_lazy_df.select(["estatustax", "grupobio"])
    .filter(
        pl.col("estatustax").is_not_null() &
        ~pl.col("estatustax").is_in(valores_a_excluir)
    )
)

# Paso 2: Agrupar y contar
df_resultado_lazy1 = ejemplares_especies_tax.group_by(
    ["estatustax", "grupobio"]
).agg(
    pl.len().alias("conteo")
)

# Paso 3: Calcular porcentaje directamente con window function
df_con_porcentaje = df_resultado_lazy1.with_columns(
    (
        pl.col("conteo") / pl.col("conteo").sum().over("grupobio") * 100
    ).alias("porcentaje_grupo")
)

# Paso 4: Materializar el resultado
df_resultado_porcentaje = df_con_porcentaje.collect()

# Mostrar resultado
print("DataFrame con porcentaje por grupo taxonómico:")
print(df_resultado_porcentaje)

# Crear el gráfico de calor
fig = px.density_heatmap(
    df_resultado_porcentaje,
    x="grupobio",
    y="estatustax",
    z="porcentaje_grupo",
    histfunc="sum",
    title=" Estatus taxonómico de grupos biológicos",
   
    labels={
        "grupobio": "Grupo Biológico",
        "estatustax": "Estatus",
        "porcentaje_grupo": "Porcentaje"
    }
)

# Editamos los títulos de ejes y centramos el título
fig.update_layout(
    xaxis_title="Grupo Biológico",
    yaxis_title=" Estatus taxonómico",
    title_x=0.5,
    coloraxis_colorbar=dict(
        title="Porcentaje"  
    )
)

# Mostrar el gráfico 

import plotly.io as pio
pio.renderers.default = "browser"

fig.show()

