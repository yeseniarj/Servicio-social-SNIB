{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "3600e31a-c6b7-4d4b-9b51-d970eaafa5c0",
   "metadata": {},
   "source": [
    "Grafica matriz de calor, muestra el procentaje del estatus taxónomico de los grupos biológicos "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "c1c91e5b-f425-4994-9b98-26a2bf2e3de1",
   "metadata": {},
   "outputs": [],
   "source": [
    "import polars as pl\n",
    "import plotly.express as px"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "f667bf95-0de1-4cf7-88fa-961cf5daf2ef",
   "metadata": {},
   "outputs": [],
   "source": [
    "SNIB_DATA_PARQUET = \"./data/SNIBEjemplares_20250803_233426.parquet\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 4,
   "id": "1e3a2cb1-ee27-498e-ad7e-0de3cd5ef62d",
   "metadata": {},
   "outputs": [],
   "source": [
    "snib_lazy_df = pl.scan_parquet(SNIB_DATA_PARQUET)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "60c007d1-3690-4b73-a461-b4ad85dc93cb",
   "metadata": {},
   "outputs": [],
   "source": [
    "snib_lazy_df.collect_schema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "aa1227af-75c5-4237-95be-6b487c740d9a",
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "DataFrame con porcentaje por grupo taxonómico:\n",
      "shape: (21, 4)\n",
      "┌────────────┬───────────────┬─────────┬──────────────────┐\n",
      "│ estatustax ┆ grupobio      ┆ conteo  ┆ porcentaje_grupo │\n",
      "│ ---        ┆ ---           ┆ ---     ┆ ---              │\n",
      "│ str        ┆ str           ┆ u32     ┆ f64              │\n",
      "╞════════════╪═══════════════╪═════════╪══════════════════╡\n",
      "│ sinónimo   ┆ Anfibios      ┆ 85801   ┆ 40.668414        │\n",
      "│ aceptado   ┆ Plantas       ┆ 7686606 ┆ 88.647989        │\n",
      "│ aceptado   ┆ Cromistas     ┆ 102937  ┆ 59.864844        │\n",
      "│ válido     ┆ Cromistas     ┆ 42811   ┆ 24.897499        │\n",
      "│ sinónimo   ┆ Invertebrados ┆ 152503  ┆ 5.621594         │\n",
      "│ …          ┆ …             ┆ …       ┆ …                │\n",
      "│ sinónimo   ┆ Aves          ┆ 659780  ┆ 2.51012          │\n",
      "│ sinónimo   ┆ Hongos        ┆ 17006   ┆ 11.595448        │\n",
      "│ sinónimo   ┆ Plantas       ┆ 984325  ┆ 11.352011        │\n",
      "│ sinónimo   ┆ Protozoarios  ┆ 801     ┆ 7.543794         │\n",
      "│ válido     ┆ Anfibios      ┆ 125176  ┆ 59.331586        │\n",
      "└────────────┴───────────────┴─────────┴──────────────────┘\n"
     ]
    }
   ],
   "source": [
    "# Paso 1: Selección y filtrado inicial\n",
    "valores_a_excluir = [\"NO APLICA\", \"\", \"NO DISPONIBLE\", \" \"]\n",
    "\n",
    "ejemplares_especies_tax = (\n",
    "    snib_lazy_df.select([\"estatustax\", \"grupobio\"])\n",
    "    .filter(\n",
    "        pl.col(\"estatustax\").is_not_null() &\n",
    "        ~pl.col(\"estatustax\").is_in(valores_a_excluir)\n",
    "    )\n",
    ")\n",
    "\n",
    "# Paso 2: Agrupar y contar\n",
    "df_resultado_lazy1 = ejemplares_especies_tax.group_by(\n",
    "    [\"estatustax\", \"grupobio\"]\n",
    ").agg(\n",
    "    pl.len().alias(\"conteo\")\n",
    ")\n",
    "\n",
    "# Paso 3: Calcular porcentaje directamente con window function\n",
    "df_con_porcentaje = df_resultado_lazy1.with_columns(\n",
    "    (\n",
    "        pl.col(\"conteo\") / pl.col(\"conteo\").sum().over(\"grupobio\") * 100\n",
    "    ).alias(\"porcentaje_grupo\")\n",
    ")\n",
    "\n",
    "# Paso 4: Materializar el resultado\n",
    "df_resultado_porcentaje = df_con_porcentaje.collect()\n",
    "\n",
    "# Mostrar resultado\n",
    "print(\"DataFrame con porcentaje por grupo taxonómico:\")\n",
    "print(df_resultado_porcentaje)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "cacb7854-0d8a-4709-b934-98c220f0d361",
   "metadata": {},
   "outputs": [],
   "source": [
    "fig = px.density_heatmap(\n",
    "    df_resultado_porcentaje,\n",
    "    x=\"grupobio\",\n",
    "    y=\"estatustax\",\n",
    "    z=\"porcentaje_grupo\",\n",
    "    histfunc=\"sum\",\n",
    "    title=\" Estatus taxonómico de grupos biológicos\",\n",
    "    \n",
    "    labels={\n",
    "        \"grupobio\": \"Grupo Biológico\",\n",
    "        \"estatustax\": \"Estatus\",\n",
    "        \"porcentaje_grupo\": \"Porcentaje\"\n",
    "    }\n",
    ")\n",
    "# Editamos los títulos de ejes y centramos el título\n",
    "fig.update_layout(\n",
    "    xaxis_title=\"Grupo Biológico\",\n",
    "    yaxis_title=\" Estatus taxonómico\",\n",
    "    title_x=0.5,\n",
    "    coloraxis_colorbar=dict(\n",
    "        title=\"Porcentaje\"   \n",
    "    )\n",
    ")\n",
    "\n",
    "fig.show()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python (exploreSNIB Polars)",
   "language": "python",
   "name": "exploresnib_polars"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.12.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
