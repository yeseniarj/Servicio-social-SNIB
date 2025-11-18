{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "72f48f53-dea6-4b78-9f78-83578b720120",
   "metadata": {},
   "source": [
    "# Grafica Sunburst \"Distribución taxonómica de registros válidos del SNIB (Reino a Género)\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 1,
   "id": "1a5f54b5-8ca1-4743-b12b-d56760d0e94f",
   "metadata": {},
   "outputs": [],
   "source": [
    "import polars as pl\n",
    "import plotly.express as px\n",
    "import plotly.graph_objects as go"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 2,
   "id": "e2271c0d-6c07-4077-a8de-4c992557d733",
   "metadata": {},
   "outputs": [],
   "source": [
    "SNIB_DATA_PARQUET = \"./data/SNIBEjemplares_20250803_233426.parquet\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 3,
   "id": "eede4d7d-6dde-4815-b93e-db977a382c38",
   "metadata": {},
   "outputs": [],
   "source": [
    "snib_lazy_df = pl.scan_parquet(SNIB_DATA_PARQUET)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "760e21f5-507f-491b-af43-aea06024730f",
   "metadata": {},
   "outputs": [],
   "source": [
    "snib_lazy_df.collect_schema()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "a4d33e32-f733-444f-a57d-62233a2a620d",
   "metadata": {},
   "outputs": [],
   "source": [
    "niveles_de_interes = [\n",
    "        \"reinovalido\",\n",
    "        \"phylumdivisionvalido\",\n",
    "        \"clasevalida\",\n",
    "        \"ordenvalido\",\n",
    "        \"familiavalida\",\n",
    "        \"generovalido\"\n",
    "    ]\n",
    "\n",
    "nivel_raiz = \"familia\"\n",
    "filtro_raiz = \"Fabacea\""
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 5,
   "id": "b8f301c9-b9ff-4fc7-878c-9a614aab04d7",
   "metadata": {},
   "outputs": [],
   "source": [
    "grupobio_a_especie = (\n",
    "    snib_lazy_df\n",
    "    .select(niveles_de_interes)\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 6,
   "id": "2c66aea5-d9af-483d-b8cf-e9762a133f02",
   "metadata": {},
   "outputs": [],
   "source": [
    "df_reducido = grupobio_a_especie.filter(\n",
    "    (pl.col(\"reinovalido\").is_not_null()) & (pl.col(\"reinovalido\") != \"\")\n",
    ")\n",
    "resultado_lazy = (\n",
    "    df_reducido\n",
    "    .group_by([\n",
    "        \"reinovalido\",\n",
    "        \"phylumdivisionvalido\",\n",
    "        \"clasevalida\",\n",
    "        \"ordenvalido\",\n",
    "        \"familiavalida\",\n",
    "        \"generovalido\"\n",
    "    ])\n",
    "    .agg([\n",
    "        pl.len().alias(\"cantidad\")\n",
    "    ])\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 7,
   "id": "7af22735-e2b2-466e-9276-276b06b6a28b",
   "metadata": {},
   "outputs": [],
   "source": [
    "top_generos = (\n",
    "    resultado_lazy.lazy()\n",
    "    .sort(\"cantidad\", descending=True)\n",
    "    .head(1000)  # toma los 1000 más grandes\n",
    "    .collect()\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 8,
   "id": "816c76df-5b77-40ca-956a-6fd94e670e7f",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Convertir Polars a diccionario de listas\n",
    "data_dict = top_generos.to_dicts()  # devuelve lista de diccionarios\n",
    "# Crear el Sunburst\n",
    "fig = px.sunburst(\n",
    "    data_dict,\n",
    "    path=[\n",
    "        \"reinovalido\",\n",
    "        \"phylumdivisionvalido\",\n",
    "        \"clasevalida\",\n",
    "        \"ordenvalido\",\n",
    "        \"familiavalida\",\n",
    "        \"generovalido\"\n",
    "    ],\n",
    "    values=\"cantidad\",\n",
    "    title=\"Distribución taxonómica de registros válidos del SNIB (Reino a Género)\",\n",
    "    width=1200,\n",
    "    height=800,\n",
    "    hover_data={\n",
    "        \"reinovalido\": True,\n",
    "        \"phylumdivisionvalido\": True,\n",
    "        \"clasevalida\": True,\n",
    "        \"ordenvalido\": True,\n",
    "        \"familiavalida\": True,\n",
    "        \"generovalido\": True,\n",
    "    }\n",
    ")"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "251d5a1f-8153-454c-86a3-d87dffa5fe32",
   "metadata": {},
   "outputs": [],
   "source": [
    "fig.update_traces(\n",
    "    textinfo=\"label+value\",  # mostrar etiqueta + cantidad\n",
    "    hovertemplate=(\n",
    "        \"<b>Reino:</b> %{customdata[0]}<br>\"\n",
    "        \"<b>Phylum:</b> %{customdata[1]}<br>\"\n",
    "         \"<b>Divisón:</b> %{customdata[1]}<br>\"\n",
    "        \"<b>Clase:</b> %{customdata[2]}<br>\"\n",
    "        \"<b>Orden:</b> %{customdata[3]}<br>\"\n",
    "        \"<b>Familia:</b> %{customdata[4]}<br>\"\n",
    "        \"<b>Género:</b> %{customdata[5]}<br>\"\n",
    "        \"<b>Cantidad:</b> %{value}<extra></extra>\"\n",
    "    )\n",
    ")\n",
    "\n",
    "#  Ajustes\n",
    "fig.update_layout(\n",
    "    title_font_size=24,\n",
    "    uniformtext=dict(minsize=12, mode='hide')\n",
    ")\n",
    "\n",
    "fig.show()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "e14e0fa1-5e20-4674-806c-b40bb628e120",
   "metadata": {},
   "outputs": [],
   "source": []
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
