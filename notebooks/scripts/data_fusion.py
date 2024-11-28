import geopandas as gpd
import pandas as pd
import numpy as np

def carregar_geojson_em_dataframe(caminho_arquivo):
    try:
        # Carrega o GeoJSON usando o geopandas
        gdf = gpd.read_file(caminho_arquivo)
        return gdf
    except Exception as e:
        print(f"Erro ao abrir o arquivo GeoJSON: {e}")
        return None


acidentes = carregar_geojson_em_dataframe("data/acidentes.geojson")

estacoes = carregar_geojson_em_dataframe("data/estacoes.geojson")