import geopandas as gpd

estacoes_proximas = gpd.read_file("../data/DATA_FUSION/BR101-Geral/br101_estacoes_proximas.geojson")

acidentes_gdf = gpd.read_file("../data/DATA_FUSION/BR101-Geral/br101_acidentes.geojson")
acidentes_gdf.dropna(subset=['geometry'], inplace=True)

estacoes_proximas['ano'] = estacoes_proximas['ano'].astype(int)

acidentes_gdf['ano'] = acidentes_gdf['ano'].astype(int)

from shapely.ops import nearest_points
import pandas as pd

def data_fusion(acidentes_input, estacoes_proximas_input):
    # Cria uma cópia dos GeoDataFrames de entrada para evitar modificar os originais
    acidentes_gdf = acidentes_input.copy()
    estacoes_proximas_gdf = estacoes_proximas_input.copy()

    acidentes_output = pd.DataFrame()
    for _, acidente in acidentes_gdf.iterrows():
        # Encontra a estação climática mais próxima
        nearest_geom = nearest_points(acidente.geometry, estacoes_proximas_gdf.unary_union)[1]
        nearest_station = estacoes_proximas_gdf[estacoes_proximas_gdf.geometry == nearest_geom]

        # Adiciona os dados da estação climática mais próxima ao acidente
        estacoes_proximas = nearest_station
        estacao_proxima_ano = estacoes_proximas[estacoes_proximas['ano'] == acidente['data_hora'].year]
        if estacao_proxima_ano.empty:
            continue
        dados_estacao_proxima_anual = pd.read_csv(estacao_proxima_ano['nome_arquivo'].values[0], sep=';', encoding='latin1', skiprows=8)
        dados_estacao_proxima_anual['Data'] = pd.to_datetime(dados_estacao_proxima_anual['Data'], format='%Y/%m/%d')

        # Ensure both dates are in the same format (date only)
        dados_estacao_proxima_anual['Data'] = dados_estacao_proxima_anual['Data'].dt.date
        acidente_data = acidente['data_hora'].date()
        dados_estacao_proxima_dia_acidente = dados_estacao_proxima_anual[dados_estacao_proxima_anual['Data'] == acidente_data]
        if dados_estacao_proxima_dia_acidente.empty:
            continue
        acidente_df = pd.DataFrame([acidente])
        acidente_output = pd.concat([acidente_df, dados_estacao_proxima_dia_acidente], axis=1)
        acidentes_output = pd.concat([acidentes_output, acidente_output], ignore_index=True)
    
    return acidentes_output

import time

start_time = time.time()
acidentes_output = data_fusion(acidentes_gdf, estacoes_proximas)
end_time = time.time()

with open("br101_acidentes_fusionados_single_time.txt", "w") as file:
    file.write(str(end_time - start_time))

acidentes_output.to_file("br101_acidentes_fusionados.geojson", driver='GeoJSON')