import geopandas as gpd

estacoes_proximas = gpd.read_file("../data/DATA_FUSION/BR101-Geral/br101_estacoes_proximas.geojson")

acidentes_gdf = gpd.read_file("../data/DATA_FUSION/BR101-Geral/br101_acidentes.geojson")
acidentes_gdf.dropna(subset=['geometry'], inplace=True)

estacoes_proximas['ano'] = estacoes_proximas['ano'].astype(int)

acidentes_gdf['ano'] = acidentes_gdf['ano'].astype(int)

from shapely.ops import nearest_points
import pandas as pd
from datetime import timedelta

def data_fusion(acidentes_input, estacoes_proximas_input) -> pd.DataFrame:
    # Cria uma cópia dos GeoDataFrames de entrada para evitar modificar os originais
    acidentes_gdf = acidentes_input.copy()
    estacoes_proximas_gdf = estacoes_proximas_input.copy()

    acidentes_output = pd.DataFrame()
    for _, acidente in acidentes_gdf.iterrows():
        print(f"\nProcessing accident {acidente['geometry']}")
        # Encontra a estação climática mais próxima
        nearest_geom = nearest_points(acidente.geometry, estacoes_proximas_gdf.unary_union)[1]
        nearest_station = estacoes_proximas_gdf[estacoes_proximas_gdf.geometry == nearest_geom]

        # Adiciona os dados da estação climática mais próxima ao acidente
        estacoes_proximas = nearest_station
        estacao_proxima_ano = estacoes_proximas[estacoes_proximas['ano'] == acidente['data_hora'].year]
        if estacao_proxima_ano.empty:
            print(f"\tNo weather station data for year {acidente['data_hora'].year}")
            continue
        dados_estacao_proxima_anual = pd.read_csv(estacao_proxima_ano['nome_arquivo'].values[0], sep=';', encoding='latin1', skiprows=8)
        dados_estacao_proxima_anual['Data'] = pd.to_datetime(dados_estacao_proxima_anual['Data'], format='%Y/%m/%d')
        dados_estacao_proxima_anual['Hora UTC'] = dados_estacao_proxima_anual['Hora UTC'].str.replace(' UTC', '').astype(int)
        dados_estacao_proxima_anual['data_hora'] = dados_estacao_proxima_anual.apply(
            lambda x: x['Data'] + timedelta(hours=x['Hora UTC'] // 100),
            axis=1
        )

        data_acidente = acidente['data_hora'].date()
        hora_acidente = acidente['data_hora'].hour
        dados_estacao_proxima_anual['Data'] = dados_estacao_proxima_anual['Data'].dt.date
        dados_estacao_proxima_dia_acidente = dados_estacao_proxima_anual[
            dados_estacao_proxima_anual['Data'] == data_acidente
        ]
        dados_estacao_proxima_dia_hora_acidente = dados_estacao_proxima_dia_acidente[
            (dados_estacao_proxima_dia_acidente['Hora UTC']//100) == hora_acidente
        ]

        if dados_estacao_proxima_dia_acidente.empty or dados_estacao_proxima_dia_hora_acidente.empty:
            print(f"\tNo weather station data for date {data_acidente}, hour {hora_acidente}")
            continue

        acidente_df = pd.DataFrame([acidente])
        dados_estacao_proxima_dia_hora_acidente.reset_index(drop=True, inplace=True)
        acidente_output = pd.concat([acidente_df.reset_index(drop=True), dados_estacao_proxima_dia_hora_acidente], axis=1)
        acidentes_output = pd.concat([acidentes_output, acidente_output], ignore_index=True)

        print(f"Finished processing accident {acidente['geometry']}\n\n")

    return acidentes_output

import time

start_time = time.time()
acidentes_output = data_fusion(acidentes_gdf, estacoes_proximas)
end_time = time.time()

acidentes_output.to_csv("br101_acidentes_fusionados_single.csv", index=False)
if acidentes_output.empty:
    print("Não salvou o arquivo")
print("Salvou o arquivo")

print("\n\n====================================\n\n",
      f"Tempo de execução: {end_time - start_time} segundos")
