import geopandas as gpd
from shapely.ops import nearest_points
import pandas as pd
from datetime import timedelta
import time


def data_fusion(acidentes_input, estacoes_proximas_input) -> pd.DataFrame:
    # Cria uma cópia dos GeoDataFrames de entrada para evitar modificar os originais
    acidentes_gdf = acidentes_input.copy()
    estacoes_proximas_gdf = estacoes_proximas_input.copy()

    # Converte a coluna 'Data' para datetime
    estacoes_proximas_gdf['Data'] = pd.to_datetime(estacoes_proximas_gdf['Data'], format='%Y-%m-%d')

    acidentes_output = pd.DataFrame()
    for _, acidente in acidentes_gdf.iterrows():
        start_time = time.time()
        print(f"\nProcessing accident {acidente['geometry']}")
        # Encontra a estação climática mais próxima
        ano_acidente = acidente['data_hora'].year
        mes_acidente = acidente['data_hora'].month
        data_acidente = acidente['data_hora'].date()
        hora_acidente = acidente['data_hora'].hour

        estacoes_ano_mes_dia = estacoes_proximas_gdf[
            (estacoes_proximas_gdf['Data'].dt.year == ano_acidente) &
            (estacoes_proximas_gdf['Data'].dt.month == mes_acidente) &
            (estacoes_proximas_gdf['Data'].dt.day == data_acidente.day)
        ]
        if estacoes_ano_mes_dia.empty:
            print(f"\tNo weather station data for year {ano_acidente}, month {mes_acidente}, day {data_acidente}")
            continue

        estacoes_ano_mes_dia_hora = estacoes_ano_mes_dia[
            (estacoes_ano_mes_dia['Hora UTC']//100) == hora_acidente
        ]
        if estacoes_ano_mes_dia_hora.empty:
            print(f"\tNo weather station data for year {ano_acidente}, month {mes_acidente}, day {data_acidente}, hour {hora_acidente}")
            continue

        nearest_geom = nearest_points(acidente.geometry, estacoes_ano_mes_dia_hora.unary_union)[1]
        nearest_station = estacoes_ano_mes_dia_hora[estacoes_ano_mes_dia_hora.geometry == nearest_geom]
        nearest_station = nearest_station.iloc[0].to_frame().T

        acidente_output = pd.concat([pd.DataFrame([acidente]).reset_index(drop=True), nearest_station.reset_index(drop=True)], axis=1)
        acidentes_output = pd.concat([acidentes_output, acidente_output], ignore_index=True)

        end_time = time.time()
        print(f"\tExecution time for accident: {end_time - start_time} seconds")
        print(f"Finished processing accident {acidente['geometry']}\n\n")


    return acidentes_output


start_time = time.time()

estacoes_proximas = pd.read_csv("../data/DATA_FUSION/BR101-Geral/dados_tempo_br101_geo.csv")

estacoes_proximas = gpd.GeoDataFrame(estacoes_proximas, geometry=gpd.points_from_xy(estacoes_proximas['long'], estacoes_proximas['lat']))
estacoes_proximas.dropna(subset=['geometry'], inplace=True)
estacoes_proximas.fillna(0, inplace=True)
estacoes_proximas['Hora UTC'] = estacoes_proximas['Hora UTC'].astype(str).str.replace(' UTC', '').astype(int)

acidentes_gdf = gpd.read_file("../data/DATA_FUSION/BR101-Geral/br101_acidentes.geojson")
acidentes_gdf.dropna(subset=['geometry'], inplace=True)
acidentes_gdf.fillna(0, inplace=True)
acidentes_gdf['ano'] = acidentes_gdf['ano'].astype(int)


acidentes_output = data_fusion(acidentes_gdf, estacoes_proximas)
end_time = time.time()

acidentes_output.to_csv("br101_acidentes_fusionados_single.csv", index=False)
if acidentes_output.empty:
    print("Não salvou o arquivo")
print("Salvou o arquivo")

print("\n\n====================================\n\n",
      f"Tempo de execução: {end_time - start_time} segundos")
