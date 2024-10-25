import pandas as pd
import geopandas as gpd
import os
import shapely


def abrir_dados_estacoes() -> gpd.GeoDataFrame:     
    dados_inmet = {
        'regiao': [],
        'estado': [],
        'codigo_estacao': [],
        'nome_estacao': [],
        'lat': [],
        'long': [],
        'alt': [],
        'data_fundacao': [],
        'nome_arquivo': []
    }
    caminho_inmet = './data/INMET'
    for dir in os.listdir(caminho_inmet):
        if os.path.isdir(os.path.join(caminho_inmet, dir)):
            for file in os.listdir(os.path.join(caminho_inmet, dir)):
                if file.endswith('.csv'):
                    f = open(os.path.join(caminho_inmet, dir, file), 'r')
                    lines = [f.readline().split(';')[-1].strip() for _ in range(8)]
                    dados_inmet['regiao'].append(lines[0])
                    dados_inmet['estado'].append(lines[1])
                    dados_inmet['codigo_estacao'].append(lines[2])
                    dados_inmet['nome_estacao'].append(lines[3])
                    dados_inmet['lat'].append(lines[4].replace(',', '.').astype(float))
                    dados_inmet['long'].append(lines[5].replace(',', '.').astype(float))
                    dados_inmet['alt'].append(lines[6].replace(',', '.').astype(float))
                    dados_inmet['data_fundacao'].append(lines[7].to_datetime())
                    dados_inmet['nome_arquivo'].append(os.path.join(caminho_inmet, dir, file))
    return gpd.GeoDataFrame(dados_inmet, geometry=gpd.points_from_xy(dados_inmet['long'], dados_inmet['lat']))


def df2gdf_linestring(df: pd.DataFrame) -> gpd.GeoDataFrame:
    df.sort_values(['rodovia', 'km_m', 'sentido'], inplace=True, ignore_index=True)
    df_return = {
        "rodovia": [],
        "sentido": [],
        "concessionaria": [],
        "km": [],
        "ano_do_pnv_snv": [],
        "geometry": []
    }
    
    pontos = []
    linha_anterior = df.iloc[0]
    km_max = df.groupby(['rodovia', 'sentido', 'concessionaria'])['km_m'].max().to_dict()
    for _, linha in df.iterrows():
        if linha['rodovia'] != linha_anterior['rodovia'] or linha['sentido'] != linha_anterior['sentido']:
            df_return["rodovia"].append(linha_anterior['rodovia'])
            df_return["sentido"].append(linha_anterior['sentido'])
            df_return["concessionaria"].append(linha_anterior['concessionaria'])
            df_return["km"].append(km_max[(linha_anterior['rodovia'], linha_anterior['sentido'], linha_anterior['concessionaria'])])
            df_return["ano_do_pnv_snv"].append(linha_anterior['ano_do_pnv_snv'])
            if len(pontos) > 1:
                df_return["geometry"].append(shapely.geometry.LineString(pontos))
            else:
                df_return["geometry"].append(shapely.geometry.LineString([(linha_anterior['longitude'], linha_anterior['latitude'])]))
            pontos = []
        pontos.append((linha['longitude'], linha['latitude']))
        linha_anterior = linha

    df_return["rodovia"].append(linha_anterior['rodovia'])
    df_return["sentido"].append(linha_anterior['sentido'])
    df_return["concessionaria"].append(linha_anterior['concessionaria'])
    df_return["km"].append(km_max[(linha_anterior['rodovia'], linha_anterior['sentido'], linha_anterior['concessionaria'])])
    df_return["ano_do_pnv_snv"].append(linha_anterior['ano_do_pnv_snv'])
    if len(pontos) > 1:
        df_return["geometry"].append(shapely.geometry.LineString(pontos))
    else:
        df_return["geometry"].append(shapely.geometry.LineString([(linha_anterior['longitude'], linha_anterior['latitude'])]))
    
    return gpd.GeoDataFrame(df_return, crs="EPSG:4326")


def abrir_rodovias() -> gpd.GeoDataFrame:
    pista_principal_df = pd.read_csv('data/km_pista_principal/dados_dos_quilometro_principal.csv', sep=';', encoding='latin1')
    pista_principal_df['latitude'] = pista_principal_df['latitude'].str.replace(',', '.').astype(float)
    pista_principal_df['longitude'] = pista_principal_df['longitude'].str.replace(',', '.').astype(float)
    pista_principal_df['km_m'] = pista_principal_df['km_m'].str.replace(',', '.').astype(float)
    return df2gdf_linestring(pista_principal_df)


def main() -> None:
    rodovias_gdf = abrir_rodovias()
    inmet_gdf = abrir_dados_estacoes()


main()