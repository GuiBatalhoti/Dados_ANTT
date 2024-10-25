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

