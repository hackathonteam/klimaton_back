import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List
from datetime import datetime as dt
import re
import random

"""
generate_map_datapoints_df() - generuje DataFrame z wszystkimi datapointami na mapie

create_map_datapoints(df) - tworzy słownik z DataFrame'a 'df' z wszystkimi obiektami na mapie
Słownik jako key zawiera adresy, a jako value zawiera kolejny słownik z danymi,
np. '[Akacjowa 3': {'nr_zbiornika': 'A41312', 'st_oddanej_do_pobranej': 0.6265984654731442}, ...]


generate_quotient_timeseries_df() - generuje DataFrame z danymi z timeseries z deklarowanych ścieków
i pobieranej wody. Ten DataFrame jest potrzebny do dwóch funkcji poniżej


graph_quotient_timeseries(df, address) - generuje graf dla punktu o adresie 'address' z DataFrame'a 'df'
z stosunkiem wody oddanej do pobranej
Przykładowy graf ma formę: 
{
    'name': 'quotient_timeseries',
    'title': 'Stosunek wody zadeklarowanej jako ścieki do pobranej na przestrzeni miesięcy',
    'data': [
                {'date': '2021-11', 'quotient': 0.625}, 
                {'date': '2021-10', 'quotient': 0.66875}, 
                {'date': '2021-09', 'quotient': 0.6774193548387096}, 
                {'date': '2021-08', 'quotient': 0.7096774193548387}, 
                {'date': '2021-07', 'quotient': 0.7034482758620689}, 
                {'date': '2021-06', 'quotient': 0.7310344827586207}
            ]
}

graph_amount_timeseries(df, address) - generuje graf dla punktu o adresie 'address' z DataFrame'a 'df'
z iloscia wody pobranej i oddanej
Przykładowy graf ma formę: 
{
    'name': 'quotient_timeseries',
    'title': 'm^3 wody zadeklarowanej jako ścieki i pobranej na przestrzeni miesięcy',
    'data': [
                {'date': '2021-11', 'pobrana': 16.5, 'deklarowana': 10.0},
                {'date': '2021-10', 'pobrana': 16.5, 'deklarowana': 9.6},
                {'date': '2021-09', 'pobrana': 16.0, 'deklarowana': 9.6},
                {'date': '2021-08', 'pobrana': 16.0, 'deklarowana': 8.5},
                {'date': '2021-07', 'pobrana': 15.0, 'deklarowana': 11.1},
                {'date': '2021-06', 'pobrana': 15.0, 'deklarowana': 10.2}
            ]
}
"""


pd.options.mode.chained_assignment = None  

def generate_map_datapoints_df():
    waterConsumption_raw = pd.read_excel("data/waterConsumption.xlsx")
    
    # counting mean values to account for zeros
    
    for i in range(143):
        if waterConsumption_raw.iloc[i, 6] != 0:
            for j in range(17):
                temp = waterConsumption_raw.iloc[i, 6 + j * 2]
                waterConsumption_raw.iloc[i, 6 + j * 2] = temp / 2
                waterConsumption_raw.iloc[i, 6 + j * 2 + 1] = temp / 2
        else:
            for j in range(17):
                temp = waterConsumption_raw.iloc[i, 6 + j * 2 + 1]
                waterConsumption_raw.iloc[i, 6 + j * 2] = temp / 2
                waterConsumption_raw.iloc[i, 6 + j * 2 + 1] = temp / 2
        waterConsumption_raw.iloc[i, 40] = waterConsumption_raw.iloc[i, 39]
    
    # zmiana nazw kolumn
    waterConsumption = waterConsumption_raw.drop(['Lp.'], axis = 1)
    c = waterConsumption.columns
    waterConsumption = waterConsumption.rename(columns = {c[0]: 'nr_licznika',
                                                          c[1]: 'osoba',
                                                          c[2]: 'adres_licznika',
                                                          c[3]: 'zuzycie_wody',
                                                          c[4]: 'srednie_zuzucie_wody'})

    for i in range(5, len(c)-1):
        waterConsumption = waterConsumption.rename(columns = 
                                                   {c[i]: dt.strptime(str(c[i]), '%Y-%m').strftime('%Y-%m')})
    
    declaredSewage_raw = pd.read_excel("data/declaredSewage.xlsx")
    declaredSewage = declaredSewage_raw.drop(['Lp.'], axis = 1)
    c = declaredSewage.columns
    declaredSewage = declaredSewage.rename(columns = {c[0]: 'nr_zbiornika',
                                                      c[1]: 'adres_licznika',
                                                      c[2]: 'data_odbioru',
                                                      c[3]: 'deklaracja_mieszkaniec',
                                                      c[4]: 'deklaracja_firma',
                                                      c[5]: 'pobrana_woda',
                                                      c[6]: 'pobrana_woda_ogrodowa',
                                                      c[7]: 'nr_pojazdu'})
    
    # drop useless columns
    waterConsumption = waterConsumption.drop(columns = ['nr_licznika', 'osoba', 'zuzycie_wody'])
    waterConsumption= waterConsumption.iloc[: , :2]
    
    declaredSewage = declaredSewage.drop(columns = ['nr_zbiornika', 'data_odbioru', 'pobrana_woda', 
                                                        'pobrana_woda_ogrodowa', 'nr_pojazdu'])
    
    declaredSewage = declaredSewage.iloc[:, :3]
    declaredSewage['srednia_deklaracji'] = \
                   (declaredSewage['deklaracja_mieszkaniec'] + declaredSewage['deklaracja_firma'])/2
                   
    declaredSewage = declaredSewage.drop(columns = ['deklaracja_firma', 'deklaracja_mieszkaniec'])
    
   
    df = pd.DataFrame()
    
    df['adres'] = waterConsumption['adres_licznika']
    
    
    df['st_oddanej_do_pobranej'] = declaredSewage['srednia_deklaracji'] / waterConsumption['srednie_zuzucie_wody']
    
    
    return df

                                                    
def create_map_datapoints(df):
    data_dict = {}
    for index, row in df.iterrows():
        row_dict = {}

        if not np.isnan(row['st_oddanej_do_pobranej']): 
            row_dict['nr_zbiornika'] = random.choice("ABCDEFGHIJKL") + str(int(random.uniform(10000, 99999)//1))
            
            row_dict['st_oddanej_do_pobranej'] = row['st_oddanej_do_pobranej']        

        data_dict[row['adres']] = row_dict
    return data_dict



def generate_quotient_timeseries_df():
    waterConsumption_raw = pd.read_excel("data/waterConsumption.xlsx")
    
    # counting mean values to account for zeros
    
    for i in range(143):
        if waterConsumption_raw.iloc[i, 6] != 0:
            for j in range(17):
                temp = waterConsumption_raw.iloc[i, 6 + j * 2]
                waterConsumption_raw.iloc[i, 6 + j * 2] = temp / 2
                waterConsumption_raw.iloc[i, 6 + j * 2 + 1] = temp / 2
        else:
            for j in range(17):
                temp = waterConsumption_raw.iloc[i, 6 + j * 2 + 1]
                waterConsumption_raw.iloc[i, 6 + j * 2] = temp / 2
                waterConsumption_raw.iloc[i, 6 + j * 2 + 1] = temp / 2
        waterConsumption_raw.iloc[i, 40] = waterConsumption_raw.iloc[i, 39]
    
    # zmiana nazw kolumn
    waterConsumption = waterConsumption_raw.drop(['Lp.'], axis = 1)
    c = waterConsumption.columns
    waterConsumption = waterConsumption.rename(columns = {c[0]: 'nr_licznika',
                                                          c[1]: 'osoba',
                                                          c[2]: 'adres_licznika',
                                                          c[3]: 'zuzycie_wody',
                                                          c[4]: 'srednie_zuzucie_wody'})

    for i in range(5, len(c)-1):
        waterConsumption = waterConsumption.rename(columns = 
                                                   {c[i]: dt.strptime(str(c[i]), '%Y-%m').strftime('%Y-%m')})
    
    declaredSewage_raw = pd.read_excel("data/declaredSewage.xlsx")
    declaredSewage = declaredSewage_raw.drop(['Lp.'], axis = 1)
    c = declaredSewage.columns
    declaredSewage = declaredSewage.rename(columns = {c[0]: 'nr_zbiornika',
                                                      c[1]: 'adres_licznika',
                                                      c[2]: 'data_odbioru',
                                                      c[3]: 'deklaracja_mieszkaniec',
                                                      c[4]: 'deklaracja_firma',
                                                      c[5]: 'pobrana_woda',
                                                      c[6]: 'pobrana_woda_ogrodowa',
                                                      c[7]: 'nr_pojazdu'})
    
    # drop useless columns
    waterConsumption = waterConsumption.drop(columns = 
                                             ['nr_licznika', 'osoba', 'zuzycie_wody','srednie_zuzucie_wody'])
    
    declaredSewage = declaredSewage.drop(columns = ['nr_zbiornika', 'data_odbioru', 'deklaracja_mieszkaniec', 
                                       'deklaracja_firma', 'pobrana_woda', 'pobrana_woda_ogrodowa', 'nr_pojazdu'])
    
    waterConsumption= waterConsumption.iloc[: , :7]
    declaredSewage = declaredSewage.iloc[:, :7]
    
    df = pd.DataFrame()
    
    df['adres'] = waterConsumption['adres_licznika']
    
    for col_cons, col_decl in zip(waterConsumption.columns[1:], declaredSewage.columns[1:]):
        df[col_cons] = declaredSewage[col_decl] / waterConsumption[col_cons]
        df["pobrana_" + col_cons] =  waterConsumption[col_cons]
        df["deklarowana_" + col_cons] = declaredSewage[col_decl] 
        
    return df

def graph_quotient_timeseries(df, address):
    row = df[df['adres'] == address]
    
    graph = {}
    graph['name'] = 'quotient_timeseries'
    graph['title'] = "Stosunek zadeklarowanych ścieków do pobranej wody"
    
    data_list = []
    
    for col in row.columns[1:]:
        # if column is year and not pobrana or deklarowana
        if re.search("^20.*$", col):
            temp_dict = {}

            temp_dict['date'] = col
            temp_dict['quotient'] = float(row[col])

            data_list.append(temp_dict)
        
        
        
    graph['data'] = data_list
    
    return graph

def graph_amount_timeseries(df, address):
    row = df[df['adres'] == address]
    
    graph = {}
    graph['name'] = 'quotient_timeseries'
    graph['title'] = "m^3 wody zadeklarowanej jako ścieki i pobranej na przestrzeni miesięcy"
    
    data_list = []
    
    for col in row.columns[1:]:
        if re.search("^20.*$", col):
            temp_dict = {}
            temp_dict['date'] = col
            temp_dict['pobrana'] = float(row["pobrana_" + col])
            temp_dict['deklarowana'] = float(row["deklarowana_" + col])
            
            data_list.append(temp_dict)

        
    graph['data'] = data_list
    
    return graph