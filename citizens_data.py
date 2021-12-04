import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List
from datetime import datetime as dt
import re
import random

pd.options.mode.chained_assignment = None  

def generate_df():
    # zuzycie01_raw = pd.read_excel("data/waterConsumption.xlsx")
    # zbiorniki02_raw = pd.read_excel("data/containers.xlsx")
    # liczniki03_raw = pd.read_excel("data/meters.xlsx")
    # firmy04_raw = pd.read_excel("data/companies.xlsx")
    # osoby05_raw = pd.read_excel("data/residents.xlsx")
    # deklarowane06_raw = pd.read_excel("data/declaredSewage.xlsx")

    zuzycie01_raw = pd.read_excel("data/01zuzycie.xlsx")
    zbiorniki02_raw = pd.read_excel("data/02zbiorniki.xlsx")
    liczniki03_raw = pd.read_excel("data/03liczniki.xlsx")
    firmy04_raw = pd.read_excel("data/04firmy.xlsx")
    osoby05_raw = pd.read_excel("data/05osoby.xlsx")
    deklarowane06_raw = pd.read_excel("data/06deklarowane.xlsx")

    # zmiana nazw kolumn
    zuzycie01 = zuzycie01_raw.drop(['Lp.'], axis = 1)
    c = zuzycie01.columns
    zuzycie01 = zuzycie01.rename(columns = {c[0]: 'nr_licznika',
                                            c[1]: 'osoba',
                                            c[2]: 'adres_licznika',
                                            c[3]: 'zuzycie_wody',
                                            c[4]: 'srednie_zuzucie_wody'})
    for i in range(5, len(c)):
        zuzycie01.rename(columns = {c[i]: str(i)})

                        
    zbiorniki02 = zbiorniki02_raw.drop(['Lp.'], axis = 1)
    c = zbiorniki02.columns
    zbiorniki02 = zbiorniki02.rename(columns = {c[0]: 'nr_zbiornika',
                                c[1]: 'osoba',
                                c[2]: 'adres_licznika',
                                c[3]: 'adres_zamieszkania'})

    liczniki03 = liczniki03_raw.drop(['Lp.'], axis = 1)
    c = liczniki03.columns
    liczniki03 = liczniki03.rename(columns = {c[0]: 'nr_licznika',
                                            c[1]: 'osoba',
                                            c[2]: 'adres_licznika',
                                            c[3]: 'adres_zamieszkania'})

    firmy04 = firmy04_raw.drop(['Lp.'], axis = 1)
    c = firmy04.columns
    firmy04 = firmy04.rename(columns = {c[0]: 'nr_koncesji',
                                c[1]: 'nazwa_firmy',
                                c[2]: 'adres_firmy',
                                c[3]: 'nr_pojazdu',
                                c[4]: 'pojemnosc_pojazdu'})

    osoby05 = osoby05_raw.drop(['Lp.'], axis = 1)
    c = osoby05.columns
    osoby05 = osoby05.rename(columns = {c[0]: 'nr_zbiornika',
                                c[1]: 'adres_licznika',
                                c[2]: 'liczba_osob_zameldowanych',
                                c[3]: 'liczba_osob_smieci'})

    deklarowane06 = deklarowane06_raw.drop(['Lp.'], axis = 1)
    c = deklarowane06.columns
    deklarowane06 = deklarowane06.rename(columns = {c[0]: 'nr_zbiornika',
                                                    c[1]: 'adres_licznika',
                                                    c[2]: 'data_odbioru',
                                                    c[3]: 'deklaracja_mieszkaniec',
                                                    c[4]: 'deklaracja_firma',
                                                    c[5]: 'pobrana_woda',
                                                    c[6]: 'pobrana_woda_ogrodowa',
                                                    c[7]: 'nr_pojazdu'})


    # wyliczenie liczby osób jako średniej z zameldowanych i bazy śmieciowej
    # i wyrzucenie tych kolumn
    osoby05['liczba_osob'] = (osoby05['liczba_osob_zameldowanych'] + osoby05['liczba_osob_smieci'])/2
    osoby05 = osoby05.drop(['liczba_osob_zameldowanych', 'liczba_osob_smieci', 'nr_zbiornika'], axis = 1)

    # czyszczenie deklarowanych
    deklarowane06 = deklarowane06.drop(['pobrana_woda_ogrodowa', 'nr_zbiornika'], axis = 1)

    # Łączenie tabel i usuwanie niepotrzebnych kolumn


    liczniki_zbiorniki = zbiorniki02.set_index(['adres_licznika', 'osoba', 'adres_zamieszkania'])\
                        .join(liczniki03.set_index(['adres_licznika', 'osoba', 'adres_zamieszkania']))\
                        .reset_index()

    # drop kolumn adres zamieszkania skoro wszedzie jest taki sam jak adres licznika
    liczniki_zbiorniki = liczniki_zbiorniki.drop(['adres_zamieszkania', 'nr_zbiornika'], axis = 1)

    liczniki_z_osobami = liczniki_zbiorniki.set_index(['adres_licznika'])\
                        .join(osoby05.set_index(['adres_licznika']))\
                        .reset_index()

    licz_os_deklaracje = liczniki_z_osobami.set_index(['adres_licznika'])\
                        .join(deklarowane06.set_index(['adres_licznika']))\
                        .reset_index()

    licz_os_dekl_pojaz = licz_os_deklaracje.set_index(['nr_pojazdu'])\
                        .join(firmy04.set_index(['nr_pojazdu']))\
                        .reset_index()

    dane_koncowe = licz_os_dekl_pojaz.set_index(['nr_licznika', 'osoba', 'adres_licznika'])\
                        .join(zuzycie01.set_index(['nr_licznika', 'osoba', 'adres_licznika']))\
                        .reset_index()


    # drop some usless columns
    df = licz_os_dekl_pojaz.drop(['osoba', 
                                'nr_licznika', 'nr_koncesji', 
                                'nazwa_firmy', 'adres_firmy',
                                'data_odbioru'], axis = 1)

    df['pobrana_woda'] = dane_koncowe['srednie_zuzucie_wody']

    df_new = df.copy()

    df_new['st_oddanej_do_pobranej'] = (df['deklaracja_mieszkaniec'] + 
                                            df['deklaracja_firma'])/2 / \
                                            df['pobrana_woda']

    df_for_api = df_new.copy()

    # add address
    adres = zbiorniki02_raw.drop(['Lp.'], axis = 1)
    c = adres.columns
    adres = adres.rename(columns = {c[0]: 'nr_zbiornika',
                                    c[1]: 'osoba',
                                    c[2]: 'adres_licznika',
                                    c[3]: 'adres_zamieszkania'})\
                    .drop(columns = ['adres_zamieszkania','osoba'])


    # calculate means for timeseries
    # divide values to account for zeroes
    zuzycie_prepered = zuzycie01_raw.copy()

    for i in range(145):
        if zuzycie_prepered.iloc[i, 6] != 0:
            for j in range(17):
                temp = zuzycie_prepered.iloc[i, 6 + j * 2]
                zuzycie_prepered.iloc[i, 6 + j * 2] = temp / 2
                zuzycie_prepered.iloc[i, 6 + j * 2 + 1] = temp / 2
        else:
            for j in range(17):
                temp = zuzycie_prepered.iloc[i, 6 + j * 2 + 1]
                zuzycie_prepered.iloc[i, 6 + j * 2] = temp / 2
                zuzycie_prepered.iloc[i, 6 + j * 2 + 1] = temp / 2
        zuzycie_prepered.iloc[i, 40] = zuzycie_prepered.iloc[i, 39]

    zuzycie_prepered = zuzycie_prepered.drop(['Lp.'], axis = 1)
    c = zuzycie_prepered.columns
    zuzycie_prepered = zuzycie_prepered.rename(columns = {c[0]: 'nr_licznika',
                                                        c[1]: 'osoba',
                                                        c[2]: 'adres_licznika',
                                                        c[3]: 'zuzycie_wody',
                                                        c[4]: 'srednie_zuzucie_wody'})
    for i in range(5, len(c)):
        zuzycie_prepered.rename(columns = {c[i]: str(i)})

    # prepare zuzycie01 for join
    zuzycie_prepered = zuzycie_prepered.drop(columns = ['nr_licznika' , 'osoba', 'zuzycie_wody', 'srednie_zuzucie_wody'])
    # drop last column
    zuzycie_prepered= zuzycie_prepered.iloc[: , :-1]

    df_for_api = df_for_api.set_index(['adres_licznika'])\
                .join(zuzycie_prepered.set_index(['adres_licznika']))\
                .reset_index()


    df_for_api = df_for_api.sort_values(by="st_oddanej_do_pobranej", ascending = False)

    return df_for_api

# uwaga df_for_api musi być zrobione ,zeby funkcja działała - mozna potem zmaknąć całą analizę z góry
# w funkcje w stylu prepare_data
def create_map_datapoints(df_for_api):
    data_dict = {}
    for index, row in df_for_api.iterrows():
        row_dict = {}

        if not np.isnan(row['st_oddanej_do_pobranej']): 
            row_dict['nr_zbiornika'] = random.choice("ABCDEFGHIJKL") + str(int(random.uniform(10000, 99999)//1))
            
            row_dict['st_oddanej_do_pobranej'] = row['st_oddanej_do_pobranej']        

        data_dict[row['adres_licznika']] = row_dict
    return data_dict

def create_pobrana_woda_timeseries(nr_zbiornika: str, df_for_api):
    graph = {}
    graph['name'] = 'pobrana_timeseries'
    graph['title'] = "Pobrana woda na przestrzeni czasu"
    graph['data'] = {}
    
    data = df_for_api[df_for_api['nr_zbiornika'] == nr_zbiornika]
    
    print(data)
    


df = generate_df()
print(create_map_datapoints(df))