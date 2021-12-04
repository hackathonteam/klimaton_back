import pandas as pd
import numpy as np


def get_data(path1, path2):
    odebrane = pd.read_excel(path1)
    deklarowane = pd.read_excel(path2)
    return odebrane, deklarowane


# ! highly dependent on data having correct structure
def data_preprocessing(odebrane, deklarowane):
    # delete nan and rows that are only in one table
    deklarowane = deklarowane.drop(labels=[70, 113], axis=0)
    odebrane = odebrane.iloc[:143, :]
    deklarowane = deklarowane.iloc[:143, :]

    # fill nan with zeroes in odebrane
    odebrane = odebrane.fillna(0)

    # uzupełnij zerowe godziny i daty zrzutu ścieków oraz nr rejestracyjne
    for i in range(1, odebrane.shape[0]):
        if odebrane.iloc[i, 8] == 0:
            odebrane.iloc[i, 8] = odebrane.iloc[i - 1, 8]
            # join addresses
            odebrane.iloc[i, 2] += '|' + odebrane.iloc[i - 1, 2]
        if odebrane.iloc[i, 6] == 0:
            odebrane.iloc[i, 6] = odebrane.iloc[i - 1, 6]
        if odebrane.iloc[i, 4] == 0:
            odebrane.iloc[i, 4] = odebrane.iloc[i - 1, 4]

            # convert int hour to datetime
    for i in range(odebrane.shape[0]):
        odebrane.iloc[i, 8] = str(odebrane.iloc[i, 8]).replace('.', ':')
        odebrane.iloc[i, 8] = str(odebrane.iloc[i, 8]).replace(',', ':')

    odebrane['Data odbioru ścieków'] = pd.to_datetime(odebrane['Data odbioru ścieków']).dt.date

    # create id based on hour and date
    odebrane["id"] = odebrane['Data odbioru ścieków'].astype('string') + ' ' + odebrane[
        'Godzina zrzutu ścieków'] + ' ' + odebrane['Nr pojazdu\n* nr rejestracyjny'].astype('string')

    # adresy jako lista
    odebrane['adres'] = np.empty((len(odebrane), 0)).tolist()
    for i in range(odebrane.shape[0]):
        odebrane.iloc[i, 11].extend(list(odebrane.iloc[i, 2].split('|')))

    # uzpuełnij zera w deklaracjach firm
    deklarowane.iloc[111:115, 5] = deklarowane.iloc[111:115, 4]
    return odebrane, deklarowane


def join_table(odebrane, deklarowane):
    # join important columns from odebrane and deklarowane
    dekl_vs_odb = odebrane[[odebrane.columns[2], odebrane.columns[4], odebrane.columns[5]]].copy(deep=True)
    firm_dekl = deklarowane[deklarowane.columns[5]].to_numpy()
    dekl_vs_odb.loc[:, "Ścieki deklarowane przez firmę"] = firm_dekl
    dekl_vs_odb.loc[:, "adres"] = odebrane['adres']
    dekl_vs_odb.loc[:, "Data odbioru ścieków"] = odebrane["Data odbioru ścieków"]
    dekl_vs_odb.loc[:, dekl_vs_odb.columns[1]] = dekl_vs_odb[dekl_vs_odb.columns[1]].astype('string')
    # add id consisting of date and hour
    dekl_vs_odb.loc[:, "id"] = odebrane["id"]
    return dekl_vs_odb

def group_by_id(dekl_vs_odb):
    # group by id using aggregation functions
    sum_vs = dekl_vs_odb.groupby("id").sum()
    sum_vs["Nr rejestracyjny pojazdu"] = dekl_vs_odb.groupby("id").min()["Nr pojazdu\n* nr rejestracyjny"]
    sum_vs["Adres"] = dekl_vs_odb.groupby("id").max()["adres"]
    sum_vs["Data odbioru ścieków"] = dekl_vs_odb.groupby("id").max()["Data odbioru ścieków"]
    sum_vs["Różnica"] = sum_vs[sum_vs.columns[1]] - sum_vs[sum_vs.columns[0]]
    sum_vs = sum_vs.sort_values("Różnica", ascending=False)
    # rename columns and change order
    sum_vs = sum_vs.rename(columns={'Ilość odprowadzonych ścieków\n[m3]x1000': 'Real_sewage',
                                    'Ścieki deklarowane przez firmę': 'Declared_sewage',
                                    'Nr rejestracyjny pojazdu': 'Plates',
                                    'Adres': 'Address',
                                    'Data odbioru ścieków': 'Collection_date',
                                    'Różnica': 'Difference'})
    sum_vs = sum_vs[['Plates', 'Collection_date', 'Address', sum_vs.columns[1], 'Real_sewage', 'Difference']]
    return sum_vs.where(sum_vs["Difference"] != 0).dropna(axis=0)


def get_truck_data():
    odebrane, deklarowane = get_data("./data/07odbior_nr_przejazdu.xlsx", "./data/06deklarowane.xlsx")
    odebrane, deklarowane = data_preprocessing(odebrane, deklarowane)
    return group_by_id(join_table(odebrane, deklarowane))

print(get_truck_data())
