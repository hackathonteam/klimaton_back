import pandas as pd
import numpy as np


def get_company_name(companies, plate):
    row = companies.index[companies[companies.columns[4]] == plate].tolist()[0]
    company_name = companies.iloc[row, 2]
    return company_name


def get_data(path1, path2):
    odebrane = pd.read_excel(path1)
    deklarowane = pd.read_excel(path2)
    return odebrane, deklarowane


# ! highly dependent on data having correct structure
def data_preprocessing(odebrane, deklarowane):
    # fill nan with zeroes in odebrane
    odebrane = odebrane.fillna(0)

    odebrane[odebrane.columns[2]] = odebrane[odebrane.columns[2]].astype('string')

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

    # repair hours
    for i in range(odebrane.shape[0]):
        if odebrane['Godzina zrzutu ścieków'].iloc[i][-2] == ':':
            odebrane.iloc[i, odebrane.columns.get_loc('Godzina zrzutu ścieków')] += '0'

    # adresy jako lista
    odebrane['adres'] = np.empty((len(odebrane), 0)).tolist()
    for i in range(odebrane.shape[0]):
        odebrane.iloc[i, 11].extend(list(odebrane.iloc[i, 2].split('|')))

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
    # add hours
    dekl_vs_odb['hour'] = odebrane['Godzina zrzutu ścieków']
    return dekl_vs_odb


def group_by_id(dekl_vs_odb):
    companies = pd.read_excel("./data/companies.xlsx")

    # group by id using aggregation functions
    sum_vs = dekl_vs_odb.groupby("id").sum()
    sum_vs["Nr rejestracyjny pojazdu"] = dekl_vs_odb.groupby("id").min()["Nr pojazdu\n* nr rejestracyjny"]
    sum_vs['hour'] = dekl_vs_odb.groupby("id").min()["hour"]
    sum_vs["Adres"] = dekl_vs_odb.groupby("id").max()["adres"]
    sum_vs["Data odbioru ścieków"] = dekl_vs_odb.groupby("id").max()["Data odbioru ścieków"].astype('string')
    sum_vs["Różnica"] = sum_vs[sum_vs.columns[1]] - sum_vs[sum_vs.columns[0]]
    sum_vs = sum_vs.sort_values("Różnica", ascending=False)

    # add copmany names
    sum_vs["companyName"] = ""
    for i in range(sum_vs.shape[0]):
        sum_vs.iloc[i, 7] = get_company_name(companies, str(sum_vs.iloc[i, 2]))

    # rename columns and change order
    sum_vs = sum_vs.rename(columns={'Ilość odprowadzonych ścieków\n[m3]x1000': 'realSewage',
                                    'Ścieki deklarowane przez firmę': 'declaredSewage',
                                    'Nr rejestracyjny pojazdu': 'plates',
                                    'Adres': 'address',
                                    'Data odbioru ścieków': 'collectionDate',
                                    'Różnica': 'difference'})
    sum_vs = sum_vs[['plates', 'companyName', 'collectionDate', 'hour', 'address', sum_vs.columns[1], 'realSewage', 'difference']]
    return sum_vs


def get_truck_data():
    odebrane, deklarowane = get_data("./data/sewageReception.xlsx", "./data/declaredSewage.xlsx")
    odebrane, deklarowane = data_preprocessing(odebrane, deklarowane)
    return group_by_id(join_table(odebrane, deklarowane)).T