import pandas as pd


def get_mean_time(path1, path2, period):
    # path1 - waterConsumption, path2 - declaredSewage, period - how many months into past (max 35)

    # get water consumption and declared sewage
    water_consumption = pd.read_excel(path1)
    declared_sewage = pd.read_excel(path2)

    # divide values to account for zeroes in water consumption
    for i in range(water_consumption.shape[0]):
        if water_consumption.iloc[i, 6] != 0:
            for j in range(17):
                temp = water_consumption.iloc[i, 6 + j * 2]
                water_consumption.iloc[i, 6 + j * 2] = temp / 2
                water_consumption.iloc[i, 6 + j * 2 + 1] = temp / 2
        else:
            for j in range(16):
                temp = water_consumption.iloc[i, 6 + j * 2 + 1]
                water_consumption.iloc[i, 6 + j * 2] = temp / 2
                water_consumption.iloc[i, 6 + j * 2 + 1] = temp / 2

    # calculate city average of water usage
    water_means = water_consumption.iloc[:, 6:(6 + period)].mean().tolist()
    # calculate city average of sewage
    sewage_means = declared_sewage.iloc[:, 9:(9 + period)].mean().tolist()

    # gen dates
    dates = pd.date_range(start='2019-01-01', periods=period, freq='M')
    dates = [str(x.month) + "-" + str(x.year) for x in dates]
    dates.reverse()
    return dates, water_means, sewage_means


def get_city_ratio(path1, path2):
    # path1 - waterConsumption, path2 - declared_sewage
    mean_water = pd.read_excel(path1).iloc[:, 5].mean()
    mean_sewage = pd.read_excel(path2).iloc[:, 6].mean()
    return mean_sewage / mean_water


print(get_mean_time("data/waterConsumption.xlsx", "data/declaredSewage.xlsx", 6))
