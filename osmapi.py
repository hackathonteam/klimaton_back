#%%

from typing import Tuple, Union
import geopy
from geopy.geocoders import Nominatim as api
from geopy.location import Location


#%%

location_return_type = Union[None, Tuple[float, float]]
def getLocation(street_name: str) -> location_return_type:
    """
    Street name in format that was given is `name` `number` `letter(A/B)`
    Api works best with `Gniezno number letter name`
    So we do that first then we ask api about long/lati of this adress

    Returns None if it wasn't found, Tuple of (latitude, longtitude) if was found
    """
    tab = street_name.split(" ")
    numery = "".join(tab[1:])
    query_str = f"Gniezno {numery} {tab[0]}"
    ret: Union[None, Location] = api(user_agent="klimaton-fg").geocode(query_str, exactly_one=True)
    if not ret:
        return None
    else:
        return (ret.latitude, ret.longitude, query_str)

#%%


print(getLocation("Roosevelta 164"))
print(getLocation("Paczkowskiego 6"))
print(getLocation("Kłeckoska 84"))
print(getLocation("Zamiejska 13"))
print(getLocation("Kłeckoska 96 A"))
print(getLocation("Trzemeszeńska 2F"))
print(getLocation("Kłeckoska 51"))
print(getLocation("Roosevelta 131 A"))
print(getLocation("Skrajna 10"))
print(getLocation("Żerniki 6"))
print(getLocation("Skrajna 11"))
print(getLocation("Wrzesińska 84"))
print(getLocation("Pomowska 14"))
print(getLocation("Ludwiczaka 38"))
print(getLocation("Grodzka 9"))

# %%
