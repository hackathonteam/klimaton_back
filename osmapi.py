from typing import Tuple, Union
import geopy
from geopy.geocoders import Nominatim as api
from geopy.location import Location

Location_Type = Tuple[float, float, str]

async def getLocation(street_name: str) -> Location_Type:
    """
    Street name in format that was given is `name` `number` `letter(A/B)`
    Api works best with `Gniezno number letter name`
    So we do that first then we ask api about long/lati of this adress

    Returns None if it wasn't found, Tuple of (latitude, longtitude) if was found
    """
    tab = street_name.split(" ")
    numery = "".join(tab[1:])
    query_str = f"Gniezno {numery} {tab[0]}"
    ret: Location = api(user_agent="klimaton-fg").geocode(query_str, exactly_one=True)
    return (ret.latitude, ret.longitude, query_str)
