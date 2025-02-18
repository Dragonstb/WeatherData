import cols
from shapely.geometry import Polygon, Point



def check_in_polygons(lon: float, lat: float, polygons) -> bool:
    """
    Checks if the point (lon, lat) is located within the (multi)polygon.

    ---
    lon: float
    Longitude (east/west) in degrees

    lat: float
    Latitude (north/south) in degrees

    polygon: List
    A list of lists of two-elemental lists. The latter lists contain the latitude and longitude of the polygon nodes, in degrees.

    ---
    returns: bool True if and only if the point is located within the (multi)polygon.
    """
    if isinstance(polygons, list):
        for polygon in polygons:
            # the depth of the nesting of the polygons differs from area to area, so we have to deal with this by recursively
            # going deeper and deeper
            if isinstance(polygon, list) and isinstance(polygon[0], list):
                if not isinstance(polygon[0][0], list):
                    poly = Polygon(polygon)
                    return poly.contains( Point(lon, lat) )
                else:
                    return check_in_polygons(lon, lat, polygon)
            else:
                # invalid argument
                return False
    return False



def find_area(lon: float, lat: float, geojson):
    """
    Finds the area in which the point is located and returns the names.

    ---
    lon: float
    Longitude (east/west) in degrees

    lat: float
    Latitude (north/south) in degrees

    geojson: JSON
    A GeoJSON containing the areas and their names.

    ---
    returns: Tuple
    A tuple with the name of the nation, the state, and the county the point is located in. If the point is outside of each polygon of the GeoJSON,
    a tuple of three pandas.NAs is returned instead.
    """

    # Translations, because we pick the names from the GeoJSON for the counties. These names do not match the names in the other GeoJSONs
    # in some cases
    nation_dict = {'Germany': 'Deutschland'}

    for feature in geojson['features']:
        within_any = check_in_polygons(lon, lat, feature['geometry']['coordinates'])
        if within_any:
            # found areas, return
            nation = feature['properties']['NAME_0']
            if nation in nation_dict.keys():
                nation = nation_dict[nation]
            state = feature['properties']['NAME_1']
            county = feature['properties']['NAME_3']
            return (nation, state, county, cols.automated)
    # no areas found
    return (None, None, None, cols.automated)

