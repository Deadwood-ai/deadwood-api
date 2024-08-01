from typing import Tuple
import overpy

QUERY_TEMPLATE = """
[out:json];is_in({lat},{lon});rel(pivot)->.w;
.w out geom;
"""


def get_admin_level_from_point(point: Tuple[float, float]):
    # initialize the Overpass API
    api = overpy.Overpass()

    # TODO: here we can also check the point and transform etc.
    lon = point[0]
    lat = point[1]

    # build the query
    query = QUERY_TEMPLATE.format(lon=lon, lat=lat)
    result = api.query(query)

    # filter the stuff
    out = []
    for relation in result.relations:
        # check if this is an administrative boundary
        if not relation.tags.get('boundary') == 'administrative':
            continue
        
        # get the stuff
        name = relation.tags.get('name')
        admin_level = relation.tags.get('admin_level')

        # continue is info is missing
        if name is None or admin_level is None:
            continue
        
        # append the info
        out.append({'name': name, 'admin_level': admin_level})

    return out


def get_admin_tags(point: Tuple[float, float]):
    """
    Returns the level 2, 4 and either 6,7 or 8, depending on 
    the availability.
    """
    tags = get_admin_level_from_point(point)

    levels = [tag['admin_level'] for tag in tags]
    names = [tag['name'] for tag in tags]

    out = [
        names[levels.index('2')],
        names[levels.index('4')] if '4' in levels else None,
    ]

    if any(level in levels for level in ['6', '7', '8']):
        out.append(names[levels.index('8')] if '8' in levels else names[levels.index('7')] if '7' in levels else names[levels.index('6')])
    else:
        out.append(None)

    return out


if __name__ == '__main__':
    point = (8.89393, 51.405)
    print(get_admin_tags(point))
