#!/usr/bin/env python

import httpx
import sys
import shapely.geometry
import sqlite_utils
import shapely
from datetime import datetime
import pytz
import bs4

MinutesToRound = 15

def round_time_to_nearest_n_minutes(dt, n):
    minutes = dt.minute
    rounded_minutes = (minutes // n) * n
    if minutes % n >= float(n)/2:
        rounded_minutes += n
    return dt.replace(minute=rounded_minutes, second=0, microsecond=0)

# https://user.govoutreach.com/newtoncityma/rest.php?cmd=requesttypeinfopick&id=51088

def get_today(include_time=False):
    dateformat = '%Y-%m-%d %H:%M' if include_time else '%Y-%m-%d'
    return round_time_to_nearest_n_minutes(datetime.now(pytz.timezone('US/Eastern'))
                                           , MinutesToRound).strftime(dateformat)

ParamsUrl = 'https://user.govoutreach.com/newtoncityma/support.php?goparms=cmd%3Dshell'
BaseUrl = 'https://user.govoutreach.com/newtoncityma/rest.php'
PrecinctUrl = 'https://raw.githubusercontent.com/NewtonMAGIS/GISData/master/Wards%20and%20Precincts/Precincts.geojson'


def get_precincts():
    precinct_json = httpx.get(PrecinctUrl).json()
    return [
        [f['properties']['Ward'], shapely.geometry.shape(f['geometry'])] for
        f in precinct_json['features']
        ]

def get_ward(longitude, latitude, precinct_info):
    if latitude == None or longitude == None:
        return None
    
    pt = shapely.geometry.Point(longitude, latitude)
    for ward, shape in precinct_info:
        if shape.contains(pt):
            return ward
    return None

def get_all_categories():
    page = httpx.get(ParamsUrl).text
    soup = bs4.BeautifulSoup(page, features="html.parser")
    divs = soup.find_all('div', {'data-topicid': True})
    for div in divs:
        id = div['data-topicid']
        label = div.find(class_='topicname').text
        description = div.find(class_='topicdescription').text

        yield(dict(id=id, label=label, value=id, description=description))

def is_location_in_newton(latitude, longitude):
    bbox = [
          -71.27029358127882,
          42.28299136464144,
          -71.15688673203238,
          42.36782519392516
    ]
    return (bbox[0] < longitude < bbox[2]) and (bbox[1] < latitude < bbox[3])

def get_locations(id):
    items = httpx.get(BaseUrl, params = dict(cmd='samerequests', cid=str(id))).json()
    for x in items:
        ret = { 
                'location': x['location'],
                'latitude': None,
                'longitude': None
            }
        if x['locationCoord']:
            longitude, latitude = [float(i) for i in x['locationCoord'].split(',')]
            if is_location_in_newton(latitude, longitude):
                ret['latitude'] = round(latitude, 6)
                ret['longitude'] = round(longitude, 6)

        yield ret


if __name__ == '__main__':
    precinct_info = get_precincts()
    db = sqlite_utils.Database(sys.argv[1])
    categories = list(get_all_categories())

    if db['categories'].exists():
        # get existing categories
        existing_categories = set(category['id'] for category in db.query('select id from categories'))
    else:
        existing_categories = set()

    # if new categories are included, just store them
    db['categories'].insert_all(categories, pk='id', ignore=True)

    newdb = db['_locations'].exists()

    # added, removed, unchanged
    # added: in the new locations, not current in the db / set new locations to current and set added time
    # removed: not in the new locations, current in the db / for existing records, unset current, set removed time
    # unchanged in the new locations and current in the db / do nothing (don't insert or modify)
    for category in categories:
        existing_category = (category['id'] in existing_categories)
            
        locations = list(get_locations(category['id']))
        for el in locations:
            el['location'] = ' '.join(el['location'].upper().split()) # clean up location
            el['category_id'] = category['id']
            el['ward'] = get_ward(el['longitude'], el['latitude'], precinct_info)
            el['added'] = ""
            el['removed'] = ""
            el['active'] = 1

        if newdb:
        # query for active locations in the category
            current_locations = list(db.query("""select rowid, * from _locations 
                                        where active = 1 AND 
                                        category_id = ?""", [category['id']]))
            
            current_locations_index = {(e['location'], e['category_id']): e for e in current_locations}
            new_locations_index = {(e['location'], e['category_id']): e for e in locations}

            added_keys = set(new_locations_index.keys()) - set(current_locations_index.keys())
            removed_keys = set(current_locations_index.keys()) - set(new_locations_index.keys())

            added_locations = [v for k,v in new_locations_index.items() if k in added_keys]

            if existing_category:
                for el in added_locations:
                    el['added'] = get_today(include_time=True)

            removed_locations = [v for k,v in current_locations_index.items() if k in removed_keys]
            for loc in removed_locations:
                loc['removed'] = get_today(include_time=False)
                loc['active'] = 0

            db['_locations'].insert_all(added_locations, foreign_keys=[['category_id', 'categories', 'id']])
            db['_locations'].upsert_all(removed_locations, pk='rowid')

        else:
            # new database
            db['_locations'].insert_all(locations, foreign_keys=[['category_id', 'categories', 'id']])

    db['locations'].drop(ignore=True) # legacy
    db['requests'].drop(ignore=True)
    db.execute("""
                   create table requests as select 
                   location, 
                   ('ward-' || ward) as ward,
                   categories.label as category,
                   category_id, 
                   (case when active = 1 then 'active' else 'inactive' end) as active,
                   added,
                   removed,
                   latitude, longitude 
                   from _locations join categories
                   on category_id = id
                          """)
    db['requests'].add_foreign_key('category_id', 'categories', 'id')

    if db['requests'].detect_fts():
        db['requests'].disable_fts()

    db["requests"].enable_fts(['location', 'category', 'active', 'added', 'removed', 'ward'])
    db['requests'].create_index(['ward', 'category', 'category_id', 'active', 'added', 'removed'])
