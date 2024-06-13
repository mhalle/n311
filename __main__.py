#!/usr/bin/env python

import httpx
import sys
import shapely.geometry
import sqlite_utils
import shapely
from datetime import datetime

def get_today():
    return datetime.today().strftime('%Y-%m-%d')

Base = 'https://user.govoutreach.com/newtoncityma/rest.php'
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
    all_items = {}
    for vowel in 'aeiou':
        items = httpx.get(Base, params = dict(cmd='searchtopics', term=vowel)).json()
        for item in items:
            all_items[item['id']] = item

    return all_items.values()

def is_location_in_newton(latitude, longitude):
    bbox = [
          -71.27029358127882,
          42.28299136464144,
          -71.15688673203238,
          42.36782519392516
    ]
    return (bbox[0] < longitude < bbox[2]) and (bbox[1] < latitude < bbox[3])

def get_locations(id):
    items = httpx.get(Base, params = dict(cmd='samerequests', cid=str(id))).json()
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
    categories = get_all_categories()

    # if new categories are included, just store them
    db['categories'].insert_all(categories, pk='id', ignore=True)

    # added, removed, unchanged
    # added: in the new locations, not current in the db / set new locations to current and set added time
    # removed: not in the new locations, current in the db / for existing records, unset current, set removed time
    # unchanged in the new locations and current in the db / do nothing (don't insert or modify)
    for category in categories:
        locations = list(get_locations(category['id']))
        for el in locations:
            el['location'] = ' '.join(el['location'].upper().split()) # clean up location
            el['category_id'] = category['id']
            el['ward'] = get_ward(el['longitude'], el['latitude'], precinct_info)
            el['added'] = get_today()
            el['removed'] = ""
            el['active'] = 1

        # query for active locations in the category
        if db['_locations'].exists():
            current_locations = list(db.query("""select rowid, * from _locations 
                                        where active = 1 AND 
                                        category_id = ?""", [category['id']]))
            
            current_locations_index = {(e['location'], e['category_id']): e for e in current_locations}
            new_locations_index = {(e['location'], e['category_id']): e for e in locations}

            added_keys = set(new_locations_index.keys()) - set(current_locations_index.keys())
            removed_keys = set(current_locations_index.keys()) - set(new_locations_index.keys())

            added_locations = [v for k,v in new_locations_index.items() if k in added_keys]
            removed_locations = [v for k,v in current_locations_index.items() if k in removed_keys]
            for loc in removed_locations:
                loc['removed'] = get_today()
                loc['active'] = 0

            db['_locations'].insert_all(added_locations, foreign_keys=[['category_id', 'categories', 'id']])
            db['_locations'].upsert_all(removed_locations, pk='rowid')

        else:
            db['_locations'].insert_all(locations, foreign_keys=[['category_id', 'categories', 'id']])

    db['locations'].drop(ignore=True)
    db.execute("""
                   create table locations as select 
                   location, 
                   ward,
                   categories.label as category,
                   category_id, 
                   (case when active = 1 then 'active' else '' end) as active,
                   added,
                   removed,
                   latitude, longitude 
                   from _locations join categories
                   on category_id = id
                          """)
    db['locations'].add_foreign_key('category_id', 'categories', 'id')

    if db['locations'].detect_fts():
        db['locations'].disable_fts()

    db["locations"].enable_fts(['location', 'category', 'active', 'added', 'removed'])
