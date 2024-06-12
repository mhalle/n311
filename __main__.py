#!/usr/bin/env python

import httpx
import sys
import shapely.geometry
import sqlite_utils
import shapely

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
                ret['latitude'] = latitude
                ret['longitude'] = longitude

        yield ret


if __name__ == '__main__':
    precinct_info = get_precincts()
    db = sqlite_utils.Database(sys.argv[1], recreate=True)
    categories = get_all_categories()
    db['categories'].insert_all(categories, pk='id')
    
    for category in categories:
        locations = list(get_locations(category['id']))
        ordered_locations = []
        for el in locations:
            ordered_locations.append({
                'location': el['location'],
                'ward': get_ward(el['longitude'], el['latitude'], precinct_info),
                'category': category['id'],
                'latitude': el['latitude'],
                'longitude': el['longitude']
            })
        db['locations'].insert_all(ordered_locations, 
                                   foreign_keys=[['category', 'categories', 'id']])
        
    db.execute("""
               CREATE VIRTUAL TABLE "locations_fts" USING FTS5 (
                    location,
                    category,
                    content="locations"
                );
               """)
    
    db.execute("""
               INSERT INTO "locations_fts" (rowid, location, category)
                SELECT locations.rowid,
                    locations.location,
                    categories.label
                    FROM locations JOIN categories ON locations.category=categories.id;
               """)
    