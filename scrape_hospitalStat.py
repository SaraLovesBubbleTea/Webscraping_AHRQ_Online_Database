# -*- coding: utf-8 -*-
"""
Created on Mon Jul 13 13:28:43 2020

@author: shera
"""

import json
import csv
import requests
import re

counties_url_base = 'https://code.highcharts.com/mapdata/countries/us'
hcupnet_url_base = 'https://hcupnet.ahrq.gov/api/hcupnet/data'
headers = {'Content-Type': 'application/json'}
years = {
    'YR_2011' : '17618',
    'YR_2012' : '17697',
    'YR_2013' : '17799',
    'YR_2014' : '47829',
    'YR_2015' : '49508',
    }

def get_api_response(api_url, is_get = True, body = None):
 if is_get:
        response = requests.get(api_url)
 else:
        response = requests.post(api_url, headers=headers, json=body)

 if response.status_code >= 500:
        print('[!] [{0}] Server Error'.format(response.status_code))
        return None
    
 elif response.status_code == 404:
        print('[!] [{0}] URL not found: [{1}]'.format(response.status_code,api_url))
        return None  
 elif response.status_code == 401:
        print('[!] [{0}] Authentication Failed'.format(response.status_code))
        return None
 elif response.status_code == 400:
        print('[!] [{0}] Bad Request'.format(response.status_code))
        return None
 elif response.status_code >= 300:
        print('[!] [{0}] Unexpected Redirect'.format(response.status_code))
        return None
 elif response.status_code == 200:
        return response.content
 else:
        print('[?] Unexpected Error: [HTTP {0}]: Content: {1}'.format(response.status_code, response.content))
 return None

def write_file_csv(filename, fieldnames, rows):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

def get_counties_fips(data):
    data_json_str = data.decode().split('] = ')[1]
    counties_info = json.loads(data_json_str)
    fips = []
    for county in counties_info['features']:
        fips.append(county['properties']['fips'])
    return fips

def call_counties_api(state):
    api_url = '{0}/{1}'.format(counties_url_base, state)
    return get_api_response(api_url)

def call_hcupnet_by_county_api(year, category, fipsstco, dataset_id):
    api_url = '{0}?detail=true'.format(hcupnet_url_base)
    body = {
        'ANALYSIS_TYPE':['AT_C'],
        'OUTCOME_MEASURES':['OM_NUMBER_C', 'OM_COST_C', 'OM_DAYS_C'],
        'YEARS':[year],
        'STATES':['ST_CA'],
        'COMMUNITY_LEVEL':['CL_COUNTY'],
        'CATEGORIZATION_TYPE':[category],
        'DATASET_SOURCE':['DS_NIS'],
        'FIPSSTCO': [fipsstco],
        'DATASET_ID': [dataset_id],
    }
    return get_api_response(api_url, False, body)

def format_row(fieldnames, fieldvalues, year, county):
    attribute = re.sub('[",]', '', '='.join(fieldvalues['attributes']).split(',')[0])
    measure = re.sub('[",]', '', '/'.join(fieldvalues['outcomeMeasures']))
    return {
        fieldnames[0]: year,
        fieldnames[1]: fieldvalues['Fipsstco'],
        fieldnames[2]: county,
        fieldnames[3]: attribute,
        fieldnames[4]: measure,
        fieldnames[5]: fieldvalues['value'],
        }

def build_hcupnet_by_county_file(fips):
    fieldnames = ['YEAR', 'FIPSSTCO', 'COUNTYNAME', 'ATTRIBUTES', 'MEASURE', 'VALUE']
    rows = []
    for year, dataset_id in years.items():
        year_aggregate_totals = False
        for fip in fips:
            year_county_totals = False
            raw_row = call_hcupnet_by_county_api(year, 'CT_ALL', fip, dataset_id)
            county_info = json.loads(raw_row)[0]
            response_year = county_info['collectionId']['Year'][0]
            county_name = ''

            if year_aggregate_totals is not True:
                # define search string for aggregates
                aggregates_pattern = re.compile('(US|State) Total')
                aggregates = filter(lambda x: aggregates_pattern.search(x['attributes'][0]), county_info['dataCells'])
                for aggregate in aggregates:
                    rows.append(format_row(fieldnames, aggregate, response_year, county_name))
                year_aggregate_totals = True

            if year_county_totals is not True:
                # define search string for county totals
                county_pattern = re.compile('[A-Za-z]+, [A-Za-z]+')
                counties = filter(lambda x: county_pattern.search(x['attributes'][0]), county_info['dataCells'])
                for county in counties:
                    county_name = county['attributes'][0].split(',')[0]
                    rows.append(format_row(fieldnames, county, response_year, county_name))
                year_county_totals = True

            attributes = filter(lambda x: len(x['attributes']) > 1, county_info['dataCells'])
            for attribute in attributes:
                rows.append(format_row(fieldnames, attribute, response_year, county_name))
    write_file_csv('counties.csv', fieldnames=fieldnames, rows=rows)

state = 'us-ca-all.js'
data = call_counties_api(state)
if data is not None:
    fips = get_counties_fips(data)
    build_hcupnet_by_county_file(fips)
else:
    print('[!] Request Failed')
