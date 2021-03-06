
from __future__ import print_function

import collections
import json
import random
import string

import gpudb


def filter():
    weather_table_name = "weather"
    weather_w_view = "weather_west"
    weather_nw_view = "weather_northwest"
    weather_country_view = "weather_country"
    weather_e_view = "weather_east"
    weather_se_view = "weather_southeast"
    weather_h_view = "weather_histogram"

    h_db = gpudb.GPUdb(encoding='BINARY', host='127.0.0.1', port='9191')

    columns = [
        [ "city", "string", "char16" ],
        [ "state_province", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR32 ],
        [ "country", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR16 ],
        [ "x", "double" ],
        [ "y", "double" ],
        [ "avg_temp", "double" ],
        [ "time_zone", "string", "char8" ]
    ]

    if h_db.has_table( table_name = weather_table_name )['table_exists']:
        h_db.clear_table( weather_table_name )

    try:
        weather_table = gpudb.GPUdbTable( columns, weather_table_name, db = h_db )
        print ( "Table successfully created.")
    except gpudb.GPUdbException as e:
        print ( "Table creation failure: {}".format( str(e) ) )

    weather_table_duplicate = gpudb.GPUdbTable( None, weather_table_name, db = h_db )

    print ( "\n")
    print ( "INSERTING DATA")
    print ( "--------------")
    print ()

    datum = collections.OrderedDict()
    datum["city"] = "Washington, D.C."
    datum["state_province"] = "--"
    datum["country"] = "USA"
    datum["x"] = -77.016389
    datum["y"] = 38.904722
    datum["avg_temp"] = 58.5
    datum["time_zone"] = "UTC-5"

    weather_table.insert_records( datum )

    datum2 = collections.OrderedDict()
    datum2["city"] = "Washington, D.C."
    datum2["state_province"] = "--"
    datum2["country"] = "USA"
    datum2["x"] = -77.016389
    datum2["y"] = 38.904722
    datum2["avg_temp"] = 58.5
    datum2["time_zone"] = "UTC-5"

    weather_record_type = weather_table.get_table_type()
    single_record = [ gpudb.GPUdbRecord( weather_record_type, datum ).binary_data ]

    response = h_db.insert_records(table_name = weather_table_name, data = single_record, list_encoding = "binary")
    print ( "Number of single records inserted:  {}".format(response["count_inserted"]))

    records = []
    records.append( ["Paris", "TX", "USA", -95.547778, 33.6625, 64.6, "UTC-6"] )
    records.append( ["Memphis", "TN", "USA", -89.971111, 35.1175, 63, "UTC-6"] )
    records.append( ["Sydney", "Nova Scotia", "Canada", -60.19551, 46.13631, 44.5, "UTC-4"] )
    records.append( ["La Paz", "Baja California Sur", "Mexico", -110.310833, 24.142222, 77, "UTC-7"] )
    records.append( ["St. Petersburg", "FL", "USA", -82.64, 27.773056, 74.5, "UTC-5"] )
    records.append( ["Oslo", "--", "Norway", 10.75, 59.95, 45.5, "UTC+1"] )
    records.append( ["Paris", "--", "France", 2.3508, 48.8567, 56.5, "UTC+1"] )
    records.append( ["Memphis", "--", "Egypt", 31.250833, 29.844722, 73, "UTC+2"] )
    records.append( ["St. Petersburg", "--", "Russia", 30.3, 59.95, 43.5, "UTC+3"] )
    records.append( ["Lagos", "Lagos", "Nigeria", 3.384082, 6.455027, 83, "UTC+1"] )
    records.append( ["La Paz", "Pedro Domingo Murillo", "Bolivia", -68.15, -16.5, 44, "UTC-4"] )
    records.append( ["Sao Paulo", "Sao Paulo", "Brazil", -46.633333, -23.55, 69.5, "UTC-3"] )
    records.append( ["Santiago", "Santiago Province", "Chile", -70.666667, -33.45, 62, "UTC-4"] )
    records.append( ["Buenos Aires", "--", "Argentina", -58.381667, -34.603333, 65, "UTC-3"] )
    records.append( ["Manaus", "Amazonas", "Brazil", -60.016667, -3.1, 83.5, "UTC-4"] )
    records.append( ["Sydney", "New South Wales", "Australia", 151.209444, -33.865, 63.5, "UTC+10"] )
    records.append( ["Auckland", "--", "New Zealand", 174.74, -36.840556, 60.5, "UTC+12"] )
    records.append( ["Jakarta", "--", "Indonesia", 106.816667, -6.2, 83, "UTC+7"] )
    records.append( ["Hobart", "--", "Tasmania", 147.325, -42.880556, 56, "UTC+10"] )
    records.append( ["Perth", "Western Australia", "Australia", 115.858889, -31.952222, 68, "UTC+8"] )

    #filter northwestern
    response = h_db.filter(table_name = weather_table_name, view_name = weather_nw_view,
                           expression = "x < 0 and y > 0" )
    print ( "Number of records in northwestern hemisphere:  {}".format( response["count"] ))

    #get records from USA
    country_map = {"country": ["USA"]}
    view3 = weather_table_duplicate.filter_by_list( column_values_map = country_map )
    print ( "Number of records from USA: {}"
            "".format( view3.size() ))


filter()