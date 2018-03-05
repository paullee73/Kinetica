import gpudb

# Establish connection with locally running instance
# of Kinetica, using binary encoding to save memory """

h_db = gpudb.GPUdb(encoding='BINARY', host='127.0.0.1', port='9191')

# Create columns(column_name, column_type, [column_property1, column_property2])
columns = []
columns.append(gpudb.GPUdbRecordColumn("city", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR16]))
columns.append(gpudb.GPUdbRecordColumn("state_province", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR32]))
columns.append(gpudb.GPUdbRecordColumn("country", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR16]))
columns.append(gpudb.GPUdbRecordColumn("x", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("y", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("avg_temp", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("time_zone", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR8]))

# Create the type object
weather_record_type = gpudb.GPUdbRecordType(columns, label="weather_record_type")

# Create type in database, needed fo creating table
weather_record_type.create_type(h_db)
weather_type_id = weather_record_type.type_id

# Create table
response = h_db.create_table(table_name=weather_table, type_id=weather_type_id)
print "Table created:  {}".format(response['status_info']['status'])

# Create ordered dictionary for keys and values of record
datum = collections.OrderedDict()
datum["city"] = "Washington, D.C."
datum["state_province"] = "--"
datum["country"] = "USA"
datum["x"] = -77.016389
datum["y"] = 38.904722
datum["avg_temp"] = 58.5
datum["time_zone"] = "UTC-5"

# Encode record and put into a single element list
single_record = [gpudb.GPUdbRecord(weather_record_type, datum).binary_data]

# Insert the record into the table
response = h_db.insert_records(table_name=weather_table, data=single_record, list_encoding="binary", options={})
print "Number of single records inserted:  {}".format(response["count_inserted"])

# Create a list of encoded in-line records
encoded_obj_list = []
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Paris", "TX", "USA", -95.547778, 33.6625, 64.6, "UTC-6"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Memphis", "TN", "USA", -89.971111, 35.1175, 63, "UTC-6"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Sydney", "Nova Scotia", "Canada", -60.19551, 46.13631, 44.5, "UTC-4"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["La Paz", "Baja California Sur", "Mexico", -110.310833, 24.142222, 77, "UTC-7"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["St. Petersburg", "FL", "USA", -82.64, 27.773056, 74.5, "UTC-5"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Oslo", "--", "Norway", 10.75, 59.95, 45.5, "UTC+1"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Paris", "--", "France", 2.3508, 48.8567, 56.5, "UTC+1"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Memphis", "--", "Egypt", 31.250833, 29.844722, 73, "UTC+2"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["St. Petersburg", "--", "Russia", 30.3, 59.95, 43.5, "UTC+3"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Lagos", "Lagos", "Nigeria", 3.384082, 6.455027, 83, "UTC+1"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["La Paz", "Pedro Domingo Murillo", "Bolivia", -68.15, -16.5, 44, "UTC-4"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Sao Paulo", "Sao Paulo", "Brazil", -46.633333, -23.55, 69.5, "UTC-3"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Santiago", "Santiago Province", "Chile", -70.666667, -33.45, 62, "UTC-4"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Buenos Aires", "--", "Argentina", -58.381667, -34.603333, 65, "UTC-3"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Manaus", "Amazonas", "Brazil", -60.016667, -3.1, 83.5, "UTC-4"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Sydney", "New South Wales", "Australia", 151.209444, -33.865, 63.5, "UTC+10"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Auckland", "--", "New Zealand", 174.74, -36.840556, 60.5, "UTC+12"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Jakarta", "--", "Indonesia", 106.816667, -6.2, 83, "UTC+7"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Hobart", "--", "Tasmania", 147.325, -42.880556, 56, "UTC+10"]).binary_data)
encoded_obj_list.append(gpudb.GPUdbRecord(weather_record_type, ["Perth", "Western Australia", "Australia", 115.858889, -31.952222, 68, "UTC+8"]).binary_data)

# Insert the records into the table
response = h_db.insert_records(table_name=weather_table, data=encoded_obj_list, list_encoding="binary", options={})
print "Number of batch records inserted:  {}".format(response["count_inserted"])

# Retrieve at most 10 elements as JSON from weather_table
# weatherLocs = h_db.get_records(table_name=weather_table, offset=0, limit=10, encoding="json", options={"sort_by":"city"})['records_json']
# print "{:<20s} {:<25s} {:<15s} {:<10s} {:<11s} {:<9s} {:<8s}".format("City","State/Province","Country","Latitude","Longitude","Avg. Temp","Time Zone")
# print "{:=<20s} {:=<25s} {:=<15s} {:=<10s} {:=<11s} {:=<9s} {:=<9s}".format("", "", "", "", "", "", "")
# for weatherLoc in weatherLocs:
#     print "{city:<20s} {state_province:<25s} {country:<15s} {y:10.6f} {x:11.6f} {avg_temp:9.1f}   {time_zone}".format(**json.loads(weatherLoc))

# Retrieve at most 25 elements using binary encoding, faster than JSON
response = h_db.get_records(table_name=weather_table, offset=10, limit=25, encoding="binary", options={"sort_by":"city"})
weatherLocs = gpudb.GPUdbRecord.decode_binary_data(response["type_schema"], response["records_binary"])
for weatherLoc in weatherLocs:
    print "{city:<20s} {state_province:<25s} {country:<15s} {y:10.6f} {x:11.6f} {avg_temp:9.1f}   {time_zone}".format(**weatherLoc)
print "\nNumber of records in new table:  {:d}".format(response["total_number_of_records"])

