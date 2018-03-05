import gpudb

# Establish connection with locally running instance
# of Kinetica, using binary encoding to save memory """

h_bd = gpudb.GPUdb(encoding='BINARY', host='127.0.0.1', port='9191')

# Create columns(column_name, column_type, [column_property1, column_property2])
columns = []
columns.append(gpudb.GPUdbRecordColumn("city", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR16]))
columns.append(gpudb.GPUdbRecordColumn("state_province", gpudb.GPUddbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR32]))
columns.append(gpudb.GPUdbRecordColumn("country", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudb.GPUdbColumnProperty.CHAR16]))
columns.append(gpudb.GPUdbRecordColumn("x", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("y", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("avg_temp", gpudb.GPUdbRecordColumn._ColumnType.DOUBLE))
columns.append(gpudb.GPUdbRecordColumn("time_zone", gpudb.GPUdbRecordColumn._ColumnType.STRING, [gpudbGPUdbColumnProperty.CHAR8]))

# Create the type object
weather_record_type = gpudb.GPUdbRecordType(columns, label="weather_record_type")

# Create table
weather_record_type.create_type(h_db)
weather_type_id = weather_record_type.type_id

response = h_db.create_table(table_name=weather_table, type_id=weather_type_id)
print "Table created: {}".format(response['status_info']['status'])

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
print "Number of single records inserted: {}".format(response["count_inserted"])