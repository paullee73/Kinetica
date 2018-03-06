
from __future__ import print_function

import collections
import json
import random
import string

import gpudb


def multithread():
    h_db = gpudb.GPUdb(encoding='BINARY', host='127.0.0.1', port='9191')

    sharded_columns = [
        [ "city", "string", "char16" ],
        [ "state_province", "string", "char2", "shard_key" ],  # shard key column
        [ "country", gpudb.GPUdbRecordColumn._ColumnType.STRING, gpudb.GPUdbColumnProperty.CHAR16 ],
        [ "airport", "string", "nullable" ], # a nullable column
        [ "x", "double" ],
        [ "y", "double" ],
        [ "avg_temp", "double" ],
        [ "time_zone", "string", "char8", "shard_key" ] # shard key column
    ]

    sharded_table = gpudb.GPUdbTable( sharded_columns, db = h_db,
                                      use_multihead_ingest = True,
                                      multihead_ingest_batch_size = 33 )

    num_records = 500
    null_likelihood = 10
    alphanum = (string.ascii_letters + string.digits)
    for i in range(0, num_records):
        record = collections.OrderedDict()
        record[ "city"          ] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 5, 16 ) )] )
        record[ "state_province"] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 0, 2 ) )] )
        record[ "country"       ] = ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 5, 16 ) )] )
        record[ "airport"       ] = None if (random.random() < null_likelihood) \
                                    else ''.join( [random.choice( alphanum ) for n in range( 0, random.randint( 2, 25 ) )] )
        record[ "x"             ] = random.uniform( -180, 180 )
        record[ "y"             ] = random.uniform(  -90,  90 )
        record[ "avg_temp"      ] = random.uniform(  -40, 110 )
        record[ "time_zone"     ] = "UTC-{}".format( random.randint( -11, 14 ) )
        sharded_table.insert_records( record )
    # end loop

    sharded_table.flush_data_to_server()

multithread()