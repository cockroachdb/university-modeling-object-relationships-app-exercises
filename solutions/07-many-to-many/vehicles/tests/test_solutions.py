#!/usr/bin/env python3
"""
Connect to the cockroach shell

Usage:
    ./test_solutions.py

Options:
    -h --help           Show this text.
"""

from datetime import datetime

import pytest
import pytz
from datetime import date
from docopt import docopt
from psycopg2.extras import RealDictCursor

from util.helpers import (crdb, create_database, read_answer_file, run_query,
                          run_sql_script, select_star,
                          show_columns, show_databases, show_indexes,
                          show_tables, check_table, check_columns, check_table_contents_by_id,check_table_contents,
                          check_foreign_key, check_query_result)


class TestClass:

    def test_create_database_command(self, crdb,
                                     query_file='load_initial_state.sql'):
        """
        Tests create_database.sql script to confirm it creates movr_vehicles;
        """
        # Action: run create_database.sql to create the database
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # Assert that the database exists
        databases = show_databases(crdb.connection)
        assert "movr_vehicles" in databases

    def test_create_vehicles(self, crdb, db="movr_vehicles",
                                  table="vehicles",
                                  query_file='load_initial_state.sql'):
        """
        Tests whether the vehicles table has the correct schema
        """
        
        expected_columns = ['id', 'vehicle_type', 'purchase_date',
            'serial_number', 'make', 'model', 'year', 'color',
            'description']
        data_types = ["UUID", "STRING", "DATE",
            "STRING", "STRING", "STRING", "INT2", "STRING",
            "STRING"]
        defaults = ['gen_random_uuid()', None, 'current_date()', 
            None, None, None, None, None,
            None ]
        nullable = [False, False, False,
            False, False, False, False, False,
            True] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)          
        
        # test that the primary key is on (id):
        primary_key = [key for key in
                       show_indexes(crdb.connection, db=db, table=table)
                       if (key['index_name'] == 'primary'
                           and key['direction'] != 'N/A')]
        for key in primary_key:  # just one key
            assert key['column_name'] == 'id'
            assert key['seq_in_index'] == 1
            assert key['direction'] == 'ASC'
            assert key['storing'] is False

    def test_vehicle_rows(self, crdb, db="movr_vehicles", table="vehicles",
                             query_file='load_initial_state.sql'):
        """
        Verify that the vehicles table contains specific expected data
        """

        expected_data = {
        '03d0a3a4-ae36-4178-819c-0c1b08e59afc':
            {'vehicle_type': 'Scooter', 
            'purchase_date':date(2022, 3, 7),  
            'serial_number':'SC9757543886484387',
            'make':'Spitfire',
            'model':'Inferno',
            'year': 2021,
            'color':'Red',
            'description':'Scratch on the left side'
            },
         '648aefea-9fbc-11ec-b909-0242ac120002':
            {'vehicle_type': 'Skateboard',
            'purchase_date' :date.today(),
            'serial_number': 'SB6694627626486622',
            'make': 'Street Slider',
            'model': 'Motherboard',
            'year':  2020,
            'color': 'Blue',
            'description': 'Alien painted on the bottom'   
            },
         'a0dd6bd9-c530-4c23-b401-185c7328a4dd':
            {'vehicle_type':'Bicycle',
            'purchase_date': date.today(),
            'serial_number':'BK6522688477384422',
            'make': 'Dirt Devilz',
            'model': 'MX-4',
            'year': 2018,
            'color': 'Orange',
            'description': None
            },   
          'e25cad53-fb7d-46d2-bd0b-0aef9fa79db6':
            {'vehicle_type': 'Bicycle',
            'purchase_date': date.today(),
            'serial_number':'BK9596625974336633',
            'make': 'Dirt Devilz',
            'model':'MX-6',
            'year': 2022,
            'color': 'Green',
            'description': 'White Wall Tires' 
           }  
        }

        check_table_contents_by_id(crdb,db,table,query_file,expected_data,search_field='id')

    def test_create_stations(self, crdb, db="movr_vehicles",
                                  table="stations",
                                  setup_files = ['load_initial_state.sql'],
                                  query_file='add_stations.sql'):

        """
        Tests whether the stations table has the correct schema
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['id','name', 'latitude', 'longitude','docks']

        data_types = ["UUID", "STRING", "FLOAT8", "FLOAT8", "INT2"]

        defaults = ['gen_random_uuid()', None, None, None, None]

        nullable = [False, True, False, False, False] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)           

    def test_stations_rows(self, crdb, db="movr_vehicles", table="stations",
                             setup_files=['load_initial_state.sql'],
                             query_file='add_stations.sql'):
        """
        Verify that the stations table contains specific expected data
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data =  {'83a52f1c-6b35-403d-b415-9cb4876b19a6': {'name':'East Park', 'latitude': 40.667668, 'longitude': -73.929062, 'docks': 10},
                            '49a81d1b-f14b-4d19-99f9-f469b07af5db': {'name':'Main St', 'latitude': 40.697533, 'longitude': -73.895632, 'docks': 8},
                            '220b7340-20c7-43a3-8edb-237abbceb8bd': {'name':'Union Square', 'latitude': 40.678177, 'longitude': -73.944161, 'docks': 5},
                            '88075a6d-6ca0-4727-a81f-123e03de0ea3': {'name':'Grand Plaza', 'latitude': 40.682725, 'longitude': -73.975109, 'docks': 10}}


        check_table_contents_by_id(crdb=crdb,db=db,table=table,
                                        query_file=query_file,
                                        expected_data=expected_data)


    def test_create_associative_table(self, crdb, db="movr_vehicles",
                                  table="vehicles_stations",
                                  setup_files = ['load_initial_state.sql', 'add_stations.sql'],
                                  query_file='add_associative_table.sql'):

        """
        Tests whether the associative table has the correct schema
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['rowid','vehicle_id', 'station_id', 'docked_ts']

        data_types = ["INT8", "UUID", "UUID", "TIMESTAMPTZ"]

        defaults = ['unique_rowid()', None, None, 'now():::TIMESTAMPTZ']

        nullable = [False, True, True, False] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)     

    def test_associative_rows(self, crdb, db="movr_vehicles", table="vehicles_stations",
                             setup_files=['load_initial_state.sql', 'add_stations.sql'],
                             query_file='add_associative_table.sql'):
        """
        Verify that the associative table contains specific expected data
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data =  [
            {'vehicle_id': 'd0e896f2-2f5c-4d56-9b26-9d98abc9856e', 'station_id': '83a52f1c-6b35-403d-b415-9cb4876b19a6', 'docked_ts': '2022-03-22 14:03:44'},
            {'vehicle_id': 'd0e896f2-2f5c-4d56-9b26-9d98abc9856e', 'station_id': '220b7340-20c7-43a3-8edb-237abbceb8bd', 'docked_ts': '2022-03-23 08:45:12'},
            {'vehicle_id': 'cedd9808-ef8d-4a90-b1c2-2062eed45c5b', 'station_id':'83a52f1c-6b35-403d-b415-9cb4876b19a6', 'docked_ts': '2022-03-22 15:12:56'},
            {'vehicle_id': 'cedd9808-ef8d-4a90-b1c2-2062eed45c5b', 'station_id':'49a81d1b-f14b-4d19-99f9-f469b07af5db','docked_ts':  '2022-03-24 13:23:44'},
            {'vehicle_id': '5e97256b-a9d2-43e3-95af-5fbe4f79cc3b', 'station_id':'220b7340-20c7-43a3-8edb-237abbceb8bd','docked_ts': '2022-03-24 11:24:46'},
            {'vehicle_id': '739b9530-7b25-4c98-91a7-184ace7642a9', 'station_id':'220b7340-20c7-43a3-8edb-237abbceb8bd', 'docked_ts': '2022-03-25 09:43:12'}]


        check_table_contents(crdb=crdb,db=db,table=table,
                                        query_file=query_file,
                                        expected_data=expected_data)

    def test_associative_vehicles_foreign_key(self, crdb, db="movr_vehicles",
                                  table="vehicles_stations",
                                  setup_files=['load_initial_state.sql', 'add_stations.sql'],
                                  query_file='add_associative_table.sql'):


        """
        Tests whether the associative table has the foreign key to the vehicles table
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)


        check_foreign_key(crdb,db=db,query_file=query_file,table=table, 
                        column='vehicle_id', ref_table='vehicles', ref_column='id')
    
    def test_associative_stations_foreign_key(self, crdb, db="movr_vehicles",
                                  table="vehicles_stations",
                                  setup_files=['load_initial_state.sql', 'add_stations.sql'],
                                  query_file='add_associative_table.sql'):


        """
        Tests whether the associative table has the foreign key to the stations table
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)


        check_foreign_key(crdb,db=db,query_file=query_file,table=table, 
                        column='station_id', ref_table='stations', ref_column='id')
    
    def test_join_get_stations(self, crdb, db="movr_vehicles",
                                  table="vehicles_stations",
                                  setup_files=['load_initial_state.sql', 'add_stations.sql','add_associative_table.sql'],
                                  query_file='get_stations.sql'):


        """
        Tests the JOIN to retrive all stations for a particulat vehicle
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data = [('East Park',), ('Main St',)] 
        check_query_result(crdb,db=db,query_file=query_file,expected_data=expected_data)   

    def test_join_get_vehicles(self, crdb, db="movr_vehicles",
                                  table="vehicles_stations",
                                  setup_files=['load_initial_state.sql', 'add_stations.sql','add_associative_table.sql'],
                                  query_file='get_vehicles.sql'):


        """
        Tests the JOIN to retreive vehicle ID and type at a particular station
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_data = [('5e97256b-a9d2-43e3-95af-5fbe4f79cc3b','Bicycle'), 
            ('739b9530-7b25-4c98-91a7-184ace7642a9','Skateboard'), 
            ('d0e896f2-2f5c-4d56-9b26-9d98abc9856e','Skateboard')] 

        check_query_result(crdb,db=db,query_file=query_file,expected_data=expected_data)           

        
    

def main():
    opts = docopt(__doc__)


if __name__ == '__main__':
    main()
