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
                          check_foreign_key)


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

    def test_create_bicycles(self, crdb, db="movr_vehicles",
                                  table="bicycles",
                                  setup_files = ['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):

        """
        Tests whether the bicycles table has the correct schema
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['rowid','vehicle_id', 'is_electric', 'battery']

        data_types = ["INT8", "UUID", "BOOL", "STRING"]

        defaults = ['unique_rowid()', None, None, None]

        nullable = [False, False, False, True] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)           

    def test_bicycle_foreign_key(self, crdb, db="movr_vehicles",
                                  table="bicycles",
                                  setup_files=['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):


        """
        Tests whether the bicycles table has the correct foreign key
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)


        check_foreign_key(crdb,db=db,query_file=query_file,table=table, 
                        column='vehicle_id', ref_table='vehicles', ref_column='id')

    def test_bicycle_rows(self, crdb, db="movr_vehicles", table="bicycles",
                             setup_files=['load_initial_state.sql'],
                             query_file='add_subclasses.sql'):
        """
        Verify that the bicycle table contains specific expected data
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data =  [
            {'vehicle_id':'5e97256b-a9d2-43e3-95af-5fbe4f79cc3b', 'is_electric':True, 'battery':'LB4523'},
            {'vehicle_id':'e25cad53-fb7d-46d2-bd0b-0aef9fa79db6',  'is_electric':False}]


        check_table_contents(crdb=crdb,db=db,table=table,
                                        query_file=query_file,
                                        expected_data=expected_data)


    def test_create_scooters(self, crdb, db="movr_vehicles",
                                  table="scooters",
                                  setup_files = ['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):

        """
        Tests whether the scooter table has the correct schema
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['rowid','vehicle_id', 'motor', 'battery']

        data_types = ["INT8", "UUID", "STRING", "STRING"]

        defaults = ['unique_rowid()', None, None, None]

        nullable = [False, False, True, True] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)  

    def test_scooters_foreign_key(self, crdb, db="movr_vehicles",
                                  table="scooters",
                                  setup_files=['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):


        """
        Tests whether the scooters table has the correct foreign key
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)


        check_foreign_key(crdb,db=db,query_file=query_file,table=table, 
                        column='vehicle_id', ref_table='vehicles', ref_column='id')

    def test_scooters_rows(self, crdb, db="movr_vehicles", table="scooters",
                             setup_files=['load_initial_state.sql'],
                             query_file='add_subclasses.sql'):
        """
        Verify that the scooters table contains specific expected data
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data =  [
            {'vehicle_id':'f675d44b-4446-400f-bf91-99b23a281161', 'motor':'MMR3023-D', 'battery':'LS3029'},
            {'vehicle_id':'cedd9808-ef8d-4a90-b1c2-2062eed45c5b',  'motor':'MMR2021-A', 'battery':'LS3032'}]


        check_table_contents(crdb=crdb,db=db,table=table,
                                        query_file=query_file,
                                        expected_data=expected_data)

    def test_create_skateboards(self, crdb, db="movr_vehicles",
                                  table="skateboards",
                                  setup_files = ['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):

        """
        Tests whether the scooter table has the correct schema
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['rowid','vehicle_id', 'type', 'motor', 'battery']

        data_types = ["INT8", "UUID", "STRING", "STRING", "STRING"]

        defaults = ['unique_rowid()', None, None, None, None]

        nullable = [False, False, False, True, True] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)  

    def test_skateboards_foreign_key(self, crdb, db="movr_vehicles",
                                  table="skateboards",
                                  setup_files=['load_initial_state.sql'],
                                  query_file='add_subclasses.sql'):


        """
        Tests whether the skateboards table has the correct foreign key
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        check_foreign_key(crdb,db=db,query_file=query_file,table=table, 
                        column='vehicle_id', ref_table='vehicles', ref_column='id')

    def test_skateboard_rows(self, crdb, db="movr_vehicles", table="skateboards",
                             setup_files=['load_initial_state.sql'],
                             query_file='add_subclasses.sql'):
        """
        Verify that the scooters table contains specific expected data
        """
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        expected_data =  [
            {'vehicle_id':'739b9530-7b25-4c98-91a7-184ace7642a9', 'type':'Cruiser', 'motor':'MTW2245-S', 'battery':'LS1123'},
            {'vehicle_id':'d0e896f2-2f5c-4d56-9b26-9d98abc9856e', 'type':'Longboard' ,'motor':'MTW2256-A', 'battery':'LS1123'}]


        check_table_contents(crdb=crdb,db=db,table=table,
                                        query_file=query_file,
                                        expected_data=expected_data)

def main():
    opts = docopt(__doc__)


if __name__ == '__main__':
    main()
