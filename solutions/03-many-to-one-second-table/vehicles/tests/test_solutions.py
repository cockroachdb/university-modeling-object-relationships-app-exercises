#!/usr/bin/env python3
"""
Connect to the cockroach shell

Usage:
    ./test_solutions.py

Options:
    -h --help           Show this text.
"""

from calendar import c
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
                          check_foreign_key, check_query_result, get_script_result)


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

    def test_create_maintenance_schedule(self, crdb, db="movr_vehicles",
                                  table="maintenance_schedule",
                                  setup_files=['load_initial_state.sql', 'add_columns.sql'],
                                  query_file='add_maintenance_schedule.sql'):
        """
        Tests whether the maintenance_schedule table has the correct schema
        """
        
        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['make', 'model', 'maintenance_frequency']
        data_types = [ 'STRING', 'STRING', 'INT2']
        defaults = [None, None, None]
        nullable = [False, False, True] 

        check_table(crdb,db,query_file,table,
            expected_columns=expected_columns,
            data_types=data_types,
            defaults=defaults,
            nullable=nullable)          
        
        # test that the primary key is on (make, model):
        expected_primary_key_columns = {'make':1, 'model':2}

        primary_key_columns = {key['column_name']:key['seq_in_index'] for key in
                       show_indexes(crdb.connection, db=db, table=table)
                       if (key['index_name'] == 'primary'
                           and key['direction'] != 'N/A')}

        assert expected_primary_key_columns == primary_key_columns                   

    def test_delete_column_vehicles(self, crdb, db="movr_vehicles",
                                  table="vehicles",
                                  setup_files=['load_initial_state.sql', 'add_columns.sql'],
                                  query_file='add_maintenance_schedule.sql'):
        """
        Tests whether the maintenance frequency column has been deleted from the vehicles table
        """

        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)

        expected_columns = ['id', 'vehicle_type', 'purchase_date',
            'serial_number', 'make', 'model', 'year', 'color',
            'description', 'mileage', 'last_maintenance']
        data_types = ["UUID", "STRING", "DATE",
            "STRING", "STRING", "STRING", "INT2", "STRING",
            "STRING", "INT8", "INT8"]
        defaults = ['gen_random_uuid()', None, 'current_date()', 
            None, None, None, None, None,
            None, '0:::INT8', None ]
        nullable = [False, False, False,
            False, False, False, False, False,
            True, False, True] 

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


    def test_maintenance_schedule_rows(self, crdb, db="movr_vehicles",
                                  table="maintenance_schedule",
                                  setup_files=['load_initial_state.sql', 'add_columns.sql'],
                                  query_file='add_maintenance_schedule.sql'):
        """
        Tests whether the maintenance schedule table has the correct data
        """      

        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script)


        expected_data = [
            {'make':'Spitfire', 'model':'Inferno', 'maintenance_frequency':300},
            {'make':'Hot Wheelies', 'model':'Citrus', 'maintenance_frequency':350},
            {'make':'Dirt Devilz', 'model':'MX-4', 'maintenance_frequency':200},
            {'make':'Dirt Devilz', 'model':'MX-6', 'maintenance_frequency':400},
            {'make':'Street Slider', 'model':'Motherboard', 'maintenance_frequency':200}]


        check_table_contents(crdb,db=db,table=table,query_file=query_file,expected_data=expected_data)    


    def test_maintenance_schedule_vehicle_join(self, crdb, db="movr_vehicles",
                                  table="maintenance_schedule",
                                  setup_files=['load_initial_state.sql', 'add_columns.sql','add_maintenance_schedule.sql'],
                                  query_file='join_vehicles_schedules.sql'):

        for script in setup_files:
            run_sql_script(conn=crdb.connection, script_name=script) 

        result = get_script_result(crdb.connection, query_file) 
         

        #can't fully check data because of the randomly generated mileage  
        # so just checking that the shape of the result is as expected
        assert len(result) ==  9
        assert len(result[0]) == 4


def main():
    opts = docopt(__doc__)


if __name__ == '__main__':
    main()
