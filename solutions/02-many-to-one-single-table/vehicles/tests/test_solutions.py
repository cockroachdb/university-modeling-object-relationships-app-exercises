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
                          set_up_and_insert_two_rows, set_up_db_and_table,
                          show_columns, show_databases, show_indexes,
                          show_tables)


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

    def test_create_table_command(self, crdb, db="movr_vehicles",
                                  table="vehicles",
                                  query_file='load_initial_state.sql'):
        """
        Tests that create_table.sql creates a table with the properties:
        * Name is `vehicles`
        * Found in the `movr_vehicles` database
        * Columns:
          * id UUID PRIMARY KEY DEFAULT gen_random_uuid()
          * purchase_date TIMESTAMPTZ DEFAULT now()
          * serial_number NOT NULL
          * make STRING NOT NULL
          * model STRING NOT NULL
          * year INT2 NOT NULL
          * color STRING NOT NULL
          * description STRING
        """
        # setup: create a single node (insecure), create the db, and run the
        #        query file against it
        create_database(crdb.connection, db=db)

        # action: run the script
        run_sql_script(crdb.connection, script_name=query_file)

        # Tests from here on out
        # Assert that a table named `vehicles` exists
        assert table in show_tables(crdb.connection, db=db)

        # Assert that the table has the 8 correct columns
        show_columns_results = show_columns(crdb.connection, table=table,
                                            db=db)
        expected_columns = ['id', 'vehicle_type', 'purchase_date',
                            'serial_number', 'make', 'model', 'year', 'color',
                            'description']
        columns = [row['column_name'] for row in show_columns_results]
        assert len(columns) == 9

        for column_properties in show_columns_results:
            column = column_properties['column_name']
            # No weird columns; got to be one of the ones below.
            assert column in expected_columns
            if column == 'id':
                assert column_properties['data_type'] == 'UUID'
                assert column_properties['column_default'] == \
                    'gen_random_uuid()'
                assert column_properties['is_nullable'] is False
            elif column == 'vehicle_type':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['column_default'] is None
                assert column_properties['is_nullable'] is False
            elif column == 'purchase_date':
                assert column_properties['data_type'] == 'DATE'
                assert column_properties['column_default'] == \
                    'current_date()'
                assert column_properties['is_nullable'] is False
            elif column == 'serial_number':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['is_nullable'] is False
                assert column_properties['column_default'] is None
            elif column == 'make':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['is_nullable'] is False
                assert column_properties['column_default'] is None
            elif column == 'model':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['is_nullable'] is False
                assert column_properties['column_default'] is None
            elif column == 'year':
                assert column_properties['data_type'] == 'INT2'
                assert column_properties['is_nullable'] is False
                assert column_properties['column_default'] is None
            elif column == 'color':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['is_nullable'] is False
                assert column_properties['column_default'] is None
            elif column == 'description':
                assert column_properties['data_type'] == 'STRING'
                assert column_properties['is_nullable'] is True
                assert column_properties['column_default'] is None

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

    def test_insert_five_rows(self, crdb, db="movr_vehicles", table="vehicles",
                             query_file='load_initial_state.sql'):
        """
        Verifies that insert.sql inserts two rows:
        * One row with a specific id
        * One row with some other id
        * Other properties found in the assertions, below
        """
        # setup: create DB, create table, perform the SQL insert statements
        set_up_db_and_table(crdb.connection, db=db, table=table)

        # action: insert two rows from the query file
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # Test
        # first, find the rows
        table_rows = select_star(conn=crdb.connection, db=db, table=table)
        found_vehicles = [False, False, False, False, False]
        for record in table_rows:
            if record['id'] == '03d0a3a4-ae36-4178-819c-0c1b08e59afc':
                assert record['vehicle_type'] == 'Scooter'
                assert record['purchase_date'] == date(2022, 3, 7)
                assert record['serial_number'] == 'SC9757543886484387'
                assert record['make'] == 'Spitfire'
                assert record['model'] == 'Inferno'
                assert record['year'] == 2021
                assert record['color'] == 'Red'
                assert record['description'] == 'Scratch on the left side'
                found_vehicles[0] = True
            elif record['id'] == '648aefea-9fbc-11ec-b909-0242ac120002':
                assert record['vehicle_type'] == 'Skateboard'
                assert record['purchase_date'] == date.today()
                assert record['serial_number'] == 'SB6694627626486622'
                assert record['make'] == 'Street Slider'
                assert record['model'] == 'Motherboard'
                assert record['year'] == 2020
                assert record['color'] == 'Blue'
                assert record['description'] == 'Alien painted on the bottom'
                found_vehicles[1] = True
            elif record['id'] == 'a0dd6bd9-c530-4c23-b401-185c7328a4dd':
                assert record['vehicle_type'] == 'Bicycle'
                assert record['purchase_date'] == date.today()
                assert record['serial_number'] == 'BK6522688477384422'
                assert record['make'] == 'Dirt Devilz'
                assert record['model'] == 'MX-4'
                assert record['year'] == 2018
                assert record['color'] == 'Orange'
                assert record['description'] == None
                found_vehicles[2] = True
            elif record['id'] == 'e25cad53-fb7d-46d2-bd0b-0aef9fa79db6':
                assert record['vehicle_type'] == 'Bicycle'
                assert record['purchase_date'] == date.today()
                assert record['serial_number'] == 'BK9596625974336633'
                assert record['make'] == 'Dirt Devilz'
                assert record['model'] == 'MX-6'
                assert record['year'] == 2022
                assert record['color'] == 'Green'
                assert record['description'] == 'White Wall Tires'
                found_vehicles[3] = True
            else:
                assert record['vehicle_type'] == 'Scooter'
                assert record['purchase_date'] == date.today()
                assert record['serial_number'] == 'SC3269997743352394'
                assert record['make'] == 'Hot Wheelies'
                assert record['model'] == 'Citrus'
                assert record['year'] == 2021
                assert record['color'] == 'Grapefruit'
                assert record['description'] == None
                found_vehicles[4] = True

        for vehicle_found in found_vehicles:
            assert vehicle_found is True

        assert len(table_rows) == 5

    def test_add_columns(self, crdb, db="movr_vehicles", table="vehicles",
                         setup_scripts=['load_initial_state.sql'],
                         query_file='add_columns.sql'):
        """
        Verifies that the 'add columns' script adds the expected three columns.
        """
        # setup
        set_up_db_and_table(crdb.connection, db=db, table=table)
        for script in setup_scripts:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        # action
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # assert
        show_columns_results = show_columns(crdb.connection, table=table,
                                            db=db)
        assert len(show_columns_results) == 12
        found_columns = {"mileage": False, "maintenance_frequency": False,
                         "last_maintenance": False}
        for column_properties in show_columns_results:
            column = column_properties['column_name']
            if column == 'mileage':
                assert column_properties['data_type'] == 'INT8'
                assert column_properties['column_default'] == '0:::INT8'
                assert column_properties['is_nullable'] is False
                found_columns['mileage'] = True
            elif column == 'maintenance_frequency':
                assert column_properties['data_type'] == 'INT8'
                assert column_properties['column_default'] is None
                assert column_properties['is_nullable'] is True
            elif column == 'last_maintenance':
                assert column_properties['data_type'] == 'INT8'
                assert column_properties['column_default'] is None
                assert column_properties['is_nullable'] is True

    def test_set_maintenance_frequency(
            self, crdb, db="movr_vehicles", table="vehicles",
            setup_scripts=['load_initial_state.sql', 'add_columns.sql'],
            query_file='set_maintenance_frequency.sql'):
        """
        Verifies that the 'add columns' script adds the expected three columns.
        """
        # setup
        set_up_db_and_table(crdb.connection, db=db, table=table)
        for script in setup_scripts:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        # action
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # assert
        vehicle_records = select_star(crdb.connection)
        for vehicle_info in vehicle_records:
            if vehicle_info['make'] == 'Dirt Devilz': 
                assert vehicle_info['maintenance_frequency'] == 200
            elif vehicle_info['make'] == 'Spitfire':
                assert vehicle_info['maintenance_frequency'] == 300
            elif vehicle_info['make'] == 'Hot Wheelies':
                assert vehicle_info['maintenance_frequency'] == 350
            elif vehicle_info['make'] == 'Street Slider':
                assert vehicle_info['maintenance_frequency'] == 200
            else:
                assert False  # Unexpected vehicle 'make' found

    def test_update_mileage(
            self, crdb, db="movr_vehicles", table="vehicles",
            setup_scripts=['load_initial_state.sql', 'add_columns.sql',
                           'set_maintenance_frequency.sql'],
            query_file='update_mileage.sql'):
        """
        Verifies that the mileage is greater than 0 for all vehicles.
        """
        # setup
        set_up_db_and_table(crdb.connection, db=db, table=table)
        for script in setup_scripts:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        # action
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # assert
        vehicle_records = select_star(crdb.connection)
        for vehicle_info in vehicle_records:
            assert vehicle_info['mileage'] > 0

    def test_update_last_maintenance(
            self, crdb, db="movr_vehicles", table="vehicles",
            setup_scripts=['load_initial_state.sql', 'add_columns.sql',
                           'set_maintenance_frequency.sql',
                           'update_mileage.sql'],
            query_file='update_last_maintenance.sql'):
        """
        """
        # setup
        set_up_db_and_table(crdb.connection, db=db, table=table)
        for script in setup_scripts:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        # action
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # assert
        vehicle_records = select_star(crdb.connection)
        for vehicle_info in vehicle_records:
            mileage = vehicle_info['mileage']
            maintenance_frequency = vehicle_info['maintenance_frequency']
            last_maintenance = vehicle_info['last_maintenance']
            if mileage <= maintenance_frequency:
                assert last_maintenance is None
            elif mileage > maintenance_frequency:
                assert last_maintenance == maintenance_frequency

    def test_update_maintenance_frequency(
            self, crdb, db="movr_vehicles", table="vehicles",
            setup_scripts=['load_initial_state.sql', 'add_columns.sql',
                           'set_maintenance_frequency.sql',
                           'update_mileage.sql',
                           'update_last_maintenance.sql'],
            query_file='update_maintenance_frequency.sql'):
        """
        """
        # setup
        set_up_db_and_table(crdb.connection, db=db, table=table)
        for script in setup_scripts:
            run_sql_script(conn=crdb.connection, script_name=script)
        
        # action
        run_sql_script(conn=crdb.connection, script_name=query_file)

        # assert
        vehicle_records = select_star(crdb.connection)
        dirt_devilz_found = 0
        for record in vehicle_records:
            if record['make'] == 'Dirt Devilz':
                dirt_devilz_found += 1
                assert record['maintenance_frequency'] == 200
        assert dirt_devilz_found >= 2


def main():
    opts = docopt(__doc__)


if __name__ == '__main__':
    main()
