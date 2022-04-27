#!/usr/bin/env python3
"""
Library for helper functions.

Should not be run on its own.
"""

from multiprocessing import Process
from subprocess import check_output, run
from sys import stdin
from time import sleep

from psycopg2 import OperationalError, connect
from psycopg2.extras import RealDictCursor
from pytest import fixture


def run_sql_script(conn, script_name):
    """
    Runs a SQL command. Does not capture any output.
    """
    script = ' '.join(read_answer_file(script_name))
    with conn.cursor() as cursor:
        cursor.execute(script)
    return True


def read_answer_file(answer_file):
    """
    Reads in an answer file and returns a list of strings.
    """
    with open(answer_file, 'r', encoding='utf-8') as answer:
        return list(answer)


def create_database(connection, db='movr_vehicles'):
    """
    Creates a database.

    Assumes the database doesn't already exist.
    """
    query = f"CREATE DATABASE {db};"
    return run_command(connection, query)


def run_command(connection, sql_command):
    """
    Runs a SQL command that performs an action.

    Does not return a result.
    """
    with connection.cursor() as curs:
        curs.execute(sql_command)
        return True


def run_query(conn, query, cursor_factory=None):
    """
    Runs a read query, then returns the results as a list of tuples.
    """
    with conn.cursor(cursor_factory=cursor_factory) as curs:
        curs.execute(query)
        return curs.fetchall()


def select_star(conn, db='movr_vehicles', table='vehicles',
                cursor_factory=RealDictCursor):
    """
    Runs a `SELECT * FROM {db}.{table} and returns the results.

    Returns
    -------

    List of dicts to represent the rows
    """
    return list(run_query(conn=conn, query=f'SELECT * FROM {db}.{table};',
                          cursor_factory=cursor_factory))

def select_condition(conn, db='movr_vehicles', table='vehicles', condition=None,
                cursor_factory=RealDictCursor):
    """
    Runs a `SELECT * FROM {db}.{table} WHERE {condition} and returns the results.

    Returns
    -------

    List of dicts to represent the rows
    """
    if condition:
        return list(run_query(conn=conn, query=f'SELECT * FROM {db}.{table} WHERE {condition};',
                            cursor_factory=cursor_factory))
    else:
        return list(run_query(conn=conn, query=f'SELECT * FROM {db}.{table};',
                            cursor_factory=cursor_factory))                                

def show_databases(conn):
    """
    Runs the `SHOW DATABASES;` command & returns the results as a list.
    """
    return list(row[0] for row in run_query(conn, query="SHOW DATABASES;"))


def show_indexes(conn, db='movr_vehicles', table='vehicles',
                 cursor_factory=RealDictCursor):
    """
    Runs a `SHOW INDEXES` command against the table & returns the result
    """
    return list(run_query(conn, query=f'SHOW INDEXES FROM {db}.{table};',
                          cursor_factory=cursor_factory))


def show_tables(conn, db='movr_vehicles'):
    """
    Runs `SHOW TABLES FROM <database>;` and returns the tables.
    """
    all_tables = run_query(conn, query=f'SHOW TABLES FROM {db};')
    return [row[1] for row in all_tables]


def show_columns(conn, table, db='movr_vehicles',
                 cursor_factory=RealDictCursor):
    """
    Returns the results of `SHOW COLUMNS FROM <db>.<table>`;
    """
    query = f'SHOW COLUMNS FROM {db}.{table};'
    result = list(run_query(conn=conn, query=query,
                            cursor_factory=cursor_factory))
    return result

def show_constraints(conn, table, db='movr_vehicles',
                 cursor_factory=RealDictCursor):
    """
    Returns the results of `SHOW CONSTRAINTS FROM <db>.<table>`;
    """
    query = f'SHOW CONSTRAINTS FROM {db}.{table};'
    result = list(run_query(conn=conn, query=query,
                            cursor_factory=cursor_factory))
    return result


def capture_stdin():
    """
    If the user inputs a stream, capture it line by line

    E.g. cat <filename.sql> | script.py
    """
    return list(stdin)


def get_sql_statement():
    """
    Accepts a sql statement over 1+ lines, terminated with a semicolon.
    """
    all_lines = [input("> ")]
    while ';' not in all_lines[-1]:
        all_lines.append(input("... "))
    return all_lines


def prompt_for_input(message):
    """
    Gives the user a prompt to capture the answers.
    """
    print(message)
    return get_sql_statement()


def find_correct_input_source(message, expected_answer_file=None):
    """
    Tries to find the best input by process of elimination.

    Priorities:
    1. <stream> | script.py  (i.e., tty)
    2. file
    3. Prompt the user if neither of the above work.
    """
    if not stdin.isatty():
        result = capture_stdin()
    elif expected_answer_file is not None:
        result = read_answer_file(expected_answer_file)
    else:
        result = prompt_for_input(message)
    return result


def get_insecure_connection(
        url='postgresql://root@127.0.0.1:26257/movr?sslmode=disable'):
    """
    Returns an insecure pyscopg2 connection object based on a URL.
    """
    return connect(dsn=url)


def start_cockroach_demo():
    """
    Starts a cockroach demo instance.
    """
    process = run("cockroach demo --insecure".split(), capture_output=True)
    return process


def spawn_cockroach_demo_background():
    """
    Starts a cockroach demo instance in the background.

    Should not output anything to stdout.
    """
    process = Process(target=start_cockroach_demo)
    process.start()
    # Give it a moment to start accepting connections
    sleep(1)
    return process


def start_cockroach_single_node():
    """
    Launches an insecure single-node CockroachDB daemon.
    """
    process = run("cockroach start-single-node --insecure".split(),
                  capture_output=True)
    return process


def spawn_cockroach_single_node_background():
    """
    Starts cockroach single node instance in the background.
    """
    process = Process(target=start_cockroach_single_node)
    process.start()
    process_still_starting = True
    tries = 0
    while process_still_starting and tries <= 3:
        try:
            connection = get_insecure_connection()
            connection.close()  # get rid of the connection
            process_still_starting = False
        except OperationalError:
            sleep(2**tries)  # 1s, 2s, 4s, 8s
            tries += 1
    # one last try
    connection = get_insecure_connection()
    connection.close()
    return process


def is_port_26257_free():
    """
    Checks to see if there's a cockroach daemon running on port 26257.
    """
    result = check_output('ps -ef | grep cockroach', shell=True
                          ).decode('utf-8').split('\n')
    for line in result:
        if 'grep' in line:
            pass
        if 'cockroach start' in line or 'cockroach demo' in line:  # daemon
            if 'port' not in line:  # default port is implicitly 26257
                return False
            if '26257' in line:  # has 'port 26257' in the command
                return False

    return True

def check_columns(show_columns_results, expected_columns, data_types, defaults, nullable):
    """
    Checks whether the properties of columns obtained from show_columns match the expected schema
    """    

    assert len(show_columns_results) == len(expected_columns)

    for row in show_columns_results:
        column_name = row['column_name']
        
        assert column_name in expected_columns

        ind = expected_columns.index(column_name)


        assert row['data_type'] == data_types[ind]
        assert row['column_default'] == defaults[ind]
        assert row['is_nullable'] == nullable[ind]
        

def check_table(crdb, db, query_file, table, 
                     expected_columns, data_types, defaults, nullable):
    """
    Executes query_file and Tests that a specific table has the expected schema:
        expected_columns is a list of names of expected columns
        data_types is a list of data types for the columns, in the same order
        defaults is the list of default values for the columns, in the same order
        nullable is the list of boolean values specifying whether each column is nullable
    """

    # action: run the script
    run_sql_script(crdb.connection, script_name=query_file)

    # Tests from here on out
    # Assert that a table exists
    assert table in show_tables(crdb.connection, db=db)

    # Actual table schema
    show_columns_results = show_columns(crdb.connection, table=table,
                                    db=db)

    # Check all columns
    check_columns(show_columns_results, expected_columns, data_types, defaults, nullable)

def check_table_contents_by_id(crdb,db,table, query_file, expected_data, search_field='id'):
    """
    Executes query_file and Tests that a specific table contains the expected data:
        expected_data: a JSON with the expected data, with ids serving as keys, i.e.
            {'12345': {'first_name': 'Alex', 'last_name':'Yarosh'}}
        search_field: the name of the field containing the id    
    """   

    # action: insert two rows from the query file
    run_sql_script(conn=crdb.connection, script_name=query_file)

    # Test
    # first, find the rows
    table_rows = select_star(conn=crdb.connection, db=db, table=table)
     
    # Then, for every expected record, try to find it and compare the expected data to the actual
    for record in expected_data:
        ids = [row[search_field] for row in table_rows]
        assert record in ids

        record_ind = ids.index(record)

        for field in expected_data[record]:
            assert table_rows[record_ind][field] == expected_data[record][field]

def check_table_contents(crdb,db,table, query_file, expected_data):
    """
    Executes query_file and Tests that a specific table contains the expected data:
        expected_data: a list of  JSONs with the expected data, i.e.
            [{'first_name': 'Alex', 'last_name':'Yarosh'}, {'first_name' : 'Will', 'last_name':'Cross}]

    """   

    # run the script
    run_sql_script(conn=crdb.connection, script_name=query_file)

     
    # Then, for every expected record, try to filter the table based on the data of the expected record
    for record in expected_data:
        condition = ' AND '.join([f"{field} = '{record[field]}'" for field in record])
        result = select_condition(conn=crdb.connection, db=db, table=table, condition=condition)
        print(record)
        print(result)
        assert len(result) > 0


def check_foreign_key(crdb,db,query_file, table, column, ref_table, ref_column, actions=None):
    """
    Executes 

    """   

    # run the script
    run_sql_script(conn=crdb.connection, script_name=query_file)

    constraints = show_constraints(crdb.connection, db=db, table=table)

    fk_constraints_details = [record['details'] for record in constraints if record['constraint_type'] == 'FOREIGN KEY']

    fk_expected_details = f"FOREIGN KEY ({column}) REFERENCES {ref_table}({ref_column})"
    if actions:
        fk_expected_details += ' ' + ' '.join(actions).upper() 
    
    assert fk_expected_details in fk_constraints_details
      

@fixture
def crdb():
    """
    Yields a CockroachSingleNodeInsecure() instance.

    Cleanup consists of killing the single node process & deleting the data
        files (and waiting until that's done).
    """
    db = CockroachSingleNodeInsecure()
    yield db

    # cleanup
    db.stop()
    db.process.join()


@fixture
def spawn_cursor(connection):
    """
    Creates a cursor from the connection object.
    """
    with connection.cursor() as curs:
        yield curs


class CockroachSingleNodeInsecure:
    """
    Starts a single-node process & creates a connection.

    stop() method to clean up when it's done.
    """

    def __init__(self):
        """
        Starts a single-node process & creates a connection.
        """
        if is_port_26257_free():  # raises uncaught exception if not
            self.process = spawn_cockroach_single_node_background()
        else:
            raise EnvironmentError("cockroach start-single-node process not "
                                   "yet terminated.")
        self.connection = connect(
            dsn='postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable')
        # Cursors expect autocommit; may cause bugs if the following is removed
        self.connection.set_session(autocommit=True)

    def stop(self):
        """
        Stops the single-node process and deletes the data files.
        """
        run("killall -9 cockroach start-single-node".split())
        tries = 0
        while (not is_port_26257_free()) and tries <= 3:
            sleep(2**tries)
        if tries > 3:
            raise EnvironmentError(
                "cockroach start-single-node process not terminating.")
        run("rm -r cockroach-data".split(), capture_output=True)
