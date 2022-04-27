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


def create_table(connection, db='movr_vehicles', table='vehicles',
                 columns=("id UUID PRIMARY KEY DEFAULT gen_random_uuid()",
                          "purchase_date TIMESTAMPTZ DEFAULT now()",
                          "serial_number STRING NOT NULL",
                          "make STRING NOT NULL",
                          "model STRING NOT NULL",
                          "year INT2 NOT NULL",
                          "color STRING NOT NULL",
                          "description STRING")):
    """
    Creates a table.

    Assumes the database has already been created.
    Assumes the table doesn't already exist.
    """
    columns = f"{', '.join(columns)}"
    query = f"CREATE TABLE {db}.{table} ({columns});"
    with connection.cursor() as cursor:
        cursor.execute(query)
    return True


def set_up_db_and_table(conn, db='movr_vehicles', table='vehicles'):
    """
    Prepares the database state for inserting two rows.
    """
    return (create_database(conn, db=db)
            and create_table(conn, db=db, table=table))


def insert_two_rows(conn, db='movr_vehicles', table='vehicles'):
    """
    Inserts two rows into the table.
    """
    known_id = "03d0a3a4-ae36-4178-819c-0c1b08e59afc"
    known_purchase_date = "2022-03-07 15:21:26.214287+00"
    query = (f"INSERT INTO {db}.{table} ("
             "    id, purchase_date,"
             "    serial_number,"
             "    make,"
             "    model,"
             "    year,"
             "    color,"
             "    description"
             "  ) VALUES ("
             f"    '{known_id}',"
             f"    '{known_purchase_date}',"
             "    '1234',"
             "    'Make',"
             "    'Model',"
             "    1984,"
             "    'Red',"
             "    'Nice.'"
             "  ), ("
             "    gen_random_uuid(),"
             "    now(),"
             "    '1235',"
             "    'Make',"
             "    'Model',"
             "    1984,"
             "    'Red',"
             "    NULL);")
    return run_command(conn, query)


def set_up_and_insert_two_rows(conn, db='movr_vehicles', table='vehicles'):
    """
    Creates the db and table, and inserts two rows into the table.
    """
    return (set_up_db_and_table(conn, db=db, table=table)
            and insert_two_rows(conn, db=db, table=table))


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
