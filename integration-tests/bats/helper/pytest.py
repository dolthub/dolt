import csv
import sys
import time
import mysql.connector

from io import StringIO
from multiprocessing import Process

def _connect(user, host, port, database):
    return mysql.connector.connect(user=user, host=host, port=port, database=database, allow_local_infile=True)

def _print_err_and_exit(e):
    print(e, file=sys.stderr)
    sys.exit(1)

def csv_to_row_maps(csv_str):
    csv_str = csv_str.replace('\\n', '\n')
    rd = csv.DictReader(StringIO(csv_str))
    rows = []
    for row in rd:
        rows.append(row)

    return rows

class DoltConnection(object):
    def __init__(self, user='root', host='127.0.0.1', port=3306, database='dolt', auto_commit=False):
        self.user = user
        self.host = host
        self.port = port
        self.database = database
        self.auto_commit = auto_commit
        self.cnx = None

    def connect(self):
        try:
            self.cnx = _connect(self.user, self.host, self.port, self.database)
            self.cnx.autocommit=self.auto_commit
        except BaseException as e:
            _print_err_and_exit(e)

    def close(self):
        self.cnx.close()

    def query(self, query_str, exit_on_err=True):
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query_str)

            if cursor.description is None:
                return [], cursor.rowcount

            raw = cursor.fetchall()

            row_maps = []
            for curr in raw:
                r = {}
                for i, k in enumerate(cursor.column_names):
                    r[k] = str(curr[i])
                row_maps.append(r)

            return row_maps, cursor.rowcount

        except BaseException as e:
            if exit_on_err:
                _print_err_and_exit(e)
            raise e

class InfiniteRetryConnection(DoltConnection):
    def connect(self):
        while True:
            try:
                self.cnx = _connect(user=self.user, host=self.host, port=self.port, database=self.database)

                try:
                    self.cnx.close()
                except BaseException:
                    pass

                return

            except BaseException:
                pass

def wait_for_connection(user='root', host='127.0.0.1', port=3306, database='dolt', timeout_ms=5000):
    timeoutf = timeout_ms / 1000.0
    exit_zero_on_connect = InfiniteRetryConnection(user=user, host=host, port=port, database=database)

    cnx_proc = Process(target=exit_zero_on_connect.connect)
    cnx_proc.start()
    cnx_proc.join(timeout=timeoutf)

    if cnx_proc.exitcode is None:
        cnx_proc.terminate()
        _print_err_and_exit(Exception("Failed to establish connection in time."))
    elif cnx_proc.exitcode != 0:
        _print_err_and_exit(Exception("Connection process exited with exit code %d." % cnx_proc.exitcode))

