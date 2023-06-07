import csv
import sys
import time
import mysql.connector

from io import StringIO
from multiprocessing import Process

def _connect(user, password, host, port, database):
    return mysql.connector.connect(user=user, password=password, host=host, port=port, database=database, allow_local_infile=True)

def _print_err_and_exit(e):
    print(e, file=sys.stderr)
    sys.exit(1)

class DoltConnection(object):
    def __init__(self, user='root', password=None, host='127.0.0.1', port=3306, database='dolt', auto_commit=False):
        self.user        = user
        self.password    = password
        self.host        = host
        self.port        = port
        self.database    = database
        self.auto_commit = auto_commit
        self.cnx         = None

    def connect(self):
        try:
            self.cnx = _connect(self.user, self.password, self.host, self.port, self.database)
            self.cnx.autocommit=self.auto_commit
        except BaseException as e:
            _print_err_and_exit(e)

    def close(self):
        self.cnx.close()

class InfiniteRetryConnection(DoltConnection):
    def connect(self):
        while True:
            try:
                self.cnx = _connect(user=self.user, password=self.password, host=self.host, port=self.port, database=self.database)

                try:
                    self.cnx.close()
                except BaseException as e:
                    print("should be fine: ", repr(e))
                    pass

                return

            except BaseException as e:
                print(repr(e))
                pass

def wait_for_connection(user='root', password=None, host='127.0.0.1', port=3306, database='dolt', timeout_ms=5000):
    timeoutf = timeout_ms / 1000.0
    exit_zero_on_connect = InfiniteRetryConnection(user=user, password=password, host=host, port=port, database=database)

    cnx_proc = Process(target=exit_zero_on_connect.connect)
    cnx_proc.start()
    cnx_proc.join(timeout=timeoutf)

    if cnx_proc.exitcode is None:
        cnx_proc.terminate()
        _print_err_and_exit(Exception("Failed to establish connection in time."))
    elif cnx_proc.exitcode != 0:
        _print_err_and_exit(Exception("Connection process exited with exit code %d." % cnx_proc.exitcode))

