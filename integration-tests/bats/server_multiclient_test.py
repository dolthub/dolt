import os
import sys

from queue import Queue
from threading import Thread

from helper.pytest import DoltConnection


# Utility functions

def print_err(e):
    print(e, file=sys.stderr)

def query(dc, query_str):
    return dc.query(query_str, False)

def query_with_expected_error(dc, non_error_msg , query_str):
    try:
        dc.query(query_str, False)
        raise Exception(non_error_msg)
    except:
        pass

def row(pk, c1, c2):
    return {"pk":str(pk),"c1":str(c1),"c2":str(c2)}

UPDATE_BRANCH_FAIL_MSG = "Failed to update branch"

def commit_and_update_branch(dc, commit_message, expected_hashes, branch_name):
    expected_hash = "("
    for i, eh in enumerate(expected_hashes):
        if i != 0:
            expected_hash += " or "
        expected_hash += "hash = %s" % eh
    expected_hash += ")"

    query_str = 'UPDATE dolt_branches SET hash = Commit("-am", "%s") WHERE name = "%s" AND %s' % (commit_message, branch_name, expected_hash)
    _, row_count = query(dc, query_str)

    if row_count != 1:
        raise Exception(UPDATE_BRANCH_FAIL_MSG)

    query(dc, 'SET @@repo1_head=HASHOF("%s");' % branch_name)

def query_and_test_results(dc, query_str, expected):
    results, _ = query(dc, query_str)

    if results != expected:
        raise Exception("Unexpected results for query:\n\t%s\nExpected:\n\t%s\nActual:\n\t%s" % (query_str, str(), str(results)))

def resolve_theirs(dc):
    query_str = "REPLACE INTO test (pk, c1, c2) SELECT their_pk, their_c1, their_c2 FROM dolt_conflicts_test WHERE their_pk IS NOT NULL;"
    query(dc, query_str)

    query_str = """DELETE FROM test WHERE pk in (
        SELECT base_pk FROM dolt_conflicts_test WHERE their_pk IS NULL
    );"""
    query(dc, query_str)

    query(dc, "DELETE FROM dolt_conflicts_test")

def create_branch(dc, branch_name):
    query_str = 'INSERT INTO dolt_branches (name, hash) VALUES ("%s", @@repo1_head);' % branch_name
    _, row_count = query(dc, query_str)

    if row_count != 1:
        raise Exception("Failed to create branch")


# work functions

def connect(dc):
    dc.connect()

def create_tables(dc):
    query(dc, 'SET @@repo1_head=HASHOF("main");')
    query(dc, """
CREATE TABLE test (
pk INT NOT NULL,
c1 INT,
c2 INT,
PRIMARY KEY(pk));""")
    commit_and_update_branch(dc, "Created tables", ["@@repo1_head"], "main")
    query_and_test_results(dc, "SHOW TABLES;", [{"Table": "test"}])

def duplicate_table_create(dc):
    query(dc, 'SET @@repo1_head=HASHOF("main");')
    query_with_expected_error(dc, "Should have failed creating duplicate table", """
CREATE TABLE test (
pk INT NOT NULL,
c1 INT,
c2 INT,
PRIMARY KEY(pk));""")


def seed_main(dc):
    query(dc, 'SET @@repo1_head=HASHOF("main");')
    _, row_count = query(dc, 'INSERT INTO test VALUES (0,0,0),(1,1,1),(2,2,2)')

    if row_count != 3:
        raise Exception("Failed to update rows")

    commit_and_update_branch(dc, "Seeded initial data", ["@@repo1_head"], "main")
    expected = [row(0,0,0), row(1,1,1), row(2,2,2)]
    query_and_test_results(dc, "SELECT pk, c1, c2 FROM test ORDER BY pk", expected)

def modify_pk0_on_main_and_commit(dc):
    query(dc, 'SET @@repo1_head=HASHOF("main");')
    query(dc, "UPDATE test SET c1=1 WHERE pk=0;")
    commit_and_update_branch(dc, "set c1 to 1", ["@@repo1_head"], "main")

def modify_pk0_on_main_no_commit(dc):
    query(dc, 'SET @@repo1_head=HASHOF("main");')
    query(dc, "UPDATE test SET c1=2 WHERE pk=0")

def fail_to_commit(dc):
    try:
        commit_and_update_branch(dc, "Created tables", ["@@repo1_head"], "main")
        raise Exception("Failed to fail commit")
    except Exception as e:
        if str(e) != UPDATE_BRANCH_FAIL_MSG:
            raise e

def commit_to_feature(dc):
    create_branch(dc, "feature")
    commit_and_update_branch(dc, "set c1 to 2", ["@@repo1_head"], "feature")

def merge_resolve_commit(dc):
    query(dc, 'SET @@repo1_head=Merge("main");')
    query_and_test_results(dc, "SELECT * from dolt_conflicts;", [{"table": "test", "num_conflicts": "1"}])
    resolve_theirs(dc)
    expected = [row(0,1,0), row(1,1,1), row(2,2,2)]
    query_and_test_results(dc, "SELECT pk, c1, c2 FROM test ORDER BY pk", expected)
    commit_and_update_branch(dc, "resolved conflicts", ['HASHOF("HEAD^1")', 'HASHOF("HEAD^2")'], "main")


# test script
MAX_SIMULTANEOUS_CONNECTIONS = 2
PORT_STR = sys.argv[1]

CONNECTIONS = [None]*MAX_SIMULTANEOUS_CONNECTIONS
for i in range(MAX_SIMULTANEOUS_CONNECTIONS):
    CONNECTIONS[i] = DoltConnection(port=int(PORT_STR), database="repo1", user='dolt', auto_commit=False)

WORK_QUEUE = Queue()

# work item run by workers
class WorkItem(object):
    def __init__(self, dc, *work_funcs):
        self.dc = dc
        self.work_funcs = work_funcs
        self.exception = None


# worker thread function
def worker():
    while True:
        try:
            item = WORK_QUEUE.get()

            for work_func in item.work_funcs:
                work_func(item.dc)

            WORK_QUEUE.task_done()
        except Exception as e:
            work_item.exception = e
            WORK_QUEUE.task_done()

# start the worker threads
for i in range(MAX_SIMULTANEOUS_CONNECTIONS):
     t = Thread(target=worker)
     t.daemon = True
     t.start()

# This defines the actual test script.  Each stage in the script has a list of work items.  Each work item
# in a stage should have a different connection associated with it.  Each connections work is done in parallel
# each of the work functions for a connection is executed in order.
work_item_stages = [
    [WorkItem(CONNECTIONS[0], connect, create_tables)],
    [WorkItem(CONNECTIONS[0], seed_main), WorkItem(CONNECTIONS[1], connect, duplicate_table_create)],
    [WorkItem(CONNECTIONS[0], modify_pk0_on_main_and_commit), WorkItem(CONNECTIONS[1], modify_pk0_on_main_no_commit)],
    [WorkItem(CONNECTIONS[1], fail_to_commit, commit_to_feature, merge_resolve_commit)]
]

# Loop through the work item stages executing each stage by sending the work items for the stage to the worker threads
# and then waiting for all of them to finish before moving on to the next one.  Checks for an error after every stage.
for stage, work_items in enumerate(work_item_stages):
    print("Running stage %d / %d" % (stage,len(work_item_stages)))
    for work_item in work_items:
        WORK_QUEUE.put(work_item)

    WORK_QUEUE.join()

    for work_item in work_items:
        if work_item.exception is not None:
            print_err(work_item.exception)
            sys.exit(1)
