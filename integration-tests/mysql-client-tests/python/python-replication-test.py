import pymysql
import sys
import time
from typing import List, Optional, Iterable, Type, TypeVar
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import (
    RotateEvent,
    FormatDescriptionEvent,
    GtidEvent,
    QueryEvent,
    HeartbeatLogEvent,
)
from pymysqlreplication.row_event import WriteRowsEvent

T = TypeVar("T")

def main():
    user = sys.argv[1]
    port = int(sys.argv[2])
    db = sys.argv[3]

    # Connect to the running SQL server to create some test data
    connection = pymysql.connect(host="127.0.0.1",
                                 port=port,
                                 user=user,
                                 db=db)
    with connection.cursor() as cursor:
        cursor.execute("CREATE TABLE t (pk int primary key, c1 varchar(100));")
        cursor.execute("INSERT INTO t VALUES (1, 'foobarbazbar');")

    # Connect to a replication event stream
    mysql_settings = {'host': '127.0.0.1', 'port': port, 'user': user, 'passwd': ''}
    stream = BinLogStreamReader(connection_settings=mysql_settings, blocking=True, server_id=100, auto_position='8e66e4f4-955a-4844-909a-33d79f78ddba:1')

    # Grab the first 10 events
    events = read_events(stream, n=10, timeout_s=15.0)

    # To help debugging a failed test, print out the event data
    for event in events:
        event.dump()

    assert_correct_events(events)
    stream.close()
    sys.exit(0)


def assert_correct_events(events):
    """
    Checks the events for expected events and values and raises an AssertionError if unexpected results are found.
    """
    # Assert that a RotateEvent is present and indicates that binlog-main.000001 is the next file
    rotateEvent = require_event(events, RotateEvent)
    next_file = getattr(rotateEvent, "next_binlog", None) or getattr(rotateEvent, "next_binlog_file", None)
    assert next_file == "binlog-main.000001", f"RotateEvent next binlog expected binlog-main.000001, got {next_file!r}"

    # Assert that a FormatDescriptionEvent is present and contains the expected binlog version number
    formatDescriptionEvent = require_event(events, FormatDescriptionEvent)
    assert getattr(formatDescriptionEvent, "binlog_version", None) == (4,), f"binlog_version expected 4, got {getattr(formatDescriptionEvent,'binlog_version',None)}"

    # Assert that the QueryEvent for the CREATE TABLE statement is present
    queryEvent = require_event(events, QueryEvent)
    query = getattr(queryEvent, "query", None) or getattr(queryEvent, "query_text", None)
    assert query is not None, "QueryEvent query field was None"
    if isinstance(query, bytes):
        query = query.decode("utf-8", errors="replace")
    assert "CREATE TABLE" in query, f"Query did not contain CREATE TABLE: {query!r}"
    assert "`t`" in query or " t " in query, f"Query did not appear to create table t: {query!r}"

    # Assert that a WriteRowsEvent is present with the correct row values, and that column names
    # can be used to access the fields (e.g. @@binlog_row_metadata=FULL is enabled and working).
    writeRowsEvent = require_event(events, WriteRowsEvent)
    row0 = writeRowsEvent.rows[0]
    values = row0.get("values") if isinstance(row0, dict) else getattr(row0, "values", None)
    assert isinstance(values, dict), f"Expected row values dict, got {type(values).__name__}"
    assert "pk" in values, f"Missing column 'pk'. Got keys: {sorted(values.keys())}"
    assert values["pk"] == 1, f"pk expected 1, got {values['pk']!r}"
    assert "c1" in values, f"Missing column 'c1'. Got keys: {sorted(values.keys())}"
    assert values["c1"] == "foobarbazbar", f"c1 expected 'foobarbazbar', got {values['c1']!r}"

def read_events(stream, n: int = 10, timeout_s: float = 30.0) -> List[object]:
    """
    Read up to `n` non-heartbeat events from `stream`, or stop after `timeout_s`.
    Returns the list of collected events (possibly fewer than n).
    """
    events: List[object] = []
    deadline = time.monotonic() + timeout_s

    try:
        for ev in stream:
            # Timeout check first so we don't hang forever
            if time.monotonic() >= deadline:
                break

            # Skip heartbeats (optional: count them, log them, etc.)
            if isinstance(ev, HeartbeatLogEvent):
                continue

            events.append(ev)
            if len(events) >= n:
                break
    finally:
        # Important: stop network/background reading to avoid hangs/leaks
        try:
            stream.close()
        except Exception:
            pass

    return events

def find_event(events: Iterable[object], event_type: Type[T]) -> Optional[T]:
    """
    Return the first event in `events` that is an instance of `event_type`,
    or None if no such event exists.
    """
    for event in events:
        if isinstance(event, event_type):
            return event
    return None

def require_event(events: Iterable[object], event_type: Type[T]) -> T:
    """
    Return the first event in `events` that is an instance of `event_type`,
    or raise an AssertionError if none is found.
    """
    event = find_event(events, event_type)
    if event is None:
        raise AssertionError(f"Expected event of type {event_type.__name__}, but none was found")
    return event


main()