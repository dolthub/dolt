"""Upsert trigger regressions, runnable against Dolt or MySQL.

Usage: python insert-trigger-test.py ROOT_USER PORT DATABASE
The database must already exist. The test creates and removes its own tables and triggers.
"""

import sys

import pymysql


def connect(user, port, database, password=""):
    return pymysql.connect(host="127.0.0.1", port=port, user=user,
                           password=password, database=database, autocommit=True)


def denied(cursor, query, code, message):
    try:
        cursor.execute(query)
    except pymysql.MySQLError as error:
        assert error.args[0] == code, (query, error)
        assert message in error.args[1], (query, error)
    else:
        raise AssertionError("Unexpectedly accepted: " + query)


def rows(cursor, query, expected):
    cursor.execute(query)
    actual = cursor.fetchall()
    assert actual == expected, (query, expected, actual)


def main():
    user, port, database = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    with connect(user, port, database) as root, root.cursor() as admin:
        try:
            admin.execute("CREATE TABLE insert_trigger_h (id INT PRIMARY KEY, body VARCHAR(32), writer VARCHAR(64))")
            admin.execute("CREATE TRIGGER insert_trigger_bi BEFORE INSERT ON insert_trigger_h "
                          "FOR EACH ROW SET NEW.writer = USER()")
            admin.execute("CREATE TRIGGER insert_trigger_bu BEFORE UPDATE ON insert_trigger_h "
                          "FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'append-only'; END")
            admin.execute("INSERT INTO insert_trigger_h (id, body) VALUES (1, 'original'), (2, 'original2')")
            admin.execute("SELECT * FROM insert_trigger_h ORDER BY id")
            original = admin.fetchall()
            denied(admin, "UPDATE insert_trigger_h SET body = 'y' WHERE id = 2", 1644, "append-only")
            denied(admin, "INSERT INTO insert_trigger_h (id, body) VALUES (2, 'ignored') "
                   "ON DUPLICATE KEY UPDATE body = 'REWRITTEN2'", 1644, "append-only")
            denied(admin, "INSERT INTO insert_trigger_h (id, body) VALUES (4, 'new'), (2, 'ignored') "
                   "ON DUPLICATE KEY UPDATE body = 'REWRITTEN2'", 1644, "append-only")
            rows(admin, "SELECT * FROM insert_trigger_h ORDER BY id", original)
            admin.execute("INSERT INTO insert_trigger_h (id, body) VALUES (5, 'new') "
                          "ON DUPLICATE KEY UPDATE body = 'updated'")
            rows(admin, "SELECT body FROM insert_trigger_h WHERE id = 5", (("new",),))

            # An AFTER UPDATE failure must undo the update and preceding insert.
            admin.execute("DROP TRIGGER insert_trigger_bu")
            admin.execute("CREATE TRIGGER insert_trigger_au AFTER UPDATE ON insert_trigger_h "
                          "FOR EACH ROW BEGIN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'after-update'; END")
            denied(admin, "INSERT INTO insert_trigger_h (id, body) VALUES (6, 'new'), (2, 'ignored') "
                   "ON DUPLICATE KEY UPDATE body = 'REWRITTEN2'", 1644, "after-update")
            rows(admin, "SELECT * FROM insert_trigger_h WHERE id <= 2 ORDER BY id", original)
            rows(admin, "SELECT id FROM insert_trigger_h WHERE id = 6", ())

            # Verify all four trigger types, row images, order, and a mixed batch.
            admin.execute("CREATE TABLE insert_trigger_rows (id INT PRIMARY KEY, v INT)")
            admin.execute("CREATE TABLE insert_trigger_audit (seq INT AUTO_INCREMENT PRIMARY KEY, event VARCHAR(2), old_v INT, new_v INT)")
            admin.execute("INSERT INTO insert_trigger_rows VALUES (1, 10)")
            for name, timing, event, body in (
                ("bi", "BEFORE", "INSERT", "BEGIN SET NEW.v = NEW.v + 1; INSERT INTO insert_trigger_audit(event, old_v, new_v) VALUES ('bi', NULL, NEW.v); END"),
                ("ai", "AFTER", "INSERT", "INSERT INTO insert_trigger_audit(event, old_v, new_v) VALUES ('ai', NULL, NEW.v)"),
                ("bu", "BEFORE", "UPDATE", "BEGIN SET NEW.v = NEW.v + OLD.v; INSERT INTO insert_trigger_audit(event, old_v, new_v) VALUES ('bu', OLD.v, NEW.v); END"),
                ("au", "AFTER", "UPDATE", "INSERT INTO insert_trigger_audit(event, old_v, new_v) VALUES ('au', OLD.v, NEW.v)"),
            ):
                admin.execute(f"CREATE TRIGGER insert_trigger_rows_{name} {timing} {event} ON insert_trigger_rows FOR EACH ROW {body}")
            admin.execute("INSERT INTO insert_trigger_rows VALUES (1, 20), (2, 30) ON DUPLICATE KEY UPDATE v = VALUES(v)")
            rows(admin, "SELECT * FROM insert_trigger_rows ORDER BY id", ((1, 31), (2, 31)))
            rows(admin, "SELECT event, old_v, new_v FROM insert_trigger_audit ORDER BY seq",
                 (("bi", None, 21), ("bu", 10, 31), ("au", 10, 31), ("bi", None, 31), ("ai", None, 31)))
            admin.execute("SELECT VERSION()")
            print("Upsert trigger checks passed:", admin.fetchone()[0])
        finally:
            admin.execute("DROP TABLE IF EXISTS insert_trigger_h, insert_trigger_rows, insert_trigger_audit")


if __name__ == "__main__":
    main()
