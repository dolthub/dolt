import pymysql
import sys

def main():
    user = sys.argv[1]
    port = int(sys.argv[2])
    db = sys.argv[3]
    expected_version_string = sys.argv[4]

    connection = pymysql.connect(host="127.0.0.1",
                                 port=port,
                                 user=user,
                                 db=db)

    if connection.server_version != expected_version_string:
        print(f"Unexpected server version, expected {expected_version_string}, got {connection.server_version}")
        sys.exit(1)

    connection.close()
    sys.exit(0)

main()
