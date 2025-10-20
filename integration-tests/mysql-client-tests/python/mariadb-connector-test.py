import mariadb
import sys

QUERY_RESPONSE = [
    {"create table test (pk int, `value` int, primary key(pk))": ()},
    {"describe test": (
        ('pk', 'int', 'NO', 'PRI', None, ''),
        ('value', 'int', 'YES', '', None, '')
    )},
    {"insert into test (pk, `value`) values (0,0)": ()},
    {"select * from test": ((0, 0),)},
    {"call dolt_add('-A');": ((0,),)},
    {"call dolt_commit('-m', 'my commit')": (('',),)},
    {"select COUNT(*) FROM dolt_log": ((2,),)},
    {"call dolt_checkout('-b', 'mybranch')": ((0, "Switched to branch 'mybranch'"),)},
    {"insert into test (pk, `value`) values (1,1)": ()},
    {"call dolt_commit('-a', '-m', 'my commit2')": (('',),)},
    {"call dolt_checkout('main')": ((0, "Switched to branch 'main'"),)},
    {"call dolt_merge('mybranch')": (('',1,0,),)},
    {"select COUNT(*) FROM dolt_log": ((3,),)},
]


def main():
    user = sys.argv[1]
    port = int(sys.argv[2])
    db = sys.argv[3]

    try:
        # Connect using MariaDB Connector/Python
        connection = mariadb.connect(
            user=user,
            host="127.0.0.1",
            port=port,
            database=db
        )
        
        print(f"Connected to MariaDB using MariaDB Connector/Python v{mariadb.__version__}")
        
        cursor = connection.cursor()

        for query_response in QUERY_RESPONSE:
            query = list(query_response.keys())[0]
            exp_results = query_response[query]
            
            cursor.execute(query)
            
            try:
                results = cursor.fetchall()
                
                # MariaDB Connector/Python returns lists, convert to tuples for comparison
                results = tuple(tuple(row) if isinstance(row, list) else row for row in results)
                
                # Skip validation for dolt_commit and dolt_merge as their results vary
                if ("dolt_commit" not in query) and ("dolt_merge" not in query):
                    if results != exp_results:
                        print("Query:")
                        print(query)
                        print("Expected:")
                        print(exp_results)
                        print("Received:")
                        print(results)
                        sys.exit(1)
            except mariadb.Error:
                # This is a write query with no results
                pass

        cursor.close()
        connection.close()
        
        print("All MariaDB Connector/Python tests passed!")
        sys.exit(0)
        
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

