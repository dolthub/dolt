import pymysql
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

    connection = pymysql.connect(host="127.0.0.1",
                                 port=port,
                                 user=user,
                                 db=db)

    for query_response in QUERY_RESPONSE:
        query = list(query_response.keys())[0]
        exp_results = query_response[query]
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
            if (results != exp_results) and ("dolt_commit" not in query) and ("dolt_merge" not in query):
                print("Query:")
                print(query)
                print("Expected:")
                print(exp_results)
                print("Received:")
                print(results)
                sys.exit(1)

    connection.close()

    sys.exit(0)


main()
