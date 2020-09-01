import pymysql
import sys

QUERY_RESPONSE = [
    { "create table test (pk int, value int, primary key(pk))": () },
    { "describe test": (
        ('pk', 'int', 'NO', 'PRI', '', ''),
        ('value', 'int', 'YES', '', '', '')
    ) },
    { "insert into test (pk, value) values (0,0)": () },
    { "select * from test": ((0,0),) }
]
    
def main():
    user = sys.argv[1]
    port = int(sys.argv[2])
    db   = sys.argv[3]
    
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
            if ( results != exp_results ):
                print("Query:")
                print(query)
                print("Expected:")
                print(exp_results)
                print("Received:")
                print(results)
                exit(1)

    connection.close()
    
    exit(0)

main()
