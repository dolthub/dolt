import mysql.connector
import sys

QUERY_RESPONSE = [
    { "create table test (pk int, value int, primary key(pk))": [] },
    { "describe test": [
        ('pk', 'int', 'NO', 'PRI', '', ''),
        ('value', 'int', 'YES', '', '', '')
    ] },
    { "insert into test (pk, value) values (0,0)": [] },
    { "select * from test": [(0,0)] }
]
    
def main():
    user = sys.argv[1]
    port = sys.argv[2]
    db   = sys.argv[3]

    connection = mysql.connector.connect(user=user,
                                         host="127.0.0.1",
                                         port=port,
                                         database=db)

    for query_response in QUERY_RESPONSE:
        query = list(query_response.keys())[0]
        exp_results = query_response[query] 
        cursor = connection.cursor()
        cursor.execute(query)
        try: 
            results = cursor.fetchall()
            print(exp_results)
            print(results)
            if ( results != exp_results ):
                print("Query:")
                print(query)
                print("Expected:")
                print(exp_results)
                print("Received:")
                print(results)
                exit(1)
        except mysql.connector.errors.InterfaceError:
            # This is a write query
            pass
             
        cursor.close()
        
    connection.close()
    
    exit(0)

main()
