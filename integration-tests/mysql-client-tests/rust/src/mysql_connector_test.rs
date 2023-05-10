// any imports?
use std::collections::HashMap;
use mysql::*;
use std::env;
use std::process::exit;

// queries
const QUERY_RESPONSE: HashMap<&str, i32> = HashMap::from([
("create table test (pk int, `value` int, primary key(pk))", 0),
("describe test", 3),
("insert into test (pk, `value`) values (0,0)", 1),
("select * from test", 1),
("call dolt_add('-A');", 1),
("call dolt_commit('-m', 'my commit')", 1),
("call dolt_checkout('-b', 'mybranch')", 1),
("insert into test (pk, `value`) values (1,1)", 1),
("call dolt_commit('-a', '-m', 'my commit2')", 1),
("call dolt_checkout('main')", 1),
("call dolt_merge('mybranch')", 1),
("select COUNT(*) FROM dolt_log", 1)
]);

fn main() {
    // get CL args
    let args: Vec<String> = env::args().collect();
    let user = &args[1];
    let port = &args[2];
    let db = &args[3];

    // open connection
    let client = HashMap::from([
        ("user", user),
        ("host", "127.0.0.1"),
        ("port", port),
        ("db_name", db)
    ]);
    let mut builder = OptsBuilder::new().from_hash_map(client);
    //let url = "mysql://" + user + "@localhost:127.0.0.1/" + db;
    //let pool = Pool::new(url).unwrap();
    let mut conn = builder.get_conn().unwrap();

    // for query in query_response...execute query
    for (query, exp_result) in QUERY_RESPONSE.iter() {
        let result = conn.query_map(query);
        if result != exp_result {
            println!("QUERY: {}", query);
            println!("EXPECTED: {}", exp_result);
            println!("RESULT: {}", result);
            exit(1)
        }
    }
    exit(0)

}