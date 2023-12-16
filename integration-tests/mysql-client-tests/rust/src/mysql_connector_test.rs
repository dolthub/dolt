use mysql::Row;
use mysql::prelude::*;
use std::env;
use std::process::exit;

fn main() {
    let args: Vec<String> = env::args().collect();
    let user = &args[1];
    let port = &args[2];
    let db = &args[3];

    let url = format!("mysql://{}@127.0.0.1:{}/{}", user, port, db);
    let connection_opts = mysql::Opts::from_url(&url).unwrap();
    let pool = mysql::Pool::new(connection_opts).unwrap();
    let mut conn = pool.get_conn().unwrap();

    let queries: Vec<(&str, usize)> = [
        ("create table test (pk int, `value` int, primary key(pk))", 0),
        ("describe test", 2),
        ("insert into test (pk, `value`) values (0,0)", 0),
        ("select * from test", 1),
        ("call dolt_add('-A');", 1),
        ("call dolt_commit('-m', 'my commit')", 1),
        ("call dolt_checkout('-b', 'mybranch')", 1),
        ("insert into test (pk, `value`) values (1,1)", 0),
        ("call dolt_commit('-a', '-m', 'my commit2')", 1),
        ("call dolt_checkout('main')", 1),
        ("call dolt_merge('mybranch')", 1),
        ("select COUNT(*) FROM dolt_log", 1)
    ].to_vec();

    for (query, expected) in queries.into_iter() {
        let result = conn.query(query);
        let response : Vec<Row> = result.expect("Error: bad response");
        println!("{:?}", response);

        // Assert that row metadata is populated
        if response.len() > 0 {
            let row = &response[0];
            for column in row.columns_ref() {
                if column.name_str().len() == 0 {
                    println!("FAIL: Column name is empty");
                    exit(1);
                }
            }
        }

        // Assert that the expected number of rows are returned
        if response.len() != expected {
            println!("LENGTH: {}", response.len());
            println!("QUERY: {}", query);
            println!("EXPECTED: {}", expected);
            println!("RESULT: {:?}", response);
            exit(1)
        }
    }

    exit(0)
}
