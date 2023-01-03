const mysql = require('mysql');

const args = process.argv.slice(2);

const user = args[0];
const port = args[1];
const db   = args[2];

const config = {
    host: '127.0.0.1',
    user: user,
    port: port,
    database: db
};

class Database {
    constructor( config ) {
				this.connection = mysql.createConnection( config );
				this.connection.connect();
    }

    query( sql, args ) {
        return new Promise( ( resolve, reject ) => {
            this.connection.query( sql, args, ( err, rows ) => {
                if ( err )
                    return reject( err );
                return resolve( rows );
            } );
        } );
    }
    close() {
      this.connection.end(err => {
				if (err) {
					console.error(err)
				} else {
					console.log("db connection closed")
				}
			})
    }
}

async function main() {
    const queries = [
			"create table test (pk int, `value` int, primary key(pk))",
			"describe test",
			"select * from test",
			"insert into test (pk, `value`) values (0,0)",
			"select * from test",
			"call dolt_add('-A');",
			"call dolt_commit('-m', 'my commit')",
			"select COUNT(*) FROM dolt_log",
			"call dolt_checkout('-b', 'mybranch')",
			"insert into test (pk, `value`) values (1,1)",
			"call dolt_commit('-a', '-m', 'my commit2')",
			"call dolt_checkout('main')",
			"call dolt_merge('mybranch')",
			"select COUNT(*) FROM dolt_log",
    ];

    const results = [
	      {
            fieldCount: 0,
            affectedRows: 0,
            insertId: 0,
            serverStatus: 2,
            warningCount: 0,
            message: '',
            protocol41: true,
            changedRows: 0
        },
	[ { Field: 'pk',
	    Type: 'int',
	    Null: 'NO',
	    Key: 'PRI',
	    Default: 'NULL',
	    Extra: '' },
	  { Field: 'value',
	    Type: 'int',
	    Null: 'YES',
	    Key: '',
	    Default: 'NULL',
	    Extra: '' }
	],
	[],
	{
	    fieldCount: 0,
	    affectedRows: 1,
	    insertId: 0,
	    serverStatus: 2,
	    warningCount: 0,
	    message: '',
	    protocol41: true,
	    changedRows: 0
	},
	[ { pk: 0, value: 0 } ],
	[ { status: 0 } ],
	[],
	[ { "COUNT(*)": 2 } ],
	[ { status: 0 } ],
	{
		fieldCount: 0,
		affectedRows: 1,
		insertId: 0,
		serverStatus: 2,
		warningCount: 0,
		message: '',
		protocol41: true,
		changedRows: 0
	},
	[],
	[ { status: 0 } ],
	[ { fast_forward: 1, conflicts: 0 } ],
	[ { "COUNT(*)": 3 } ],
    ];

    const database = new Database(config);

		await Promise.all(queries.map((query, idx) => {
			const expected = results[idx];
			return database.query(query).then(rows => {
				const resultStr = JSON.stringify(rows);
				const result = JSON.parse(resultStr);
				if (resultStr !== JSON.stringify(expected) && !(query.includes("dolt_commit"))) {
					console.log("Query:", query);
					console.log("Results:", result);
					console.log("Expected:", expected);
					throw new Error("Query failed")
				} else {
					console.log("Query succeeded:", query)
				}
			}).catch(err => {
				console.error(err)
				process.exit(1);
			});
		}));

		database.close()
    process.exit(0)
}

main();
    



