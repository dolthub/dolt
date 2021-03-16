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
			"select * from test"
    ];

    const results = [
	[],
	[ { Field: 'pk',
	    Type: 'int',
	    Null: 'NO',
	    Key: 'PRI',
	    Default: '',
	    Extra: '' },
	  { Field: 'value',
	    Type: 'int',
	    Null: 'YES',
	    Key: '',
	    Default: '',
	    Extra: '' }
	],
	[],
	{
	    fieldCount: 0,
	    affectedRows: 1,
	    insertId: 0,
	    serverStatus: 0,
	    warningCount: 0,
	    message: '',
	    protocol41: true,
	    changedRows: 0
	},
	[ { pk: 0, value: 0 } ]
    ];

    const database = new Database(config);

		await Promise.all(queries.map((query, idx) => {
			const expected = results[idx];
			return database.query(query).then(rows => {
				const resultStr = JSON.stringify(rows);
				const result = JSON.parse(resultStr);
				if (resultStr !== JSON.stringify(expected) ) {
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
    



