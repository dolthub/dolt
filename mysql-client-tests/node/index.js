let mysql = require('mysql');

var args = process.argv.slice(2);

var user = args[0];
var port = args[1];
var db   = args[2];

var config = {
    host: '127.0.0.1',
    user: user,
    port: port,
    database: db
};

class Database {
    constructor( config ) {
        this.connection = mysql.createConnection( config );
    }

    query( sql, args ) {
        return new Promise( ( resolve, reject ) => {
            this.connection.query( sql, args, ( err, rows ) => {
                if ( err )
                    return reject( err );
                resolve( rows );
            } );
        } );
    }
    close() {
        return new Promise( ( resolve, reject ) => {
            this.connection.end( err => {
                if ( err )
                    return reject( err );
		process.exit(0);
                resolve();
            } );
        } );
    }
}


async function main() {
    var queries = [
	"create table test (pk int, value int, primary key(pk))",
	"describe test",
	"select * from test",
	"insert into test (pk, value) values (0,0)",
	"select * from test"
    ];

    var results = [
	{
	    fieldCount: 0,
	    affectedRows: 0,
	    insertId: 0,
	    serverStatus: 0,
	    warningCount: 0,
	    message: '',
	    protocol41: true,
	    changedRows: 0
	},
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

    var database = new Database(config);

    var i;
    for (i = 0; i < queries.length; i++) {
	var query    = queries[i];
	var expected = results[i];
	await database.query(query).then( rows => {
	    var result = JSON.parse(JSON.stringify(rows))
	    if ( JSON.stringify(result) !== JSON.stringify(expected) ) {
		console.log("Query:");
		console.log(query);
		console.log("Results:");
		console.log(result);
		console.log("Expected:");
		console.log(expected);
		process.exit(1)
	    }
	});
    }

    database.close()
}

main();
    



