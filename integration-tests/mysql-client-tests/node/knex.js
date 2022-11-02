const knex = require("knex");
const wtfnode = require("wtfnode")
Socket = require('net').Socket;

const args = process.argv.slice(2);
const user = args[0];
const port = args[1];
const dbName  = args[2];

const db = knex({
    client: "mysql2",
    connection: {
        host: "127.0.0.1",
        port: port,
        user: user,
        database: dbName,
    },
});

async function createTable() {
    let val = await db.schema.createTable('test2', (table) => {
        table.integer('id').primary()
        table.integer('foo')
    });
    return val
}

async function upsert(table, data) {
    let val = await db(table).insert(data).onConflict().merge();
    return val
}

async function select() {
    let val = await db.select('id', 'foo').from('test2');
    return val
}

async function main() {
    await createTable();
    await Promise.all([
        upsert("test2", { id: 1, foo: 1 }),
        upsert("test2", { id: 2, foo: 2 }),
    ])

    let expectedResult = JSON.stringify([ { id: 1, foo: 1 }, { id: 2, foo: 2 } ])
    let result = await select();
    if (JSON.stringify(result) !== expectedResult) {
        console.log("Results:", result);
        console.log("Expected:", expectedResult);
        process.exit(1)
        throw new Error("Query failed")
    }

    await db.destroy();

    // cc: https://github.com/dolthub/dolt/issues/3752
    setTimeout(async () => {
        let sockets = await getOpenSockets();

        if (sockets.length > 0) {
            wtfnode.dump();
            process.exit(1);
            throw new Error("Database not properly destroyed. Hanging server connections");
        }

    }, 3000);
}

// cc: https://github.com/myndzi/wtfnode/blob/master/index.js#L457
async function getOpenSockets() {
    let sockets = []
    process._getActiveHandles().forEach(function (h) {
        // handles can be null now? early exit to guard against this
        if (!h) { return; }

        if (h instanceof Socket) {
            if ((h.fd == null) && (h.localAddress) && !(h.destroyed)) { sockets.push(h); }
        }
    });

    return sockets
}

main();
