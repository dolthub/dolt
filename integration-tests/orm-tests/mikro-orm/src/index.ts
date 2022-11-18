import { MikroORM } from "@mikro-orm/core";
import { MySqlDriver } from '@mikro-orm/mysql';
import { User } from "./entity/User";

async function connectAndGetOrm() {
    const orm = await MikroORM.init<MySqlDriver>({
        entities: [User],
        type: "mysql",
        clientUrl: "mysql://localhost:3306",
        dbName: "dolt",
        user: "dolt",
        password: "",
        persistOnCreate: true,
    });

    return orm;
}

connectAndGetOrm().then(async orm => {
    console.log("Connected");
    const em = orm.em.fork();

    // this creates the tables if not exist
    const generator = orm.getSchemaGenerator();
    await generator.updateSchema();

    console.log("Inserting a new user into the database...")
    const user = new User("Timber", "Saw", 25)
    await em.persistAndFlush(user)
    console.log("Saved a new user with id: " + user.id)

    console.log("Loading users from the database...")
    const users = await em.findOne(User, 1)
    console.log("Loaded users: ", users)

    orm.close();
    console.log("Smoke test passed!")
    process.exit(0)
}).catch(error => {
    console.log(error)
    console.log("Smoke test failed!")
    process.exit(1)
});
