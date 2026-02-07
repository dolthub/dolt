import "reflect-metadata"
import { DataSource } from "typeorm"
import { User } from "./entity/User"

const host = process.env.DB_HOST ?? "localhost"
const portValue = process.env.DB_PORT ?? ""
const parsedPort = Number.parseInt(portValue, 10)
const port = Number.isNaN(parsedPort) ? 3306 : parsedPort
const username = process.env.DB_USER ?? "root"
const password = process.env.DB_PASSWORD ?? ""
const database = process.env.DB_NAME ?? "dolt"

export const AppDataSource = new DataSource({
    type: "mysql",
    host,
    port,
    username,
    password,
    database,
    synchronize: true,
    logging: false,
    entities: [User],
    migrations: [],
    subscribers: [],
})
