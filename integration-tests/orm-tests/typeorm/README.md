# TypeORM Smoke Test

This project was created with: `typeorm init --name typeorm-smoketest --database mysql`

Database settings are inside `data-source.ts` file and is configured to hit a Dolt sql-server on the default port, for the dolt root, with no password, for the database named "dolt". 

The `index.ts` file is the main entry point and will insert a new record into the database, then load it, print 
success, and exit with a zero exit code. If any errors are encountered, they are logged, and the process exits with a
non-zero exit code. 

To run this smoke test project:
1. Run `npm install` command
2. Run `npm start` command
