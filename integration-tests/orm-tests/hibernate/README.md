# Hibernate-ORM Smoke Test

The smoke test is run by Maven using MySQL JDBC driver. To install Maven, go to `https://maven.apache.org/install.html`.

Database settings are inside `hibernate.cfg.xml` file and is configured to hit a Dolt sql-server 
on the default port, for the user "dolt", with no password, for the database named "dolt".

`Test.java` file is the main entry point and will insert a new record into the database, then print the data
before changes, and update and delete rows, and print the data again after changes. Exit with a zero exit code.
If any errors are encountered, they are logged, and the process exits with a non-zero exit code.

To run this smoke test project run these commands:
1. `cd DoltHibernateSmokeTest` 
2. `mvn clean install`
3. `mvn clean package`
4. `mvn exec:java`
