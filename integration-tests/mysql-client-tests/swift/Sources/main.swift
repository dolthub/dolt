import Foundation
import MariaDB

func main() throws {
    // Get connection parameters from command line
    guard CommandLine.arguments.count >= 4 else {
        print("Usage: MariaDBTest <user> <port> <database>")
        exit(1)
    }
    
    let user = CommandLine.arguments[1]
    let port = UInt32(CommandLine.arguments[2]) ?? 3306
    let database = CommandLine.arguments[3]
    
    // Connect to the database
    let mysql = MySQL()
    
    guard mysql.connect(
        host: "127.0.0.1",
        user: user,
        password: "",
        db: database,
        port: port
    ) else {
        print("Connection failed: \(mysql.errorMessage())")
        exit(1)
    }
    
    print("Connected to MariaDB successfully")
    
    // Test queries
    let queries = [
        "CREATE TABLE test (pk INT, value INT, PRIMARY KEY(pk))",
        "INSERT INTO test (pk, value) VALUES (0, 0)",
        "SELECT * FROM test",
        "CALL dolt_add('-A')",
        "CALL dolt_commit('-m', 'my commit')",
        "SELECT COUNT(*) FROM dolt_log",
        "CALL dolt_checkout('-b', 'mybranch')",
        "INSERT INTO test (pk, value) VALUES (10, 10)",
        "CALL dolt_commit('-a', '-m', 'my commit2')",
        "CALL dolt_checkout('main')",
        "CALL dolt_merge('mybranch')",
        "SELECT COUNT(*) FROM dolt_log"
    ]
    
    for query in queries {
        guard mysql.query(statement: query) else {
            print("Query failed: \(query)")
            print("Error: \(mysql.errorMessage())")
            exit(1)
        }
        
        // Consume results if any
        let results = mysql.storeResults()
        results?.close()
    }
    
    // Test prepared statements
    print("Testing prepared statements...")
    
    let stmt = MySQLStmt(mysql)
    
    // Test SELECT with prepared statement
    let selectQuery = "SELECT * FROM test WHERE pk = ?"
    guard stmt.prepare(statement: selectQuery) else {
        print("Failed to prepare SELECT statement: \(stmt.errorMessage())")
        exit(1)
    }
    
    stmt.bindParam(1)
    
    guard stmt.execute() else {
        print("Failed to execute SELECT statement: \(stmt.errorMessage())")
        exit(1)
    }
    
    let resultSet = stmt.results()
    resultSet.close()
    stmt.close()
    
    // Test INSERT with prepared statement
    let insertStmt = MySQLStmt(mysql)
    
    let insertQuery = "INSERT INTO test VALUES (?, ?)"
    guard insertStmt.prepare(statement: insertQuery) else {
        print("Failed to prepare INSERT statement: \(insertStmt.errorMessage())")
        exit(1)
    }
    
    insertStmt.bindParam(2)
    insertStmt.bindParam(20)
    
    guard insertStmt.execute() else {
        print("Failed to execute INSERT statement: \(insertStmt.errorMessage())")
        exit(1)
    }
    
    insertStmt.close()
    
    // Verify the insert
    guard mysql.query(statement: "SELECT * FROM test WHERE pk = 2") else {
        print("Failed to verify insert")
        exit(1)
    }
    
    if let verifyResults = mysql.storeResults() {
        let row = verifyResults.next()
        guard row != nil else {
            print("Expected row not found after insert")
            exit(1)
        }
        verifyResults.close()
    }
    
    print("All Swift tests passed!")
    
    mysql.close()
}

do {
    try main()
} catch {
    print("Error: \(error)")
    exit(1)
}

