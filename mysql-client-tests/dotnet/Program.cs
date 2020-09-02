using System;
using System.Data;

using MySql.Data;
using MySql.Data.MySqlClient;

public class Tutorial1
{
    public static int Main(string[] args)
    {
        if (args.Length != 3)
        {
            System.Console.WriteLine("Must supply $USER $PORT $REPO arguments.");
            return 1;
        }

        var user = args[0];
        var port = args[1];
        var db = args[2];

        string connStr = $"server=127.0.0.1;user={user};database={db};port={port};";
        Console.WriteLine(connStr);
        MySqlConnection conn = new MySqlConnection(connStr);
        try
        {
            Console.WriteLine("Connecting to MySQL...");
            conn.Open();
            // Perform database operations

            SetupTest(conn);
            ExecuteTest(conn);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }

        conn.Close();
        Console.WriteLine("Done.");
        return 0;
    }

    public static void SetupTest(MySqlConnection conn)
    {
        using var cmd = new MySqlCommand();
        cmd.Connection = conn;

        cmd.CommandText = "DROP TABLE IF EXISTS test";
        cmd.ExecuteNonQuery();

        cmd.CommandText = @"CREATE TABLE test (pk int, value int, primary key(pk))";
        cmd.ExecuteNonQuery();

        cmd.CommandText = "INSERT INTO test(pk, value) VALUES(0,0)";
        cmd.ExecuteNonQuery();
    }

    public static void ExecuteTest(MySqlConnection conn)
    {
        string sql = "SELECT count(*) FROM test";
        using var cmd = new MySqlCommand(sql, conn);

        //using MySqlDataReader rdr = cmd.ExecuteReader();

        try
        {
            object result = cmd.ExecuteScalar();
            if (result != null)
            {
                int r = Convert.ToInt32(result);
                Console.WriteLine("Number of entries in the database is: " + r);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }

        //object result = cmd.ExecuteScalar();
        //if (result != null)
        //{
        //    int r = Convert.ToInt32(result);
        //    Console.WriteLine("Number of entries in the database is: " + r);
        //}

        //while (rdr.Read())
        //{
        //    // make sure results are correct
        //    Console.WriteLine(rdr);
        //}
        //rdr.Close();
    }
}
