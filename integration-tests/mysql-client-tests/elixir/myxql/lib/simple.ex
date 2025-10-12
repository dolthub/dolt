defmodule SmokeTest do
  def main(_args \\ []) do
    IO.puts("Starting SmokeTest.main/1")
    
    cli_args = Burrito.Util.Args.get_arguments()
    IO.puts("Received CLI args: #{inspect(cli_args)}")
    
    result = run(cli_args)
    System.halt(0)
    result
  end

  defp run(args) do
    if length(args) < 3 do
      IO.puts("Usage: simple <user> <port> <database>")
      System.halt(1)
    end
    
    user = Enum.at(args, 0)
    port_str = Enum.at(args, 1)
    database = Enum.at(args, 2)
    
    {port, _} = Integer.parse(port_str)

    {:ok, pid} = MyXQL.start_link(username: user, port: port, database: database)
    {:ok, _} = MyXQL.query(pid, "drop table if exists test")
    {:ok, _} = MyXQL.query(pid, "create table test (pk int, `value` int, primary key(pk))")
    {:ok, _} = MyXQL.query(pid, "describe test")

    {:ok, result} = MyXQL.query(pid, "select * from test")
    myTestFunc(result.num_rows, 0)

    {:ok, _} = MyXQL.query(pid, "insert into test (pk, `value`) values (0,0)")
    {:ok, result} = MyXQL.query(pid, "UPDATE test SET pk = pk where pk = 0")
    myTestFunc(result.num_rows, 1)

    {:ok, result} = MyXQL.query(pid, "INSERT INTO test VALUES (0, 0) ON DUPLICATE KEY UPDATE `value` = `value`")
    myTestFunc(result.num_rows, 1)

    {:ok, result} = MyXQL.query(pid, "SELECT * FROM test")
    myTestFunc(result.num_rows, 1)
    myTestFunc(result.rows, [[0, 0]])

    {:ok, _} = MyXQL.query(pid, "call dolt_add('-A');")
    {:ok, _} = MyXQL.query(pid, "call dolt_commit('-m', 'my commit')")
    {:ok, _} = MyXQL.query(pid, "select COUNT(*) FROM dolt_log")
    {:ok, _} = MyXQL.query(pid, "call dolt_checkout('-b', 'mybranch')")
    {:ok, _} = MyXQL.query(pid, "insert into test (pk, `value`) values (1,1)")
    {:ok, _} = MyXQL.query(pid, "call dolt_commit('-a', '-m', 'my commit2')")
    {:ok, _} = MyXQL.query(pid, "call dolt_checkout('main')")
    {:ok, _} = MyXQL.query(pid, "call dolt_merge('mybranch')")

    {:ok, result} = MyXQL.query(pid, "select COUNT(*) FROM dolt_log")
    myTestFunc(result.num_rows, 1)
    myTestFunc(result.rows, [[3]])
    :ok
  end

  defp myTestFunc(arg1, arg2) do
    if arg1 != arg2 do
      raise "Test error: expected #{inspect(arg2)}, got #{inspect(arg1)}"
    end
  end
end
